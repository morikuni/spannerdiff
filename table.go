package spannerdiff

import (
	"slices"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type table struct {
	node *ast.CreateTable
	// foreignKeys are foreign keys referencing other tables, which aren't in CREATE TABLE (see normalizeDDLs).
	foreignKeys []*ast.TableConstraint
}

func newTable(ct *ast.CreateTable) *table {
	return &table{ct, nil}
}

func (t *table) id() identifier {
	return t.tableID()
}

func (t *table) tableID() tableID {
	return newTableIDFromPath(t.node.Name)
}

func (t *table) schemaID() optional[schemaID] {
	return t.tableID().schemaID
}

func (t *table) astNode() ast.Node {
	return t.node
}

func (t *table) add() []ast.DDL {
	return []ast.DDL{t.node}
}

func (t *table) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropTable{
		Name: t.node.Name,
	})
}

func (t *table) alter(tgt definition, m *migration) {
	base := t
	target := tgt.(*table)

	// https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	// - Change the ON DELETE action of an interleaved table.
	// - Convert between INTERLEAVE IN and INTERLEAVE IN PARENT.
	// --- not documented ---
	// - Add or remove a synonym from an existing table.
	// - Set table options.

	if !equalNodes(base.primaryKeys(), target.primaryKeys()) {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	// Columns are migrated by column definitions.
	// Other fields are cleared once they are handled, and the remaining differences require recreating the table.
	baseRest := *base.node
	targetRest := *target.node
	baseRest.Columns = nil
	targetRest.Columns = nil
	baseRest.PrimaryKeys = nil
	targetRest.PrimaryKeys = nil

	var ddls []ast.DDL
	if alteration, ok := alterCluster(base.node.Cluster, target.node.Cluster); ok {
		if alteration != nil {
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: alteration})
		}
		baseRest.Cluster = nil
		targetRest.Cluster = nil
	}
	// Row deletion policies are migrated by row deletion policy definitions.
	baseRest.RowDeletionPolicy = nil
	targetRest.RowDeletionPolicy = nil
	if !equalNodes(base.node.Synonyms, target.node.Synonyms) {
		baseSynonyms := make(map[string]struct{}, len(base.node.Synonyms))
		for _, syn := range base.node.Synonyms {
			baseSynonyms[nameOf(syn.Name)] = struct{}{}
		}
		targetSynonyms := make(map[string]struct{}, len(target.node.Synonyms))
		for _, syn := range target.node.Synonyms {
			targetSynonyms[nameOf(syn.Name)] = struct{}{}
		}
		for _, syn := range base.node.Synonyms {
			if _, ok := targetSynonyms[nameOf(syn.Name)]; !ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropSynonym{Name: syn.Name}})
			}
		}
		for _, syn := range target.node.Synonyms {
			if _, ok := baseSynonyms[nameOf(syn.Name)]; !ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddSynonym{Name: syn.Name}})
			}
		}
	}
	baseRest.Synonyms = nil
	targetRest.Synonyms = nil
	// Constraints are migrated by constraint definitions.
	baseRest.TableConstraints = nil
	targetRest.TableConstraints = nil
	if options := diffOptions(base.node.Options, target.node.Options); options != nil {
		ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AlterTableSetOptions{Options: options}})
	}
	baseRest.Options = nil
	targetRest.Options = nil

	if !equalNode(&baseRest, &targetRest) {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}
	if len(ddls) == 0 {
		return
	}

	m.updateStateIfUndefined(newAlterState(base, target, ddls...))
}

// tableOrQueueAlteration is an alteration that both ALTER TABLE and ALTER QUEUE accept.
type tableOrQueueAlteration interface {
	ast.TableAlteration
	ast.QueueAlteration
}

// alterCluster returns the alteration to change the INTERLEAVE IN clause of a table or a queue.
// It returns false if the change requires recreating the table or the queue, and a nil alteration if nothing changes.
func alterCluster(base, target *ast.Cluster) (tableOrQueueAlteration, bool) {
	switch {
	case base == nil && target == nil:
		return nil, true
	case base == nil || target == nil:
		return nil, false
	case !equalNode(base.TableName, target.TableName):
		return nil, false
	case base.Enforced != target.Enforced:
		return &ast.SetInterleaveIn{TableName: target.TableName, Enforced: target.Enforced, OnDelete: target.OnDelete}, true
	case normalizeOnDeleteAction(base.OnDelete) != normalizeOnDeleteAction(target.OnDelete):
		return &ast.SetOnDelete{OnDelete: normalizeOnDeleteAction(target.OnDelete)}, true
	default:
		return nil, true
	}
}

func (t *table) dependsOn() []dependency {
	var deps []dependency
	if schemaID, ok := t.schemaID().get(); ok {
		deps = append(deps, dependency{schemaID, dependencyKindRecreate})
	}
	if t.node.Cluster != nil {
		parentID := newTableIDFromPath(t.node.Cluster.TableName)
		deps = append(deps, dependency{parentID, dependencyKindRecreate})
		// The key columns of an interleaved table must match those of its parent table,
		// so the table is created after the key columns of the parent table are altered.
		// Recreating a key column recreates the parent table, which is handled as a dependency on the table.
		// Key columns that aren't in the parent table have no migration state and are ignored.
		for _, key := range t.primaryKeys() {
			deps = append(deps, dependency{newColumnID(parentID, key.Name), dependencyKindOrder})
		}
	}
	// CREATE TABLE contains default values, so sequences used by them must exist before the table is created.
	for _, col := range t.node.Columns {
		deps = append(deps, newDependencies(dependencyKindOrder, sequencesInColumn(col))...)
	}
	return deps
}

// dropDependsOn returns the dependencies of DROP TABLE.
// DROP TABLE also drops unnamed foreign keys referencing other tables, because they can't be dropped by name,
// so the table must be dropped before the tables and the columns referenced by them.
func (t *table) dropDependsOn() []identifier {
	var refs []identifier
	for _, tc := range t.foreignKeys {
		if tc.Name != nil {
			continue
		}
		fk := tc.Constraint.(*ast.ForeignKey)
		refID := newTableIDFromPath(fk.ReferenceTable)
		refs = append(refs, refID)
		for _, col := range fk.ReferenceColumns {
			refs = append(refs, newColumnID(refID, col))
		}
	}
	// The order of dependencies affects the order of operations, so it must not depend on the order of the statements.
	slices.SortFunc(refs, func(a, b identifier) int { return strings.Compare(a.ID(), b.ID()) })
	return append(dependencyIDs(t.dependsOn()), refs...)
}

// primaryKeys returns the primary key regardless of whether it's written after the column definitions,
// as a table constraint, or as a column constraint.
func (t *table) primaryKeys() []*ast.IndexKey {
	if len(t.node.PrimaryKeys) > 0 {
		return t.node.PrimaryKeys
	}
	for _, tc := range t.node.TableConstraints {
		if pk, ok := tc.Constraint.(*ast.TablePrimaryKey); ok {
			return pk.Columns
		}
	}
	for _, col := range t.node.Columns {
		if col.PrimaryKey {
			return []*ast.IndexKey{{Name: col.Name}}
		}
	}
	return nil
}

func (t *table) columns() map[columnID]*ast.ColumnDef {
	m := make(map[columnID]*ast.ColumnDef)
	for _, col := range t.node.Columns {
		m[newColumnID(newTableIDFromPath(t.node.Name), col.Name)] = col
	}
	return m
}

func (t *table) alterRowDeletionPolicy(alteration tableOrQueueAlteration) ast.DDL {
	return &ast.AlterTable{Name: t.node.Name, TableAlteration: alteration}
}
