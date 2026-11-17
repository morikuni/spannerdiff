package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type index struct {
	node *ast.CreateIndex
}

func newIndex(ci *ast.CreateIndex) *index {
	return &index{ci}
}

func (i *index) id() identifier {
	return i.indexID()
}

func (i *index) indexID() indexID {
	return newIndexID(i.node.Name)
}
func (i *index) tableID() tableID {
	return newTableIDFromPath(i.node.TableName)
}

func (i *index) schemaID() optional[schemaID] {
	return i.indexID().schemaID
}

func (i *index) astNode() ast.Node {
	return i.node
}

func (i *index) add() []ast.DDL {
	return []ast.DDL{i.node}
}

func (i *index) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropIndex{
		Name: i.node.Name,
	})
}

func (i *index) alter(tgt definition, m *migration) {
	base := i
	target := tgt.(*index)

	// --- not documented ---
	// Add or remove a stored column from an existing index.

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Storing = nil
	targetCopy.Storing = nil

	if equalNode(&baseCopy, &targetCopy) {
		ops := alterStoring(base, target, base.tableID(), base.node.Storing, target.node.Storing, func(a storedColumnAlteration) ast.DDL {
			return &ast.AlterIndex{Name: target.node.Name, IndexAlteration: a}
		})
		m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
		return
	}
	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (i *index) dependsOn() []dependency {
	var ids []identifier
	for _, key := range i.node.Keys {
		// A key is a column name or an expression.
		// Identifiers in the expression may not be columns, but it's harmless to depend on non-existent columns.
		ast.Inspect(key, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok {
				ids = append(ids, newColumnID(newTableIDFromPath(i.node.TableName), ident))
			}
			return true
		})
	}
	if i.node.Storing != nil {
		for _, col := range i.node.Storing.Columns {
			ids = append(ids, newColumnID(newTableIDFromPath(i.node.TableName), col))
		}
	}
	if schemaID, ok := i.schemaID().get(); ok {
		ids = append(ids, schemaID)
	}
	ids = append(ids, i.tableID())
	if i.node.InterleaveIn != nil {
		ids = append(ids, newTableIDFromIdent(i.node.InterleaveIn.TableName))
	}
	return newDependencies(dependencyKindRecreate, ids)
}

// storedColumnAlteration is an alteration of the stored columns, which the ALTER statements of all kinds of indexes accept.
type storedColumnAlteration interface {
	ast.IndexAlteration
	ast.VectorIndexAlteration
}

// alterStoring returns the operations to change the stored columns of an index, a search index or a vector index.
// alter returns the ALTER statement of the index with the alteration.
func alterStoring(base, target definition, tableID tableID, baseStoring, targetStoring *ast.Storing, alter func(storedColumnAlteration) ast.DDL) []operation {
	columns := func(s *ast.Storing) []*ast.Ident {
		if s == nil {
			return nil
		}
		return s.Columns
	}
	toSet := func(cols []*ast.Ident) map[columnID]struct{} {
		set := make(map[columnID]struct{}, len(cols))
		for _, col := range cols {
			set[newColumnID(tableID, col)] = struct{}{}
		}
		return set
	}
	baseSet, targetSet := toSet(columns(baseStoring)), toSet(columns(targetStoring))

	var ops []operation
	for _, col := range columns(baseStoring) {
		if _, ok := targetSet[newColumnID(tableID, col)]; !ok {
			// Stored columns must be dropped from the index before the columns are dropped.
			ops = append(ops, newDropPhaseOperation(base, alter(&ast.DropStoredColumn{Name: col})))
		}
	}
	for _, col := range columns(targetStoring) {
		if _, ok := baseSet[newColumnID(tableID, col)]; !ok {
			ops = append(ops, newAddPhaseOperation(target, alter(&ast.AddStoredColumn{Name: col})))
		}
	}
	return ops
}
