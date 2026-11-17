package spannerdiff

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type constraint struct {
	node  *ast.TableConstraint
	table *table
	cid   constraintID
}

func newConstraint(table *table, tc *ast.TableConstraint) *constraint {
	return &constraint{tc, table, newConstraintID(table.tableID(), tc)}
}

func (c *constraint) id() identifier {
	return c.cid
}

// astNode returns the constraint without its name because the name is a part of the identifier.
func (c *constraint) astNode() ast.Node {
	return c.node.Constraint
}

func (c *constraint) add() []ast.DDL {
	return []ast.DDL{
		&ast.AlterTable{
			Name:            c.table.node.Name,
			TableAlteration: &ast.AddTableConstraint{TableConstraint: c.node},
		},
	}
}

func (c *constraint) drop() optional[ast.DDL] {
	if c.node.Name == nil {
		// An unnamed constraint can't be dropped by DDL. This is reported as an error by the migration.
		return none[ast.DDL]()
	}
	return some[ast.DDL](&ast.AlterTable{
		Name:            c.table.node.Name,
		TableAlteration: &ast.DropConstraint{Name: c.node.Name},
	})
}

func (c *constraint) alter(tgt definition, m *migration) {
	m.updateStateIfUndefined(newDropAndAddState(c, tgt))
}

func (c *constraint) dependsOn() []dependency {
	tableID := c.table.tableID()
	switch con := c.node.Constraint.(type) {
	case *ast.ForeignKey:
		refTableID := newTableIDFromPath(con.ReferenceTable)
		// A foreign key referencing another table isn't in CREATE TABLE, so it's not simply owned by its table.
		tableKind := dependencyKindOwner
		if refTableID != tableID {
			tableKind = dependencyKindCustom
		}
		deps := []dependency{{tableID, tableKind}}
		// Altering a column may also require recreating the foreign key, unlike other dependencies.
		for _, col := range con.Columns {
			deps = append(deps, dependency{newColumnID(tableID, col), dependencyKindCustom})
		}
		if refTableID != tableID {
			deps = append(deps, dependency{refTableID, dependencyKindRecreate})
		}
		for _, col := range con.ReferenceColumns {
			deps = append(deps, dependency{newColumnID(refTableID, col), dependencyKindCustom})
		}
		return deps
	case *ast.Check:
		deps := []dependency{{tableID, dependencyKindOwner}}
		// Identifiers in the expression may not be columns, but it's harmless to depend on non-existent columns.
		ast.Inspect(con.Expr, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok {
				deps = append(deps, dependency{newColumnID(tableID, ident), dependencyKindRecreate})
			}
			return true
		})
		return deps
	default:
		return []dependency{{tableID, dependencyKindOwner}}
	}
}

// onDependencyChange handles a change of the table of a foreign key referencing another table,
// and a change of the columns of a foreign key.
func (c *constraint) onDependencyChange(me, dependency migrationState, m *migration) {
	if me.kind == migrationKindNone {
		return
	}
	switch dep := dependency.definition().(type) {
	case *table:
		switch dependency.kind {
		case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
			// A foreign key referencing another table isn't in CREATE TABLE, so it's added after the table is created.
			// A named one is dropped before DROP TABLE, because the referenced table may also be dropped.
			// An unnamed one can't be dropped by name, so it's dropped by DROP TABLE (see table.dropDependsOn).
			dropBase := me.base.valid && me.base.mustGet().(*constraint).node.Name != nil
			addTarget := me.target.valid
			switch {
			case dropBase && addTarget:
				m.updateState(me.updateKind(migrationKindDropAndAdd))
			case dropBase:
				m.updateState(me.updateKind(migrationKindDrop))
			case addTarget:
				m.updateState(me.updateKind(migrationKindAdd))
			default:
				m.updateState(me.updateKind(migrationKindNone))
			}
		}
	case *column:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.recreateWithDependency(me)
		case migrationKindAlter:
			// Spanner can't change NOT NULL or the type of a column used by a foreign key,
			// so the foreign key is dropped before the column is altered and added again after it.
			if !me.base.valid {
				return
			}
			if _, ok := me.base.mustGet().(*constraint).node.Constraint.(*ast.ForeignKey); ok && changesNullabilityOrType(dependency) {
				m.recreateWithDependency(me)
			}
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on constraint: %T", dep))
	}
}

// changesNullabilityOrType reports whether the migration of a column changes NOT NULL or the type of the column.
func changesNullabilityOrType(ms migrationState) bool {
	base, target := ms.base.mustGet().(*column), ms.target.mustGet().(*column)
	return base.node.NotNull != target.node.NotNull || !equalTypesIgnoringLength(base.node.Type, target.node.Type)
}

// resolveUnnamedConstraints matches unnamed constraints in target with constraints in base that have
// the same definition, so that a constraint named by Spanner is not recreated.
func resolveUnnamedConstraints(base, target *definitions) {
	var baseConstraints []*constraint
	for _, def := range base.all {
		if c, ok := def.(*constraint); ok {
			baseConstraints = append(baseConstraints, c)
		}
	}
	slices.SortFunc(baseConstraints, func(a, b *constraint) int {
		return strings.Compare(a.cid.ID(), b.cid.ID())
	})

	var unnamed []*constraint
	for _, def := range target.all {
		if c, ok := def.(*constraint); ok && c.node.Name == nil {
			if _, ok := base.all[c.cid]; !ok {
				unnamed = append(unnamed, c)
			}
		}
	}
	slices.SortFunc(unnamed, func(a, b *constraint) int {
		return strings.Compare(a.cid.ID(), b.cid.ID())
	})

	matched := make(map[constraintID]bool)
	for _, tc := range unnamed {
		for _, bc := range baseConstraints {
			if matched[bc.cid] || bc.cid.tableID != tc.cid.tableID || !equalNode(bc.astNode(), tc.astNode()) {
				continue
			}
			if _, ok := target.all[bc.cid]; ok {
				continue
			}
			matched[bc.cid] = true
			delete(target.all, tc.cid)
			tc.cid = bc.cid
			target.all[tc.cid] = tc
			break
		}
	}
}
