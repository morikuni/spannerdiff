package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type view struct {
	node *ast.CreateView
}

func newView(cv *ast.CreateView) *view {
	return &view{cv}
}

func (v *view) id() identifier {
	return newViewIDFromPath(v.node.Name)
}

func (v *view) astNode() ast.Node {
	return v.node
}

func (v *view) add() []ast.DDL {
	return []ast.DDL{v.node}
}

func (v *view) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropView{
		Name: v.node.Name,
	})
}

func (v *view) alter(tgt definition, m *migration) {
	base := v
	target := tgt.(*view)

	if m.dropsAnyOf(dependencyIDs(base.dependsOn())) {
		// The view must stop referencing the dropped objects before they are dropped.
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (v *view) dependsOn() []dependency {
	var ids []identifier
	var tableIDs []tableID
	paths, idents := tablesOrViewsInQueryExpr(v.node.Query)
	// Can't distinguish between tables and views, so add both.
	for _, ident := range idents {
		ids = append(ids,
			newTableIDFromIdent(ident),
			newViewIDFromIdent(ident),
		)
		tableIDs = append(tableIDs, newTableIDFromIdent(ident))
	}
	for _, path := range paths {
		ids = append(ids,
			newTableIDFromPath(path),
			newViewIDFromPath(path),
		)
		tableIDs = append(tableIDs, newTableIDFromPath(path))
	}
	// Column names can't be resolved without the table definitions, so every identifier in the query
	// is treated as a column of every referenced table. Identifiers that aren't columns have no state and are ignored.
	ast.Inspect(v.node.Query, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok {
			for _, tableID := range tableIDs {
				ids = append(ids, newColumnID(tableID, ident))
			}
		}
		return true
	})
	return newDependencies(dependencyKindRecreate, ids)
}
