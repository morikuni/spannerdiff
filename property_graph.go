package spannerdiff

import (
	"fmt"
	"slices"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type propertyGraph struct {
	node *ast.CreatePropertyGraph
}

func newPropertyGraph(cpg *ast.CreatePropertyGraph) *propertyGraph {
	return &propertyGraph{cpg}
}

func (pg *propertyGraph) id() identifier {
	return newPropertyGraphID(pg.node.Name)
}

func (pg *propertyGraph) astNode() ast.Node {
	return pg.node
}

func (pg *propertyGraph) add() []ast.DDL {
	return []ast.DDL{pg.node}
}

func (pg *propertyGraph) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropPropertyGraph{
		Name: pg.node.Name,
	})
}

func (pg *propertyGraph) alter(tgt definition, m *migration) {
	base := pg
	target := tgt.(*propertyGraph)

	if m.dropsAnyOf(dependencyIDs(base.dependsOn())) {
		// The property graph must stop referencing the dropped objects before they are dropped.
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (pg *propertyGraph) dependsOn() []dependency {
	var ids []identifier
	elements := pg.node.Content.NodeTables.Tables.Elements
	if pg.node.Content.EdgeTables != nil {
		elements = append(slices.Clone(elements), pg.node.Content.EdgeTables.Tables.Elements...)
	}
	for _, elem := range elements {
		tableID := newTableIDFromIdent(elem.Name)
		ids = append(ids, tableID)
		if elem.Keys == nil {
			continue
		}
		switch keys := elem.Keys.(type) {
		case *ast.PropertyGraphNodeElementKey:
			for _, key := range keys.Key.Keys.ColumnNameList {
				ids = append(ids, newColumnID(tableID, key))
			}
		case *ast.PropertyGraphEdgeElementKeys:
			if keys.Element != nil {
				for _, key := range keys.Element.Keys.ColumnNameList {
					ids = append(ids, newColumnID(tableID, key))
				}
			}
			ids = append(ids, edgeKeyDependencies(tableID, keys.Source.Keys, keys.Source.ElementReference, keys.Source.ReferenceColumns)...)
			ids = append(ids, edgeKeyDependencies(tableID, keys.Destination.Keys, keys.Destination.ElementReference, keys.Destination.ReferenceColumns)...)
		default:
			panic(fmt.Sprintf("unexpected property graph type: %T", keys))
		}
	}
	return newDependencies(dependencyKindRecreate, ids)
}

func edgeKeyDependencies(tableID tableID, keys *ast.PropertyGraphColumnNameList, ref *ast.Ident, refColumns *ast.PropertyGraphColumnNameList) []identifier {
	var ids []identifier
	for _, key := range keys.ColumnNameList {
		ids = append(ids, newColumnID(tableID, key))
	}
	refTableID := newTableIDFromIdent(ref)
	ids = append(ids, refTableID)
	if refColumns != nil {
		for _, key := range refColumns.ColumnNameList {
			ids = append(ids, newColumnID(refTableID, key))
		}
	}
	return ids
}
