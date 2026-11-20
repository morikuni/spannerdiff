package spannerdiff

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type propertyGraph struct {
	node *ast.CreatePropertyGraph
	// normalized is the node whose omitted keys, labels and properties are written as Spanner stores them.
	// It is compared instead of the node, so that a definition read from Spanner equals the one that omits them.
	normalized *ast.CreatePropertyGraph
}

func newPropertyGraph(cpg *ast.CreatePropertyGraph, tableOf func(tableID) (*table, bool)) *propertyGraph {
	return &propertyGraph{cpg, normalizePropertyGraph(cpg, tableOf)}
}

func (pg *propertyGraph) id() identifier {
	return newPropertyGraphID(pg.node.Name)
}

func (pg *propertyGraph) astNode() ast.Node {
	return pg.normalized
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
	elements := pg.normalized.Content.NodeTables.Tables.Elements
	if pg.normalized.Content.EdgeTables != nil {
		elements = append(slices.Clone(elements), pg.normalized.Content.EdgeTables.Tables.Elements...)
	}
	for _, elem := range elements {
		tableID := newTableIDFromIdent(elem.Name)
		ids = append(ids, tableID)
		ids = append(ids, propertyColumns(tableID, elem.Properties)...)
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

// propertyColumns returns the columns that the properties reference.
// Identifiers in the expressions may not be columns, but it's harmless to depend on non-existent columns.
func propertyColumns(tableID tableID, properties ast.PropertyGraphLabelsOrProperties) []identifier {
	var ids []identifier
	ast.Inspect(properties, func(n ast.Node) bool {
		if property, ok := n.(*ast.PropertyGraphDerivedProperty); ok {
			ast.Inspect(property.Expr, func(n ast.Node) bool {
				if ident, ok := n.(*ast.Ident); ok {
					ids = append(ids, newColumnID(tableID, ident))
				}
				return true
			})
			return false
		}
		return true
	})
	return ids
}

// normalizePropertyGraph writes the keys, labels and properties that the definition omits, in the way Spanner stores them:
//   - The key of an element is the primary key of its table.
//   - The columns that an edge references are the key of the referenced node.
//   - The label of an element is its alias or its table name.
//   - The properties of a label are all columns of the table, and they are sorted by their names.
//
// An element is not changed if its table is not defined.
func normalizePropertyGraph(cpg *ast.CreatePropertyGraph, tableOf func(tableID) (*table, bool)) *ast.CreatePropertyGraph {
	content := *cpg.Content
	nodeTables := *content.NodeTables
	nodeTables.Tables = normalizePropertyGraphElements(nodeTables.Tables, tableOf, nil)
	content.NodeTables = &nodeTables
	if content.EdgeTables != nil {
		edgeTables := *content.EdgeTables
		edgeTables.Tables = normalizePropertyGraphElements(edgeTables.Tables, tableOf, nodeTables.Tables.Elements)
		content.EdgeTables = &edgeTables
	}
	normalized := *cpg
	normalized.Content = &content
	return &normalized
}

// normalizePropertyGraphElements normalizes node tables, or edge tables if nodes are given.
func normalizePropertyGraphElements(list *ast.PropertyGraphElementList, tableOf func(tableID) (*table, bool), nodes []*ast.PropertyGraphElement) *ast.PropertyGraphElementList {
	normalized := *list
	normalized.Elements = make([]*ast.PropertyGraphElement, 0, len(list.Elements))
	for _, elem := range list.Elements {
		table, ok := tableOf(newTableIDFromIdent(elem.Name))
		if !ok {
			normalized.Elements = append(normalized.Elements, elem)
			continue
		}
		var primaryKey, columns []*ast.Ident
		for _, key := range table.primaryKeys() {
			primaryKey = append(primaryKey, key.Name)
		}
		for _, col := range table.node.Columns {
			columns = append(columns, col.Name)
		}

		e := *elem
		switch keys := e.Keys.(type) {
		case nil:
			e.Keys = &ast.PropertyGraphNodeElementKey{Key: propertyGraphElementKey(primaryKey)}
		case *ast.PropertyGraphEdgeElementKeys:
			k := *keys
			if k.Element == nil {
				k.Element = propertyGraphElementKey(primaryKey)
			}
			if k.Source.ReferenceColumns == nil {
				source := *k.Source
				source.ReferenceColumns = propertyGraphNodeKey(nodes, source.ElementReference)
				k.Source = &source
			}
			if k.Destination.ReferenceColumns == nil {
				destination := *k.Destination
				destination.ReferenceColumns = propertyGraphNodeKey(nodes, destination.ElementReference)
				k.Destination = &destination
			}
			e.Keys = &k
		}

		name := e.Name
		if e.Alias != nil {
			name = e.Alias
		}
		label := func(l ast.PropertyGraphElementLabel) ast.PropertyGraphElementLabel {
			if _, ok := l.(*ast.PropertyGraphElementLabelLabelName); ok {
				return l
			}
			return &ast.PropertyGraphElementLabelLabelName{Name: name}
		}
		var labels []*ast.PropertyGraphLabelAndProperties
		switch p := e.Properties.(type) {
		case nil:
			labels = append(labels, &ast.PropertyGraphLabelAndProperties{Label: label(nil), Properties: normalizeProperties(nil, columns)})
		case *ast.PropertyGraphSingleProperties:
			labels = append(labels, &ast.PropertyGraphLabelAndProperties{Label: label(nil), Properties: normalizeProperties(p.Properties, columns)})
		case *ast.PropertyGraphLabelAndPropertiesList:
			for _, lp := range p.LabelAndProperties {
				labels = append(labels, &ast.PropertyGraphLabelAndProperties{Label: label(lp.Label), Properties: normalizeProperties(lp.Properties, columns)})
			}
		}
		e.Properties = &ast.PropertyGraphLabelAndPropertiesList{LabelAndProperties: labels}
		normalized.Elements = append(normalized.Elements, &e)
	}
	return &normalized
}

func propertyGraphElementKey(columns []*ast.Ident) *ast.PropertyGraphElementKey {
	return &ast.PropertyGraphElementKey{Keys: &ast.PropertyGraphColumnNameList{ColumnNameList: columns}}
}

// propertyGraphNodeKey returns the key of the node that an edge references, or nil if it is unknown.
func propertyGraphNodeKey(nodes []*ast.PropertyGraphElement, ref *ast.Ident) *ast.PropertyGraphColumnNameList {
	for _, node := range nodes {
		name := node.Name
		if node.Alias != nil {
			name = node.Alias
		}
		if nameOf(name) != nameOf(ref) {
			continue
		}
		if key, ok := node.Keys.(*ast.PropertyGraphNodeElementKey); ok {
			return key.Key.Keys
		}
	}
	return nil
}

// normalizeProperties writes the properties as the list of them sorted by their names.
func normalizeProperties(properties ast.PropertyGraphElementProperties, columns []*ast.Ident) ast.PropertyGraphElementProperties {
	var derived []*ast.PropertyGraphDerivedProperty
	switch p := properties.(type) {
	case *ast.PropertyGraphNoProperties:
		return p
	case *ast.PropertyGraphDerivedPropertyList:
		derived = slices.Clone(p.DerivedProperties)
	case nil, *ast.PropertyGraphPropertiesAre:
		excepted := make(map[string]struct{})
		if are, ok := p.(*ast.PropertyGraphPropertiesAre); ok && are.ExceptColumns != nil {
			for _, col := range are.ExceptColumns.ColumnNameList {
				excepted[nameOf(col)] = struct{}{}
			}
		}
		for _, col := range columns {
			if _, ok := excepted[nameOf(col)]; !ok {
				derived = append(derived, &ast.PropertyGraphDerivedProperty{Expr: col})
			}
		}
	default:
		return properties
	}
	propertyName := func(p *ast.PropertyGraphDerivedProperty) string {
		if p.Alias != nil {
			return strings.ToLower(p.Alias.Name)
		}
		if ident, ok := p.Expr.(*ast.Ident); ok {
			return strings.ToLower(ident.Name)
		}
		return p.Expr.SQL()
	}
	slices.SortStableFunc(derived, func(a, b *ast.PropertyGraphDerivedProperty) int {
		return cmp.Compare(propertyName(a), propertyName(b))
	})
	return &ast.PropertyGraphDerivedPropertyList{DerivedProperties: derived}
}
