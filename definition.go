package spannerdiff

import (
	"fmt"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type definition interface {
	id() identifier
	astNode() ast.Node
	add() ast.DDL
	drop() ast.DDL
	dependsOn() []identifier
	onDependencyChange(me, dependency migrationState, m *migration)
}

var _ = []definition{
	&table{},
	&column{},
	&index{},
	&searchIndex{},
	&propertyGraph{},
}

type table struct {
	node *ast.CreateTable
}

func newTable(ct *ast.CreateTable) *table {
	return &table{ct}
}

func (c *table) id() identifier {
	return c.tableID()
}

func (c *table) tableID() tableID {
	return newTableIDFromPath(c.node.Name)
}

func (c *table) astNode() ast.Node {
	return c.node
}

func (c *table) add() ast.DDL {
	return c.node
}

func (c *table) drop() ast.DDL {
	return &ast.DropTable{
		Name: c.node.Name,
	}
}

func (c *table) dependsOn() []identifier {
	return nil
}

func (c *table) onDependencyChange(me, dependency migrationState, m *migration) {
}

func (c *table) columns() map[columnID]*ast.ColumnDef {
	m := make(map[columnID]*ast.ColumnDef)
	for _, col := range c.node.Columns {
		m[newColumnID(newTableIDFromPath(c.node.Name), col.Name)] = col
	}
	return m
}

type column struct {
	node  *ast.ColumnDef
	table *table
}

func newColumn(table *table, col *ast.ColumnDef) *column {
	return &column{col, table}
}

func (c *column) id() identifier {
	return c.columnID()
}

func (c *column) columnID() columnID {
	return newColumnID(c.table.tableID(), c.node.Name)
}

func (c *column) astNode() ast.Node {
	return c.node
}

func (c *column) add() ast.DDL {
	return &ast.AlterTable{
		Name: c.table.node.Name,
		TableAlteration: &ast.AddColumn{
			Column: c.node,
		},
	}
}

func (c *column) drop() ast.DDL {
	return &ast.AlterTable{
		Name: c.table.node.Name,
		TableAlteration: &ast.DropColumn{
			Name: c.node.Name,
		},
	}
}

func (c *column) dependsOn() []identifier {
	return []identifier{c.table.tableID()}
}

func (c *column) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *table:
		switch dependency.kind {
		case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
			// If the table is being added or dropped, the column is also being added or dropped.
			m.updateState(me.updateKind(migrationKindNone))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on column: %T", dep))
	}
}

type index struct {
	node *ast.CreateIndex
}

func newIndex(ci *ast.CreateIndex) *index {
	return &index{ci}
}

func (c *index) id() identifier {
	return c.indexID()
}

func (c *index) indexID() indexID {
	return newIndexID(c.node.Name)
}
func (c *index) tableID() tableID {
	return newTableIDFromPath(c.node.TableName)
}

func (c *index) astNode() ast.Node {
	return c.node
}

func (c *index) add() ast.DDL {
	return c.node
}

func (c *index) drop() ast.DDL {
	return &ast.DropIndex{
		Name: c.node.Name,
	}
}

func (c *index) dependsOn() []identifier {
	var ids []identifier
	for _, col := range c.node.Keys {
		ids = append(ids, newColumnID(newTableIDFromPath(c.node.TableName), col.Name))
	}
	if c.node.Storing != nil {
		for _, col := range c.node.Storing.Columns {
			ids = append(ids, newColumnID(newTableIDFromPath(c.node.TableName), col))
		}
	}
	ids = append(ids, c.tableID())
	return ids
}

func (c *index) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on index: %T", dep))
	}
}

type searchIndex struct {
	node *ast.CreateSearchIndex
}

func newSearchIndex(csi *ast.CreateSearchIndex) *searchIndex {
	return &searchIndex{csi}
}

func (c *searchIndex) id() identifier {
	return c.searchIndexID()
}

func (c *searchIndex) searchIndexID() searchIndexID {
	return newSearchIndexID(c.node.Name)
}

func (c *searchIndex) tableID() tableID {
	return newTableIDFromIdent(c.node.TableName)
}

func (c *searchIndex) astNode() ast.Node {
	return c.node
}

func (c *searchIndex) add() ast.DDL {
	return c.node
}

func (c *searchIndex) drop() ast.DDL {
	return &ast.DropSearchIndex{
		Name: c.node.Name,
	}
}

func (c *searchIndex) dependsOn() []identifier {
	var ids []identifier
	for _, col := range c.node.TokenListPart {
		ids = append(ids, newColumnID(newTableIDFromIdent(c.node.TableName), col))
	}
	ids = append(ids, c.tableID())
	return ids
}

func (c *searchIndex) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on search index: %T", dep))
	}
}

type propertyGraph struct {
	node *ast.CreatePropertyGraph
}

func newPropertyGraph(cpg *ast.CreatePropertyGraph) *propertyGraph {
	return &propertyGraph{cpg}
}

func (c *propertyGraph) id() identifier {
	return newPropertyGraphID(c.node.Name)
}

func (c *propertyGraph) propertyGraphID() propertyGraphID {
	return newPropertyGraphID(c.node.Name)
}

func (c *propertyGraph) astNode() ast.Node {
	return c.node
}

func (c *propertyGraph) add() ast.DDL {
	return c.node
}

func (c *propertyGraph) drop() ast.DDL {
	return &ast.DropPropertyGraph{
		Name: c.node.Name,
	}
}

func (c *propertyGraph) dependsOn() []identifier {
	var ids []identifier
	for _, elem := range c.node.Content.NodeTables.Tables.Elements {
		tableID := newTableIDFromIdent(elem.Name)
		ids = append(ids, tableID)
		if elem.Keys != nil {
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
				for _, key := range keys.Source.Keys.ColumnNameList {
					ids = append(ids, newColumnID(tableID, key))
				}
				for _, key := range keys.Source.ReferenceColumns.ColumnNameList {
					ids = append(ids, newColumnID(newTableIDFromIdent(keys.Source.ElementReference), key))
				}
			default:
				panic(fmt.Sprintf("unexpected property graph type: %T", keys))
			}
		}
	}
	if c.node.Content.EdgeTables != nil {
		for _, elem := range c.node.Content.EdgeTables.Tables.Elements {
			tableID := newTableIDFromIdent(elem.Name)
			ids = append(ids, tableID)
			if elem.Keys != nil {
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
					for _, key := range keys.Source.Keys.ColumnNameList {
						ids = append(ids, newColumnID(tableID, key))
					}
					for _, key := range keys.Source.ReferenceColumns.ColumnNameList {
						ids = append(ids, newColumnID(newTableIDFromIdent(keys.Source.ElementReference), key))
					}
				default:
					panic(fmt.Sprintf("unexpected property graph type: %T", keys))
				}
			}
		}
	}
	return ids
}

func (c *propertyGraph) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on property graph: %T", dep))
	}
}

type createView struct {
	node *ast.CreateView
}

func newView(cv *ast.CreateView) *createView {
	return &createView{cv}
}

func (c *createView) id() identifier {
	return newViewID(c.node.Name)
}

func (c *createView) viewID() viewID {
	return newViewID(c.node.Name)
}

func (c *createView) astNode() ast.Node {
	return c.node
}

func (c *createView) add() ast.DDL {
	return c.node
}

func (c *createView) drop() ast.DDL {
	return &ast.DropView{
		Name: c.node.Name,
	}
}

func (c *createView) dependsOn() []identifier {
	return nil
}

func (c *createView) onDependencyChange(me, dependency migrationState, m *migration) {
	return
}
