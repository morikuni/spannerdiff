package spannerdiff

import (
	"fmt"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type definition interface {
	id() identifier
	add() ast.DDL
	drop() ast.DDL
	dependsOn() []identifier
	onDependencyChange(me, dependency migrationState, m *migration)
}

var _ = []definition{
	&createTable{},
	&column{},
	&createIndex{},
	&createSearchIndex{},
	&createPropertyGraph{},
}

type createTable struct {
	node *ast.CreateTable
}

func newCreateTable(ct *ast.CreateTable) *createTable {
	return &createTable{ct}
}

func (c *createTable) id() identifier {
	return c.tableID()
}

func (c *createTable) tableID() tableID {
	return newTableIDFromPath(c.node.Name)
}

func (c *createTable) add() ast.DDL {
	return c.node
}

func (c *createTable) drop() ast.DDL {
	return &ast.DropTable{
		Name: c.node.Name,
	}
}

func (c *createTable) dependsOn() []identifier {
	return nil
}

func (c *createTable) onDependencyChange(me, dependency migrationState, m *migration) {
}

func (c *createTable) columns() map[columnID]*ast.ColumnDef {
	m := make(map[columnID]*ast.ColumnDef)
	for _, col := range c.node.Columns {
		m[newColumnID(newTableIDFromPath(c.node.Name), col.Name)] = col
	}
	return m
}

type column struct {
	node  *ast.ColumnDef
	table *createTable
}

func newColumn(table *createTable, col *ast.ColumnDef) *column {
	return &column{col, table}
}

func (c *column) id() identifier {
	return c.columnID()
}

func (c *column) columnID() columnID {
	return newColumnID(c.table.tableID(), c.node.Name)
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
	case *createTable:
		switch dependency.kind {
		case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
			// If the table is being added or dropped, the column is also being added or dropped.
			m.updateState(me.updateKind(migrationKindNone))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on column: %T", dep))
	}
}

type createIndex struct {
	node *ast.CreateIndex
}

func newCreateIndex(ci *ast.CreateIndex) *createIndex {
	return &createIndex{ci}
}

func (c *createIndex) id() identifier {
	return c.indexID()
}

func (c *createIndex) indexID() indexID {
	return newIndexID(c.node.Name)
}
func (c *createIndex) tableID() tableID {
	return newTableIDFromPath(c.node.TableName)
}

func (c *createIndex) add() ast.DDL {
	return c.node
}

func (c *createIndex) drop() ast.DDL {
	return &ast.DropIndex{
		Name: c.node.Name,
	}
}

func (c *createIndex) dependsOn() []identifier {
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

func (c *createIndex) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *createTable:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on index: %T", dep))
	}
}

type createSearchIndex struct {
	node *ast.CreateSearchIndex
}

func newCreateSearchIndex(csi *ast.CreateSearchIndex) *createSearchIndex {
	return &createSearchIndex{csi}
}

func (c *createSearchIndex) id() identifier {
	return c.searchIndexID()
}

func (c *createSearchIndex) searchIndexID() searchIndexID {
	return newSearchIndexID(c.node.Name)
}

func (c *createSearchIndex) tableID() tableID {
	return newTableIDFromIdent(c.node.TableName)
}

func (c *createSearchIndex) add() ast.DDL {
	return c.node
}

func (c *createSearchIndex) drop() ast.DDL {
	return &ast.DropSearchIndex{
		Name: c.node.Name,
	}
}

func (c *createSearchIndex) dependsOn() []identifier {
	var ids []identifier
	for _, col := range c.node.TokenListPart {
		ids = append(ids, newColumnID(newTableIDFromIdent(c.node.TableName), col))
	}
	ids = append(ids, c.tableID())
	return ids
}

func (c *createSearchIndex) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *createTable:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on search index: %T", dep))
	}
}

type createPropertyGraph struct {
	node *ast.CreatePropertyGraph
}

func newCreatePropertyGraph(cpg *ast.CreatePropertyGraph) *createPropertyGraph {
	return &createPropertyGraph{cpg}
}

func (c *createPropertyGraph) id() identifier {
	return newPropertyGraphID(c.node.Name)
}

func (c *createPropertyGraph) propertyGraphID() propertyGraphID {
	return newPropertyGraphID(c.node.Name)
}

func (c *createPropertyGraph) add() ast.DDL {
	return c.node
}

func (c *createPropertyGraph) drop() ast.DDL {
	return &ast.DropPropertyGraph{
		Name: c.node.Name,
	}
}

func (c *createPropertyGraph) dependsOn() []identifier {
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

func (c *createPropertyGraph) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *createTable:
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

func newCreateView(cv *ast.CreateView) *createView {
	return &createView{cv}
}

func (c *createView) id() identifier {
	return newViewID(c.node.Name)
}

func (c *createView) viewID() viewID {
	return newViewID(c.node.Name)
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
