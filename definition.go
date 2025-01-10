package spannerdiff

import (
	"fmt"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type definition interface {
	id() identifier
	isDefinition()
	add() ast.DDL
	drop() ast.DDL
}

var _ = []definition{
	&createTable{},
	&column{},
	&createIndex{},
	&createSearchIndex{},
}

type migrationReceiver interface {
	id() identifier
	onDependencyChange(me, dependency migrationState) migrationState
}

var _ = []migrationReceiver{
	&column{},
	&createIndex{},
	&createSearchIndex{},
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
	return newTableID(c.node.Name)
}

func (c *createTable) isDefinition() {}

func (c *createTable) add() ast.DDL {
	return c.node
}

func (c *createTable) drop() ast.DDL {
	return &ast.DropTable{
		Name: c.node.Name,
	}
}

func (c *createTable) columns() map[columnID]*ast.ColumnDef {
	m := make(map[columnID]*ast.ColumnDef)
	for _, col := range c.node.Columns {
		m[newColumnID(newTableID(c.node.Name), col.Name)] = col
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

func (c *column) isDefinition() {}

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

func (c *column) onDependencyChange(me, dependency migrationState) migrationState {
	switch dep := dependency.def.(type) {
	case *createTable:
		switch dependency.kind {
		case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
			// If the table is being added or dropped, the column is also being added or dropped.
			return newMigrationState(c.columnID(), c, migrationKindNone)
		default:
			return me
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on column: %T", dep))
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
	return newTableID(c.node.TableName)
}

func (c *createIndex) dependsOn() []identifier {
	var ids []identifier
	for _, col := range c.node.Keys {
		ids = append(ids, newColumnID(newTableID(c.node.TableName), col.Name))
	}
	if c.node.Storing != nil {
		for _, col := range c.node.Storing.Columns {
			ids = append(ids, newColumnID(newTableID(c.node.TableName), col))
		}
	}
	ids = append(ids, c.tableID())
	return ids
}

func (c *createIndex) onDependencyChange(me, dependency migrationState) migrationState {
	switch dependency.def.(type) {
	case *column, *createTable:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			return newMigrationState(c.indexID(), c, migrationKindDropAndAdd)
		default:
			return me
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on index: %T", dependency.def))
	}
}

func (c *createIndex) isDefinition() {}

func (c *createIndex) add() ast.DDL {
	return c.node
}

func (c *createIndex) drop() ast.DDL {
	return &ast.DropIndex{
		Name: c.node.Name,
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
	return newTableID(&ast.Path{Idents: []*ast.Ident{c.node.TableName}})
}

func (c *createSearchIndex) dependsOn() []identifier {
	var ids []identifier
	for _, col := range c.node.TokenListPart {
		ids = append(ids, newColumnID(newTableID(&ast.Path{Idents: []*ast.Ident{c.node.TableName}}), col))
	}
	ids = append(ids, c.tableID())
	return ids
}

func (c *createSearchIndex) onDependencyChange(me, dependency migrationState) migrationState {
	switch dependency.def.(type) {
	case *column, *createTable:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			return newMigrationState(c.searchIndexID(), c, migrationKindDropAndAdd)
		default:
			return me
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on search index: %T", dependency.def))
	}
}

func (c *createSearchIndex) isDefinition() {}

func (c *createSearchIndex) add() ast.DDL {
	return c.node
}

func (c *createSearchIndex) drop() ast.DDL {
	return &ast.DropSearchIndex{
		Name: c.node.Name,
	}
}
