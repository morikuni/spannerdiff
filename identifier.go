package spannerdiff

import (
	"fmt"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type identifier interface {
	ID() string
	String() string
}

var _ = []identifier{
	schemaID{},
	tableID{},
	columnID{},
	indexID{},
	searchIndexID{},
	propertyGraphID{},
	viewID{},
}

var _ = []struct{}{
	isComparable(schemaID{}),
	isComparable(tableID{}),
	isComparable(columnID{}),
	isComparable(indexID{}),
	isComparable(searchIndexID{}),
	isComparable(propertyGraphID{}),
	isComparable(viewID{}),
}

func isComparable[C comparable](_ C) struct{} { return struct{}{} }

type schemaID struct {
	name string
}

func newSchemaID(ident *ast.Ident) schemaID {
	return schemaID{ident.Name}
}

func (s schemaID) ID() string {
	return fmt.Sprintf("Schema(%s)", s.name)
}

func (s schemaID) String() string {
	return s.ID()
}

type tableID struct {
	schema string
	name   string
}

func newTableIDFromPath(path *ast.Path) tableID {
	switch len(path.Idents) {
	case 1:
		return tableID{"", path.Idents[0].Name}
	case 2:
		return tableID{path.Idents[0].Name, path.Idents[1].Name}
	default:
		panic(fmt.Sprintf("unexpected table name: %s", path.SQL()))
	}
}
func newTableIDFromIdent(ident *ast.Ident) tableID {
	return newTableIDFromPath(&ast.Path{Idents: []*ast.Ident{ident}})
}

func (t tableID) ID() string {
	if t.schema == "" {
		return fmt.Sprintf("Table(%s)", t.name)
	}
	return fmt.Sprintf("Table(%s.%s)", t.schema, t.name)
}

func (t tableID) String() string {
	return t.ID()
}

type columnID struct {
	tableID tableID
	name    string
}

func newColumnID(tableID tableID, ident *ast.Ident) columnID {
	return columnID{tableID, ident.Name}
}

func (c columnID) ID() string {
	return fmt.Sprintf("Column(%s:%s)", c.tableID.ID(), c.name)
}

func (c columnID) String() string {
	return c.ID()
}

type indexID struct {
	schema string
	name   string
}

func newIndexID(path *ast.Path) indexID {
	switch len(path.Idents) {
	case 1:
		return indexID{"", path.Idents[0].Name}
	case 2:
		return indexID{path.Idents[0].Name, path.Idents[1].Name}
	default:
		panic(fmt.Sprintf("unexpected index name: %s", path.SQL()))
	}
}

func (i indexID) ID() string {
	if i.schema == "" {
		return fmt.Sprintf("Index(%s)", i.name)
	}
	return fmt.Sprintf("Index(%s.%s)", i.schema, i.name)
}

func (i indexID) String() string {
	return i.ID()
}

type searchIndexID struct {
	name string
}

func newSearchIndexID(ident *ast.Ident) searchIndexID {
	return searchIndexID{ident.Name}
}

func (i searchIndexID) ID() string {
	return fmt.Sprintf("SearchIndex(%s)", i.name)
}

func (i searchIndexID) String() string {
	return i.ID()
}

type propertyGraphID struct {
	name string
}

func newPropertyGraphID(ident *ast.Ident) propertyGraphID {
	return propertyGraphID{ident.Name}
}

func (i propertyGraphID) ID() string {
	return fmt.Sprintf("PropertyGraph(%s)", i.name)
}

func (i propertyGraphID) String() string {
	return i.ID()
}

type viewID struct {
	schema string
	name   string
}

func newViewID(path *ast.Path) viewID {
	switch len(path.Idents) {
	case 1:
		return viewID{"", path.Idents[0].Name}
	case 2:
		return viewID{path.Idents[0].Name, path.Idents[1].Name}
	default:
		panic(fmt.Sprintf("unexpected view name: %s", path.SQL()))
	}
}

func (i viewID) ID() string {
	if i.schema == "" {
		return fmt.Sprintf("View(%s)", i.name)
	}
	return fmt.Sprintf("View(%s.%s)", i.schema, i.name)
}

func (i viewID) String() string {
	return i.ID()
}
