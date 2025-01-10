package spannerdiff

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type identifier interface {
	ID() string
}

var _ = []identifier{
	tableID{},
	columnID{},
	indexID{},
	searchIndexID{},
	propertyGraphID{},
}

var _ = []struct{}{
	isComparable(tableID{}),
	isComparable(columnID{}),
	isComparable(indexID{}),
	isComparable(searchIndexID{}),
	isComparable(propertyGraphID{}),
}

func isComparable[C comparable](_ C) struct{} { return struct{}{} }

type tableID [2]string

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
	if t[0] == "" {
		return fmt.Sprintf("TableID(%s)", t[1])
	}
	return fmt.Sprintf("TableID(%s:%s)", t[0], t[1])
}

type columnID struct {
	tableID tableID
	name    string
}

func newColumnID(tableID tableID, ident *ast.Ident) columnID {
	return columnID{tableID, ident.Name}
}

func (c columnID) ID() string {
	return fmt.Sprintf("ColumnID(%s:%s)", c.tableID.ID(), c.name)
}

type indexID [2]string

func newIndexID(path *ast.Path) indexID {
	switch len(path.Idents) {
	case 1:
		return indexID{path.Idents[0].Name, ""}
	case 2:
		return indexID{path.Idents[0].Name, path.Idents[1].Name}
	default:
		panic(fmt.Sprintf("unexpected index name: %s", path.SQL()))
	}
}

func (i indexID) ID() string {
	return fmt.Sprintf("IndexID(%s)", strings.Join(i[:], "."))
}

type searchIndexID struct {
	name string
}

func newSearchIndexID(ident *ast.Ident) searchIndexID {
	return searchIndexID{ident.Name}
}

func (i searchIndexID) ID() string {
	return fmt.Sprintf("SearchIndexID(%s)", i.name)
}

type propertyGraphID struct {
	name string
}

func newPropertyGraphID(ident *ast.Ident) propertyGraphID {
	return propertyGraphID{ident.Name}
}

func (i propertyGraphID) ID() string {
	return fmt.Sprintf("PropertyGraphID(%s)", i.name)
}
