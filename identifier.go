package spannerdiff

import (
	"fmt"
	"strings"

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
	vectorIndexID{},
	propertyGraphID{},
	viewID{},
	changeStreamID{},
	sequenceID{},
	modelID{},
	protoBundleID{},
	roleID{},
	grantID{},
	databaseID{},
}

var _ = []struct{}{
	isComparable(schemaID{}),
	isComparable(tableID{}),
	isComparable(columnID{}),
	isComparable(indexID{}),
	isComparable(searchIndexID{}),
	isComparable(vectorIndexID{}),
	isComparable(propertyGraphID{}),
	isComparable(viewID{}),
	isComparable(changeStreamID{}),
	isComparable(sequenceID{}),
	isComparable(modelID{}),
	isComparable(protoBundleID{}),
	isComparable(roleID{}),
	isComparable(grantID{}),
	isComparable(databaseID{}),
}

func isComparable[C comparable](_ C) struct{} { return struct{}{} }

// nameOf normalizes an identifier name because identifiers are case-insensitive in Spanner.
func nameOf(ident *ast.Ident) string {
	return strings.ToLower(ident.Name)
}

type schemaID struct {
	name string
}

func newSchemaID(ident *ast.Ident) schemaID {
	return schemaID{nameOf(ident)}
}

func (s schemaID) ID() string {
	return fmt.Sprintf("Schema(%s)", s.name)
}

func (s schemaID) String() string {
	return s.ID()
}

type tableID struct {
	schemaID optional[schemaID]
	name     string
}

func newTableIDFromPath(path *ast.Path) tableID {
	switch len(path.Idents) {
	case 1:
		return tableID{none[schemaID](), nameOf(path.Idents[0])}
	case 2:
		return tableID{some(newSchemaID(path.Idents[0])), nameOf(path.Idents[1])}
	default:
		panic(fmt.Sprintf("unexpected table name: %s", path.SQL()))
	}
}
func newTableIDFromIdent(ident *ast.Ident) tableID {
	return newTableIDFromPath(&ast.Path{Idents: []*ast.Ident{ident}})
}

func (t tableID) ID() string {
	if schemaID, ok := t.schemaID.get(); ok {
		return fmt.Sprintf("Table(%s.%s)", schemaID.name, t.name)
	}
	return fmt.Sprintf("Table(%s)", t.name)
}

func (t tableID) String() string {
	return t.ID()
}

type columnID struct {
	tableID tableID
	name    string
}

func newColumnID(tableID tableID, ident *ast.Ident) columnID {
	return columnID{tableID, nameOf(ident)}
}

func (c columnID) ID() string {
	return fmt.Sprintf("%s:Column(%s)", c.tableID.ID(), c.name)
}

func (c columnID) String() string {
	return c.ID()
}

type indexID struct {
	schemaID optional[schemaID]
	name     string
}

func newIndexID(path *ast.Path) indexID {
	switch len(path.Idents) {
	case 1:
		return indexID{none[schemaID](), nameOf(path.Idents[0])}
	case 2:
		return indexID{some(newSchemaID(path.Idents[0])), nameOf(path.Idents[1])}
	default:
		panic(fmt.Sprintf("unexpected index name: %s", path.SQL()))
	}
}

func (i indexID) ID() string {
	if schemaID, ok := i.schemaID.get(); ok {
		return fmt.Sprintf("Index(%s.%s)", schemaID.name, i.name)
	}
	return fmt.Sprintf("Index(%s)", i.name)
}

func (i indexID) String() string {
	return i.ID()
}

type searchIndexID struct {
	schemaID optional[schemaID]
	name     string
}

func newSearchIndexID(path *ast.Path) searchIndexID {
	switch len(path.Idents) {
	case 1:
		return searchIndexID{none[schemaID](), nameOf(path.Idents[0])}
	case 2:
		return searchIndexID{some(newSchemaID(path.Idents[0])), nameOf(path.Idents[1])}
	default:
		panic(fmt.Sprintf("unexpected search index name: %s", path.SQL()))
	}
}

func (i searchIndexID) ID() string {
	if schemaID, ok := i.schemaID.get(); ok {
		return fmt.Sprintf("SearchIndex(%s.%s)", schemaID.name, i.name)
	}
	return fmt.Sprintf("SearchIndex(%s)", i.name)
}

func (i searchIndexID) String() string {
	return i.ID()
}

type vectorIndexID struct {
	name string
}

func newVectorIndexID(ident *ast.Ident) vectorIndexID {
	return vectorIndexID{nameOf(ident)}
}

func (i vectorIndexID) ID() string {
	return fmt.Sprintf("VectorIndex(%s)", i.name)
}

func (i vectorIndexID) String() string {
	return i.ID()
}

type propertyGraphID struct {
	name string
}

func newPropertyGraphID(ident *ast.Ident) propertyGraphID {
	return propertyGraphID{nameOf(ident)}
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

func newViewIDFromPath(path *ast.Path) viewID {
	switch len(path.Idents) {
	case 1:
		return viewID{"", nameOf(path.Idents[0])}
	case 2:
		return viewID{nameOf(path.Idents[0]), nameOf(path.Idents[1])}
	default:
		panic(fmt.Sprintf("unexpected view name: %s", path.SQL()))
	}
}

func newViewIDFromIdent(ident *ast.Ident) viewID {
	return newViewIDFromPath(&ast.Path{Idents: []*ast.Ident{ident}})
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

type changeStreamID struct {
	name string
}

func newChangeStreamID(ident *ast.Ident) changeStreamID {
	return changeStreamID{nameOf(ident)}
}

// Change streams cannot belong to a named schema, so only a single identifier is accepted.
func newChangeStreamIDFromPath(path *ast.Path) changeStreamID {
	if len(path.Idents) != 1 {
		panic(fmt.Sprintf("unexpected change stream name: %s", path.SQL()))
	}
	return newChangeStreamID(path.Idents[0])
}

func (i changeStreamID) ID() string {
	return fmt.Sprintf("ChangeStream(%s)", i.name)
}

func (i changeStreamID) String() string {
	return i.ID()
}

type sequenceID struct {
	schemaID optional[schemaID]
	name     string
}

func newSequenceID(ident *ast.Path) sequenceID {
	switch len(ident.Idents) {
	case 1:
		return sequenceID{none[schemaID](), nameOf(ident.Idents[0])}
	case 2:
		return sequenceID{some(newSchemaID(ident.Idents[0])), nameOf(ident.Idents[1])}
	default:
		panic(fmt.Sprintf("unexpected sequence name: %s", ident.SQL()))
	}
}

func (i sequenceID) ID() string {
	if schemaID, ok := i.schemaID.get(); ok {
		return fmt.Sprintf("Sequence(%s.%s)", schemaID.name, i.name)
	}
	return fmt.Sprintf("Sequence(%s)", i.name)
}

func (i sequenceID) String() string {
	return i.ID()
}

type modelID struct {
	name string
}

func newModelID(ident *ast.Ident) modelID {
	return modelID{nameOf(ident)}
}

func (i modelID) ID() string {
	return fmt.Sprintf("Model(%s)", i.name)
}

func (i modelID) String() string {
	return i.ID()
}

// Only one proto bundle can be defined in a schema.
type protoBundleID struct{}

func newProtoBundleID() protoBundleID {
	return protoBundleID{}
}

func (i protoBundleID) ID() string {
	return "ProtoBundle"
}

func (i protoBundleID) String() string {
	return i.ID()
}

type roleID struct {
	name string
}

func newRoleID(ident *ast.Ident) roleID {
	return roleID{nameOf(ident)}
}

func (i roleID) ID() string {
	return fmt.Sprintf("Role(%s)", i.name)
}

func (i roleID) String() string {
	return i.ID()
}

type grantID struct {
	roleID      roleID
	privilegeID identifier
}

type grantPrivilegeID interface {
	tableID | viewID | changeStreamID | roleID | changeStreamReadFunctionID
}

func newGrantID[ID grantPrivilegeID](roleID roleID, privilegeID ID) grantID {
	return grantID{roleID, identifier(privilegeID)}
}

func (i grantID) ID() string {
	return fmt.Sprintf("Grant(%s):%s", i.roleID.ID(), i.privilegeID.ID())
}

func (i grantID) String() string {
	return i.ID()
}

type changeStreamReadFunctionID struct {
	name string
}

func newChangeStreamReadFunctionID(name *ast.Ident) changeStreamReadFunctionID {
	return changeStreamReadFunctionID{nameOf(name)}
}

// Change stream read functions cannot belong to a named schema, so only a single identifier is accepted.
func newChangeStreamReadFunctionIDFromPath(path *ast.Path) changeStreamReadFunctionID {
	if len(path.Idents) != 1 {
		panic(fmt.Sprintf("unexpected change stream read function name: %s", path.SQL()))
	}
	return newChangeStreamReadFunctionID(path.Idents[0])
}

func (i changeStreamReadFunctionID) ID() string {
	return fmt.Sprintf("ChangeStreamReadFunction(%s)", i.name)
}

func (i changeStreamReadFunctionID) String() string {
	return i.ID()
}

type databaseID struct {
	name string
}

func newDatabaseID(ident *ast.Ident) databaseID {
	return databaseID{nameOf(ident)}
}

func (i databaseID) ID() string {
	return fmt.Sprintf("Database(%s)", i.name)
}

func (i databaseID) String() string {
	return i.ID()
}
