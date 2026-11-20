package spannerdiff

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type definition interface {
	id() identifier
	astNode() ast.Node
	add() []ast.DDL
	drop() optional[ast.DDL]
	alter(target definition, m *migration)
	dependsOn() []dependency
}

// dependencyKind is how a change of a dependency affects the definition that depends on it.
// The kinds are ordered by strength: when a definition depends on the same definition in several ways, the strongest kind is used.
type dependencyKind int

const (
	// dependencyKindOrder only orders the operations: the definition is added after its dependency and dropped before it.
	dependencyKindOrder dependencyKind = iota
	// dependencyKindRecreate also recreates the definition when its dependency is recreated,
	// because the dependency can't be dropped while the definition references it.
	dependencyKindRecreate
	// dependencyKindOwner is a dependency whose CREATE and DROP statements also add and drop the definition,
	// so the definition is not migrated by itself when its owner is added, dropped or recreated.
	dependencyKindOwner
	// dependencyKindCustom notifies the definition of every change of its dependency by dependencyChangeHandler.
	dependencyKindCustom
)

type dependency struct {
	id   identifier
	kind dependencyKind
}

func newDependencies(kind dependencyKind, ids []identifier) []dependency {
	deps := make([]dependency, 0, len(ids))
	for _, id := range ids {
		deps = append(deps, dependency{id, kind})
	}
	return deps
}

func dependencyIDs(deps []dependency) []identifier {
	ids := make([]identifier, 0, len(deps))
	for _, dep := range deps {
		ids = append(ids, dep.id)
	}
	return ids
}

// mergeDependencies merges the dependencies on the same definition into one with the strongest kind.
func mergeDependencies(deps []dependency) []dependency {
	indexes := make(map[identifier]int, len(deps))
	result := make([]dependency, 0, len(deps))
	for _, dep := range deps {
		if i, ok := indexes[dep.id]; ok {
			result[i].kind = max(result[i].kind, dep.kind)
			continue
		}
		indexes[dep.id] = len(result)
		result = append(result, dep)
	}
	return result
}

// dependencyChangeHandler is a definition that has dependencies of dependencyKindCustom.
type dependencyChangeHandler interface {
	onDependencyChange(me, dependency migrationState, m *migration)
}

var _ = []dependencyChangeHandler{
	&column{},
	&constraint{},
	&changeStream{},
}

var _ = []definition{
	&schema{},
	&table{},
	&column{},
	&constraint{},
	&rowDeletionPolicy{},
	&index{},
	&searchIndex{},
	&vectorIndex{},
	&propertyGraph{},
	&view{},
	&changeStream{},
	&sequence{},
	&model{},
	&protoBundle{},
	&queue{},
	&role{},
	&grant{},
	&database{},
}

type merger interface {
	merge(other definition) (couldMerge bool)
}

var _ = []merger{
	&grant{},
}

type definitions struct {
	all map[identifier]definition
}

// normalizeDDLs moves foreign keys referencing other tables out of CREATE TABLE into ALTER TABLE ADD,
// and other constraints added by ALTER TABLE into CREATE TABLE, so that a constraint is handled in the same way
// wherever it's written. Spanner returns some foreign keys as ALTER TABLE ADD instead of in CREATE TABLE.
// The ALTER TABLE statements are placed after all CREATE TABLE statements.
func normalizeDDLs(ddls []ast.DDL) []ast.DDL {
	var result, alters []ast.DDL
	tables := make(map[tableID]*ast.CreateTable)
	for _, ddl := range ddls {
		switch ddl := ddl.(type) {
		case *ast.CreateTable:
			copied := *ddl // don't modify the parsed AST
			copied.TableConstraints = nil
			tables[newTableIDFromPath(ddl.Name)] = &copied
			result = append(result, &copied)
		case *ast.AlterTable:
			if !isAddTableConstraint(ddl) {
				result = append(result, ddl)
			}
		default:
			result = append(result, ddl)
		}
	}

	place := func(tableName *ast.Path, tc *ast.TableConstraint, alter *ast.AlterTable) {
		if !referencesOtherTable(newTableIDFromPath(tableName), tc) {
			if ct, ok := tables[newTableIDFromPath(tableName)]; ok {
				ct.TableConstraints = append(ct.TableConstraints, tc)
				return
			}
		}
		if alter == nil {
			alter = &ast.AlterTable{Name: tableName, TableAlteration: &ast.AddTableConstraint{TableConstraint: tc}}
		}
		alters = append(alters, alter)
	}
	for _, ddl := range ddls {
		switch ddl := ddl.(type) {
		case *ast.CreateTable:
			for _, tc := range ddl.TableConstraints {
				place(ddl.Name, tc, nil)
			}
		case *ast.AlterTable:
			if isAddTableConstraint(ddl) {
				place(ddl.Name, ddl.TableAlteration.(*ast.AddTableConstraint).TableConstraint, ddl)
			}
		}
	}

	return append(result, alters...)
}

// isAddTableConstraint reports whether the DDL is a supported ALTER TABLE ADD of a constraint.
// A table name with more than two parts is not supported, as in CREATE TABLE.
func isAddTableConstraint(at *ast.AlterTable) bool {
	_, ok := at.TableAlteration.(*ast.AddTableConstraint)
	return ok && len(at.Name.Idents) <= 2
}

func referencesOtherTable(tableID tableID, tc *ast.TableConstraint) bool {
	fk, ok := tc.Constraint.(*ast.ForeignKey)
	return ok && newTableIDFromPath(fk.ReferenceTable) != tableID
}

func newDefinitions(ddls []ast.DDL, option DiffOption) (*definitions, error) {
	d := &definitions{
		make(map[identifier]definition),
	}

	var duplicated map[identifier]struct{}
	add := func(def definition) {
		id := def.id()
		if old, exists := d.all[id]; exists {
			if m, ok := old.(merger); ok && m.merge(def) {
				return
			}
			if duplicated == nil {
				duplicated = make(map[identifier]struct{})
			}
			duplicated[id] = struct{}{}
			return
		}
		d.all[id] = def
	}
	unsupported := func(ddl ast.DDL) error {
		if option.ErrorOnUnsupportedDDL {
			return fmt.Errorf("unsupported DDL: %s", ddl.SQL())
		}
		if option.OnUnsupportedDDL != nil {
			option.OnUnsupportedDDL(ddl.SQL())
		}
		return nil
	}

	// Property graphs are defined after all tables, because what they omit is written with the definitions of the tables.
	var propertyGraphs []*ast.CreatePropertyGraph
	for _, ddl := range normalizeDDLs(ddls) {
		switch ddl := ddl.(type) {
		case *ast.CreateSchema:
			add(newSchema(ddl))
		case *ast.CreateTable:
			table := newTable(ddl)
			add(table)
			for _, col := range table.columns() {
				add(newColumn(table, col, option.ZeroDefaultForNotNullColumns))
			}
			for _, tc := range ddl.TableConstraints {
				if _, ok := tc.Constraint.(*ast.TablePrimaryKey); ok {
					// The primary key is a part of the table definition.
					continue
				}
				add(newConstraint(table, tc))
			}
			if ddl.RowDeletionPolicy != nil {
				add(newRowDeletionPolicy(table, ddl.RowDeletionPolicy.RowDeletionPolicy))
			}
		case *ast.CreateIndex:
			add(newIndex(ddl))
		case *ast.CreateSearchIndex:
			add(newSearchIndex(ddl))
		case *ast.CreatePropertyGraph:
			propertyGraphs = append(propertyGraphs, ddl)
		case *ast.CreateView:
			add(newView(ddl))
		case *ast.CreateChangeStream:
			add(newChangeStream(ddl))
		case *ast.CreateSequence:
			add(newSequence(ddl))
		case *ast.CreateVectorIndex:
			add(newVectorIndex(ddl))
		case *ast.CreateModel:
			add(newModel(ddl))
		case *ast.CreateProtoBundle:
			add(newProtoBundle(ddl))
		case *ast.CreateQueue:
			queue := newQueue(ddl)
			add(queue)
			if ddl.RowDeletionPolicy != nil {
				add(newRowDeletionPolicy(queue, ddl.RowDeletionPolicy.RowDeletionPolicy))
			}
		case *ast.CreateRole:
			add(newRole(ddl))
		case *ast.Grant:
			grants, err := newGrant(ddl)
			if err != nil {
				return nil, err
			}
			for _, g := range grants {
				add(g)
			}
		case *ast.AlterDatabase:
			add(newDatabase(ddl))
		case *ast.AlterTable:
			if !isAddTableConstraint(ddl) {
				if err := unsupported(ddl); err != nil {
					return nil, err
				}
				continue
			}
			// normalizeDDLs places ALTER TABLE after CREATE TABLE, so the table is already defined.
			table, ok := d.all[newTableIDFromPath(ddl.Name)].(*table)
			if !ok {
				return nil, fmt.Errorf("table not found: %s", ddl.SQL())
			}
			tc := ddl.TableAlteration.(*ast.AddTableConstraint).TableConstraint
			table.foreignKeys = append(table.foreignKeys, tc)
			add(newConstraint(table, tc))
		default:
			if err := unsupported(ddl); err != nil {
				return nil, err
			}
		}
	}

	for _, ddl := range propertyGraphs {
		add(newPropertyGraph(ddl, func(id tableID) (*table, bool) {
			t, ok := d.all[id].(*table)
			return t, ok
		}))
	}

	if duplicated != nil {
		ids := make([]string, 0, len(duplicated))
		for id := range duplicated {
			ids = append(ids, id.String())
		}
		slices.Sort(ids)
		return nil, fmt.Errorf("duplicated definition found: %s", strings.Join(ids, ", "))
	}

	return d, nil
}
