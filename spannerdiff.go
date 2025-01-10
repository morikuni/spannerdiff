package spannerdiff

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
	"v.io/x/lib/toposort"
)

type DiffOption struct {
	ErrorOnUnsupportedDDL bool
}

func Diff(baseSQL, targetSQL io.Reader, output io.Writer, option DiffOption) error {
	base, err := io.ReadAll(baseSQL)
	if err != nil {
		return fmt.Errorf("failed to read base SQL: %w", err)
	}
	target, err := io.ReadAll(targetSQL)
	if err != nil {
		return fmt.Errorf("failed to read target SQL: %w", err)
	}

	baseDDLs, err := memefish.ParseDDLs("base", string(base))
	if err != nil {
		return fmt.Errorf("failed to parse base SQL: %w", err)
	}
	targetDDLs, err := memefish.ParseDDLs("target", string(target))
	if err != nil {
		return fmt.Errorf("failed to parse target SQL: %w", err)
	}

	baseDefs, err := newDefinitions(baseDDLs, option.ErrorOnUnsupportedDDL)
	if err != nil {
		return err
	}
	targetDefs, err := newDefinitions(targetDDLs, option.ErrorOnUnsupportedDDL)
	if err != nil {
		return err
	}

	stmts, err := diffDefinitions(baseDefs, targetDefs)
	if err != nil {
		return err
	}

	for i, stmt := range stmts {
		_, err = fmt.Fprint(output, stmt.SQL())
		if err != nil {
			return fmt.Errorf("failed to write migration DDL: %w", err)
		}
		_, err = fmt.Fprintln(output, ";")
		if err != nil {
			return fmt.Errorf("failed to write migration DDL: %w", err)
		}
		if i < len(stmts)-1 {
			_, err = fmt.Fprintln(output)
			if err != nil {
				return fmt.Errorf("failed to write migration DDL: %w", err)
			}
		}
	}

	return nil
}

type definitions struct {
	tables         map[tableID]*createTable
	columns        map[columnID]*column
	indexes        map[indexID]*createIndex
	searchIndexes  map[searchIndexID]*createSearchIndex
	propertyGraphs map[propertyGraphID]*createPropertyGraph
}

func newDefinitions(ddls []ast.DDL, errorOnUnsupported bool) (*definitions, error) {
	d := &definitions{
		make(map[tableID]*createTable),
		make(map[columnID]*column),
		make(map[indexID]*createIndex),
		make(map[searchIndexID]*createSearchIndex),
		make(map[propertyGraphID]*createPropertyGraph),
	}

	for _, ddl := range ddls {
		switch ddl := ddl.(type) {
		case *ast.CreateTable:
			table := newCreateTable(ddl)
			d.tables[table.tableID()] = table
			for id, col := range table.columns() {
				d.columns[id] = newColumn(table, col)
			}
		case *ast.CreateIndex:
			d.indexes[newIndexID(ddl.Name)] = newCreateIndex(ddl)
		case *ast.CreateSearchIndex:
			d.searchIndexes[newSearchIndexID(ddl.Name)] = newCreateSearchIndex(ddl)
		case *ast.CreatePropertyGraph:
			d.propertyGraphs[newPropertyGraphID(ddl.Name)] = newCreatePropertyGraph(ddl)
		default:
			if errorOnUnsupported {
				return nil, fmt.Errorf("unsupported DDL: %s", ddl.SQL())
			}
		}
	}

	return d, nil
}

type migrationKind string

const (
	// keep = no change
	// none = stop doing anything
	migrationKindUndefined  migrationKind = "undefined"
	migrationKindNone       migrationKind = "none"
	migrationKindAdd        migrationKind = "add"
	migrationKindAlter      migrationKind = "alter"
	migrationKindDrop       migrationKind = "drop"
	migrationKindDropAndAdd migrationKind = "drop_and_add"
)

func (mk migrationKind) String() string {
	return string(mk)
}

type migrationState struct {
	id        identifier
	def       definition
	kind      migrationKind
	alterDDLs []ast.DDL
}

func newMigrationState(id identifier, def definition, kind migrationKind, alters ...ast.DDL) migrationState {
	return migrationState{id, def, kind, alters}
}

func (ms migrationState) DDLs() []ast.DDL {
	switch ms.kind {
	case migrationKindAdd:
		return []ast.DDL{ms.def.add()}
	case migrationKindAlter:
		return ms.alterDDLs
	case migrationKindDrop:
		return []ast.DDL{ms.def.drop()}
	case migrationKindDropAndAdd:
		return []ast.DDL{ms.def.drop(), ms.def.add()}
	case migrationKindNone, migrationKindUndefined:
		return nil
	default:
		panic(fmt.Sprintf("unexpected migration kind: %s", ms.kind))
	}
}

func (ms migrationState) operations() []operation {
	switch ms.kind {
	case migrationKindAdd:
		return []operation{newOperation(ms, operationKindAdd, ms.def.add())}
	case migrationKindAlter:
		ops := make([]operation, 0, len(ms.alterDDLs))
		for _, ddl := range ms.alterDDLs {
			ops = append(ops, newOperation(ms, operationKindAlter, ddl))
		}
		return ops
	case migrationKindDrop:
		return []operation{newOperation(ms, operationKindDrop, ms.def.drop())}
	case migrationKindDropAndAdd:
		return []operation{
			newOperation(ms, operationKindDrop, ms.def.drop()),
			newOperation(ms, operationKindAdd, ms.def.add()),
		}
	case migrationKindNone, migrationKindUndefined:
		return nil
	default:
		panic(fmt.Sprintf("unexpected migration kind: %s", ms.kind))
	}
}

type migration struct {
	baseDefs   *definitions
	targetDefs *definitions
	states     map[identifier]migrationState
	dependOn   map[identifier][]definition
}

func newMigration(base, target *definitions) *migration {
	m := &migration{
		base,
		target,
		make(map[identifier]migrationState),
		make(map[identifier][]definition),
	}

	for _, def := range base.tables {
		m.initializeState(def)
	}
	for _, def := range base.columns {
		m.initializeState(def)
	}
	for _, def := range base.indexes {
		m.initializeState(def)
	}
	for _, def := range base.searchIndexes {
		m.initializeState(def)
	}
	for _, def := range base.propertyGraphs {
		m.initializeState(def)
	}

	for _, def := range target.tables {
		m.initializeState(def)
	}
	for _, def := range target.columns {
		m.initializeState(def)
	}
	for _, def := range target.indexes {
		m.initializeState(def)
	}
	for _, def := range target.searchIndexes {
		m.initializeState(def)
	}
	for _, def := range target.propertyGraphs {
		m.initializeState(def)
	}

	return m
}

func (m *migration) initializeState(def definition) {
	m.states[def.id()] = newMigrationState(def.id(), def, migrationKindUndefined)
	for _, id := range def.dependsOn() {
		m.dependOn[id] = append(m.dependOn[id], def)
	}
}

func (m *migration) updateState(s migrationState) {
	m.states[s.id] = s
	for _, receiver := range m.dependOn[s.id] {
		m.updateState(receiver.onDependencyChange(m.states[receiver.id()], s))
	}
}

func (m *migration) kind(id identifier) migrationKind {
	return m.states[id].kind
}

func diffDefinitions(base, target *definitions) ([]ast.DDL, error) {
	m := newMigration(base, target)

	// Supported schema update: https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	m.drops(base, target)
	m.alters(base, target)
	m.adds(base, target)

	var operations []operation
	for _, state := range m.states {
		operations = append(operations, state.operations()...)
	}

	operations, err := sortOperations(operations)
	if err != nil {
		return nil, err
	}

	ddls := make([]ast.DDL, 0, len(operations))
	for _, op := range operations {
		ddls = append(ddls, op.ddl)
	}
	return ddls, nil
}

func (m *migration) drops(baseDefs, targetDefs *definitions) {
	for indexID, def := range baseDefs.indexes {
		if _, ok := targetDefs.indexes[indexID]; !ok {
			m.updateState(newMigrationState(indexID, def, migrationKindDrop))
		}
	}
	for searchIndexID, def := range baseDefs.searchIndexes {
		if _, ok := targetDefs.searchIndexes[searchIndexID]; !ok {
			m.updateState(newMigrationState(searchIndexID, def, migrationKindDrop))
		}
	}
	for tableID, def := range baseDefs.tables {
		if _, ok := targetDefs.tables[tableID]; !ok {
			m.updateState(newMigrationState(tableID, def, migrationKindDrop))
		}
	}
	for columnID, def := range baseDefs.columns {
		if _, ok := targetDefs.columns[columnID]; !ok {
			if m.kind(columnID) == migrationKindUndefined {
				m.updateState(newMigrationState(columnID, def, migrationKindDrop))
			}
		}
	}
	for propertyGraphID, def := range baseDefs.propertyGraphs {
		if _, ok := targetDefs.propertyGraphs[propertyGraphID]; !ok {
			m.updateState(newMigrationState(propertyGraphID, def, migrationKindDrop))
		}
	}
}

func (m *migration) adds(base, target *definitions) {
	for tableID, def := range target.tables {
		if _, ok := base.tables[tableID]; !ok {
			m.updateState(newMigrationState(tableID, def, migrationKindAdd))
		}
	}
	for columnID, def := range target.columns {
		if _, ok := base.columns[columnID]; !ok {
			if m.kind(columnID) == migrationKindUndefined {
				m.updateState(newMigrationState(columnID, def, migrationKindAdd))
			}
		}
	}
	for indexID, def := range target.indexes {
		if _, ok := base.indexes[indexID]; !ok {
			m.updateState(newMigrationState(indexID, def, migrationKindAdd))
		}
	}
	for searchIndexID, def := range target.searchIndexes {
		if _, ok := base.searchIndexes[searchIndexID]; !ok {
			m.updateState(newMigrationState(searchIndexID, def, migrationKindAdd))
		}
	}
	for propertyGraphID, def := range target.propertyGraphs {
		if _, ok := base.propertyGraphs[propertyGraphID]; !ok {
			m.updateState(newMigrationState(propertyGraphID, def, migrationKindAdd))
		}
	}
}

func (m *migration) alters(base, target *definitions) {
	for tableID, targetTable := range target.tables {
		baseTable, ok := base.tables[tableID]
		if !ok {
			continue
		}
		if equalNode(baseTable.node, targetTable.node) {
			continue
		}
		m.alterTable(baseTable, targetTable)
	}
	for columnID, targetColumn := range target.columns {
		baseColumn, ok := base.columns[columnID]
		if !ok {
			continue
		}
		if equalNode(baseColumn.node, targetColumn.node) {
			continue
		}
		m.alterColumn(baseColumn, targetColumn)
	}
	for indexID, targetIndex := range target.indexes {
		baseIndex, ok := base.indexes[indexID]
		if !ok {
			continue
		}
		if equalNode(baseIndex.node, targetIndex.node) {
			continue
		}
		m.alterIndex(baseIndex, targetIndex)
	}
	for searchIndexID, targetSearchIndex := range target.searchIndexes {
		baseSearchIndex, ok := base.searchIndexes[searchIndexID]
		if !ok {
			continue
		}
		if equalNode(baseSearchIndex.node, targetSearchIndex.node) {
			continue
		}
		m.alterSearchIndex(baseSearchIndex, targetSearchIndex)
	}
	for propertyGraphID, targetPropertyGraph := range target.propertyGraphs {
		basePropertyGraph, ok := base.propertyGraphs[propertyGraphID]
		if !ok {
			continue
		}
		if equalNode(basePropertyGraph.node, targetPropertyGraph.node) {
			continue
		}
		m.alterPropertyGraph(basePropertyGraph, targetPropertyGraph)
	}
}

func (m *migration) alterTable(base, target *createTable) {
	// https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	// - Add or remove a foreign key from an existing table.
	// - Add or remove a check constraint from an existing table.
	// --- not documented ---
	// - Add or remove a synonym from an existing table.
	// - Add, replace or remove a row deletion policy from an existing table.

	if !equalNodes(base.node.PrimaryKeys, target.node.PrimaryKeys) {
		m.updateState(newMigrationState(target.tableID(), target, migrationKindDropAndAdd))
		return
	}

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Columns = nil
	targetCopy.Columns = nil
	if equalNode(&baseCopy, &targetCopy) {
		// If only the columns are different, the migration is done by altering the columns.
		return
	}

	var ddls []ast.DDL
	if !equalNode(base.node.RowDeletionPolicy, target.node.RowDeletionPolicy) {
		switch {
		case base.node.RowDeletionPolicy == nil && target.node.RowDeletionPolicy != nil:
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddRowDeletionPolicy{RowDeletionPolicy: target.node.RowDeletionPolicy.RowDeletionPolicy}})
		case base.node.RowDeletionPolicy != nil && target.node.RowDeletionPolicy == nil:
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropRowDeletionPolicy{}})
		default:
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.ReplaceRowDeletionPolicy{RowDeletionPolicy: target.node.RowDeletionPolicy.RowDeletionPolicy}})
		}
	}
	if !equalNodes(base.node.Synonyms, target.node.Synonyms) {
		baseSynonyms := make(map[string]struct{}, len(base.node.Synonyms))
		for _, syn := range base.node.Synonyms {
			baseSynonyms[syn.Name.Name] = struct{}{}
		}
		targetSynonyms := make(map[string]struct{}, len(target.node.Synonyms))
		for _, syn := range target.node.Synonyms {
			targetSynonyms[syn.Name.Name] = struct{}{}
		}
		for syn := range targetSynonyms {
			if _, ok := baseSynonyms[syn]; !ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddSynonym{Name: &ast.Ident{Name: syn}}})
			}
		}
		for syn := range baseSynonyms {
			if _, ok := targetSynonyms[syn]; !ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropSynonym{Name: &ast.Ident{Name: syn}}})
			}
		}
	}
	if !equalNodes(base.node.TableConstraints, target.node.TableConstraints) {
		baseConstraints := make(map[string]*ast.TableConstraint, len(base.node.TableConstraints))
		for _, tc := range base.node.TableConstraints {
			if tc.Name != nil {
				baseConstraints[tc.Name.Name] = tc
			}
		}
		targetConstraints := make(map[string]*ast.TableConstraint, len(target.node.TableConstraints))
		for _, tc := range target.node.TableConstraints {
			if tc.Name != nil {
				targetConstraints[tc.Name.Name] = tc
			}
		}
		added := make(map[string]*ast.TableConstraint, len(targetConstraints))
		for name, tc := range targetConstraints {
			if _, ok := baseConstraints[name]; !ok {
				added[name] = tc
			}
		}
		dropped := make(map[string]*ast.TableConstraint, len(baseConstraints))
		for name, tc := range baseConstraints {
			if _, ok := targetConstraints[name]; !ok {
				dropped[name] = tc
			}
		}
		dropAndCreated := make(map[string]*ast.TableConstraint, len(baseConstraints))
		for name, baseTC := range baseConstraints {
			if targetTC, ok := targetConstraints[name]; ok {
				if !equalNode(baseTC, targetTC) {
					dropAndCreated[name] = targetTC
				}
			}
		}
		for _, tc := range added {
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddTableConstraint{TableConstraint: tc}})
		}
		for name := range dropped {
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropConstraint{Name: &ast.Ident{Name: name}}})
		}
		for _, tc := range dropAndCreated {
			ddls = append(ddls,
				&ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropConstraint{Name: &ast.Ident{Name: tc.Name.Name}}},
				&ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddTableConstraint{TableConstraint: tc}},
			)
		}
	}

	if len(ddls) == 0 {
		// If there are no DDLs, the table was changed but could not alter. Therefore, drop and create.
		m.updateState(newMigrationState(target.tableID(), target, migrationKindDropAndAdd))
		return
	}

	m.updateState(newMigrationState(target.tableID(), target, migrationKindAlter, ddls...))
}

func (m *migration) alterColumn(base, target *column) {
	// https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	// - Add NOT NULL to a non-key column, excluding ARRAY columns.
	// - Remove NOT NULL from a non-key column.
	// - Change a STRING column to a BYTES column or a BYTES column to a STRING column.
	// - Change a PROTO column to a BYTES column or a BYTES column to a PROTO column.
	// - Change the proto message type of a PROTO column.
	// - Increase or decrease the length limit for a STRING or BYTES type (including to MAX), unless it is a primary key column inherited by one or more child tables.
	// - Increase or decrease the length limit for an ARRAY<STRING>, ARRAY<BYTES>, or ARRAY<PROTO> column to the maximum allowed.
	// - Enable or disable commit timestamps in value and primary key columns.
	// - Set, change or drop the default value of a column.

	if m.kind(base.columnID()) == migrationKindNone {
		// The table is added or created, so column is also added or created.
		return
	}

	if equalNode(base.node.Type, target.node.Type) {
		var ddls []ast.DDL
		var defaultSet bool
		if base.node.NotNull != target.node.NotNull {
			// We don't set default values for columns that will be NOT NULL from NULL-able.
			if target.node.DefaultSemantics == nil {
				ddls = append(ddls, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:    target.node.Type,
					NotNull: target.node.NotNull,
				}}})
			} else if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				defaultSet = true
				ddls = append(ddls, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:        target.node.Type,
					NotNull:     target.node.NotNull,
					DefaultExpr: defaultExpr,
				}}})
			}
		}

		if !equalNode(base.node.Options, target.node.Options) {
			// Need to unset options that are not in the target?
			ddls = append(ddls, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnSetOptions{Options: target.node.Options}}})
		}

		if !defaultSet && !equalNode(base.node.DefaultSemantics, target.node.DefaultSemantics) {
			if target.node.DefaultSemantics == nil {
				ddls = append(ddls, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnDropDefault{}}})
			} else if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnSetDefault{DefaultExpr: defaultExpr}}})
			}
		}
		m.updateState(newMigrationState(target.columnID(), target, migrationKindAlter, ddls...))
	} else {
		switch tupleOf(columnTypeOf(base.node.Type), columnTypeOf(target.node.Type)) {
		case tupleOf(scalar{ast.StringTypeName}, scalar{ast.BytesTypeName}),
			tupleOf(scalar{ast.BytesTypeName}, scalar{ast.StringTypeName}),
			tupleOf(protoOrEnum{}, scalar{ast.BytesTypeName}),
			tupleOf(scalar{ast.BytesTypeName}, protoOrEnum{}),
			tupleOf(scalar{ast.StringTypeName}, scalar{ast.StringTypeName}),
			tupleOf(scalar{ast.BytesTypeName}, scalar{ast.BytesTypeName}),
			tupleOf(array{scalar{ast.StringTypeName}}, array{scalar{ast.StringTypeName}}),
			tupleOf(array{scalar{ast.BytesTypeName}}, array{scalar{ast.BytesTypeName}}),
			tupleOf(array{protoOrEnum{}}, array{protoOrEnum{}}):
			if target.node.DefaultSemantics == nil {
				m.updateState(newMigrationState(target.columnID(), target, migrationKindAlter, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:    target.node.Type,
					NotNull: target.node.NotNull,
				}}}))
				return
			} else if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				m.updateState(newMigrationState(target.columnID(), target, migrationKindAlter, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:        target.node.Type,
					NotNull:     target.node.NotNull,
					DefaultExpr: defaultExpr,
				}}}))
				return
			}
		default:
			m.updateState(newMigrationState(target.columnID(), target, migrationKindDropAndAdd))
			return
		}
		m.updateState(newMigrationState(target.columnID(), target, migrationKindDropAndAdd))
	}
}

func (m *migration) alterIndex(base, target *createIndex) {
	// --- not documented ---
	// Add or remove a stored column from an existing index.

	if m.kind(base.indexID()) == migrationKindNone {
		// The table or column is added or created, so index is also added or created.
		return
	}

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Storing = nil
	targetCopy.Storing = nil

	if equalNode(&baseCopy, &targetCopy) {
		var baseStoring, targetStoring map[columnID]*ast.Ident
		if base.node.Storing != nil {
			baseStoring = make(map[columnID]*ast.Ident, len(base.node.Storing.Columns))
			for _, col := range base.node.Storing.Columns {
				baseStoring[newColumnID(base.tableID(), col)] = col
			}
		}
		if target.node.Storing != nil {
			targetStoring = make(map[columnID]*ast.Ident, len(target.node.Storing.Columns))
			for _, col := range target.node.Storing.Columns {
				targetStoring[newColumnID(target.tableID(), col)] = col
			}
		}
		added := make(map[columnID]*ast.Ident, len(targetStoring))
		dropped := make(map[columnID]*ast.Ident, len(baseStoring))
		for colID, col := range targetStoring {
			if _, ok := baseStoring[colID]; !ok {
				added[colID] = col
			}
		}
		for colID, col := range baseStoring {
			if _, ok := targetStoring[colID]; !ok {
				dropped[colID] = col
			}
		}
		var ddls []ast.DDL
		for _, col := range added {
			ddls = append(ddls, &ast.AlterIndex{Name: target.node.Name, IndexAlteration: &ast.AddStoredColumn{Name: col}})
		}
		for _, col := range dropped {
			ddls = append(ddls, &ast.AlterIndex{Name: target.node.Name, IndexAlteration: &ast.DropStoredColumn{Name: col}})
		}
		m.updateState(newMigrationState(target.indexID(), target, migrationKindAlter, ddls...))
		return
	}
	m.updateState(newMigrationState(target.indexID(), target, migrationKindDropAndAdd))
}

func (m *migration) alterSearchIndex(base, target *createSearchIndex) {
	// --- not documented ---
	// Add or remove a stored column from an existing search index.

	if m.kind(base.searchIndexID()) == migrationKindNone {
		// The table or column is added or created, so search index is also added or created.
		return
	}

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Storing = nil
	targetCopy.Storing = nil

	if equalNode(&baseCopy, &targetCopy) {
		var baseStoring, targetStoring map[columnID]*ast.Ident
		if base.node.Storing != nil {
			baseStoring = make(map[columnID]*ast.Ident, len(base.node.Storing.Columns))
			for _, col := range base.node.Storing.Columns {
				baseStoring[newColumnID(base.tableID(), col)] = col
			}
		}
		if target.node.Storing != nil {
			targetStoring = make(map[columnID]*ast.Ident, len(target.node.Storing.Columns))
			for _, col := range target.node.Storing.Columns {
				targetStoring[newColumnID(target.tableID(), col)] = col
			}
		}
		added := make(map[columnID]*ast.Ident, len(targetStoring))
		dropped := make(map[columnID]*ast.Ident, len(baseStoring))
		for colID, col := range targetStoring {
			if _, ok := baseStoring[colID]; !ok {
				added[colID] = col
			}
		}
		for colID, col := range baseStoring {
			if _, ok := targetStoring[colID]; !ok {
				dropped[colID] = col
			}
		}
		var ddls []ast.DDL
		for _, col := range added {
			ddls = append(ddls, &ast.AlterSearchIndex{Name: target.node.Name, IndexAlteration: &ast.AddStoredColumn{Name: col}})
		}
		for _, col := range dropped {
			ddls = append(ddls, &ast.AlterSearchIndex{Name: target.node.Name, IndexAlteration: &ast.DropStoredColumn{Name: col}})
		}
		m.updateState(newMigrationState(target.searchIndexID(), target, migrationKindAlter, ddls...))
		return
	}
	m.updateState(newMigrationState(target.searchIndexID(), target, migrationKindDropAndAdd))
}

func (m *migration) alterPropertyGraph(base, target *createPropertyGraph) {
	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateState(newMigrationState(target.propertyGraphID(), target, migrationKindAlter, &targetCopy))
}

type operation struct {
	id        identifier
	kind      operationKind
	ddl       ast.DDL
	dependsOn []identifier
}

func newOperation(s migrationState, kind operationKind, ddl ast.DDL) operation {
	return operation{s.id, kind, ddl, s.def.dependsOn()}
}

type operationKind string

const (
	operationKindAdd   operationKind = "add"
	operationKindAlter operationKind = "alter"
	operationKindDrop  operationKind = "drop"
)

func sortOperations(ops []operation) ([]operation, error) {
	// sort operations before topological sort to fix the sorted result.
	slices.SortFunc(ops, func(i, j operation) int {
		return cmp.Or(
			cmp.Compare(i.id.ID(), j.id.ID()),
			cmp.Compare(i.kind, j.kind),
		)
	})

	var addAlterOps, dropOps []operation
	for _, op := range ops {
		switch op.kind {
		case operationKindDrop:
			dropOps = append(dropOps, op)
		case operationKindAdd, operationKindAlter:
			addAlterOps = append(addAlterOps, op)
		default:
			panic(fmt.Sprintf("unexpected operation kind: %s", op.kind))
		}
	}

	sortedAddAlter, err := topologicalSort(addAlterOps)
	if err != nil {
		return nil, err
	}
	sortedDrop, err := topologicalSort(dropOps)
	if err != nil {
		return nil, err
	}
	reverse(sortedDrop)

	return append(sortedDrop, sortedAddAlter...), nil
}

func topologicalSort(ops []operation) ([]operation, error) {
	s := &toposort.Sorter{}

	nodeMap := make(map[identifier]*operation, len(ops))
	for i := range ops {
		nodeMap[ops[i].id] = &ops[i]
		s.AddNode(&ops[i])
	}

	for i := range ops {
		opPtr := &ops[i]
		for _, dep := range opPtr.dependsOn {
			if depPtr, ok := nodeMap[dep]; ok {
				s.AddEdge(opPtr, depPtr)
			}
		}
	}

	sorted, cycles := s.Sort()
	if len(cycles) > 0 {
		return nil, errors.New("dependency cycle detected")
	}

	result := make([]operation, 0, len(sorted))
	for _, v := range sorted {
		if opPtr, ok := v.(*operation); ok {
			result = append(result, *opPtr)
		}
	}
	return result, nil
}

func reverse(ops []operation) {
	for i, j := 0, len(ops)-1; i < j; i, j = i+1, j-1 {
		ops[i], ops[j] = ops[j], ops[i]
	}
}
