package spannerdiff

import (
	"fmt"
	"io"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
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
	base      definition
	target    definition
	kind      migrationKind
	alterDDLs []ast.DDL
}

func newMigrationState(id identifier, base, target definition, kind migrationKind, alters ...ast.DDL) migrationState {
	return migrationState{id, base, target, kind, alters}
}

func (ms migrationState) updateKind(kind migrationKind) migrationState {
	ms.kind = kind
	return ms
}

func (ms migrationState) operations() []operation {
	switch ms.kind {
	case migrationKindAdd:
		return []operation{newOperation(ms.target, operationKindAdd, ms.target.add())}
	case migrationKindAlter:
		ops := make([]operation, 0, len(ms.alterDDLs))
		for _, ddl := range ms.alterDDLs {
			ops = append(ops, newOperation(ms.target, operationKindAlter, ddl))
		}
		return ops
	case migrationKindDrop:
		return []operation{newOperation(ms.base, operationKindDrop, ms.base.drop())}
	case migrationKindDropAndAdd:
		return []operation{
			newOperation(ms.base, operationKindDrop, ms.base.drop()),
			newOperation(ms.target, operationKindAdd, ms.target.add()),
		}
	case migrationKindNone, migrationKindUndefined:
		return nil
	default:
		panic(fmt.Sprintf("unexpected migration kind: %s: %s", ms.kind, ms.id))
	}
}

func (ms migrationState) definition() definition {
	if ms.target != nil {
		return ms.target
	}
	return ms.base
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

	for _, base := range base.all {
		m.initializeState(base, target.all[base.id()])
	}
	for _, target := range target.all {
		m.initializeState(base.all[target.id()], target)
	}

	return m
}

func (m *migration) initializeState(base, target definition) {
	var def definition
	if target != nil {
		def = target
	} else {
		def = base
	}
	m.states[def.id()] = newMigrationState(def.id(), base, target, migrationKindUndefined)
	for _, id := range def.dependsOn() {
		m.dependOn[id] = append(m.dependOn[id], def)
	}
}

func (m *migration) updateStateIfUndefined(s migrationState) {
	if m.kind(s.id) != migrationKindUndefined {
		return
	}
	m.updateState(s)
}

func (m *migration) updateState(s migrationState) {
	m.states[s.id] = s
	for _, receiver := range m.dependOn[s.id] {
		receiver.onDependencyChange(m.states[receiver.id()], s, m)
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
	for id, base := range baseDefs.all {
		if target, ok := targetDefs.all[id]; !ok {
			m.updateStateIfUndefined(newMigrationState(id, base, target, migrationKindDrop))
		}
	}
}

func (m *migration) adds(base, target *definitions) {
	for id, target := range target.all {
		if _, ok := base.all[id]; !ok {
			m.updateStateIfUndefined(newMigrationState(id, nil, target, migrationKindAdd))
		}
	}
}

func (m *migration) alters(base, target *definitions) {
	for tableID, targetTable := range target.all {
		baseTable, ok := base.all[tableID]
		if !ok {
			continue
		}
		if equalNode(baseTable.astNode(), targetTable.astNode()) {
			continue
		}
		switch targetTable := targetTable.(type) {
		case *table:
			m.alterTable(baseTable.(*table), targetTable)
		case *column:
			m.alterColumn(baseTable.(*column), targetTable)
		case *index:
			m.alterIndex(baseTable.(*index), targetTable)
		case *searchIndex:
			m.alterSearchIndex(baseTable.(*searchIndex), targetTable)
		case *propertyGraph:
			m.alterPropertyGraph(baseTable.(*propertyGraph), targetTable)
		case *view:
			m.alterView(baseTable.(*view), targetTable)
		default:
			panic(fmt.Sprintf("unexpected definition: %T", targetTable))
		}
	}
}

func (m *migration) alterTable(base, target *table) {
	// https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	// - Add or remove a foreign key from an existing table.
	// - Add or remove a check constraint from an existing table.
	// --- not documented ---
	// - Add or remove a synonym from an existing table.
	// - Add, replace or remove a row deletion policy from an existing table.

	if !equalNodes(base.node.PrimaryKeys, target.node.PrimaryKeys) {
		m.updateStateIfUndefined(newMigrationState(target.tableID(), base, target, migrationKindDropAndAdd))
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
		dropAndAdd := make(map[string]*ast.TableConstraint, len(baseConstraints))
		for name, baseTC := range baseConstraints {
			if targetTC, ok := targetConstraints[name]; ok {
				if !equalNode(baseTC, targetTC) {
					dropAndAdd[name] = targetTC
				}
			}
		}
		for _, tc := range added {
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddTableConstraint{TableConstraint: tc}})
		}
		for name := range dropped {
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropConstraint{Name: &ast.Ident{Name: name}}})
		}
		for _, tc := range dropAndAdd {
			ddls = append(ddls,
				&ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropConstraint{Name: &ast.Ident{Name: tc.Name.Name}}},
				&ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddTableConstraint{TableConstraint: tc}},
			)
		}
	}

	if len(ddls) == 0 {
		// If there are no DDLs, the table was changed but could not alter. Therefore, drop and create.
		m.updateStateIfUndefined(newMigrationState(target.tableID(), base, target, migrationKindDropAndAdd))
		return
	}

	m.updateStateIfUndefined(newMigrationState(target.tableID(), base, target, migrationKindAlter, ddls...))
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
		m.updateStateIfUndefined(newMigrationState(target.columnID(), base, target, migrationKindAlter, ddls...))
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
				m.updateStateIfUndefined(newMigrationState(target.columnID(), base, target, migrationKindAlter, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:    target.node.Type,
					NotNull: target.node.NotNull,
				}}}))
				return
			} else if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				m.updateStateIfUndefined(newMigrationState(target.columnID(), base, target, migrationKindAlter, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:        target.node.Type,
					NotNull:     target.node.NotNull,
					DefaultExpr: defaultExpr,
				}}}))
				return
			}
		default:
			m.updateStateIfUndefined(newMigrationState(target.columnID(), base, target, migrationKindDropAndAdd))
			return
		}
		m.updateStateIfUndefined(newMigrationState(target.columnID(), base, target, migrationKindDropAndAdd))
	}
}

func (m *migration) alterIndex(base, target *index) {
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
		m.updateStateIfUndefined(newMigrationState(target.indexID(), base, target, migrationKindAlter, ddls...))
		return
	}
	m.updateStateIfUndefined(newMigrationState(target.indexID(), base, target, migrationKindDropAndAdd))
}

func (m *migration) alterSearchIndex(base, target *searchIndex) {
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
		m.updateStateIfUndefined(newMigrationState(target.searchIndexID(), base, target, migrationKindAlter, ddls...))
		return
	}
	m.updateStateIfUndefined(newMigrationState(target.searchIndexID(), base, target, migrationKindDropAndAdd))
}

func (m *migration) alterPropertyGraph(base, target *propertyGraph) {
	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newMigrationState(target.propertyGraphID(), base, target, migrationKindAlter, &targetCopy))
}

func (m *migration) alterView(base, target *view) {
	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newMigrationState(target.viewID(), base, target, migrationKindAlter, &targetCopy))
}
