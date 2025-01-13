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
	alter(target definition, m *migration)
	dependsOn() []identifier
	onDependencyChange(me, dependency migrationState, m *migration)
}

var _ = []definition{
	&table{},
	&column{},
	&index{},
	&searchIndex{},
	&vectorIndex{},
	&propertyGraph{},
	&view{},
	&changeStream{},
	&sequence{},
}

type definitions struct {
	all map[identifier]definition
}

func newDefinitions(ddls []ast.DDL, errorOnUnsupported bool) (*definitions, error) {
	d := &definitions{
		make(map[identifier]definition),
	}

	for _, ddl := range ddls {
		switch ddl := ddl.(type) {
		case *ast.CreateSchema:
			d.all[newSchemaID(ddl.Name)] = newSchema(ddl)
		case *ast.CreateTable:
			table := newTable(ddl)
			d.all[table.id()] = table
			for id, col := range table.columns() {
				d.all[id] = newColumn(table, col)
			}
		case *ast.CreateIndex:
			d.all[newIndexID(ddl.Name)] = newIndex(ddl)
		case *ast.CreateSearchIndex:
			d.all[newSearchIndexID(ddl.Name)] = newSearchIndex(ddl)
		case *ast.CreatePropertyGraph:
			d.all[newPropertyGraphID(ddl.Name)] = newPropertyGraph(ddl)
		case *ast.CreateView:
			d.all[newViewID(ddl.Name)] = newView(ddl)
		case *ast.CreateChangeStream:
			d.all[newChangeStreamID(ddl.Name)] = newChangeStream(ddl)
		case *ast.CreateSequence:
			d.all[newSequenceID(ddl.Name)] = newSequence(ddl)
		case *ast.CreateVectorIndex:
			d.all[newVectorIndexID(ddl.Name)] = newVectorIndex(ddl)
		default:
			if errorOnUnsupported {
				return nil, fmt.Errorf("unsupported DDL: %s", ddl.SQL())
			}
		}
	}

	return d, nil
}

type schema struct {
	node *ast.CreateSchema
}

func newSchema(cs *ast.CreateSchema) *schema {
	return &schema{cs}
}

func (s *schema) id() identifier {
	return newSchemaID(s.node.Name)
}

func (s *schema) astNode() ast.Node {
	return s.node
}

func (s *schema) add() ast.DDL {
	return s.node
}

func (s *schema) drop() ast.DDL {
	return &ast.DropSchema{
		Name: s.node.Name,
	}
}

func (s *schema) alter(tgt definition, m *migration) {
	m.updateStateIfUndefined(newDropAndAddState(s, tgt))
}

func (s *schema) dependsOn() []identifier {
	return nil
}

func (s *schema) onDependencyChange(me, dependency migrationState, m *migration) {}

type table struct {
	node *ast.CreateTable
}

func newTable(ct *ast.CreateTable) *table {
	return &table{ct}
}

func (t *table) id() identifier {
	return t.tableID()
}

func (t *table) tableID() tableID {
	return newTableIDFromPath(t.node.Name)
}

func (t *table) schemaID() optional[schemaID] {
	return t.tableID().schemaID
}

func (t *table) astNode() ast.Node {
	return t.node
}

func (t *table) add() ast.DDL {
	return t.node
}

func (t *table) drop() ast.DDL {
	return &ast.DropTable{
		Name: t.node.Name,
	}
}

func (t *table) alter(tgt definition, m *migration) {
	base := t
	target := tgt.(*table)

	// https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	// - Add or remove a foreign key from an existing table.
	// - Add or remove a check constraint from an existing table.
	// --- not documented ---
	// - Add or remove a synonym from an existing table.
	// - Add, replace or remove a row deletion policy from an existing table.

	if !equalNodes(base.node.PrimaryKeys, target.node.PrimaryKeys) {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
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
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	m.updateStateIfUndefined(newAlterState(base, target, ddls...))
}

func (t *table) dependsOn() []identifier {
	if schemaID, ok := t.schemaID().get(); ok {
		return []identifier{schemaID}
	}
	return nil
}

func (t *table) onDependencyChange(me, dependency migrationState, m *migration) {}

func (t *table) columns() map[columnID]*ast.ColumnDef {
	m := make(map[columnID]*ast.ColumnDef)
	for _, col := range t.node.Columns {
		m[newColumnID(newTableIDFromPath(t.node.Name), col.Name)] = col
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

func (c *column) alter(tgt definition, m *migration) {
	base := c
	target := tgt.(*column)

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

	if m.kind(base.id()) == migrationKindNone {
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
		m.updateStateIfUndefined(newAlterState(base, target, ddls...))
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
				m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:    target.node.Type,
					NotNull: target.node.NotNull,
				}}}))
				return
			} else if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:        target.node.Type,
					NotNull:     target.node.NotNull,
					DefaultExpr: defaultExpr,
				}}}))
				return
			}
		default:
			m.updateStateIfUndefined(newDropAndAddState(base, target))
			return
		}
		m.updateStateIfUndefined(newDropAndAddState(base, target))
	}
}

func (c *column) dependsOn() []identifier {
	return []identifier{c.table.id()}
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

func (i *index) id() identifier {
	return i.indexID()
}

func (i *index) indexID() indexID {
	return newIndexID(i.node.Name)
}
func (i *index) tableID() tableID {
	return newTableIDFromPath(i.node.TableName)
}

func (i *index) schemaID() optional[schemaID] {
	return i.indexID().schemaID
}

func (i *index) astNode() ast.Node {
	return i.node
}

func (i *index) add() ast.DDL {
	return i.node
}

func (i *index) drop() ast.DDL {
	return &ast.DropIndex{
		Name: i.node.Name,
	}
}

func (i *index) alter(tgt definition, m *migration) {
	base := i
	target := tgt.(*index)

	// --- not documented ---
	// Add or remove a stored column from an existing index.

	if m.kind(base.id()) == migrationKindNone {
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
		m.updateStateIfUndefined(newAlterState(base, target, ddls...))
		return
	}
	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (i *index) dependsOn() []identifier {
	var ids []identifier
	for _, col := range i.node.Keys {
		ids = append(ids, newColumnID(newTableIDFromPath(i.node.TableName), col.Name))
	}
	if i.node.Storing != nil {
		for _, col := range i.node.Storing.Columns {
			ids = append(ids, newColumnID(newTableIDFromPath(i.node.TableName), col))
		}
	}
	if schemaID, ok := i.schemaID().get(); ok {
		ids = append(ids, schemaID)
	}
	ids = append(ids, i.tableID())
	return ids
}

func (i *index) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *table, *schema:
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

func (si *searchIndex) id() identifier {
	return newSearchIndexID(si.node.Name)
}

func (si *searchIndex) tableID() tableID {
	return newTableIDFromIdent(si.node.TableName)
}

func (si *searchIndex) astNode() ast.Node {
	return si.node
}

func (si *searchIndex) add() ast.DDL {
	return si.node
}

func (si *searchIndex) drop() ast.DDL {
	return &ast.DropSearchIndex{
		Name: si.node.Name,
	}
}

func (si *searchIndex) alter(tgt definition, m *migration) {
	base := si
	target := tgt.(*searchIndex)

	// --- not documented ---
	// Add or remove a stored column from an existing search index.

	if m.kind(base.id()) == migrationKindNone {
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
		m.updateStateIfUndefined(newAlterState(base, target, ddls...))
		return
	}
	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (si *searchIndex) dependsOn() []identifier {
	var ids []identifier
	for _, col := range si.node.TokenListPart {
		ids = append(ids, newColumnID(newTableIDFromIdent(si.node.TableName), col))
	}
	ids = append(ids, si.tableID())
	return ids
}

func (si *searchIndex) onDependencyChange(me, dependency migrationState, m *migration) {
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

type vectorIndex struct {
	node *ast.CreateVectorIndex
}

func newVectorIndex(cvi *ast.CreateVectorIndex) *vectorIndex {
	return &vectorIndex{cvi}
}

func (vi *vectorIndex) id() identifier {
	return newVectorIndexID(vi.node.Name)
}

func (vi *vectorIndex) tableID() tableID {
	return newTableIDFromIdent(vi.node.TableName)
}

func (vi *vectorIndex) astNode() ast.Node {
	return vi.node
}

func (vi *vectorIndex) add() ast.DDL {
	return vi.node
}

func (vi *vectorIndex) drop() ast.DDL {
	return &ast.DropVectorIndex{
		Name: vi.node.Name,
	}
}

func (vi *vectorIndex) alter(tgt definition, m *migration) {
	// ALTER VECTOR INDEX is not supported.
	m.updateStateIfUndefined(newDropAndAddState(vi, tgt))
}

func (vi *vectorIndex) dependsOn() []identifier {
	var ids []identifier
	ids = append(ids, newColumnID(newTableIDFromIdent(vi.node.TableName), vi.node.ColumnName))
	ids = append(ids, vi.tableID())
	return ids
}

func (vi *vectorIndex) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on vector index: %T", dep))
	}
}

type propertyGraph struct {
	node *ast.CreatePropertyGraph
}

func newPropertyGraph(cpg *ast.CreatePropertyGraph) *propertyGraph {
	return &propertyGraph{cpg}
}

func (pg *propertyGraph) id() identifier {
	return newPropertyGraphID(pg.node.Name)
}

func (pg *propertyGraph) astNode() ast.Node {
	return pg.node
}

func (pg *propertyGraph) add() ast.DDL {
	return pg.node
}

func (pg *propertyGraph) drop() ast.DDL {
	return &ast.DropPropertyGraph{
		Name: pg.node.Name,
	}
}

func (pg *propertyGraph) alter(tgt definition, m *migration) {
	base := pg
	target := tgt.(*propertyGraph)

	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (pg *propertyGraph) dependsOn() []identifier {
	var ids []identifier
	for _, elem := range pg.node.Content.NodeTables.Tables.Elements {
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
	if pg.node.Content.EdgeTables != nil {
		for _, elem := range pg.node.Content.EdgeTables.Tables.Elements {
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

func (pg *propertyGraph) onDependencyChange(me, dependency migrationState, m *migration) {
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

type view struct {
	node *ast.CreateView
}

func newView(cv *ast.CreateView) *view {
	return &view{cv}
}

func (v *view) id() identifier {
	return newViewID(v.node.Name)
}

func (v *view) astNode() ast.Node {
	return v.node
}

func (v *view) add() ast.DDL {
	return v.node
}

func (v *view) drop() ast.DDL {
	return &ast.DropView{
		Name: v.node.Name,
	}
}

func (v *view) alter(tgt definition, m *migration) {
	base := v
	target := tgt.(*view)

	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (v *view) dependsOn() []identifier {
	// TODO: process query to find dependencies
	return nil
}

func (v *view) onDependencyChange(me, dependency migrationState, m *migration) {}

type changeStream struct {
	node *ast.CreateChangeStream
}

func newChangeStream(ccs *ast.CreateChangeStream) *changeStream {
	return &changeStream{ccs}
}

func (cs *changeStream) id() identifier {
	return newChangeStreamID(cs.node.Name)
}

func (cs *changeStream) astNode() ast.Node {
	return cs.node
}

func (cs *changeStream) add() ast.DDL {
	return cs.node
}

func (cs *changeStream) drop() ast.DDL {
	return &ast.DropChangeStream{
		Name: cs.node.Name,
	}
}

func (cs *changeStream) alter(tgt definition, m *migration) {
	base := cs
	target := tgt.(*changeStream)

	var ddls []ast.DDL
	if !equalNode(base.node.For, target.node.For) {
		if target.node.For == nil {
			ddls = append(ddls, &ast.AlterChangeStream{Name: base.node.Name, ChangeStreamAlteration: &ast.ChangeStreamDropForAll{}})
		} else {
			ddls = append(ddls, &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: target.node.For}})
		}
	}
	if !equalNode(base.node.Options, target.node.Options) {
		ddls = append(ddls, &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: target.node.Options}})
	}
	if len(ddls) == 0 {
		return
	}
	m.updateStateIfUndefined(newAlterState(base, target, ddls...))
}

func (cs *changeStream) dependsOn() []identifier {
	if cs.node.For == nil {
		return nil
	}
	switch f := cs.node.For.(type) {
	case *ast.ChangeStreamForAll:
		return nil
	case *ast.ChangeStreamForTables:
		var ids []identifier
		for _, table := range f.Tables {
			ids = append(ids, newTableIDFromIdent(table.TableName))
			for _, col := range table.Columns {
				ids = append(ids, newColumnID(newTableIDFromIdent(table.TableName), col))
			}
		}
		return ids
	default:
		panic(fmt.Sprintf("unexpected change stream for type: %T", cs.node.For))
	}
}

func (cs *changeStream) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			if _, ok := cs.node.For.(*ast.ChangeStreamForAll); ok {
				return
			}

			// Ideally, we should remove the only recreated columns/tables from the change stream's FOR, then add them again after they are recreated.
			m.updateState(me.updateKind(migrationKindAlter,
				newOperation(me.definition(), operationKindDrop, &ast.AlterChangeStream{Name: cs.node.Name, ChangeStreamAlteration: &ast.ChangeStreamDropForAll{}}),
				newOperation(me.definition(), operationKindAdd, &ast.AlterChangeStream{Name: cs.node.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: cs.node.For}}),
			))
		}
	default:
		panic(fmt.Sprintf("unexpected dependOn type on property graph: %T", dep))
	}
}

type sequence struct {
	node *ast.CreateSequence
}

func newSequence(cs *ast.CreateSequence) *sequence {
	return &sequence{cs}
}

func (s *sequence) id() identifier {
	return s.sequenceID()
}

func (s *sequence) sequenceID() sequenceID {
	return newSequenceID(s.node.Name)
}

func (s *sequence) schemaID() optional[schemaID] {
	return s.sequenceID().schemaID
}

func (s *sequence) astNode() ast.Node {
	return s.node
}

func (s *sequence) add() ast.DDL {
	return s.node
}

func (s *sequence) drop() ast.DDL {
	return &ast.DropSequence{
		Name: s.node.Name,
	}
}

func (s *sequence) alter(tgt definition, m *migration) {
	base := s
	target := tgt.(*sequence)

	if !equalNode(base.node.Options, target.node.Options) {
		m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterSequence{Name: target.node.Name, Options: target.node.Options}))
		return
	}

	panic(fmt.Sprintf("unsupported sequence alternation on: %s", target.node.SQL()))
}

func (s *sequence) dependsOn() []identifier {
	if schemaID, ok := s.schemaID().get(); ok {
		return []identifier{schemaID}
	}
	return nil
}

func (s *sequence) onDependencyChange(me, dependency migrationState, m *migration) {}
