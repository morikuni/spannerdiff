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
	add() ast.DDL
	drop() optional[ast.DDL]
	alter(target definition, m *migration)
	dependsOn() []identifier
	onDependencyChange(me, dependency migrationState, m *migration)
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

	for _, ddl := range ddls {
		switch ddl := ddl.(type) {
		case *ast.CreateSchema:
			add(newSchema(ddl))
		case *ast.CreateTable:
			table := newTable(ddl)
			add(table)
			for _, col := range table.columns() {
				add(newColumn(table, col))
			}
			for _, tc := range ddl.TableConstraints {
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
			add(newPropertyGraph(ddl))
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
		case *ast.CreateRole:
			add(newRole(ddl))
		case *ast.Grant:
			for _, g := range newGrant(ddl) {
				add(g)
			}
		case *ast.AlterDatabase:
			add(newDatabase(ddl))
		default:
			if option.ErrorOnUnsupportedDDL {
				return nil, fmt.Errorf("unsupported DDL: %s", ddl.SQL())
			}
			if option.OnUnsupportedDDL != nil {
				option.OnUnsupportedDDL(ddl.SQL())
			}
		}
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

func (s *schema) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropSchema{
		Name: s.node.Name,
	})
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

func (t *table) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropTable{
		Name: t.node.Name,
	})
}

func (t *table) alter(tgt definition, m *migration) {
	base := t
	target := tgt.(*table)

	// https://cloud.google.com/spanner/docs/schema-updates?t#supported-updates
	// - Change the ON DELETE action of an interleaved table.
	// - Convert between INTERLEAVE IN and INTERLEAVE IN PARENT.
	// --- not documented ---
	// - Add or remove a synonym from an existing table.
	// - Set table options.

	if !equalNodes(base.node.PrimaryKeys, target.node.PrimaryKeys) {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	// Columns are migrated by column definitions.
	// Other fields are cleared once they are handled, and the remaining differences require recreating the table.
	baseRest := *base.node
	targetRest := *target.node
	baseRest.Columns = nil
	targetRest.Columns = nil

	var ddls []ast.DDL
	if alteration, ok := alterCluster(base.node.Cluster, target.node.Cluster); ok {
		if alteration != nil {
			ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: alteration})
		}
		baseRest.Cluster = nil
		targetRest.Cluster = nil
	}
	// Row deletion policies are migrated by row deletion policy definitions.
	baseRest.RowDeletionPolicy = nil
	targetRest.RowDeletionPolicy = nil
	if !equalNodes(base.node.Synonyms, target.node.Synonyms) {
		baseSynonyms := make(map[string]struct{}, len(base.node.Synonyms))
		for _, syn := range base.node.Synonyms {
			baseSynonyms[nameOf(syn.Name)] = struct{}{}
		}
		targetSynonyms := make(map[string]struct{}, len(target.node.Synonyms))
		for _, syn := range target.node.Synonyms {
			targetSynonyms[nameOf(syn.Name)] = struct{}{}
		}
		for _, syn := range base.node.Synonyms {
			if _, ok := targetSynonyms[nameOf(syn.Name)]; !ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.DropSynonym{Name: syn.Name}})
			}
		}
		for _, syn := range target.node.Synonyms {
			if _, ok := baseSynonyms[nameOf(syn.Name)]; !ok {
				ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AddSynonym{Name: syn.Name}})
			}
		}
	}
	baseRest.Synonyms = nil
	targetRest.Synonyms = nil
	// Constraints are migrated by constraint definitions.
	baseRest.TableConstraints = nil
	targetRest.TableConstraints = nil
	if options := diffOptions(base.node.Options, target.node.Options); options != nil {
		ddls = append(ddls, &ast.AlterTable{Name: target.node.Name, TableAlteration: &ast.AlterTableSetOptions{Options: options}})
	}
	baseRest.Options = nil
	targetRest.Options = nil

	if !equalNode(&baseRest, &targetRest) {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}
	if len(ddls) == 0 {
		return
	}

	m.updateStateIfUndefined(newAlterState(base, target, ddls...))
}

// alterCluster returns the alteration to change the INTERLEAVE IN clause of a table.
// It returns false if the change requires recreating the table, and a nil alteration if nothing changes.
func alterCluster(base, target *ast.Cluster) (ast.TableAlteration, bool) {
	onDelete := func(c *ast.Cluster) ast.OnDeleteAction {
		if c.OnDelete == "" {
			return ast.OnDeleteNoAction
		}
		return c.OnDelete
	}
	switch {
	case base == nil && target == nil:
		return nil, true
	case base == nil || target == nil:
		return nil, false
	case !equalNode(base.TableName, target.TableName):
		return nil, false
	case base.Enforced != target.Enforced:
		return &ast.SetInterleaveIn{TableName: target.TableName, Enforced: target.Enforced, OnDelete: target.OnDelete}, true
	case onDelete(base) != onDelete(target):
		return &ast.SetOnDelete{OnDelete: onDelete(target)}, true
	default:
		return nil, true
	}
}

func (t *table) dependsOn() []identifier {
	var ids []identifier
	if schemaID, ok := t.schemaID().get(); ok {
		ids = append(ids, schemaID)
	}
	if t.node.Cluster != nil {
		ids = append(ids, newTableIDFromPath(t.node.Cluster.TableName))
	}
	// CREATE TABLE contains default values, so sequences used by them must exist before the table is created.
	for _, col := range t.node.Columns {
		ids = append(ids, sequencesInColumn(col)...)
	}
	// CREATE TABLE contains foreign keys, so referenced tables must exist before the table is created.
	for _, tc := range t.node.TableConstraints {
		if fk, ok := tc.Constraint.(*ast.ForeignKey); ok {
			if refID := newTableIDFromPath(fk.ReferenceTable); refID != t.tableID() {
				ids = append(ids, refID)
			}
		}
	}
	return ids
}

func (t *table) onDependencyChange(me, dependency migrationState, m *migration) {
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *schema:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	case *sequence:
		// The sequence is only used for ordering operations.
	case *table:
		// A table referenced by foreign keys is handled by constraint definitions,
		// so only an interleaved table is recreated along with its parent.
		if t.node.Cluster == nil || newTableIDFromPath(t.node.Cluster.TableName) != dep.tableID() {
			return
		}
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on table: %T", dep))
	}
}

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

func (c *column) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.AlterTable{
		Name: c.table.node.Name,
		TableAlteration: &ast.DropColumn{
			Name: c.node.Name,
		},
	})
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

	if !equalNode(base.node.Type, target.node.Type) || base.node.NotNull != target.node.NotNull || !equalNode(base.node.DefaultSemantics, target.node.DefaultSemantics) {
		switch {
		case isGeneratedColumn(base.node) || isGeneratedColumn(target.node):
			// Generated columns cannot be altered, but their values can be recomputed from other columns.
			base.recreate(target, m)
			return
		case !equalNode(base.node.DefaultSemantics, target.node.DefaultSemantics) && (isIdentityColumn(base.node) || isIdentityColumn(target.node)):
			m.addError(fmt.Errorf("changing identity or auto increment of a column is not supported: %s", target.id()))
			return
		}
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

		if options := diffOptions(base.node.Options, target.node.Options); options != nil {
			ddls = append(ddls, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnSetOptions{Options: options}}})
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
			tupleOf(protoOrEnum{}, protoOrEnum{}),
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
			base.recreate(target, m)
			return
		}
		base.recreate(target, m)
	}
}

func (c *column) recreate(target *column, m *migration) {
	if !c.isPrimaryKey() {
		m.updateStateIfUndefined(newDropAndAddState(c, target))
		return
	}
	// Primary key columns can't be dropped, so the table is recreated.
	tableState := m.states[c.table.id()]
	switch tableState.kind {
	case migrationKindDropAndAdd:
		return
	case migrationKindUndefined, migrationKindAlter:
		m.updateState(tableState.updateKind(migrationKindDropAndAdd))
	default:
		panic(fmt.Sprintf("unexpected migration kind of table: %s: %s", tableState.kind, tableState.id))
	}
}

func (c *column) isPrimaryKey() bool {
	for _, key := range c.table.node.PrimaryKeys {
		if equalNode(key.Name, c.node.Name) {
			return true
		}
	}
	return false
}

func isGeneratedColumn(col *ast.ColumnDef) bool {
	_, ok := col.DefaultSemantics.(*ast.GeneratedColumnExpr)
	return ok
}

func isIdentityColumn(col *ast.ColumnDef) bool {
	switch col.DefaultSemantics.(type) {
	case *ast.IdentityColumn, *ast.AutoIncrement:
		return true
	default:
		return false
	}
}

func (c *column) dependsOn() []identifier {
	return append([]identifier{c.table.id()}, sequencesInColumn(c.node)...)
}

// sequencesInColumn returns sequences used by GET_NEXT_SEQUENCE_VALUE in the default value of the column.
func sequencesInColumn(col *ast.ColumnDef) []identifier {
	if col.DefaultSemantics == nil {
		return nil
	}
	var ids []identifier
	ast.Inspect(col.DefaultSemantics, func(n ast.Node) bool {
		arg, ok := n.(*ast.SequenceArg)
		if !ok {
			return true
		}
		switch e := arg.Expr.(type) {
		case *ast.Ident:
			ids = append(ids, newSequenceID(&ast.Path{Idents: []*ast.Ident{e}}))
		case *ast.Path:
			ids = append(ids, newSequenceID(e))
		}
		return true
	})
	return ids
}

func (c *column) onDependencyChange(me, dependency migrationState, m *migration) {
	switch dep := dependency.definition().(type) {
	case *table:
		switch dependency.kind {
		case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
			// If the table is being added or dropped, the column is also being added or dropped.
			m.updateState(me.updateKind(migrationKindNone))
		}
	case *sequence:
		// The sequence is only used for ordering operations.
	default:
		panic(fmt.Sprintf("unexpected dependency type on column: %T", dep))
	}
}

type constraint struct {
	node  *ast.TableConstraint
	table *table
	cid   constraintID
}

func newConstraint(table *table, tc *ast.TableConstraint) *constraint {
	return &constraint{tc, table, newConstraintID(table.tableID(), tc)}
}

func (c *constraint) id() identifier {
	return c.cid
}

// astNode returns the constraint without its name because the name is a part of the identifier.
func (c *constraint) astNode() ast.Node {
	return c.node.Constraint
}

func (c *constraint) add() ast.DDL {
	return &ast.AlterTable{
		Name:            c.table.node.Name,
		TableAlteration: &ast.AddTableConstraint{TableConstraint: c.node},
	}
}

func (c *constraint) drop() optional[ast.DDL] {
	if c.node.Name == nil {
		// An unnamed constraint can't be dropped by DDL. This is reported as an error by the migration.
		return none[ast.DDL]()
	}
	return some[ast.DDL](&ast.AlterTable{
		Name:            c.table.node.Name,
		TableAlteration: &ast.DropConstraint{Name: c.node.Name},
	})
}

func (c *constraint) alter(tgt definition, m *migration) {
	if m.kind(c.id()) == migrationKindNone {
		// The table is added or recreated, so the constraint is also added or recreated.
		return
	}
	m.updateStateIfUndefined(newDropAndAddState(c, tgt))
}

func (c *constraint) dependsOn() []identifier {
	tableID := c.table.tableID()
	ids := []identifier{tableID}
	switch con := c.node.Constraint.(type) {
	case *ast.ForeignKey:
		for _, col := range con.Columns {
			ids = append(ids, newColumnID(tableID, col))
		}
		refTableID := newTableIDFromPath(con.ReferenceTable)
		if refTableID != tableID {
			ids = append(ids, refTableID)
		}
		for _, col := range con.ReferenceColumns {
			ids = append(ids, newColumnID(refTableID, col))
		}
	case *ast.Check:
		// Identifiers in the expression may not be columns, but it's harmless to depend on non-existent columns.
		ast.Inspect(con.Expr, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok {
				ids = append(ids, newColumnID(tableID, ident))
			}
			return true
		})
	}
	return ids
}

func (c *constraint) onDependencyChange(me, dependency migrationState, m *migration) {
	if me.kind == migrationKindNone {
		return
	}
	switch dep := dependency.definition().(type) {
	case *table:
		switch {
		case dep.tableID() == c.table.tableID():
			switch dependency.kind {
			case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
				// CREATE TABLE and DROP TABLE also add and drop constraints.
				m.updateState(me.updateKind(migrationKindNone))
			}
		case me.kind != migrationKindDrop && dependency.kind == migrationKindDropAndAdd:
			// The referenced table can't be dropped while a foreign key references it.
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	case *column:
		if me.kind != migrationKindDrop && dependency.kind == migrationKindDropAndAdd {
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on constraint: %T", dep))
	}
}

type rowDeletionPolicy struct {
	node  *ast.RowDeletionPolicy
	table *table
}

func newRowDeletionPolicy(table *table, rdp *ast.RowDeletionPolicy) *rowDeletionPolicy {
	return &rowDeletionPolicy{rdp, table}
}

func (r *rowDeletionPolicy) id() identifier {
	return newRowDeletionPolicyID(r.table.tableID())
}

func (r *rowDeletionPolicy) astNode() ast.Node {
	return r.node
}

func (r *rowDeletionPolicy) add() ast.DDL {
	return &ast.AlterTable{
		Name:            r.table.node.Name,
		TableAlteration: &ast.AddRowDeletionPolicy{RowDeletionPolicy: r.node},
	}
}

func (r *rowDeletionPolicy) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.AlterTable{
		Name:            r.table.node.Name,
		TableAlteration: &ast.DropRowDeletionPolicy{},
	})
}

func (r *rowDeletionPolicy) alter(tgt definition, m *migration) {
	base := r
	target := tgt.(*rowDeletionPolicy)

	if m.kind(base.id()) == migrationKindNone {
		// The table is added or recreated, so the policy is also added or recreated.
		return
	}

	if !equalNode(base.node.ColumnName, target.node.ColumnName) {
		// The policy must be dropped before the column of the base policy is dropped.
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}
	m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterTable{
		Name:            target.table.node.Name,
		TableAlteration: &ast.ReplaceRowDeletionPolicy{RowDeletionPolicy: target.node},
	}))
}

func (r *rowDeletionPolicy) dependsOn() []identifier {
	return []identifier{r.table.tableID(), newColumnID(r.table.tableID(), r.node.ColumnName)}
}

func (r *rowDeletionPolicy) onDependencyChange(me, dependency migrationState, m *migration) {
	if me.kind == migrationKindNone {
		return
	}
	switch dep := dependency.definition().(type) {
	case *table:
		switch dependency.kind {
		case migrationKindAdd, migrationKindDropAndAdd, migrationKindDrop:
			// CREATE TABLE and DROP TABLE also add and drop the policy.
			m.updateState(me.updateKind(migrationKindNone))
		}
	case *column:
		if me.kind != migrationKindDrop && dependency.kind == migrationKindDropAndAdd {
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on row deletion policy: %T", dep))
	}
}

// resolveUnnamedConstraints matches unnamed constraints in target with constraints in base that have
// the same definition, so that a constraint named by Spanner is not recreated.
func resolveUnnamedConstraints(base, target *definitions) {
	var baseConstraints []*constraint
	for _, def := range base.all {
		if c, ok := def.(*constraint); ok {
			baseConstraints = append(baseConstraints, c)
		}
	}
	slices.SortFunc(baseConstraints, func(a, b *constraint) int {
		return strings.Compare(a.cid.ID(), b.cid.ID())
	})

	var unnamed []*constraint
	for _, def := range target.all {
		if c, ok := def.(*constraint); ok && c.node.Name == nil {
			if _, ok := base.all[c.cid]; !ok {
				unnamed = append(unnamed, c)
			}
		}
	}
	slices.SortFunc(unnamed, func(a, b *constraint) int {
		return strings.Compare(a.cid.ID(), b.cid.ID())
	})

	matched := make(map[constraintID]bool)
	for _, tc := range unnamed {
		for _, bc := range baseConstraints {
			if matched[bc.cid] || bc.cid.tableID != tc.cid.tableID || !equalNode(bc.astNode(), tc.astNode()) {
				continue
			}
			if _, ok := target.all[bc.cid]; ok {
				continue
			}
			matched[bc.cid] = true
			delete(target.all, tc.cid)
			tc.cid = bc.cid
			target.all[tc.cid] = tc
			break
		}
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

func (i *index) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropIndex{
		Name: i.node.Name,
	})
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
		added, dropped := diffStoring(base.tableID(), base.node.Storing, target.node.Storing)
		var ops []operation
		for _, col := range dropped {
			// Stored columns must be dropped from the index before the columns are dropped.
			ops = append(ops, newDropPhaseOperation(base, &ast.AlterIndex{Name: target.node.Name, IndexAlteration: &ast.DropStoredColumn{Name: col}}))
		}
		for _, col := range added {
			ops = append(ops, newAddPhaseOperation(target, &ast.AlterIndex{Name: target.node.Name, IndexAlteration: &ast.AddStoredColumn{Name: col}}))
		}
		m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
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
	if i.node.InterleaveIn != nil {
		ids = append(ids, newTableIDFromIdent(i.node.InterleaveIn.TableName))
	}
	return ids
}

func (i *index) onDependencyChange(me, dependency migrationState, m *migration) {
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *column, *table, *schema:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on index: %T", dep))
	}
}

func diffStoring(tableID tableID, base, target *ast.Storing) (added, dropped []*ast.Ident) {
	columns := func(s *ast.Storing) []*ast.Ident {
		if s == nil {
			return nil
		}
		return s.Columns
	}
	toSet := func(cols []*ast.Ident) map[columnID]struct{} {
		set := make(map[columnID]struct{}, len(cols))
		for _, col := range cols {
			set[newColumnID(tableID, col)] = struct{}{}
		}
		return set
	}
	baseSet, targetSet := toSet(columns(base)), toSet(columns(target))
	for _, col := range columns(target) {
		if _, ok := baseSet[newColumnID(tableID, col)]; !ok {
			added = append(added, col)
		}
	}
	for _, col := range columns(base) {
		if _, ok := targetSet[newColumnID(tableID, col)]; !ok {
			dropped = append(dropped, col)
		}
	}
	return added, dropped
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
	return newTableIDFromPath(si.node.TableName)
}

func (si *searchIndex) astNode() ast.Node {
	return si.node
}

func (si *searchIndex) add() ast.DDL {
	return si.node
}

func (si *searchIndex) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropSearchIndex{
		Name: si.node.Name,
	})
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
		added, dropped := diffStoring(base.tableID(), base.node.Storing, target.node.Storing)
		var ops []operation
		for _, col := range dropped {
			// Stored columns must be dropped from the index before the columns are dropped.
			ops = append(ops, newDropPhaseOperation(base, &ast.AlterSearchIndex{Name: target.node.Name, IndexAlteration: &ast.DropStoredColumn{Name: col}}))
		}
		for _, col := range added {
			ops = append(ops, newAddPhaseOperation(target, &ast.AlterSearchIndex{Name: target.node.Name, IndexAlteration: &ast.AddStoredColumn{Name: col}}))
		}
		m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
		return
	}
	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (si *searchIndex) dependsOn() []identifier {
	var ids []identifier
	columns := slices.Clone(si.node.TokenListPart)
	if si.node.Storing != nil {
		columns = append(columns, si.node.Storing.Columns...)
	}
	columns = append(columns, si.node.PartitionColumns...)
	if si.node.OrderBy != nil {
		ast.Inspect(si.node.OrderBy, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok {
				columns = append(columns, ident)
			}
			return true
		})
	}
	for _, col := range columns {
		ids = append(ids, newColumnID(si.tableID(), col))
	}
	ids = append(ids, si.tableID())
	if si.node.Interleave != nil {
		ids = append(ids, newTableIDFromIdent(si.node.Interleave.TableName))
	}
	return ids
}

func (si *searchIndex) onDependencyChange(me, dependency migrationState, m *migration) {
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on search index: %T", dep))
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

func (vi *vectorIndex) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropVectorIndex{
		Name: vi.node.Name,
	})
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
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on vector index: %T", dep))
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

func (pg *propertyGraph) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropPropertyGraph{
		Name: pg.node.Name,
	})
}

func (pg *propertyGraph) alter(tgt definition, m *migration) {
	base := pg
	target := tgt.(*propertyGraph)

	if m.dropsAnyOf(base.dependsOn()) {
		// The property graph must stop referencing the dropped objects before they are dropped.
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (pg *propertyGraph) dependsOn() []identifier {
	var ids []identifier
	elements := pg.node.Content.NodeTables.Tables.Elements
	if pg.node.Content.EdgeTables != nil {
		elements = append(slices.Clone(elements), pg.node.Content.EdgeTables.Tables.Elements...)
	}
	for _, elem := range elements {
		tableID := newTableIDFromIdent(elem.Name)
		ids = append(ids, tableID)
		if elem.Keys == nil {
			continue
		}
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
			ids = append(ids, edgeKeyDependencies(tableID, keys.Source.Keys, keys.Source.ElementReference, keys.Source.ReferenceColumns)...)
			ids = append(ids, edgeKeyDependencies(tableID, keys.Destination.Keys, keys.Destination.ElementReference, keys.Destination.ReferenceColumns)...)
		default:
			panic(fmt.Sprintf("unexpected property graph type: %T", keys))
		}
	}
	return ids
}

func edgeKeyDependencies(tableID tableID, keys *ast.PropertyGraphColumnNameList, ref *ast.Ident, refColumns *ast.PropertyGraphColumnNameList) []identifier {
	var ids []identifier
	for _, key := range keys.ColumnNameList {
		ids = append(ids, newColumnID(tableID, key))
	}
	refTableID := newTableIDFromIdent(ref)
	ids = append(ids, refTableID)
	if refColumns != nil {
		for _, key := range refColumns.ColumnNameList {
			ids = append(ids, newColumnID(refTableID, key))
		}
	}
	return ids
}

func (pg *propertyGraph) onDependencyChange(me, dependency migrationState, m *migration) {
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *column, *table:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on property graph: %T", dep))
	}
}

type view struct {
	node *ast.CreateView
}

func newView(cv *ast.CreateView) *view {
	return &view{cv}
}

func (v *view) id() identifier {
	return newViewIDFromPath(v.node.Name)
}

func (v *view) astNode() ast.Node {
	return v.node
}

func (v *view) add() ast.DDL {
	return v.node
}

func (v *view) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropView{
		Name: v.node.Name,
	})
}

func (v *view) alter(tgt definition, m *migration) {
	base := v
	target := tgt.(*view)

	if m.dropsAnyOf(base.dependsOn()) {
		// The view must stop referencing the dropped objects before they are dropped.
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	targetCopy := *target.node
	targetCopy.OrReplace = true
	m.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (v *view) dependsOn() []identifier {
	var ids []identifier
	paths, idents := tablesOrViewsInQueryExpr(v.node.Query)
	// Can't distinguish between tables and views, so add both.
	for _, ident := range idents {
		ids = append(ids,
			newTableIDFromIdent(ident),
			newViewIDFromIdent(ident),
		)
	}
	for _, path := range paths {
		ids = append(ids,
			newTableIDFromPath(path),
			newViewIDFromPath(path),
		)
	}
	// TODO: Add dependencies on columns.
	// But it's difficult to extract column names from the query!
	return ids
}

func (v *view) onDependencyChange(me, dependency migrationState, m *migration) {
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *column, *table, *view:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on view: %T", dep))
	}
}

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

func (cs *changeStream) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropChangeStream{
		Name: cs.node.Name,
	})
}

func (cs *changeStream) alter(tgt definition, m *migration) {
	base := cs
	target := tgt.(*changeStream)

	var ops []operation
	if !equalNode(base.node.For, target.node.For) {
		ops = append(ops, base.alterFor(target, m)...)
	}
	if options := diffOptions(base.node.Options, target.node.Options); options != nil {
		ops = append(ops, newAddPhaseOperation(target, &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: options}}))
	}
	if len(ops) == 0 {
		return
	}
	m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
}

// alterFor returns operations to change the FOR clause.
// Tables and columns watched by a change stream can't be dropped, so they are removed from the
// change stream in the drop phase, and new tables and columns are added in the add phase.
func (cs *changeStream) alterFor(target *changeStream, m *migration) []operation {
	alter := func(a ast.ChangeStreamAlteration) *ast.AlterChangeStream {
		return &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: a}
	}

	targetTables, ok := target.node.For.(*ast.ChangeStreamForTables)
	if !ok {
		if target.node.For == nil {
			return []operation{newDropPhaseOperation(cs, alter(&ast.ChangeStreamDropForAll{}))}
		}
		return []operation{newAddPhaseOperation(target, alter(&ast.ChangeStreamSetFor{For: target.node.For}))}
	}
	if _, ok := cs.node.For.(*ast.ChangeStreamForTables); !ok {
		return []operation{newAddPhaseOperation(target, alter(&ast.ChangeStreamSetFor{For: target.node.For}))}
	}

	// existing keeps the tables and columns in the target that already exist in the base schema.
	existing := &ast.ChangeStreamForTables{}
	for _, t := range targetTables.Tables {
		tableID := newTableIDFromIdent(t.TableName)
		if _, ok := m.baseDefs.all[tableID]; !ok {
			continue
		}
		var columns []*ast.Ident
		for _, col := range t.Columns {
			if _, ok := m.baseDefs.all[newColumnID(tableID, col)]; ok {
				columns = append(columns, col)
			}
		}
		if len(t.Columns) > 0 && len(columns) == 0 {
			continue
		}
		table := *t
		table.Columns = columns
		existing.Tables = append(existing.Tables, &table)
	}

	var ops []operation
	if len(existing.Tables) == 0 {
		ops = append(ops, newDropPhaseOperation(cs, alter(&ast.ChangeStreamDropForAll{})))
	} else {
		ops = append(ops, newDropPhaseOperation(cs, alter(&ast.ChangeStreamSetFor{For: existing})))
	}
	if !equalNode(existing, targetTables) {
		ops = append(ops, newAddPhaseOperation(target, alter(&ast.ChangeStreamSetFor{For: target.node.For})))
	}
	return ops
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
	switch me.kind {
	case migrationKindDrop:
		return
	}
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
		panic(fmt.Sprintf("unexpected dependency type on change stream: %T", dep))
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

func (s *sequence) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropSequence{
		Name: s.node.Name,
	})
}

func (s *sequence) alter(tgt definition, m *migration) {
	base := s
	target := tgt.(*sequence)

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Options = nil
	targetCopy.Options = nil
	if equalNode(&baseCopy, &targetCopy) {
		m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterSequence{Name: target.node.Name, Options: diffOptions(base.node.Options, target.node.Options)}))
		return
	}

	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (s *sequence) dependsOn() []identifier {
	if schemaID, ok := s.schemaID().get(); ok {
		return []identifier{schemaID}
	}
	return nil
}

func (s *sequence) onDependencyChange(me, dependency migrationState, m *migration) {}

type model struct {
	node *ast.CreateModel
}

func newModel(cm *ast.CreateModel) *model {
	return &model{cm}
}

func (m *model) id() identifier {
	return newModelID(m.node.Name)
}

func (m *model) astNode() ast.Node {
	return m.node
}

func (m *model) add() ast.DDL {
	return m.node
}

func (m *model) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropModel{
		Name: m.node.Name,
	})
}

func (m *model) alter(tgt definition, migration *migration) {
	base := m
	target := tgt.(*model)

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Options = nil
	targetCopy.Options = nil
	if equalNode(&baseCopy, &targetCopy) {
		migration.updateStateIfUndefined(newAlterState(base, target, &ast.AlterModel{Name: target.node.Name, Options: diffOptions(base.node.Options, target.node.Options)}))
		return
	}

	targetCopy = *target.node
	targetCopy.OrReplace = true
	migration.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (m *model) dependsOn() []identifier {
	return nil
}

func (m *model) onDependencyChange(me, dependency migrationState, migration *migration) {}

type protoBundle struct {
	node *ast.CreateProtoBundle
}

func newProtoBundle(cp *ast.CreateProtoBundle) *protoBundle {
	return &protoBundle{cp}
}

func (pb *protoBundle) id() identifier {
	// Only one proto bundle can be defined in a schema.
	return newProtoBundleID()
}

func (pb *protoBundle) astNode() ast.Node {
	return pb.node
}

func (pb *protoBundle) add() ast.DDL {
	return pb.node
}

func (pb *protoBundle) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropProtoBundle{})
}

func (pb *protoBundle) alter(tgt definition, migration *migration) {
	base := pb
	target := tgt.(*protoBundle)

	baseNames := make(map[string]*ast.NamedType, len(base.node.Types.Types))
	for _, t := range base.node.Types.Types {
		baseNames[t.SQL()] = t
	}
	targetNames := make(map[string]*ast.NamedType, len(target.node.Types.Types))
	for _, t := range target.node.Types.Types {
		targetNames[t.SQL()] = t
	}
	added := make([]*ast.NamedType, 0, len(targetNames))
	dropped := make([]*ast.NamedType, 0, len(baseNames))
	for _, t := range target.node.Types.Types {
		if _, ok := baseNames[t.SQL()]; !ok {
			added = append(added, t)
		}
	}
	for _, t := range base.node.Types.Types {
		if _, ok := targetNames[t.SQL()]; !ok {
			dropped = append(dropped, t)
		}
	}
	ddl := &ast.AlterProtoBundle{}
	if len(added) > 0 {
		ddl.Insert = &ast.AlterProtoBundleInsert{Types: &ast.ProtoBundleTypes{Types: added}}
	}
	if len(dropped) > 0 {
		ddl.Delete = &ast.AlterProtoBundleDelete{Types: &ast.ProtoBundleTypes{Types: dropped}}
	}
	migration.updateStateIfUndefined(newAlterState(base, target, ddl))
}

func (pb *protoBundle) dependsOn() []identifier {
	return nil
}

func (pb *protoBundle) onDependencyChange(me, dependency migrationState, migration *migration) {}

type role struct {
	node *ast.CreateRole
}

func newRole(cr *ast.CreateRole) *role {
	return &role{cr}
}

func (r *role) id() identifier {
	return newRoleID(r.node.Name)
}

func (r *role) astNode() ast.Node {
	return r.node
}

func (r *role) add() ast.DDL {
	return r.node
}

func (r *role) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropRole{
		Name: r.node.Name,
	})
}

func (r *role) alter(tgt definition, m *migration) {
	base := r
	target := tgt.(*role)

	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (r *role) dependsOn() []identifier {
	return nil
}

func (r *role) onDependencyChange(me, dependency migrationState, m *migration) {}

type grant struct {
	node    *ast.Grant
	grantID grantID
}

func newGrant(g *ast.Grant) []definition {
	var grants []definition
	switch t := g.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		for _, r := range g.Roles {
			for _, tableName := range t.Names {
				grants = append(
					grants,
					&grant{
						&ast.Grant{
							Roles: []*ast.Ident{r},
							Privilege: &ast.PrivilegeOnTable{
								Privileges: t.Privileges,
								Names:      []*ast.Path{tableName},
							},
						},
						newGrantID(newRoleID(r), newTableIDFromPath(tableName)),
					},
				)
			}
		}
	case *ast.SelectPrivilegeOnView:
		for _, r := range g.Roles {
			for _, viewName := range t.Names {
				grants = append(grants, &grant{
					&ast.Grant{
						Roles: []*ast.Ident{r},
						Privilege: &ast.SelectPrivilegeOnView{
							Names: []*ast.Path{viewName},
						},
					},
					newGrantID(newRoleID(r), newViewIDFromPath(viewName)),
				})
			}
		}
	case *ast.SelectPrivilegeOnChangeStream:
		for _, r := range g.Roles {
			for _, csName := range t.Names {
				grants = append(grants, &grant{
					&ast.Grant{
						Roles: []*ast.Ident{r},
						Privilege: &ast.SelectPrivilegeOnChangeStream{
							Names: []*ast.Path{csName},
						},
					},
					newGrantID(newRoleID(r), newChangeStreamIDFromPath(csName)),
				})
			}
		}
	case *ast.ExecutePrivilegeOnTableFunction:
		for _, r := range g.Roles {
			for _, csrfName := range t.Names {
				grants = append(grants, &grant{
					&ast.Grant{
						Roles: []*ast.Ident{r},
						Privilege: &ast.ExecutePrivilegeOnTableFunction{
							Names: []*ast.Path{csrfName},
						},
					},
					newGrantID(newRoleID(r), newChangeStreamReadFunctionIDFromPath(csrfName)),
				})
			}
		}
	case *ast.RolePrivilege:
		for _, r := range g.Roles {
			for _, roleName := range t.Names {
				grants = append(grants, &grant{
					&ast.Grant{
						Roles: []*ast.Ident{r},
						Privilege: &ast.RolePrivilege{
							Names: []*ast.Ident{roleName},
						},
					},
					newGrantID(newRoleID(r), newRoleID(roleName)),
				})
			}
		}
	default:
		panic(fmt.Sprintf("unexpected grant type: %T: %s", t, g.SQL()))
	}
	return grants
}

func (g *grant) merge(other definition) bool {
	oth := other.(*grant)
	switch p1 := g.node.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		p2 := oth.node.Privilege.(*ast.PrivilegeOnTable)
		var hasSelect, hasUpdate, hasInsert, hasDelete bool
		var selectWithColumn, updateWithColumn, insertWithColumn []*ast.Ident
		for _, p := range append(p1.Privileges, p2.Privileges...) {
			switch t := p.(type) {
			case *ast.SelectPrivilege:
				if len(t.Columns) == 0 {
					hasSelect = true
				} else {
					selectWithColumn = append(selectWithColumn, t.Columns...)
				}
			case *ast.UpdatePrivilege:
				if len(t.Columns) == 0 {
					hasUpdate = true
				} else {
					updateWithColumn = append(updateWithColumn, t.Columns...)
				}
			case *ast.InsertPrivilege:
				if len(t.Columns) == 0 {
					hasInsert = true
				} else {
					insertWithColumn = append(insertWithColumn, t.Columns...)
				}
			case *ast.DeletePrivilege:
				hasDelete = true
			}
		}
		selectWithColumn = uniqueIdent(selectWithColumn)
		updateWithColumn = uniqueIdent(updateWithColumn)
		insertWithColumn = uniqueIdent(insertWithColumn)
		var privileges []ast.TablePrivilege
		if hasSelect {
			privileges = append(privileges, &ast.SelectPrivilege{})
		}
		if len(selectWithColumn) > 0 {
			privileges = append(privileges, &ast.SelectPrivilege{Columns: selectWithColumn})
		}
		if hasUpdate {
			privileges = append(privileges, &ast.UpdatePrivilege{})
		}
		if len(updateWithColumn) > 0 {
			privileges = append(privileges, &ast.UpdatePrivilege{Columns: updateWithColumn})
		}
		if hasInsert {
			privileges = append(privileges, &ast.InsertPrivilege{})
		}
		if len(insertWithColumn) > 0 {
			privileges = append(privileges, &ast.InsertPrivilege{Columns: insertWithColumn})
		}
		if hasDelete {
			privileges = append(privileges, &ast.DeletePrivilege{})
		}
		p1.Privileges = privileges
		return true
	case *ast.SelectPrivilegeOnView, *ast.SelectPrivilegeOnChangeStream, *ast.ExecutePrivilegeOnTableFunction, *ast.RolePrivilege:
		// no additional parameters exist
		return true
	default:
		panic(fmt.Sprintf("unexpected grant type: %T: %s", g.node.Privilege, g.node.SQL()))
	}
}

func (g *grant) id() identifier {
	return g.grantID
}

func (g *grant) astNode() ast.Node {
	return g.node
}

func (g *grant) add() ast.DDL {
	return g.node
}

func (g *grant) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.Revoke{
		Roles:     g.node.Roles,
		Privilege: g.node.Privilege,
	})
}

func (g *grant) alter(tgt definition, m *migration) {
	base := g
	target := tgt.(*grant)

	switch baseP := base.node.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		targetP := target.node.Privilege.(*ast.PrivilegeOnTable)
		processPrivilegeDetails := func(privileges []ast.TablePrivilege) (hasSelect, hasUpdate, hasInsert, hasDelete bool, selectColumnIDs, updateColumnIDs, insertColumnIDs []columnID, selectWithColumn, updateWithColumn, insertWithColumn map[columnID]*ast.Ident) {
			selectWithColumn = make(map[columnID]*ast.Ident)
			updateWithColumn = make(map[columnID]*ast.Ident)
			insertWithColumn = make(map[columnID]*ast.Ident)

			for _, p := range privileges {
				switch t := p.(type) {
				case *ast.SelectPrivilege:
					if len(t.Columns) == 0 {
						hasSelect = true
					} else {
						for _, col := range t.Columns {
							colID := newColumnID(newTableIDFromPath(baseP.Names[0]), col)
							if _, ok := selectWithColumn[colID]; !ok {
								selectWithColumn[colID] = col
								selectColumnIDs = append(selectColumnIDs, colID)
							}
						}
					}
				case *ast.UpdatePrivilege:
					if len(t.Columns) == 0 {
						hasUpdate = true
					} else {
						for _, col := range t.Columns {
							colID := newColumnID(newTableIDFromPath(baseP.Names[0]), col)
							if _, ok := updateWithColumn[colID]; !ok {
								updateWithColumn[colID] = col
								updateColumnIDs = append(updateColumnIDs, colID)
							}
						}
					}
				case *ast.InsertPrivilege:
					if len(t.Columns) == 0 {
						hasInsert = true
					} else {
						for _, col := range t.Columns {
							colID := newColumnID(newTableIDFromPath(baseP.Names[0]), col)
							if _, ok := insertWithColumn[colID]; !ok {
								insertWithColumn[colID] = col
								insertColumnIDs = append(insertColumnIDs, colID)
							}
						}
					}
				case *ast.DeletePrivilege:
					hasDelete = true
				}
			}

			return hasSelect, hasUpdate, hasInsert, hasDelete, selectColumnIDs, updateColumnIDs, insertColumnIDs, selectWithColumn, updateWithColumn, insertWithColumn
		}

		baseHasSelect, baseHasUpdate, baseHasInsert, baseHasDelete, baseSelectColumIDs, baseUpdateColumnIDs, baseInsertColumnIDs, baseSelectWithColumn, baseUpdateWithColumn, baseInsertWithColumn := processPrivilegeDetails(baseP.Privileges)
		targetHasSelect, targetHasUpdate, targetHasInsert, targetHasDelete, targetSelectColumIDs, targetUpdateColumnIDs, targetInsertColumnIDs, targetSelectWithColumn, targetUpdateWithColumn, targetInsertWithColumn := processPrivilegeDetails(targetP.Privileges)

		var added, dropped []ast.TablePrivilege
		if baseHasSelect != targetHasSelect {
			if targetHasSelect {
				added = append(added, &ast.SelectPrivilege{})
			} else {
				dropped = append(dropped, &ast.SelectPrivilege{})
			}
		}
		if !slices.Equal(baseSelectColumIDs, targetSelectColumIDs) {
			var addedColumns, droppedColumns []*ast.Ident
			for _, colID := range targetSelectColumIDs {
				if _, ok := baseSelectWithColumn[colID]; !ok {
					addedColumns = append(addedColumns, targetSelectWithColumn[colID])
				}
			}
			for _, colID := range baseSelectColumIDs {
				if _, ok := targetSelectWithColumn[colID]; !ok {
					droppedColumns = append(droppedColumns, baseSelectWithColumn[colID])
				}
			}
			if len(addedColumns) > 0 {
				added = append(added, &ast.SelectPrivilege{Columns: addedColumns})
			}
			if len(droppedColumns) > 0 {
				dropped = append(dropped, &ast.SelectPrivilege{Columns: droppedColumns})
			}
		}
		if baseHasUpdate != targetHasUpdate {
			if targetHasUpdate {
				added = append(added, &ast.UpdatePrivilege{})
			} else {
				dropped = append(dropped, &ast.UpdatePrivilege{})
			}
		}
		if !slices.Equal(baseUpdateColumnIDs, targetUpdateColumnIDs) {
			var addedColumns, droppedColumns []*ast.Ident
			for _, colID := range targetUpdateColumnIDs {
				if _, ok := baseUpdateWithColumn[colID]; !ok {
					addedColumns = append(addedColumns, targetUpdateWithColumn[colID])
				}
			}
			for _, colID := range baseUpdateColumnIDs {
				if _, ok := targetUpdateWithColumn[colID]; !ok {
					droppedColumns = append(droppedColumns, baseUpdateWithColumn[colID])
				}
			}
			if len(addedColumns) > 0 {
				added = append(added, &ast.UpdatePrivilege{Columns: addedColumns})
			}
			if len(droppedColumns) > 0 {
				dropped = append(dropped, &ast.UpdatePrivilege{Columns: droppedColumns})
			}
		}
		if baseHasInsert != targetHasInsert {
			if targetHasInsert {
				added = append(added, &ast.InsertPrivilege{})
			} else {
				dropped = append(dropped, &ast.InsertPrivilege{})
			}
		}
		if !slices.Equal(baseInsertColumnIDs, targetInsertColumnIDs) {
			var addedColumns, droppedColumns []*ast.Ident
			for _, colID := range targetInsertColumnIDs {
				if _, ok := baseInsertWithColumn[colID]; !ok {
					addedColumns = append(addedColumns, targetInsertWithColumn[colID])
				}
			}
			for _, colID := range baseInsertColumnIDs {
				if _, ok := targetInsertWithColumn[colID]; !ok {
					droppedColumns = append(droppedColumns, baseInsertWithColumn[colID])
				}
			}
			if len(addedColumns) > 0 {
				added = append(added, &ast.InsertPrivilege{Columns: addedColumns})
			}
			if len(droppedColumns) > 0 {
				dropped = append(dropped, &ast.InsertPrivilege{Columns: droppedColumns})
			}
		}
		if baseHasDelete != targetHasDelete {
			if targetHasDelete {
				added = append(added, &ast.DeletePrivilege{})
			} else {
				dropped = append(dropped, &ast.DeletePrivilege{})
			}
		}
		var ops []operation
		if len(dropped) > 0 {
			// Privileges must be revoked before the columns are dropped.
			ops = append(ops, newDropPhaseOperation(base, &ast.Revoke{
				Roles:     target.node.Roles,
				Privilege: &ast.PrivilegeOnTable{Privileges: dropped, Names: targetP.Names},
			}))
		}
		if len(added) > 0 {
			ops = append(ops, newAddPhaseOperation(target, &ast.Grant{
				Roles:     target.node.Roles,
				Privilege: &ast.PrivilegeOnTable{Privileges: added, Names: targetP.Names},
			}))
		}

		m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
	case *ast.SelectPrivilegeOnView, *ast.SelectPrivilegeOnChangeStream, *ast.ExecutePrivilegeOnTableFunction, *ast.RolePrivilege:
		// never come here, because grant type handles only single target name (1 view, change stream, table function or role per grant type)
		panic(fmt.Sprintf("unsupported GRANT alteration on: %s", target.node.SQL()))
	}
}

func (g *grant) dependsOn() []identifier {
	var ids []identifier
	for _, role := range g.node.Roles {
		ids = append(ids, newRoleID(role))
	}
	switch p := g.node.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		for _, tableName := range p.Names {
			ids = append(ids, newTableIDFromPath(tableName))
		}
		for _, tp := range p.Privileges {
			switch t := tp.(type) {
			case *ast.SelectPrivilege:
				for _, col := range t.Columns {
					ids = append(ids, newColumnID(newTableIDFromPath(p.Names[0]), col))
				}
			case *ast.UpdatePrivilege:
				for _, col := range t.Columns {
					ids = append(ids, newColumnID(newTableIDFromPath(p.Names[0]), col))
				}
			case *ast.InsertPrivilege:
				for _, col := range t.Columns {
					ids = append(ids, newColumnID(newTableIDFromPath(p.Names[0]), col))
				}
			case *ast.DeletePrivilege:
				// none
			default:
				panic(fmt.Sprintf("unexpected privilege type: %T", tp))
			}
		}
	case *ast.SelectPrivilegeOnView:
		for _, viewName := range p.Names {
			ids = append(ids, newViewIDFromPath(viewName))
		}
	case *ast.SelectPrivilegeOnChangeStream:
		for _, csName := range p.Names {
			ids = append(ids, newChangeStreamIDFromPath(csName))
		}
	case *ast.ExecutePrivilegeOnTableFunction:
		// none
	case *ast.RolePrivilege:
		for _, roleName := range p.Names {
			ids = append(ids, newRoleID(roleName))
		}
	}
	return ids
}

func (g *grant) onDependencyChange(me, dependency migrationState, m *migration) {
	switch me.kind {
	case migrationKindDrop:
		return
	}
	switch dep := dependency.definition().(type) {
	case *role, *table, *column, *view, *changeStream:
		switch dependency.kind {
		case migrationKindDropAndAdd:
			m.updateState(me.updateKind(migrationKindDropAndAdd))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on grant: %T", dep))
	}
}

type database struct {
	node *ast.AlterDatabase
}

func newDatabase(ad *ast.AlterDatabase) *database {
	return &database{ad}
}

func (d *database) id() identifier {
	return newDatabaseID()
}

func (d *database) astNode() ast.Node {
	return d.node
}

func (d *database) add() ast.DDL {
	return d.node
}

func (d *database) drop() optional[ast.DDL] {
	return none[ast.DDL]()
}

func (d *database) alter(tgt definition, m *migration) {
	base := d
	target := tgt.(*database)

	options := diffOptions(base.node.Options, target.node.Options)
	if options == nil {
		// Only the database name is different.
		return
	}
	m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterDatabase{Name: target.node.Name, Options: options}))
}

func (d *database) dependsOn() []identifier {
	return nil
}

func (d *database) onDependencyChange(me, dependency migrationState, m *migration) {}
