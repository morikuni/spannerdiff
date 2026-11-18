package spannerdiff

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
)

type column struct {
	node        *ast.ColumnDef
	table       *table
	zeroDefault bool
}

func newColumn(table *table, col *ast.ColumnDef, zeroDefault bool) *column {
	return &column{col, table, zeroDefault}
}

func (c *column) id() identifier {
	return newColumnID(c.table.tableID(), c.node.Name)
}

// astNode returns the column without PRIMARY KEY because primary keys are compared by the table.
func (c *column) astNode() ast.Node {
	node := *c.node
	node.PrimaryKey = false
	return &node
}

// add adds a NOT NULL column without a default value with its zero value as a temporary default value when zeroDefault is set.
// Spanner can't add such a column to an existing table even if the table is empty.
func (c *column) add() []ast.DDL {
	if c.zeroDefault && c.node.NotNull && c.node.DefaultSemantics == nil {
		if zero, ok := zeroValueOf(c.node.Type).get(); ok {
			node := *c.node
			node.DefaultSemantics = &ast.ColumnDefaultExpr{Expr: zero}
			// A commit timestamp column only allows PENDING_COMMIT_TIMESTAMP() as its default value,
			// so the options are set after the default value is dropped.
			node.Options = nil
			ddls := []ast.DDL{
				&ast.AlterTable{Name: c.table.node.Name, TableAlteration: &ast.AddColumn{Column: &node}},
				&ast.AlterTable{Name: c.table.node.Name, TableAlteration: &ast.AlterColumn{Name: c.node.Name, Alteration: &ast.AlterColumnDropDefault{}}},
			}
			if c.node.Options != nil {
				ddls = append(ddls, &ast.AlterTable{Name: c.table.node.Name, TableAlteration: &ast.AlterColumn{Name: c.node.Name, Alteration: &ast.AlterColumnSetOptions{Options: c.node.Options}}})
			}
			return ddls
		}
	}
	return []ast.DDL{
		&ast.AlterTable{
			Name: c.table.node.Name,
			TableAlteration: &ast.AddColumn{
				Column: c.node,
			},
		},
	}
}

// zeroValueOf returns the zero value of the column type, or none if it is unknown.
func zeroValueOf(t ast.SchemaType) optional[ast.Expr] {
	switch t := columnTypeOf(t).(type) {
	case array:
		return some[ast.Expr](&ast.ArrayLiteral{})
	case scalar:
		switch t.t {
		case ast.BoolTypeName:
			return some[ast.Expr](&ast.BoolLiteral{Value: false})
		case ast.Int64TypeName:
			return some[ast.Expr](&ast.IntLiteral{Base: 10, Value: "0"})
		case ast.Float32TypeName, ast.Float64TypeName:
			return some[ast.Expr](&ast.FloatLiteral{Value: "0"})
		case ast.StringTypeName:
			return some[ast.Expr](&ast.StringLiteral{Value: ""})
		case ast.BytesTypeName:
			return some[ast.Expr](&ast.BytesLiteral{Value: []byte{}})
		case ast.DateTypeName:
			return some[ast.Expr](&ast.DateLiteral{Value: &ast.StringLiteral{Value: "0001-01-01"}})
		case ast.TimestampTypeName:
			return some[ast.Expr](&ast.TimestampLiteral{Value: &ast.StringLiteral{Value: "0001-01-01T00:00:00Z"}})
		case ast.NumericTypeName:
			return some[ast.Expr](&ast.NumericLiteral{Value: &ast.StringLiteral{Value: "0"}})
		case ast.JSONTypeName:
			return some[ast.Expr](&ast.JSONLiteral{Value: &ast.StringLiteral{Value: "{}"}})
		case uuidTypeName:
			return some[ast.Expr](&ast.CastExpr{
				Expr: &ast.StringLiteral{Value: "00000000-0000-0000-0000-000000000000"},
				Type: &ast.NamedType{Path: []*ast.Ident{{Name: string(uuidTypeName)}}},
			})
		}
	}
	return none[ast.Expr]()
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
	// --- not documented ---
	// - Change the element type of an ARRAY column between STRING and BYTES, or between PROTO and BYTES, as the emulator allows.

	if base.node.Hidden.Invalid() != target.node.Hidden.Invalid() {
		// Spanner can't change HIDDEN of an existing column. A generated column is recreated because its values are
		// computed from other columns, but recreating other columns loses their data, so it is reported as an error.
		if isGeneratedColumn(base.node) || isGeneratedColumn(target.node) {
			base.recreate(target, m)
			return
		}
		m.addError(target.id(), fmt.Errorf("changing HIDDEN of a column that isn't generated is not supported because the column would be recreated and lose its data: %s", target.id()))
		return
	}

	// Spanner rejects altering a key column that a parent table and its interleaved tables share,
	// except for allow_commit_timestamp.
	if base.isPrimaryKey() && !onlyCommitTimestampChanged(base.node, target.node) {
		if base.isInheritedKey(m) {
			// The table is recreated with the new key column after the key column of the parent table is altered.
			base.recreate(target, m)
			return
		}
		// The interleaved tables are recreated, so the key column is altered while they don't exist.
		base.recreateInterleavedTables(m)
	}

	if !equalNode(base.node.Type, target.node.Type) || base.node.NotNull != target.node.NotNull || !equalNode(base.node.DefaultSemantics, target.node.DefaultSemantics) {
		switch {
		case isGeneratedColumn(base.node) || isGeneratedColumn(target.node):
			if base.canAlterGeneratedExpr(target, m) {
				m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:          target.node.Type,
					Null:          token.InvalidPos,
					GeneratedExpr: target.node.DefaultSemantics.(*ast.GeneratedColumnExpr),
				}}}))
				return
			}
			// Other changes of generated columns can't be altered, but their values can be recomputed from other columns.
			base.recreate(target, m)
			return
		case isIdentityColumn(base.node) || isIdentityColumn(target.node):
			base.alterIdentity(target, m)
			return
		}
	}

	if equalNode(base.node.Type, target.node.Type) {
		var notNullDDLs, optionsDDLs []ast.DDL
		var defaultSet bool
		if base.node.NotNull != target.node.NotNull {
			// We don't set default values for columns that will be NOT NULL from NULL-able.
			if target.node.DefaultSemantics == nil {
				notNullDDLs = append(notNullDDLs, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:    target.node.Type,
					NotNull: target.node.NotNull,
				}}})
			} else if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
				defaultSet = true
				notNullDDLs = append(notNullDDLs, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnType{
					Type:        target.node.Type,
					NotNull:     target.node.NotNull,
					DefaultExpr: defaultExpr,
				}}})
			}
		}

		if options := diffOptions(base.node.Options, target.node.Options); options != nil {
			optionsDDLs = append(optionsDDLs, &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnSetOptions{Options: options}}})
		}

		dropDefault := &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnDropDefault{}}}
		dropOnUpdate := &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnDropOnUpdate{}}}
		var setDefault, setOnUpdate ast.DDL
		if defaultExpr, ok := target.node.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
			// SET DEFAULT can't have ON UPDATE, so ON UPDATE is set by SET ON UPDATE.
			setDefault = &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnSetDefault{DefaultExpr: withoutOnUpdate(defaultExpr)}}}
			if defaultExpr.OnUpdate != nil {
				setOnUpdate = &ast.AlterTable{Name: target.table.node.Name, TableAlteration: &ast.AlterColumn{Name: target.node.Name, Alteration: &ast.AlterColumnSetOnUpdate{OnUpdate: defaultExpr.OnUpdate}}}
			}
		}
		defaultChanged := !defaultSet && !equalNode(defaultValueOf(base.node), defaultValueOf(target.node))

		// Spanner rejects dropping or changing the default value of a column with ON UPDATE,
		// and ON UPDATE requires the identical default value.
		// So ON UPDATE of the base is dropped before any other change of the default value or ON UPDATE,
		// and ON UPDATE of the target is set after the default value, unless the NOT NULL alteration sets both.
		baseHasOnUpdate := hasOnUpdate(base.node)
		dropsOnUpdate := baseHasOnUpdate && !equalNode(base.node.DefaultSemantics, target.node.DefaultSemantics)
		setsOnUpdate := setOnUpdate != nil && !defaultSet && (!baseHasOnUpdate || dropsOnUpdate)

		var ddls []ast.DDL
		if dropsOnUpdate {
			ddls = append(ddls, dropOnUpdate)
		}
		if allowsCommitTimestamp(base.node) == allowsCommitTimestamp(target.node) {
			ddls = append(ddls, notNullDDLs...)
			ddls = append(ddls, optionsDDLs...)
			if defaultChanged {
				if target.node.DefaultSemantics == nil {
					ddls = append(ddls, dropDefault)
				} else if setDefault != nil {
					ddls = append(ddls, setDefault)
				}
			}
		} else {
			// Spanner rejects a column that allows commit timestamps with a default value other than PENDING_COMMIT_TIMESTAMP(),
			// and PENDING_COMMIT_TIMESTAMP() as the default value of a column that doesn't allow commit timestamps.
			// So the default value of the base is dropped before allow_commit_timestamp changes,
			// and the default value of the target is set after it, including by the NOT NULL alteration.
			if base.node.DefaultSemantics != nil && !equalNode(defaultValueOf(base.node), defaultValueOf(target.node)) {
				ddls = append(ddls, dropDefault)
			}
			ddls = append(ddls, optionsDDLs...)
			ddls = append(ddls, notNullDDLs...)
			if defaultChanged && setDefault != nil {
				ddls = append(ddls, setDefault)
			}
		}
		if setsOnUpdate {
			ddls = append(ddls, setOnUpdate)
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
			tupleOf(array{scalar{ast.StringTypeName}}, array{scalar{ast.BytesTypeName}}),
			tupleOf(array{scalar{ast.BytesTypeName}}, array{scalar{ast.StringTypeName}}),
			tupleOf(array{protoOrEnum{}}, array{scalar{ast.BytesTypeName}}),
			tupleOf(array{scalar{ast.BytesTypeName}}, array{protoOrEnum{}}),
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
	for _, key := range c.table.primaryKeys() {
		if equalNode(key.Name, c.node.Name) {
			return true
		}
	}
	return false
}

// isInheritedKey reports whether the column is a key column of the parent table in the base schema,
// and the parent table remains in the target schema.
func (c *column) isInheritedKey(m *migration) bool {
	if c.table.node.Cluster == nil {
		return false
	}
	parentID := newTableIDFromPath(c.table.node.Cluster.TableName)
	parent, ok := m.baseDefs.all[parentID].(*table)
	if !ok {
		return false
	}
	if _, ok := m.targetDefs.all[parentID]; !ok {
		return false
	}
	return slices.ContainsFunc(parent.primaryKeys(), func(key *ast.IndexKey) bool {
		return equalNode(key.Name, c.node.Name)
	})
}

// recreateInterleavedTables recreates the tables interleaved in the table of the key column,
// so they are dropped before the key column is altered.
// The tables dropped in the target schema are dropped anyway.
func (c *column) recreateInterleavedTables(m *migration) {
	for _, id := range slices.SortedFunc(maps.Keys(m.baseDefs.all), func(a, b identifier) int { return strings.Compare(a.ID(), b.ID()) }) {
		child, ok := m.baseDefs.all[id].(*table)
		if !ok || child.node.Cluster == nil || newTableIDFromPath(child.node.Cluster.TableName) != c.table.tableID() {
			continue
		}
		if _, ok := m.targetDefs.all[id]; !ok {
			continue
		}
		switch state := m.states[id]; state.kind {
		case migrationKindUndefined, migrationKindAlter:
			m.updateState(state.updateKind(migrationKindDropAndAdd))
		}
	}
}

// onlyCommitTimestampChanged reports whether only allow_commit_timestamp differs between the column definitions.
func onlyCommitTimestampChanged(base, target *ast.ColumnDef) bool {
	b, t := *base, *target
	b.PrimaryKey, t.PrimaryKey = false, false
	b.Options, t.Options = nil, nil
	return equalNode(&b, &t) && onlyOptions(diffOptions(base.Options, target.Options), "allow_commit_timestamp")
}

// alterIdentity migrates a column whose base or target is an identity column.
// Only the skip range can be altered by ALTER IDENTITY, and other changes are reported as errors
// because recreating the column loses the data.
func (c *column) alterIdentity(target *column, m *migration) {
	switch {
	case !isIdentityColumn(c.node) || !isIdentityColumn(target.node):
		m.addError(target.id(), fmt.Errorf("converting a column to or from an identity column is not supported: %s", target.id()))
		return
	case !equalNode(c.node.Type, target.node.Type) || c.node.NotNull != target.node.NotNull:
		m.addError(target.id(), fmt.Errorf("changing the type or NOT NULL of an identity column is not supported: %s", target.id()))
		return
	}

	base, tgt := identityParamsOf(c.node), identityParamsOf(target.node)
	if !equalNode(base.startCounterWith, tgt.startCounterWith) {
		m.addError(target.id(), fmt.Errorf("changing START COUNTER WITH of an identity column is not supported: %s", target.id()))
		return
	}
	if equalNode(base.skipRange, tgt.skipRange) {
		// The sequence kind is ignored because an omitted kind means default_sequence_kind of the database,
		// and the kind can't be changed after creation.
		return
	}

	var alteration ast.IdentityAlteration = &ast.SetSkipRange{SkipRange: tgt.skipRange}
	if tgt.skipRange == nil {
		alteration = &ast.SetNoSkipRange{NoSkipRange: &ast.NoSkipRange{}}
	}
	m.updateStateIfUndefined(newAlterState(c, target, &ast.AlterTable{
		Name: target.table.node.Name,
		TableAlteration: &ast.AlterColumn{
			Name:       target.node.Name,
			Alteration: &ast.AlterColumnAlterIdentity{Alteration: alteration},
		},
	}))
}

type identityParams struct {
	skipRange        *ast.SkipRange
	startCounterWith *ast.StartCounterWith
}

// identityParamsOf returns the parameters of an identity column. AUTO_INCREMENT is an identity column without parameters.
func identityParamsOf(col *ast.ColumnDef) identityParams {
	var params identityParams
	ic, ok := col.DefaultSemantics.(*ast.IdentityColumn)
	if !ok {
		return params
	}
	for _, p := range ic.Params {
		switch p := p.(type) {
		case *ast.SkipRange:
			params.skipRange = p
		case *ast.StartCounterWith:
			params.startCounterWith = p
		}
	}
	return params
}

// canAlterGeneratedExpr reports whether only the expression of a generated column changes and it can be altered.
// The expression of a stored or indexed generated column can't be modified.
func (c *column) canAlterGeneratedExpr(target *column, m *migration) bool {
	if !isNonStoredGeneratedColumn(c.node) || !isNonStoredGeneratedColumn(target.node) {
		return false
	}
	if !equalNode(c.node.Type, target.node.Type) || c.node.NotNull != target.node.NotNull {
		return false
	}
	// ALTER COLUMN is executed after drops, so the column is recreated if a column that the base expression references
	// is dropped or changes, because the column may be dropped and re-added before the expression is altered.
	for _, id := range columnsInGeneratedColumn(c) {
		base, inBase := m.baseDefs.all[id]
		target, inTarget := m.targetDefs.all[id]
		if inBase && (!inTarget || !equalNode(base.astNode(), target.astNode())) {
			return false
		}
	}
	for _, def := range m.baseDefs.all {
		switch def.(type) {
		case *index, *searchIndex, *vectorIndex:
			if slices.Contains(dependencyIDs(def.dependsOn()), c.id()) {
				return false
			}
		}
	}
	return true
}

func isNonStoredGeneratedColumn(col *ast.ColumnDef) bool {
	expr, ok := col.DefaultSemantics.(*ast.GeneratedColumnExpr)
	return ok && expr.Stored.Invalid()
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

// hasOnUpdate reports whether the column has ON UPDATE.
func hasOnUpdate(col *ast.ColumnDef) bool {
	defaultExpr, ok := col.DefaultSemantics.(*ast.ColumnDefaultExpr)
	return ok && defaultExpr.OnUpdate != nil
}

// defaultValueOf returns the default semantics of the column without ON UPDATE,
// so the default value is compared separately from ON UPDATE.
func defaultValueOf(col *ast.ColumnDef) ast.ColumnDefaultSemantics {
	if defaultExpr, ok := col.DefaultSemantics.(*ast.ColumnDefaultExpr); ok {
		return withoutOnUpdate(defaultExpr)
	}
	return col.DefaultSemantics
}

// withoutOnUpdate returns a copy of the default value expression without ON UPDATE.
func withoutOnUpdate(defaultExpr *ast.ColumnDefaultExpr) *ast.ColumnDefaultExpr {
	expr := *defaultExpr
	expr.OnUpdate = nil
	return &expr
}

// allowsCommitTimestamp reports whether the column has allow_commit_timestamp = true in its options.
func allowsCommitTimestamp(col *ast.ColumnDef) bool {
	if col.Options == nil {
		return false
	}
	for _, r := range col.Options.Records {
		if nameOf(r.Name) == "allow_commit_timestamp" {
			b, ok := r.Value.(*ast.BoolLiteral)
			return ok && b.Value
		}
	}
	return false
}

func (c *column) dependsOn() []dependency {
	deps := []dependency{{c.table.id(), dependencyKindOwner}}
	// The sequences must exist before the column is added.
	deps = append(deps, newDependencies(dependencyKindOrder, sequencesInColumn(c.node))...)
	return append(deps, newDependencies(dependencyKindCustom, columnsInGeneratedColumn(c))...)
}

// columnsInGeneratedColumn returns the columns of the same table that the expression of a generated column references.
// Identifiers in the expression may not be columns, but it's harmless to depend on non-existent columns.
func columnsInGeneratedColumn(c *column) []identifier {
	expr, ok := c.node.DefaultSemantics.(*ast.GeneratedColumnExpr)
	if !ok {
		return nil
	}
	var ids []identifier
	ast.Inspect(expr.Expr, func(n ast.Node) bool {
		// A column must not depend on itself, because a change of a dependency is propagated to its dependents.
		if ident, ok := n.(*ast.Ident); ok && nameOf(ident) != nameOf(c.node.Name) {
			ids = append(ids, newColumnID(c.table.tableID(), ident))
		}
		return true
	})
	return ids
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

// onDependencyChange handles a change of a column that the expression of the generated column references.
// Spanner can't drop a column or change its type while a generated column references it,
// so the generated column is dropped before the change and added again after it.
func (c *column) onDependencyChange(me, dependency migrationState, m *migration) {
	if !me.base.valid || !me.target.valid || me.kind == migrationKindNone || me.kind == migrationKindDropAndAdd {
		return
	}
	switch dependency.kind {
	case migrationKindDropAndAdd:
	case migrationKindAlter:
		base, target := dependency.base.mustGet().(*column), dependency.definition().(*column)
		if equalTypesIgnoringLength(base.node.Type, target.node.Type) {
			return
		}
	default:
		return
	}
	if c.isPrimaryKey() {
		c.recreate(me.target.mustGet().(*column), m)
		return
	}
	m.updateState(me.updateKind(migrationKindDropAndAdd))
}
