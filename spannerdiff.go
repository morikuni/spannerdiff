package spannerdiff

import (
	"fmt"
	"io"

	"github.com/cloudspannerecosystem/memefish"
	"github.com/cloudspannerecosystem/memefish/ast"
)

type DiffOption struct {
	ErrorOnUnsupportedDDL bool
	Printer               Printer
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

	printer := option.Printer
	if printer == nil {
		printer = NoStylePrinter{}
	}
	ctx := PrintContext{TotalSQLs: len(stmts)}
	for i, stmt := range stmts {
		ctx.Index = i
		if err := printer.Print(ctx, output, stmt.SQL()+";\n"); err != nil {
			return fmt.Errorf("failed to write migration DDL: %w", err)
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
	id     identifier
	base   optional[definition]
	target optional[definition]
	kind   migrationKind
	alters []operation
}

func newInitialState(base, target optional[definition]) migrationState {
	return migrationState{target.or(base).mustGet().id(), base, target, migrationKindUndefined, nil}
}

func newAddState(target definition) migrationState {
	return migrationState{target.id(), none[definition](), some(target), migrationKindAdd, nil}
}

func newAlterState(base, target definition, alters ...ast.DDL) migrationState {
	var operations []operation
	for _, ddl := range alters {
		operations = append(operations, newOperation(target, operationKindAlter, ddl))
	}
	return migrationState{base.id(), some(base), some(target), migrationKindAlter, operations}
}

func newDropState(base definition) migrationState {
	return migrationState{base.id(), some(base), none[definition](), migrationKindDrop, nil}
}

func newDropAndAddState(base, target definition) migrationState {
	return migrationState{base.id(), some(base), some(target), migrationKindDropAndAdd, nil}
}

func (ms migrationState) updateKind(kind migrationKind, alters ...operation) migrationState {
	ms.kind = kind
	ms.alters = alters
	return ms
}

func (ms migrationState) operations() []operation {
	switch ms.kind {
	case migrationKindAdd:
		return []operation{newOperation(ms.target.mustGet(), operationKindAdd, ms.target.mustGet().add())}
	case migrationKindAlter:
		return ms.alters
	case migrationKindDrop:
		return []operation{newOperation(ms.base.mustGet(), operationKindDrop, ms.base.mustGet().drop())}
	case migrationKindDropAndAdd:
		return []operation{
			newOperation(ms.base.mustGet(), operationKindDrop, ms.base.mustGet().drop()),
			newOperation(ms.target.mustGet(), operationKindAdd, ms.target.mustGet().add()),
		}
	case migrationKindNone, migrationKindUndefined:
		return nil
	default:
		panic(fmt.Sprintf("unexpected migration kind: %s: %s", ms.kind, ms.id))
	}
}

func (ms migrationState) definition() definition {
	return ms.target.or(ms.base).mustGet()
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

	for id := range base.all {
		m.initializeState(id)
	}
	for id := range target.all {
		m.initializeState(id)
	}

	return m
}

func (m *migration) initializeState(id identifier) {
	var baseOpt, targetOpt optional[definition]
	if base, ok := m.baseDefs.all[id]; ok {
		baseOpt = some(base)
	}
	if target, ok := m.targetDefs.all[id]; ok {
		targetOpt = some(target)
	}
	def := targetOpt.or(baseOpt).mustGet()

	if _, ok := m.states[def.id()]; ok {
		return
	}

	m.states[def.id()] = newInitialState(baseOpt, targetOpt)
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
		if _, ok := targetDefs.all[id]; !ok {
			m.updateStateIfUndefined(newDropState(base))
		}
	}
}

func (m *migration) adds(base, target *definitions) {
	for id, target := range target.all {
		if _, ok := base.all[id]; !ok {
			m.updateStateIfUndefined(newAddState(target))
		}
	}
}

func (m *migration) alters(base, target *definitions) {
	for id, t := range target.all {
		b, ok := base.all[id]
		if !ok {
			continue
		}
		if equalNode(b.astNode(), t.astNode()) {
			continue
		}
		b.alter(t, m)
	}
}
