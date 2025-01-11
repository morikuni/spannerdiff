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
