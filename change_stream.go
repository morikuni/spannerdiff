package spannerdiff

import (
	"fmt"

	"github.com/cloudspannerecosystem/memefish/ast"
)

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

func (cs *changeStream) add() []ast.DDL {
	return []ast.DDL{cs.node}
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
		// FOR ALL doesn't prevent tables and columns from being dropped, so it's set before the tables and columns of the base are dropped.
		return []operation{newDropPhaseOperation(cs, alter(&ast.ChangeStreamSetFor{For: target.node.For}))}
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

func (cs *changeStream) dependsOn() []dependency {
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
		return newDependencies(dependencyKindCustom, ids)
	default:
		panic(fmt.Sprintf("unexpected change stream for type: %T", cs.node.For))
	}
}

func (cs *changeStream) onDependencyChange(me, dependency migrationState, m *migration) {
	if me.kind == migrationKindDrop || !me.base.valid {
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
			// These operations replace the ones of alter, so the change stream stops watching all the tables and columns of the base
			// before they are dropped, and the options are changed in the same way as alter.
			base := me.base.mustGet().(*changeStream)
			target := me.target.mustGet().(*changeStream)
			ops := []operation{
				newOperation(base, operationKindDrop, &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: &ast.ChangeStreamDropForAll{}}),
				newOperation(target, operationKindAdd, &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: &ast.ChangeStreamSetFor{For: target.node.For}}),
			}
			if options := diffOptions(base.node.Options, target.node.Options); options != nil {
				ops = append(ops, newAddPhaseOperation(target, &ast.AlterChangeStream{Name: target.node.Name, ChangeStreamAlteration: &ast.ChangeStreamSetOptions{Options: options}}))
			}
			m.updateState(me.updateKind(migrationKindAlter, ops...))
		}
	default:
		panic(fmt.Sprintf("unexpected dependency type on change stream: %T", dep))
	}
}
