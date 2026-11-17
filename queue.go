package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type queue struct {
	node *ast.CreateQueue
}

func newQueue(cq *ast.CreateQueue) *queue {
	return &queue{cq}
}

func (q *queue) id() identifier {
	return newQueueIDFromPath(q.node.Name)
}

func (q *queue) astNode() ast.Node {
	return q.node
}

func (q *queue) add() []ast.DDL {
	return []ast.DDL{q.node}
}

func (q *queue) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropQueue{
		Name: q.node.Name,
	})
}

func (q *queue) alter(tgt definition, m *migration) {
	base := q
	target := tgt.(*queue)

	// https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language#alter_queue
	// - Set queue options.
	// - Change the ON DELETE action of an interleaved queue.
	// - Convert between INTERLEAVE IN and INTERLEAVE IN PARENT.
	// - Add, drop or replace the row deletion policy.

	// Columns and primary keys can't be altered, so the queue is recreated.
	if !equalNodes(base.node.PrimaryKeys, target.node.PrimaryKeys) || !equalQueueColumns(base.node.Columns, target.node.Columns) {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	// Other fields are cleared once they are handled, and the remaining differences require recreating the queue.
	baseRest := *base.node
	targetRest := *target.node
	baseRest.Columns = nil
	targetRest.Columns = nil
	baseRest.PrimaryKeys = nil
	targetRest.PrimaryKeys = nil

	var ddls []ast.DDL
	if alteration, ok := alterCluster(base.node.Cluster, target.node.Cluster); ok {
		if alteration != nil {
			ddls = append(ddls, &ast.AlterQueue{Name: target.node.Name, QueueAlteration: alteration})
		}
		baseRest.Cluster = nil
		targetRest.Cluster = nil
	}
	// Row deletion policies are migrated by row deletion policy definitions.
	baseRest.RowDeletionPolicy = nil
	targetRest.RowDeletionPolicy = nil
	if options := diffOptions(base.node.Options, target.node.Options); options != nil {
		ddls = append(ddls, &ast.AlterQueue{Name: target.node.Name, QueueAlteration: &ast.QueueSetOptions{Options: options}})
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

// equalQueueColumns compares columns of queues regardless of their order, as columns of tables are compared.
func equalQueueColumns(base, target []*ast.ColumnDef) bool {
	if len(base) != len(target) {
		return false
	}
	baseColumns := make(map[string]*ast.ColumnDef, len(base))
	for _, col := range base {
		baseColumns[nameOf(col.Name)] = col
	}
	for _, col := range target {
		baseCol, ok := baseColumns[nameOf(col.Name)]
		if !ok || !equalNode(baseCol, col) {
			return false
		}
	}
	return true
}

func (q *queue) alterRowDeletionPolicy(alteration tableOrQueueAlteration) ast.DDL {
	return &ast.AlterQueue{Name: q.node.Name, QueueAlteration: alteration}
}

func (q *queue) dependsOn() []dependency {
	if q.node.Cluster == nil {
		return nil
	}
	return []dependency{{newTableIDFromPath(q.node.Cluster.TableName), dependencyKindRecreate}}
}
