package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

// rowDeletionPolicyOwner is a table or a queue, which can have a row deletion policy.
type rowDeletionPolicyOwner interface {
	definition
	alterRowDeletionPolicy(alteration tableOrQueueAlteration) ast.DDL
}

type rowDeletionPolicy struct {
	node  *ast.RowDeletionPolicy
	owner rowDeletionPolicyOwner
}

func newRowDeletionPolicy(owner rowDeletionPolicyOwner, rdp *ast.RowDeletionPolicy) *rowDeletionPolicy {
	return &rowDeletionPolicy{rdp, owner}
}

func (r *rowDeletionPolicy) id() identifier {
	return newRowDeletionPolicyID(r.owner.id())
}

func (r *rowDeletionPolicy) astNode() ast.Node {
	return r.node
}

func (r *rowDeletionPolicy) add() []ast.DDL {
	return []ast.DDL{r.owner.alterRowDeletionPolicy(&ast.AddRowDeletionPolicy{RowDeletionPolicy: r.node})}
}

func (r *rowDeletionPolicy) drop() optional[ast.DDL] {
	return some(r.owner.alterRowDeletionPolicy(&ast.DropRowDeletionPolicy{}))
}

func (r *rowDeletionPolicy) alter(tgt definition, m *migration) {
	base := r
	target := tgt.(*rowDeletionPolicy)

	if !equalNode(base.node.ColumnName, target.node.ColumnName) {
		// The policy must be dropped before the column of the base policy is dropped.
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}
	m.updateStateIfUndefined(newAlterState(base, target,
		target.owner.alterRowDeletionPolicy(&ast.ReplaceRowDeletionPolicy{RowDeletionPolicy: target.node}),
	))
}

func (r *rowDeletionPolicy) dependsOn() []dependency {
	deps := []dependency{{r.owner.id(), dependencyKindOwner}}
	// Columns of a queue are not separate definitions because changing them recreates the queue.
	if t, ok := r.owner.(*table); ok {
		deps = append(deps, dependency{newColumnID(t.tableID(), r.node.ColumnName), dependencyKindRecreate})
	}
	return deps
}
