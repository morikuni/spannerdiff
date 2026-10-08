package spannerdiff

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/cloudspannerecosystem/memefish/ast"
	"v.io/x/lib/toposort"
)

type operation struct {
	id        identifier
	kind      operationKind
	ddl       ast.DDL
	dependsOn []identifier
}

func newOperation(def definition, kind operationKind, ddl ast.DDL) operation {
	return operation{def.id(), kind, ddl, def.dependsOn()}
}

type operationKind string

const (
	operationKindAdd   operationKind = "add"
	operationKindAlter operationKind = "alter"
	operationKindDrop  operationKind = "drop"
)

func sortOperations(ops []operation) ([]operation, error) {
	// sort operations before topological sort to fix the sorted result.
	// The sort must be stable to keep the order of operations generated for the same definition.
	slices.SortStableFunc(ops, func(i, j operation) int {
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

	sortedAddAlter, err := topologicalSort(addAlterOps, false)
	if err != nil {
		return nil, err
	}
	sortedDrop, err := topologicalSort(dropOps, true)
	if err != nil {
		return nil, err
	}
	slices.Reverse(sortedDrop)

	return append(sortedDrop, sortedAddAlter...), nil
}

// topologicalSort sorts operations so that each operation comes after its dependencies.
// Operations of the same definition keep their original order. If reversed is true,
// the caller reverses the result, so the operations of the same definition are chained in reverse.
func topologicalSort(ops []operation, reversed bool) ([]operation, error) {
	s := &toposort.Sorter{}

	nodeMap := make(map[identifier][]*operation, len(ops))
	for i := range ops {
		op := &ops[i]
		s.AddNode(op)
		if prevs := nodeMap[op.id]; len(prevs) > 0 {
			prev := prevs[len(prevs)-1]
			if reversed {
				s.AddEdge(prev, op)
			} else {
				s.AddEdge(op, prev)
			}
		}
		nodeMap[op.id] = append(nodeMap[op.id], op)
	}

	for i := range ops {
		op := &ops[i]
		for _, dep := range op.dependsOn {
			if dep == op.id {
				continue
			}
			for _, depOp := range nodeMap[dep] {
				s.AddEdge(op, depOp)
			}
		}
	}

	sorted, cycles := s.Sort()
	if len(cycles) > 0 {
		return nil, fmt.Errorf("dependency cycle detected: %s", toposort.DumpCycles(cycles, func(n any) string {
			return n.(*operation).id.ID()
		}))
	}

	result := make([]operation, 0, len(sorted))
	for _, v := range sorted {
		result = append(result, *v.(*operation))
	}
	return result, nil
}
