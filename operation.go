package spannerdiff

import (
	"cmp"
	"errors"
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
	slices.SortFunc(ops, func(i, j operation) int {
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

	sortedAddAlter, err := topologicalSort(addAlterOps)
	if err != nil {
		return nil, err
	}
	sortedDrop, err := topologicalSort(dropOps)
	if err != nil {
		return nil, err
	}
	reverse(sortedDrop)

	return append(sortedDrop, sortedAddAlter...), nil
}

func topologicalSort(ops []operation) ([]operation, error) {
	s := &toposort.Sorter{}

	nodeMap := make(map[identifier]*operation, len(ops))
	for i := range ops {
		nodeMap[ops[i].id] = &ops[i]
		s.AddNode(&ops[i])
	}

	for i := range ops {
		opPtr := &ops[i]
		for _, dep := range opPtr.dependsOn {
			if depPtr, ok := nodeMap[dep]; ok {
				s.AddEdge(opPtr, depPtr)
			}
		}
	}

	sorted, cycles := s.Sort()
	if len(cycles) > 0 {
		return nil, errors.New("dependency cycle detected")
	}

	result := make([]operation, 0, len(sorted))
	for _, v := range sorted {
		if opPtr, ok := v.(*operation); ok {
			result = append(result, *opPtr)
		}
	}
	return result, nil
}

func reverse(ops []operation) {
	for i, j := 0, len(ops)-1; i < j; i, j = i+1, j-1 {
		ops[i], ops[j] = ops[j], ops[i]
	}
}
