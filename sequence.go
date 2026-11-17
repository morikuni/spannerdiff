package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

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

func (s *sequence) add() []ast.DDL {
	return []ast.DDL{s.node}
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

func (s *sequence) dependsOn() []dependency {
	if schemaID, ok := s.schemaID().get(); ok {
		return []dependency{{schemaID, dependencyKindRecreate}}
	}
	return nil
}
