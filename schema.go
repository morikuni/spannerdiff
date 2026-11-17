package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

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

func (s *schema) add() []ast.DDL {
	return []ast.DDL{s.node}
}

func (s *schema) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropSchema{
		Name: s.node.Name,
	})
}

func (s *schema) alter(tgt definition, m *migration) {
	m.updateStateIfUndefined(newDropAndAddState(s, tgt))
}

func (s *schema) dependsOn() []dependency {
	return nil
}
