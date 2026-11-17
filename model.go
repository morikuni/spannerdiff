package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type model struct {
	node *ast.CreateModel
}

func newModel(cm *ast.CreateModel) *model {
	return &model{cm}
}

func (m *model) id() identifier {
	return newModelID(m.node.Name)
}

func (m *model) astNode() ast.Node {
	return m.node
}

func (m *model) add() []ast.DDL {
	return []ast.DDL{m.node}
}

func (m *model) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropModel{
		Name: m.node.Name,
	})
}

func (m *model) alter(tgt definition, migration *migration) {
	base := m
	target := tgt.(*model)

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Options = nil
	targetCopy.Options = nil
	if equalNode(&baseCopy, &targetCopy) {
		migration.updateStateIfUndefined(newAlterState(base, target, &ast.AlterModel{Name: target.node.Name, Options: diffOptions(base.node.Options, target.node.Options)}))
		return
	}

	targetCopy = *target.node
	targetCopy.OrReplace = true
	migration.updateStateIfUndefined(newAlterState(base, target, &targetCopy))
}

func (m *model) dependsOn() []dependency {
	return nil
}
