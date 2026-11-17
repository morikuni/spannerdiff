package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type role struct {
	node *ast.CreateRole
}

func newRole(cr *ast.CreateRole) *role {
	return &role{cr}
}

func (r *role) id() identifier {
	return newRoleID(r.node.Name)
}

func (r *role) astNode() ast.Node {
	return r.node
}

func (r *role) add() []ast.DDL {
	return []ast.DDL{r.node}
}

func (r *role) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropRole{
		Name: r.node.Name,
	})
}

func (r *role) alter(tgt definition, m *migration) {
	base := r
	target := tgt.(*role)

	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (r *role) dependsOn() []dependency {
	return nil
}
