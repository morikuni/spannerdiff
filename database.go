package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type database struct {
	node *ast.AlterDatabase
}

func newDatabase(ad *ast.AlterDatabase) *database {
	return &database{ad}
}

func (d *database) id() identifier {
	return newDatabaseID()
}

func (d *database) astNode() ast.Node {
	return d.node
}

func (d *database) add() []ast.DDL {
	return []ast.DDL{d.node}
}

func (d *database) drop() optional[ast.DDL] {
	return none[ast.DDL]()
}

func (d *database) alter(tgt definition, m *migration) {
	base := d
	target := tgt.(*database)

	options := diffOptions(base.node.Options, target.node.Options)
	if options == nil {
		// Only the database name is different.
		return
	}
	// The migration is applied to the base database, whose name may differ from the target.
	m.updateStateIfUndefined(newAlterState(base, target, &ast.AlterDatabase{Name: base.node.Name, Options: options}))
}

func (d *database) dependsOn() []dependency {
	return nil
}
