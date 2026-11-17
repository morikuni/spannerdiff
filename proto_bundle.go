package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type protoBundle struct {
	node *ast.CreateProtoBundle
}

func newProtoBundle(cp *ast.CreateProtoBundle) *protoBundle {
	return &protoBundle{cp}
}

func (pb *protoBundle) id() identifier {
	// Only one proto bundle can be defined in a schema.
	return newProtoBundleID()
}

func (pb *protoBundle) astNode() ast.Node {
	return pb.node
}

func (pb *protoBundle) add() []ast.DDL {
	return []ast.DDL{pb.node}
}

func (pb *protoBundle) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropProtoBundle{})
}

func (pb *protoBundle) alter(tgt definition, migration *migration) {
	base := pb
	target := tgt.(*protoBundle)

	baseNames := make(map[string]*ast.NamedType, len(base.node.Types.Types))
	for _, t := range base.node.Types.Types {
		baseNames[t.SQL()] = t
	}
	targetNames := make(map[string]*ast.NamedType, len(target.node.Types.Types))
	for _, t := range target.node.Types.Types {
		targetNames[t.SQL()] = t
	}
	added := make([]*ast.NamedType, 0, len(targetNames))
	dropped := make([]*ast.NamedType, 0, len(baseNames))
	for _, t := range target.node.Types.Types {
		if _, ok := baseNames[t.SQL()]; !ok {
			added = append(added, t)
		}
	}
	for _, t := range base.node.Types.Types {
		if _, ok := targetNames[t.SQL()]; !ok {
			dropped = append(dropped, t)
		}
	}
	ddl := &ast.AlterProtoBundle{}
	if len(added) > 0 {
		ddl.Insert = &ast.AlterProtoBundleInsert{Types: &ast.ProtoBundleTypes{Types: added}}
	}
	if len(dropped) > 0 {
		ddl.Delete = &ast.AlterProtoBundleDelete{Types: &ast.ProtoBundleTypes{Types: dropped}}
	}
	migration.updateStateIfUndefined(newAlterState(base, target, ddl))
}

func (pb *protoBundle) dependsOn() []dependency {
	return nil
}
