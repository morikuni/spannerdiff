package spannerdiff

import (
	"github.com/cloudspannerecosystem/memefish/ast"
)

type vectorIndex struct {
	node *ast.CreateVectorIndex
}

func newVectorIndex(cvi *ast.CreateVectorIndex) *vectorIndex {
	return &vectorIndex{cvi}
}

func (vi *vectorIndex) id() identifier {
	return newVectorIndexID(vi.node.Name)
}

func (vi *vectorIndex) tableID() tableID {
	return newTableIDFromIdent(vi.node.TableName)
}

func (vi *vectorIndex) astNode() ast.Node {
	return vi.node
}

func (vi *vectorIndex) add() []ast.DDL {
	return []ast.DDL{vi.node}
}

func (vi *vectorIndex) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropVectorIndex{
		Name: vi.node.Name,
	})
}

func (vi *vectorIndex) alter(tgt definition, m *migration) {
	base := vi
	target := tgt.(*vectorIndex)

	// https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language#alter_vector_index
	// - Add or remove a stored column.
	// - Set disable_search option.

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Storing, targetCopy.Storing = nil, nil
	baseCopy.Options, targetCopy.Options = nil, nil
	options := diffOptions(base.node.Options, target.node.Options)
	if !equalNode(&baseCopy, &targetCopy) || !onlyOptions(options, "disable_search") {
		m.updateStateIfUndefined(newDropAndAddState(base, target))
		return
	}

	name := &ast.Path{Idents: []*ast.Ident{target.node.Name}}
	ops := alterStoring(base, target, base.tableID(), base.node.Storing, target.node.Storing, func(a storedColumnAlteration) ast.DDL {
		return &ast.AlterVectorIndex{Name: name, Alteration: a}
	})
	if options != nil {
		ops = append(ops, newAddPhaseOperation(target, &ast.AlterVectorIndex{Name: name, Alteration: &ast.VectorIndexSetOptions{Options: options}}))
	}
	m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
}

func (vi *vectorIndex) dependsOn() []dependency {
	var ids []identifier
	ids = append(ids, newColumnID(vi.tableID(), vi.node.ColumnName))
	if vi.node.Storing != nil {
		for _, col := range vi.node.Storing.Columns {
			ids = append(ids, newColumnID(vi.tableID(), col))
		}
	}
	ids = append(ids, vi.tableID())
	return newDependencies(dependencyKindRecreate, ids)
}
