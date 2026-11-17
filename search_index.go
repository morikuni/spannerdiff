package spannerdiff

import (
	"slices"

	"github.com/cloudspannerecosystem/memefish/ast"
)

type searchIndex struct {
	node *ast.CreateSearchIndex
}

func newSearchIndex(csi *ast.CreateSearchIndex) *searchIndex {
	return &searchIndex{csi}
}

func (si *searchIndex) id() identifier {
	return newSearchIndexID(si.node.Name)
}

func (si *searchIndex) tableID() tableID {
	return newTableIDFromPath(si.node.TableName)
}

func (si *searchIndex) astNode() ast.Node {
	return si.node
}

func (si *searchIndex) add() []ast.DDL {
	return []ast.DDL{si.node}
}

func (si *searchIndex) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.DropSearchIndex{
		Name: si.node.Name,
	})
}

func (si *searchIndex) alter(tgt definition, m *migration) {
	base := si
	target := tgt.(*searchIndex)

	// --- not documented ---
	// Add or remove a stored column from an existing search index.

	baseCopy := *base.node
	targetCopy := *target.node
	baseCopy.Storing = nil
	targetCopy.Storing = nil

	if equalNode(&baseCopy, &targetCopy) {
		ops := alterStoring(base, target, base.tableID(), base.node.Storing, target.node.Storing, func(a storedColumnAlteration) ast.DDL {
			return &ast.AlterSearchIndex{Name: target.node.Name, IndexAlteration: a}
		})
		m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
		return
	}
	m.updateStateIfUndefined(newDropAndAddState(base, target))
}

func (si *searchIndex) dependsOn() []dependency {
	var ids []identifier
	columns := slices.Clone(si.node.TokenListPart)
	if si.node.Storing != nil {
		columns = append(columns, si.node.Storing.Columns...)
	}
	columns = append(columns, si.node.PartitionColumns...)
	if si.node.OrderBy != nil {
		ast.Inspect(si.node.OrderBy, func(n ast.Node) bool {
			if ident, ok := n.(*ast.Ident); ok {
				columns = append(columns, ident)
			}
			return true
		})
	}
	for _, col := range columns {
		ids = append(ids, newColumnID(si.tableID(), col))
	}
	ids = append(ids, si.tableID())
	if si.node.Interleave != nil {
		ids = append(ids, newTableIDFromIdent(si.node.Interleave.TableName))
	}
	return newDependencies(dependencyKindRecreate, ids)
}
