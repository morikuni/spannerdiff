package spannerdiff

import (
	"fmt"
	"slices"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
)

type grant struct {
	node    *ast.Grant
	grantID grantID
}

// newGrant splits GRANT into the definitions of each pair of a role and an object,
// because privileges are granted and revoked for each pair.
func newGrant(g *ast.Grant) ([]definition, error) {
	var grants []definition
	for _, r := range g.Roles {
		roleID := newRoleID(r)
		// The privilege is created for each definition, because merge modifies it.
		add := func(id grantID, privilege ast.Privilege) {
			grants = append(grants, &grant{&ast.Grant{Roles: []*ast.Ident{r}, Privilege: privilege}, id})
		}
		switch t := g.Privilege.(type) {
		case *ast.PrivilegeOnTable:
			for _, name := range t.Names {
				add(newGrantID(roleID, newTableIDFromPath(name)), &ast.PrivilegeOnTable{Privileges: t.Privileges, Names: []*ast.Path{name}})
			}
		case *ast.SelectPrivilegeOnView:
			for _, name := range t.Names {
				add(newGrantID(roleID, newViewIDFromPath(name)), &ast.SelectPrivilegeOnView{Names: []*ast.Path{name}})
			}
		case *ast.SelectPrivilegeOnChangeStream:
			for _, name := range t.Names {
				add(newGrantID(roleID, newChangeStreamIDFromPath(name)), &ast.SelectPrivilegeOnChangeStream{Names: []*ast.Path{name}})
			}
		case *ast.ExecutePrivilegeOnTableFunction:
			for _, name := range t.Names {
				add(newGrantID(roleID, newChangeStreamReadFunctionIDFromPath(name)), &ast.ExecutePrivilegeOnTableFunction{Names: []*ast.Path{name}})
			}
		case *ast.RolePrivilege:
			for _, name := range t.Names {
				add(newGrantID(roleID, newRoleID(name)), &ast.RolePrivilege{Names: []*ast.Ident{name}})
			}
		case *ast.PrivilegeOnSequence:
			for _, name := range t.Names {
				add(newGrantID(roleID, newSequenceID(name)), &ast.PrivilegeOnSequence{Privileges: t.Privileges, Names: []*ast.Path{name}})
			}
		case *ast.UsagePrivilegeOnSchema:
			if !t.Default.Invalid() {
				add(newGrantID(roleID, newDefaultSchemaID()), &ast.UsagePrivilegeOnSchema{Default: t.Default})
				continue
			}
			for _, name := range t.Schemas {
				add(newGrantID(roleID, schemaIDOfGrant(name)), &ast.UsagePrivilegeOnSchema{Default: token.InvalidPos, Schemas: []*ast.Path{name}})
			}
		case *ast.PrivilegeOnAllTablesInSchema, *ast.PrivilegeOnAllSequencesInSchema, *ast.SelectPrivilegeOnAllViewsInSchema, *ast.SelectPrivilegeOnAllChangeStreamsInSchema:
			// Spanner applies these privileges to each object, so they can't be compared with the base schema.
			return nil, fmt.Errorf("GRANT ON ALL ... IN SCHEMA is not supported, grant privileges on each object instead: %s", g.SQL())
		default:
			return nil, fmt.Errorf("unsupported GRANT: %s", g.SQL())
		}
	}
	return grants, nil
}

// schemaIDOfGrant returns the schema of GRANT USAGE ON SCHEMA, which may be qualified by the database name.
func schemaIDOfGrant(path *ast.Path) schemaID {
	return newSchemaID(path.Idents[len(path.Idents)-1])
}

// columnPrivilege is a privilege that can be granted on a whole table and on its columns.
type columnPrivilege struct {
	all     bool
	columns []*ast.Ident
}

func (p columnPrivilege) grant(columns []*ast.Ident) columnPrivilege {
	if len(columns) == 0 {
		p.all = true
		return p
	}
	p.columns = uniqueIdent(slices.Concat(p.columns, columns))
	return p
}

// minus returns the privilege in p that is not in other.
func (p columnPrivilege) minus(other columnPrivilege) columnPrivilege {
	names := make(map[string]struct{}, len(other.columns))
	for _, col := range other.columns {
		names[nameOf(col)] = struct{}{}
	}
	result := columnPrivilege{all: p.all && !other.all}
	for _, col := range p.columns {
		if _, ok := names[nameOf(col)]; !ok {
			result.columns = append(result.columns, col)
		}
	}
	return result
}

// tablePrivileges is the set of privileges on a table or a sequence, where the same privilege may be written many times.
// A sequence has only SELECT and UPDATE without columns.
type tablePrivileges struct {
	sel, update, insert columnPrivilege
	delete              bool
}

func newTablePrivileges(privileges []ast.TablePrivilege) tablePrivileges {
	var p tablePrivileges
	for _, privilege := range privileges {
		switch t := privilege.(type) {
		case *ast.SelectPrivilege:
			p.sel = p.sel.grant(t.Columns)
		case *ast.UpdatePrivilege:
			p.update = p.update.grant(t.Columns)
		case *ast.InsertPrivilege:
			p.insert = p.insert.grant(t.Columns)
		case *ast.DeletePrivilege:
			p.delete = true
		default:
			panic(fmt.Sprintf("unexpected privilege type: %T", privilege))
		}
	}
	return p
}

// minus returns the privileges in p that are not in other.
func (p tablePrivileges) minus(other tablePrivileges) tablePrivileges {
	return tablePrivileges{
		sel:    p.sel.minus(other.sel),
		update: p.update.minus(other.update),
		insert: p.insert.minus(other.insert),
		delete: p.delete && !other.delete,
	}
}

func (p tablePrivileges) privileges() []ast.TablePrivilege {
	var privileges []ast.TablePrivilege
	add := func(cp columnPrivilege, newPrivilege func(columns []*ast.Ident) ast.TablePrivilege) {
		if cp.all {
			privileges = append(privileges, newPrivilege(nil))
		}
		if len(cp.columns) > 0 {
			privileges = append(privileges, newPrivilege(cp.columns))
		}
	}
	add(p.sel, func(columns []*ast.Ident) ast.TablePrivilege { return &ast.SelectPrivilege{Columns: columns} })
	add(p.update, func(columns []*ast.Ident) ast.TablePrivilege { return &ast.UpdatePrivilege{Columns: columns} })
	add(p.insert, func(columns []*ast.Ident) ast.TablePrivilege { return &ast.InsertPrivilege{Columns: columns} })
	if p.delete {
		privileges = append(privileges, &ast.DeletePrivilege{})
	}
	return privileges
}

// merge merges the privileges granted to the same role on the same object by another GRANT.
func (g *grant) merge(other definition) bool {
	oth := other.(*grant)
	switch p1 := g.node.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		p2 := oth.node.Privilege.(*ast.PrivilegeOnTable)
		p1.Privileges = newTablePrivileges(slices.Concat(p1.Privileges, p2.Privileges)).privileges()
		return true
	case *ast.PrivilegeOnSequence:
		p2 := oth.node.Privilege.(*ast.PrivilegeOnSequence)
		p1.Privileges = newTablePrivileges(slices.Concat(p1.Privileges, p2.Privileges)).privileges()
		return true
	case *ast.SelectPrivilegeOnView, *ast.SelectPrivilegeOnChangeStream, *ast.ExecutePrivilegeOnTableFunction, *ast.RolePrivilege, *ast.UsagePrivilegeOnSchema:
		// no additional parameters exist
		return true
	default:
		panic(fmt.Sprintf("unexpected grant type: %T: %s", g.node.Privilege, g.node.SQL()))
	}
}

func (g *grant) id() identifier {
	return g.grantID
}

func (g *grant) astNode() ast.Node {
	return g.node
}

func (g *grant) add() []ast.DDL {
	return []ast.DDL{g.node}
}

func (g *grant) drop() optional[ast.DDL] {
	return some[ast.DDL](&ast.Revoke{
		Roles:     g.node.Roles,
		Privilege: g.node.Privilege,
	})
}

func (g *grant) alter(tgt definition, m *migration) {
	base := g
	target := tgt.(*grant)

	// alter revokes the privileges only in base and grants the privileges only in target.
	alter := func(basePrivileges, targetPrivileges []ast.TablePrivilege, newPrivilege func([]ast.TablePrivilege) ast.Privilege) {
		basePs, targetPs := newTablePrivileges(basePrivileges), newTablePrivileges(targetPrivileges)
		var ops []operation
		if dropped := basePs.minus(targetPs).privileges(); len(dropped) > 0 {
			// Privileges must be revoked before the columns are dropped.
			ops = append(ops, newDropPhaseOperation(base, &ast.Revoke{Roles: target.node.Roles, Privilege: newPrivilege(dropped)}))
		}
		if added := targetPs.minus(basePs).privileges(); len(added) > 0 {
			ops = append(ops, newAddPhaseOperation(target, &ast.Grant{Roles: target.node.Roles, Privilege: newPrivilege(added)}))
		}
		m.updateStateIfUndefined(newAlterStateWithOperations(base, target, ops...))
	}

	switch baseP := base.node.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		targetP := target.node.Privilege.(*ast.PrivilegeOnTable)
		alter(baseP.Privileges, targetP.Privileges, func(privileges []ast.TablePrivilege) ast.Privilege {
			return &ast.PrivilegeOnTable{Privileges: privileges, Names: targetP.Names}
		})
	case *ast.PrivilegeOnSequence:
		targetP := target.node.Privilege.(*ast.PrivilegeOnSequence)
		alter(baseP.Privileges, targetP.Privileges, func(privileges []ast.TablePrivilege) ast.Privilege {
			return &ast.PrivilegeOnSequence{Privileges: privileges, Names: targetP.Names}
		})
	case *ast.SelectPrivilegeOnView, *ast.SelectPrivilegeOnChangeStream, *ast.ExecutePrivilegeOnTableFunction, *ast.RolePrivilege, *ast.UsagePrivilegeOnSchema:
		// never come here, because grant type handles only single target name (1 view, change stream, table function or role per grant type)
		panic(fmt.Sprintf("unsupported GRANT alteration on: %s", target.node.SQL()))
	}
}

func (g *grant) dependsOn() []dependency {
	var ids []identifier
	for _, role := range g.node.Roles {
		ids = append(ids, newRoleID(role))
	}
	switch p := g.node.Privilege.(type) {
	case *ast.PrivilegeOnTable:
		for _, tableName := range p.Names {
			ids = append(ids, newTableIDFromPath(tableName))
		}
		for _, tp := range p.Privileges {
			switch t := tp.(type) {
			case *ast.SelectPrivilege:
				for _, col := range t.Columns {
					ids = append(ids, newColumnID(newTableIDFromPath(p.Names[0]), col))
				}
			case *ast.UpdatePrivilege:
				for _, col := range t.Columns {
					ids = append(ids, newColumnID(newTableIDFromPath(p.Names[0]), col))
				}
			case *ast.InsertPrivilege:
				for _, col := range t.Columns {
					ids = append(ids, newColumnID(newTableIDFromPath(p.Names[0]), col))
				}
			case *ast.DeletePrivilege:
				// none
			default:
				panic(fmt.Sprintf("unexpected privilege type: %T", tp))
			}
		}
	case *ast.SelectPrivilegeOnView:
		for _, viewName := range p.Names {
			ids = append(ids, newViewIDFromPath(viewName))
		}
	case *ast.SelectPrivilegeOnChangeStream:
		for _, csName := range p.Names {
			ids = append(ids, newChangeStreamIDFromPath(csName))
		}
	case *ast.ExecutePrivilegeOnTableFunction:
		// none
	case *ast.RolePrivilege:
		for _, roleName := range p.Names {
			ids = append(ids, newRoleID(roleName))
		}
	case *ast.PrivilegeOnSequence:
		for _, seqName := range p.Names {
			ids = append(ids, newSequenceID(seqName))
		}
	case *ast.UsagePrivilegeOnSchema:
		for _, schemaName := range p.Schemas {
			ids = append(ids, schemaIDOfGrant(schemaName))
		}
	}
	return newDependencies(dependencyKindRecreate, ids)
}
