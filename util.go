package spannerdiff

import (
	"fmt"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

type optional[T any] struct {
	value T
	valid bool
}

func none[T comparable]() optional[T] {
	return optional[T]{}
}

func some[T comparable](value T) optional[T] {
	return optional[T]{value, true}
}

func (o optional[T]) get() (T, bool) {
	return o.value, o.valid
}

func (o optional[T]) mustGet() T {
	if o.valid {
		return o.value
	}
	panic("optional value is not valid")
}

func (o optional[T]) or(a optional[T]) optional[T] {
	if o.valid {
		return o
	}
	return a
}

func equalNode(a, b ast.Node) bool {
	return cmp.Equal(a, b, equalOptions()...)
}

func equalOptions() []cmp.Option {
	return []cmp.Option{
		cmpopts.IgnoreTypes(token.Pos(0)),
		// IF NOT EXISTS and OR REPLACE do not affect the resulting schema.
		cmp.FilterPath(func(p cmp.Path) bool {
			sf, ok := p.Last().(cmp.StructField)
			return ok && (sf.Name() == "IfNotExists" || sf.Name() == "OrReplace")
		}, cmp.Ignore()),
		// Identifiers are case-insensitive in Spanner.
		cmp.Comparer(func(a, b *ast.Ident) bool {
			if a == nil || b == nil {
				return a == b
			}
			return strings.EqualFold(a.Name, b.Name)
		}),
		// Proto and enum type names are case-sensitive.
		cmp.Comparer(func(a, b *ast.NamedType) bool {
			if a == nil || b == nil {
				return a == b
			}
			return a.SQL() == b.SQL()
		}),
		cmp.Comparer(func(a, b *ast.Options) bool {
			if a == nil && b == nil {
				return true
			}
			if (a == nil) != (b == nil) {
				return false
			}
			// compare ast.Options.Records without order.
			ma := make(map[string]ast.Expr)
			mb := make(map[string]ast.Expr)
			for _, o := range a.Records {
				ma[nameOf(o.Name)] = o.Value
			}
			for _, o := range b.Records {
				mb[nameOf(o.Name)] = o.Value
			}
			return cmp.Equal(ma, mb, equalOptions()...)
		}),
		cmp.Comparer(func(a, b *ast.IndexKey) bool {
			if a == nil || b == nil {
				return a == b
			}
			aVal := *a
			bVal := *b
			if aVal.Dir == "" {
				aVal.Dir = ast.DirectionAsc
			}
			if bVal.Dir == "" {
				bVal.Dir = ast.DirectionAsc
			}
			return cmp.Equal(aVal, bVal, equalOptions()...)
		}),
	}
}

func equalNodes[T ast.Node](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !equalNode(a[i], b[i]) {
			return false
		}
	}
	return true
}

func columnTypeOf(a ast.SchemaType) columnType {
	switch a := a.(type) {
	case *ast.ArraySchemaType:
		return array{columnTypeOf(a.Item)}
	case *ast.ScalarSchemaType:
		return scalar{a.Name}
	case *ast.SizedSchemaType:
		return scalar{a.Name}
	case *ast.NamedType:
		return protoOrEnum{}
	default:
		panic(fmt.Sprintf("unexpected column type: %s", a.SQL()))
	}
}

type columnType interface {
	isColumnType()
}

var _ = []struct{}{
	isComparable(scalar{}),
	isComparable(array{}),
	isComparable(protoOrEnum{}),
}

type scalar struct {
	t ast.ScalarTypeName
}

func (s scalar) isColumnType() {}

type array struct {
	item columnType
}

func (a array) isColumnType() {}

type protoOrEnum struct{}

func (n protoOrEnum) isColumnType() {}

type tuple struct {
	first  columnType
	second columnType
}

func tupleOf(a, b columnType) tuple {
	return tuple{a, b}
}

func unique[T comparable](is []T) []T {
	m := make(map[T]struct{})
	result := make([]T, 0, len(is))
	for _, i := range is {
		if _, ok := m[i]; !ok {
			m[i] = struct{}{}
			result = append(result, i)
		}
	}
	return result
}

func uniqueByFunc[A any, B comparable](is []A, f func(A) B) []A {
	m := make(map[B]A)
	result := make([]A, 0, len(is))
	for _, i := range is {
		key := f(i)
		if _, ok := m[key]; !ok {
			m[key] = i
			result = append(result, i)
		}
	}
	return result
}

func uniqueIdent(is []*ast.Ident) []*ast.Ident {
	return uniqueByFunc(is, nameOf)
}

func tablesOrViewsInQueryExpr(expr ast.QueryExpr) ([]*ast.Path, []*ast.Ident) {
	var idents []*ast.Ident
	var paths []*ast.Path

	ast.Inspect(expr, func(n ast.Node) bool {
		switch t := n.(type) {
		case *ast.TableName:
			idents = append(idents, t.Table)
		case *ast.PathTableExpr:
			paths = append(paths, t.Path)
		}
		return true
	})
	return paths, idents
}
