package spannerdiff

import (
	"fmt"

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
	return cmp.Equal(a, b,
		cmpopts.IgnoreTypes(token.Pos(0)),
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
				ma[o.Name.Name] = o.Value
			}
			for _, o := range b.Records {
				mb[o.Name.Name] = o.Value
			}
			return cmp.Equal(ma, mb, cmpopts.IgnoreTypes(token.Pos(0)))
		}),
	)
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
