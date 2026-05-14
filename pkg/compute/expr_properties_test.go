// Copyright 2023-2026 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compute

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/assert"
)

// ============================================================
// Expr property tests — mirrors DuckDB expression semantics
// ============================================================

func TestExpr_IsConstant(t *testing.T) {
	// Constants are constant
	assert.True(t, intConst(42).IsConstant())
	assert.True(t, strConst("hello").IsConstant())

	// Columns are not constant
	assert.False(t, colExpr("t", "a", 1, 0, common.IntegerType()).IsConstant())

	// Function of constants is constant
	addConst := cmpExpr("=", intConst(1), intConst(2))
	assert.True(t, addConst.IsConstant())

	// Function with column is not constant
	addCol := cmpExpr("=", colExpr("t", "a", 1, 0, common.IntegerType()), intConst(1))
	assert.False(t, addCol.IsConstant())

	// Subquery is not constant
	subq := &Expr{Typ: ET_Subquery, Info: &SubqueryInfo{SubqueryTyp: ET_SubqueryTypeScalar}}
	assert.False(t, subq.IsConstant())
}

func TestExpr_IsFoldable(t *testing.T) {
	// Same as IsConstant for now
	assert.True(t, intConst(42).IsFoldable())
	assert.False(t, colExpr("t", "a", 1, 0, common.IntegerType()).IsFoldable())

	addConst := cmpExpr("=", intConst(1), intConst(2))
	assert.True(t, addConst.IsFoldable())
}

func TestExpr_IsVolatile(t *testing.T) {
	// Constants are not volatile
	assert.False(t, intConst(42).IsVolatile())

	// Columns are not volatile
	assert.False(t, colExpr("t", "a", 1, 0, common.IntegerType()).IsVolatile())

	// Normal functions are not volatile (default stability = Consistent)
	addExpr := cmpExpr("=", intConst(1), intConst(2))
	assert.False(t, addExpr.IsVolatile())

	// Volatile function expression is volatile
	volatileFunc := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.IntegerType(),
		Children: []*Expr{intConst(1)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "random", _stability: VolatileFunction},
		},
	}
	assert.True(t, volatileFunc.IsVolatile())

	// Nested volatile function
	nested := cmpExpr("=", volatileFunc, intConst(1))
	assert.True(t, nested.IsVolatile())
}

func TestExpr_IsScalar(t *testing.T) {
	// Constants are scalar
	assert.True(t, intConst(42).IsScalar())

	// Columns are not scalar
	assert.False(t, colExpr("t", "a", 1, 0, common.IntegerType()).IsScalar())

	// Scalar function of constants is scalar
	assert.True(t, cmpExpr("=", intConst(1), intConst(2)).IsScalar())

	// Aggregate is not scalar
	aggExpr := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.IntegerType(),
		Children: []*Expr{colExpr("t", "a", 1, 0, common.IntegerType())},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "sum", _funcTyp: AggregateFuncType},
		},
	}
	assert.False(t, aggExpr.IsScalar())
}

func TestExpr_IsAggregate(t *testing.T) {
	// Constants are not aggregate
	assert.False(t, intConst(42).IsAggregate())

	// Aggregate function is aggregate
	aggExpr := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.IntegerType(),
		Children: []*Expr{colExpr("t", "a", 1, 0, common.IntegerType())},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "sum", _funcTyp: AggregateFuncType},
		},
	}
	assert.True(t, aggExpr.IsAggregate())

	// Scalar function is not aggregate
	assert.False(t, cmpExpr("=", intConst(1), intConst(2)).IsAggregate())

	// Nested aggregate
	nested := cmpExpr("=", aggExpr, intConst(1))
	assert.True(t, nested.IsAggregate())
}

func TestExpr_HasSubquery(t *testing.T) {
	// Constants have no subquery
	assert.False(t, intConst(42).HasSubquery())

	// Subquery expression has subquery
	subq := &Expr{Typ: ET_Subquery, Info: &SubqueryInfo{SubqueryTyp: ET_SubqueryTypeScalar}}
	assert.True(t, subq.HasSubquery())

	// Function containing subquery
	funcWithSubq := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.BooleanType(),
		Children: []*Expr{intConst(1), subq},
		Info:     &FunctionInfo{FunImpl: &Function{_name: FuncEqual}},
	}
	assert.True(t, funcWithSubq.HasSubquery())
}

func TestExpr_PropagatesNullValues(t *testing.T) {
	// Constants propagate null (vacuously)
	assert.True(t, intConst(42).PropagatesNullValues())

	// Arithmetic propagates null
	addExpr := cmpExpr("=", intConst(1), intConst(2))
	assert.True(t, addExpr.PropagatesNullValues())

	// AND does not propagate null
	andExpr := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.BooleanType(),
		Children: []*Expr{intConst(1), intConst(0)},
		Info:     &FunctionInfo{FunImpl: &Function{_name: FuncAnd}},
	}
	assert.False(t, andExpr.PropagatesNullValues())

	// OR does not propagate null
	orExpr := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.BooleanType(),
		Children: []*Expr{intConst(1), intConst(0)},
		Info:     &FunctionInfo{FunImpl: &Function{_name: FuncOr}},
	}
	assert.False(t, orExpr.PropagatesNullValues())

	// Special null handling function does not propagate null
	specialFunc := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.BooleanType(),
		Children: []*Expr{intConst(1)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "coalesce", _nullHandling: SpecialHandling},
		},
	}
	assert.False(t, specialFunc.PropagatesNullValues())
}

func TestExpr_CanThrow(t *testing.T) {
	// Constants cannot throw
	assert.False(t, intConst(42).CanThrow())

	// Add cannot throw (default NoRuntimeError)
	addExpr := cmpExpr("=", intConst(1), intConst(2))
	assert.False(t, addExpr.CanThrow())

	// Cast can throw
	castExpr := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.IntegerType(),
		Children: []*Expr{strConst("abc")},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: FuncCast, _errorMode: CanThrowRuntimeError},
		},
	}
	assert.True(t, castExpr.CanThrow())

	// Divide can throw
	divExpr := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.FloatType(),
		Children: []*Expr{intConst(1), intConst(0)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: FuncDivide, _errorMode: CanThrowRuntimeError},
		},
	}
	assert.True(t, divExpr.CanThrow())
}

// ============================================================
// Function metadata tests
// ============================================================

func TestFunction_GetStability_Default(t *testing.T) {
	// Default stability (zero value) is ConsistentFunction
	f := &Function{_name: FuncAdd}
	assert.Equal(t, ConsistentFunction, f.GetStability())
	assert.False(t, f.IsVolatile())
}

func TestFunction_GetStability_Volatile(t *testing.T) {
	f := &Function{_name: "random", _stability: VolatileFunction}
	assert.Equal(t, VolatileFunction, f.GetStability())
	assert.True(t, f.IsVolatile())
}

func TestFunction_GetErrorMode(t *testing.T) {
	f := &Function{_name: FuncAdd}
	assert.Equal(t, NoRuntimeError, f.GetErrorMode())
	assert.False(t, f.CanThrow())

	f2 := &Function{_name: FuncCast, _errorMode: CanThrowRuntimeError}
	assert.Equal(t, CanThrowRuntimeError, f2.GetErrorMode())
	assert.True(t, f2.CanThrow())
}

// ============================================================
// ExprIterator tests
// ============================================================

func TestExprEnumerateChildren(t *testing.T) {
	addExpr := cmpExpr("=", intConst(1), intConst(2))
	var children []*Expr
	ExprEnumerateChildren(addExpr, func(child *Expr) {
		children = append(children, child)
	})
	assert.Len(t, children, 2)
}

func TestExprEnumerateNodes(t *testing.T) {
	// Tree: +(1, *(2, col))
	mulExpr := cmpExpr("=", intConst(2), colExpr("t", "a", 1, 0, common.IntegerType()))
	addExpr := cmpExpr("=", intConst(1), mulExpr)
	var nodes []ET
	ExprEnumerateNodes(addExpr, func(node *Expr) {
		nodes = append(nodes, node.Typ)
	})
	assert.Equal(t, []ET{ET_Func, ET_Const, ET_Func, ET_Const, ET_Column}, nodes)
}

func TestExprEnumerateLeafs(t *testing.T) {
	mulExpr := cmpExpr("=", intConst(2), colExpr("t", "a", 1, 0, common.IntegerType()))
	addExpr := cmpExpr("=", intConst(1), mulExpr)
	var leafs []ET
	ExprEnumerateLeafs(addExpr, func(leaf *Expr) {
		leafs = append(leafs, leaf.Typ)
	})
	assert.Equal(t, []ET{ET_Const, ET_Const, ET_Column}, leafs)
}

func TestTransformExpressions(t *testing.T) {
	// Replace all constants with constant 999
	addExpr := cmpExpr("=", intConst(1), intConst(2))
	result := TransformExpressions(addExpr, func(node *Expr) *Expr {
		if node.Typ == ET_Const {
			return intConst(999)
		}
		return nil
	})
	assert.True(t, result.IsConstant())
	assert.Equal(t, int64(999), result.Children[0].ConstValue.Integer)
	assert.Equal(t, int64(999), result.Children[1].ConstValue.Integer)
}

func TestReplaceExpression(t *testing.T) {
	oldCol := colExpr("t", "a", 1, 0, common.IntegerType())
	newCol := colExpr("t", "b", 1, 1, common.IntegerType())
	addExpr := cmpExpr("=", oldCol, intConst(1))
	result := ReplaceExpression(addExpr, oldCol, newCol)
	assert.Equal(t, ET_Column, result.Children[0].Typ)
	assert.Equal(t, "b", result.Children[0].Name)
}

// ============================================================
// Matcher integration tests — updated Foldable/Stable matchers
// ============================================================

func TestFoldableConstantMatcher_UsesIsFoldable(t *testing.T) {
	m := &FoldableConstantMatcher{}

	// Constant matches
	ok, bindings := m.Match(intConst(42), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Column does not match
	ok, bindings = m.Match(colExpr("t", "a", 1, 0, common.IntegerType()), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)

	// Function of constants matches (now that IsFoldable works)
	addExpr := cmpExpr("=", intConst(1), intConst(2))
	ok, bindings = m.Match(addExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Function with column does not match
	addCol := cmpExpr("=", colExpr("t", "a", 1, 0, common.IntegerType()), intConst(1))
	ok, bindings = m.Match(addCol, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)

	// Volatile function does not match
	volatileFunc := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.IntegerType(),
		Children: []*Expr{intConst(1)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "random", _stability: VolatileFunction},
		},
	}
	ok, bindings = m.Match(volatileFunc, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestStableExpressionMatcher_UsesIsVolatile(t *testing.T) {
	m := &StableExpressionMatcher{}

	// Column is stable (non-volatile)
	ok, bindings := m.Match(colExpr("t", "a", 1, 0, common.IntegerType()), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Constant is stable
	ok, bindings = m.Match(intConst(42), nil)
	assert.True(t, ok)

	// Normal function is stable
	ok, bindings = m.Match(cmpExpr("=", intConst(1), intConst(2)), nil)
	assert.True(t, ok)

	// Subquery without volatile internals is stable
	subq := &Expr{Typ: ET_Subquery}
	ok, bindings = m.Match(subq, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Volatile function is not stable
	volatileFunc := &Expr{
		Typ:      ET_Func,
		DataTyp:  common.IntegerType(),
		Children: []*Expr{intConst(1)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "random", _stability: VolatileFunction},
		},
	}
	ok, bindings = m.Match(volatileFunc, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}
