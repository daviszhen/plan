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
	"github.com/stretchr/testify/require"
)

// ============================================================
// Stage 1: Leaf Matchers
// ============================================================

func TestAnyExprMatcher(t *testing.T) {
	m := &AnyExprMatcher{}
	col := mkCol("t", "a", 1, 0)
	ok, bindings := m.Match(col, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)
	assert.Same(t, col, bindings[0])

	ok, bindings = m.Match(nil, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestTypeExprMatcher_Match(t *testing.T) {
	m := &TypeExprMatcher{Typ: ET_Func}
	ok, bindings := m.Match(mkFunc("=", mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)
}

func TestTypeExprMatcher_Mismatch(t *testing.T) {
	m := &TypeExprMatcher{Typ: ET_Func}
	ok, bindings := m.Match(mkConst(42), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestConstExprMatcher(t *testing.T) {
	m := &ConstExprMatcher{}
	ok, bindings := m.Match(mkConst(42), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkCol("t", "a", 1, 0), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestColumnExprMatcher(t *testing.T) {
	m := &ColumnExprMatcher{}
	col := mkCol("t", "a", 1, 0)
	ok, bindings := m.Match(col, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkConst(1), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestColumnExprMatcher_WithTableFilter(t *testing.T) {
	m := &ColumnExprMatcher{Table: "t"}
	ok, _ := m.Match(mkCol("t", "a", 1, 0), nil)
	assert.True(t, ok)

	ok, _ = m.Match(mkCol("s", "a", 2, 0), nil)
	assert.False(t, ok)
}

func TestColumnExprMatcher_WithNameFilter(t *testing.T) {
	m := &ColumnExprMatcher{Name: "a"}
	ok, _ := m.Match(mkCol("t", "a", 1, 0), nil)
	assert.True(t, ok)

	ok, _ = m.Match(mkCol("t", "b", 1, 1), nil)
	assert.False(t, ok)
}

func TestFuncExprMatcher_NoChildren(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
	}
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)
}

func TestFuncExprMatcher_WithChildren_Ordered(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncEqual},
		Children: []ExprMatcher{
			&ColumnExprMatcher{},
			&ConstExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	ok, bindings := m.Match(mkFunc(FuncEqual, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3) // root + 2 children

	// Wrong order: const, col
	ok, bindings = m.Match(mkFunc(FuncEqual, mkConst(1), mkCol("t", "a", 1, 0)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestFuncExprMatcher_MismatchName(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
	}
	ok, bindings := m.Match(mkFunc(FuncSubtract, mkConst(1), mkConst(2)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestFuncExprMatcher_MismatchType(t *testing.T) {
	m := &FuncExprMatcher{
		TypeMatcher: &SpecificExprTypeMatcher{Typ: ET_Func},
	}
	ok, bindings := m.Match(mkConst(1), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// ============================================================
// Stage 2: Attribute Matchers
// ============================================================

func TestSpecificFuncNameMatcher(t *testing.T) {
	m := &SpecificFuncNameMatcher{Name: FuncEqual}
	assert.True(t, m.Match(FuncEqual))
	assert.False(t, m.Match(FuncAdd))
}

func TestManyFuncNameMatcher(t *testing.T) {
	m := NewManyFuncNameMatcher(FuncAdd, FuncSubtract)
	assert.True(t, m.Match(FuncAdd))
	assert.True(t, m.Match(FuncSubtract))
	assert.False(t, m.Match(FuncMultiply))
}

func TestSpecificDataTypeMatcher(t *testing.T) {
	m := &SpecificDataTypeMatcher{Type: common.IntegerType()}
	assert.True(t, m.Match(common.IntegerType()))
	assert.False(t, m.Match(common.BigintType()))
}

func TestNumericDataTypeMatcher(t *testing.T) {
	m := &NumericDataTypeMatcher{}
	assert.True(t, m.Match(common.IntegerType()))
	assert.True(t, m.Match(common.FloatType()))
	assert.True(t, m.Match(common.DecimalType(18, 2)))
	assert.False(t, m.Match(common.VarcharType()))
}

func TestIntegerDataTypeMatcher(t *testing.T) {
	m := &IntegerDataTypeMatcher{}
	assert.True(t, m.Match(common.IntegerType()))
	assert.True(t, m.Match(common.BigintType()))
	assert.False(t, m.Match(common.FloatType()))
	assert.False(t, m.Match(common.VarcharType()))
}

func TestFuncExprMatcher_WithFuncNameMatcher(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncAdd, FuncSubtract),
	}
	ok, _ := m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)

	ok, _ = m.Match(mkFunc(FuncSubtract, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)

	ok, _ = m.Match(mkFunc(FuncMultiply, mkConst(1), mkConst(2)), nil)
	assert.False(t, ok)
}

func TestFuncExprMatcher_WithDataTypeMatcher(t *testing.T) {
	m := &FuncExprMatcher{
		DataTypeMatcher: &IntegerDataTypeMatcher{},
	}
	// mkFunc returns an expression bound with actual types; for simple arithmetic
	// the binder may resolve to a non-integer type. Construct manually instead.
	expr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{mkConst(1), mkConst(2)},
		Info:    &FunctionInfo{FunImpl: &Function{_name: FuncAdd}},
	}
	ok, bindings := m.Match(expr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	expr.DataTyp = common.VarcharType()
	ok, bindings = m.Match(expr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// ============================================================
// Stage 3: SetMatcher Policies
// ============================================================

func TestSetMatch_Ordered_Success(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkCol("t", "a", 1, 0)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyOrdered)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)
}

func TestSetMatch_Ordered_FailCount(t *testing.T) {
	matchers := []ExprMatcher{&ConstExprMatcher{}}
	entries := []*Expr{mkConst(1), mkCol("t", "a", 1, 0)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyOrdered)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestSetMatch_Ordered_FailOrder(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkCol("t", "a", 1, 0), mkConst(1)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyOrdered)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestSetMatch_Unordered_Success(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkCol("t", "a", 1, 0), mkConst(1)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyUnordered)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)
}

func TestSetMatch_Unordered_FailExtra(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkCol("t", "a", 1, 0), mkConst(2)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyUnordered)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestSetMatch_Some_Success(t *testing.T) {
	matchers := []ExprMatcher{&ConstExprMatcher{}}
	entries := []*Expr{mkCol("t", "a", 1, 0), mkConst(1), mkCol("t", "b", 1, 1)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicySome)
	assert.True(t, ok)
	require.Len(t, bindings, 1)
	assert.Equal(t, ET_Const, bindings[0].Typ)
}

func TestSetMatch_Some_FailNoMatch(t *testing.T) {
	matchers := []ExprMatcher{&ConstExprMatcher{}}
	entries := []*Expr{mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicySome)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestSetMatch_SomeOrdered_Success(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkCol("t", "a", 1, 0), mkConst(2)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicySomeOrdered)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)
}

func TestSetMatch_SomeOrdered_FailWrongPrefix(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkCol("t", "a", 1, 0), mkConst(1), mkConst(2)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicySomeOrdered)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// ============================================================
// Stage 4: Complex Expression Patterns
// ============================================================

func TestComparisonExprMatcher(t *testing.T) {
	m := &ComparisonExprMatcher{
		Children: []ExprMatcher{
			&ColumnExprMatcher{},
			&ConstExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	ok, bindings := m.Match(mkFunc(FuncGreater, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// Arithmetic expression should not match
	ok, bindings = m.Match(mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestFuncExprMatcher_ArithmeticSimplificationPattern(t *testing.T) {
	// Pattern: +(const, any) or +(any, const)  — matches x+0 or 0+x
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&AnyExprMatcher{},
		},
		Policy: PolicySome,
	}
	// const + col
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(0), mkCol("t", "a", 1, 0)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// col + const
	ok, bindings = m.Match(mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkConst(0)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// col + col — no const, should not match
	ok, bindings = m.Match(mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestAggregateExprMatcher(t *testing.T) {
	m := &AggregateExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: "sum"},
		Children:        []ExprMatcher{&AnyExprMatcher{}},
		Policy:          PolicyOrdered,
	}
	// Build a sum aggregate expression manually since mkFunc binds scalar funcs.
	expr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{mkCol("t", "a", 1, 0)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "sum", _funcTyp: AggregateFuncType},
		},
	}
	ok, bindings := m.Match(expr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)

	// Non-aggregate should not match
	expr2 := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{mkCol("t", "a", 1, 0)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "sum", _funcTyp: ScalarFuncType},
		},
	}
	ok, bindings = m.Match(expr2, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestNestedMatcher(t *testing.T) {
	// Pattern: +(const, *(const, col))
	innerMul := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncMultiply},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&ColumnExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	outerAdd := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			innerMul,
		},
		Policy: PolicyOrdered,
	}
	// Build: +(1, *(2, col))
	mulExpr := mkFunc(FuncMultiply, mkConst(2), mkCol("t", "a", 1, 0))
	addExpr := mkFunc(FuncAdd, mkConst(1), mulExpr)
	ok, bindings := outerAdd.Match(addExpr, nil)
	assert.True(t, ok)
	// bindings: addExpr, const(1), mulExpr, const(2), col
	assert.Len(t, bindings, 5)
	assert.Equal(t, ET_Func, bindings[0].Typ)
	assert.Equal(t, ET_Const, bindings[1].Typ)
	assert.Equal(t, ET_Func, bindings[2].Typ)
	assert.Equal(t, ET_Const, bindings[3].Typ)
	assert.Equal(t, ET_Column, bindings[4].Typ)
}

func TestBindingsOrder(t *testing.T) {
	// Verify bindings are captured root-first, depth-first.
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&AnyExprMatcher{},
			&AnyExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	c1 := mkConst(1)
	c2 := mkConst(2)
	expr := mkFunc(FuncAdd, c1, c2)
	ok, bindings := m.Match(expr, nil)
	assert.True(t, ok)
	require.Len(t, bindings, 3)
	assert.Same(t, expr, bindings[0])
	assert.Same(t, c1, bindings[1])
	assert.Same(t, c2, bindings[2])
}

// ============================================================
// Stage 5: LogicalOperator Matcher
// ============================================================

func TestSpecificLogicalOpMatcher(t *testing.T) {
	m := &SpecificLogicalOpMatcher{Typ: LOT_Filter}
	assert.True(t, m.Match(&LogicalOperator{Typ: LOT_Filter}))
	assert.False(t, m.Match(&LogicalOperator{Typ: LOT_Scan}))
	assert.False(t, m.Match(nil))
}

func TestAnyLogicalOpMatcher(t *testing.T) {
	m := &AnyLogicalOpMatcher{}
	assert.True(t, m.Match(&LogicalOperator{Typ: LOT_Filter}))
	assert.True(t, m.Match(&LogicalOperator{Typ: LOT_Scan}))
	assert.False(t, m.Match(nil))
}

func TestLogicalOpWithChildMatcher(t *testing.T) {
	m := &LogicalOpWithChildMatcher{
		OpMatcher:    &SpecificLogicalOpMatcher{Typ: LOT_Project},
		ChildMatcher: &SpecificLogicalOpMatcher{Typ: LOT_Filter},
		ChildIndex:   0,
	}
	proj := &LogicalOperator{
		Typ:      LOT_Project,
		Children: []*LogicalOperator{{Typ: LOT_Filter}},
	}
	assert.True(t, m.Match(proj))

	proj.Children[0].Typ = LOT_Scan
	assert.False(t, m.Match(proj))
}

func TestLogicalOpMatcher_InTree(t *testing.T) {
	plan := buildTestPlan() // from visitor_test.go: Project -> Filter -> Join -> (Scan, Scan)
	m := &SpecificLogicalOpMatcher{Typ: LOT_JOIN}
	var found bool
	var walk func(op *LogicalOperator)
	walk = func(op *LogicalOperator) {
		if m.Match(op) {
			found = true
		}
		for _, child := range op.Children {
			walk(child)
		}
	}
	walk(plan)
	assert.True(t, found)
}

// ============================================================
// Stage 6: Utility matchers
// ============================================================

func TestFoldableConstantMatcher(t *testing.T) {
	m := &FoldableConstantMatcher{}
	ok, bindings := m.Match(mkConst(42), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkCol("t", "a", 1, 0), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestStableExpressionMatcher(t *testing.T) {
	m := &StableExpressionMatcher{}
	ok, bindings := m.Match(mkCol("t", "a", 1, 0), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
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
		Children: []*Expr{mkConst(1)},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "random", _stability: VolatileFunction},
		},
	}
	ok, bindings = m.Match(volatileFunc, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestExpressionEqualityMatcher(t *testing.T) {
	target := mkConst(42)
	m := &ExpressionEqualityMatcher{Expression: target}
	ok, bindings := m.Match(target, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkConst(42), nil)
	assert.False(t, ok) // pointer equality
	assert.Empty(t, bindings)
}

// ============================================================
// Stage 7: Integration — matcher on real expression trees
// ============================================================

func TestMatcher_OnFilterExpression(t *testing.T) {
	// Simulate: t.a = 1 AND t.b > 2
	andExpr := mkFunc(FuncAnd,
		mkFunc(FuncEqual, mkCol("t", "a", 1, 0), mkConst(1)),
		mkFunc(FuncGreater, mkCol("t", "b", 1, 1), mkConst(2)),
	)

	// Match any comparison inside an AND
	compareInAnd := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAnd},
		Children: []ExprMatcher{
			&ComparisonExprMatcher{},
			&ComparisonExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	ok, bindings := compareInAnd.Match(andExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3) // and + 2 comparisons
}

func TestMatcher_BacktrackingWithDuplicateMatchers(t *testing.T) {
	// Two identical matchers in unordered mode must match two distinct entries.
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ConstExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkConst(2)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyUnordered)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)
	assert.Equal(t, int64(1), bindings[0].ConstValue.Integer)
	assert.Equal(t, int64(2), bindings[1].ConstValue.Integer)
}

func TestMatcher_PartialMatchWithSomePolicy(t *testing.T) {
	// Pattern: AND(const, any, any) but we only want to match (const, any)
	// Manually construct since mkFunc does not bind logical operators.
	andExpr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.BooleanType(),
		Children: []*Expr{
			mkConst(1),
			mkCol("t", "a", 1, 0),
			mkCol("t", "b", 1, 1),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncAnd}},
	}
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAnd},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&AnyExprMatcher{},
		},
		Policy: PolicySome,
	}
	ok, bindings := m.Match(andExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3) // and + const + one col
}

// ============================================================
// Stage 8: Edge cases
// ============================================================

func TestMatcher_NilExpression(t *testing.T) {
	matchers := []ExprMatcher{
		&AnyExprMatcher{},
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
		&FuncExprMatcher{},
	}
	for _, m := range matchers {
		ok, bindings := m.Match(nil, nil)
		assert.False(t, ok, "matcher %T should not match nil", m)
		assert.Empty(t, bindings)
	}
}

func TestMatcher_EmptyChildren(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children:        []ExprMatcher{},
		Policy:          PolicyOrdered,
	}
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1) // only root bound, no child matching attempted
}

func TestMatcher_SetMatch_EmptyMatchers(t *testing.T) {
	ok, bindings := SetMatch([]ExprMatcher{}, []*Expr{mkConst(1)}, nil, PolicyOrdered)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

func TestMatcher_SetMatch_EmptyEntries(t *testing.T) {
	ok, bindings := SetMatch([]ExprMatcher{&AnyExprMatcher{}}, []*Expr{}, nil, PolicySome)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}


// ============================================================
// Stage 9: DuckDB-inspired integration patterns
// ============================================================

// TestManyExprTypeMatcher verifies matching against a set of expression types.
func TestManyExprTypeMatcher(t *testing.T) {
	m := NewManyExprTypeMatcher(ET_Func, ET_Const)
	assert.True(t, m.Match(ET_Func))
	assert.True(t, m.Match(ET_Const))
	assert.False(t, m.Match(ET_Column))
	assert.False(t, m.Match(ET_Subquery))
}

// TestFuncExprMatcher_DefaultPolicyOrdered verifies that when Policy is zero,
// FuncExprMatcher defaults to PolicyOrdered for child matching.
func TestFuncExprMatcher_DefaultPolicyOrdered(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&ColumnExprMatcher{},
		},
		// Policy intentionally left as zero
	}
	// const + col should match in ordered mode
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1), mkCol("t", "a", 1, 0)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// col + const should NOT match in ordered mode
	ok, bindings = m.Match(mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestFuncExprMatcher_TypeMatcher verifies that FuncExprMatcher respects
// the TypeMatcher field (e.g., matching only ET_Func).
func TestFuncExprMatcher_TypeMatcher(t *testing.T) {
	m := &FuncExprMatcher{
		TypeMatcher: &SpecificExprTypeMatcher{Typ: ET_Func},
		Children:    []ExprMatcher{&AnyExprMatcher{}},
	}
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)

	// ET_Const should not match
	ok, bindings = m.Match(mkConst(42), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestFuncExprMatcher_ManyExprTypeMatcher verifies FuncExprMatcher with
// a ManyExprTypeMatcher.
func TestFuncExprMatcher_ManyExprTypeMatcher(t *testing.T) {
	m := &FuncExprMatcher{
		TypeMatcher: NewManyExprTypeMatcher(ET_Func, ET_Const),
	}
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkConst(42), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	ok, bindings = m.Match(mkCol("t", "a", 1, 0), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestFuncExprMatcher_ChildrenFailRollback verifies that when a child
// matcher fails, the root binding is correctly removed from bindings.
func TestFuncExprMatcher_ChildrenFailRollback(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&ConstExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	// First child matches, second does not — root binding should be rolled back.
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1), mkCol("t", "a", 1, 0)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestComparisonExprMatcher_Unordered verifies ComparisonExprMatcher
// with PolicyUnordered (left/right can be in any order).
func TestComparisonExprMatcher_Unordered(t *testing.T) {
	m := &ComparisonExprMatcher{
		Children: []ExprMatcher{
			&ColumnExprMatcher{},
			&ConstExprMatcher{},
		},
		Policy: PolicyUnordered,
	}
	// col > const — matches
	ok, bindings := m.Match(mkFunc(FuncGreater, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// const > col — also matches because unordered
	ok, bindings = m.Match(mkFunc(FuncGreater, mkConst(1), mkCol("t", "a", 1, 0)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// col > col — no const, fails
	ok, bindings = m.Match(mkFunc(FuncGreater, mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestComparisonExprMatcher_Some verifies ComparisonExprMatcher with
// PolicySome (only one child needs to match the provided matchers).
func TestComparisonExprMatcher_Some(t *testing.T) {
	m := &ComparisonExprMatcher{
		Children: []ExprMatcher{
			&ConstExprMatcher{},
		},
		Policy: PolicySome,
	}
	// comparison with one constant side
	ok, bindings := m.Match(mkFunc(FuncEqual, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 2) // comparison + const

	// comparison with two constants — at least one matches
	ok, bindings = m.Match(mkFunc(FuncEqual, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)

	// comparison with no constants — fails
	ok, bindings = m.Match(mkFunc(FuncEqual, mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestAggregateExprMatcher_Unordered verifies AggregateExprMatcher
// with PolicyUnordered children.
func TestAggregateExprMatcher_Unordered(t *testing.T) {
	m := &AggregateExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: "sum"},
		Children: []ExprMatcher{
			&ColumnExprMatcher{},
			&ConstExprMatcher{},
		},
		Policy: PolicyUnordered,
	}
	expr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{
			mkConst(1),
			mkCol("t", "a", 1, 0),
		},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "sum", _funcTyp: AggregateFuncType},
		},
	}
	// const, col — unordered matches
	ok, bindings := m.Match(expr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// col, const — also matches
	expr.Children = []*Expr{mkCol("t", "a", 1, 0), mkConst(1)}
	ok, bindings = m.Match(expr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)
}

// TestAggregateExprMatcher_Some verifies AggregateExprMatcher with
// PolicySome children.
func TestAggregateExprMatcher_Some(t *testing.T) {
	m := &AggregateExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: "avg"},
		Children: []ExprMatcher{
			&ColumnExprMatcher{},
		},
		Policy: PolicySome,
	}
	expr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.DoubleType(),
		Children: []*Expr{
			mkConst(1),
			mkCol("t", "a", 1, 0),
		},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "avg", _funcTyp: AggregateFuncType},
		},
	}
	// at least one child is a column
	ok, bindings := m.Match(expr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 2) // avg + col

	// no column at all — fails
	expr.Children = []*Expr{mkConst(1), mkConst(2)}
	ok, bindings = m.Match(expr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestFuncExprMatcher_Unordered verifies FuncExprMatcher with
// PolicyUnordered for arithmetic operators.
func TestFuncExprMatcher_Unordered(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncAdd, FuncMultiply),
		Children: []ExprMatcher{
			&ColumnExprMatcher{},
			&ConstExprMatcher{},
		},
		Policy: PolicyUnordered,
	}
	// col + const — matches
	ok, bindings := m.Match(mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkConst(1)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// const * col — also matches (unordered, different function)
	ok, bindings = m.Match(mkFunc(FuncMultiply, mkConst(2), mkCol("t", "a", 1, 0)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)
}

// TestColumnExprMatcher_TableAndName verifies ColumnExprMatcher with
// both Table and Name filters applied simultaneously.
func TestColumnExprMatcher_TableAndName(t *testing.T) {
	m := &ColumnExprMatcher{Table: "t", Name: "a"}
	ok, _ := m.Match(mkCol("t", "a", 1, 0), nil)
	assert.True(t, ok)

	ok, _ = m.Match(mkCol("t", "b", 1, 1), nil)
	assert.False(t, ok)

	ok, _ = m.Match(mkCol("s", "a", 2, 0), nil)
	assert.False(t, ok)

	ok, _ = m.Match(mkCol("s", "b", 2, 1), nil)
	assert.False(t, ok)
}

// TestSetMatch_Some_MultipleMatchers verifies PolicySome with multiple
// matchers where only a subset of matchers needs to find a match.
func TestSetMatch_Some_MultipleMatchers(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{
		mkCol("t", "a", 1, 0),
		mkCol("t", "b", 1, 1),
		mkConst(1),
	}
	ok, bindings := SetMatch(matchers, entries, nil, PolicySome)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)
	// Verify we got one const and one column (order depends on backtracking)
	types := make(map[ET]int)
	for _, b := range bindings {
		types[b.Typ]++
	}
	assert.Equal(t, 1, types[ET_Const])
	assert.Equal(t, 1, types[ET_Column])
}

// TestSetMatch_Unordered_Backtracking verifies that Unordered policy
// correctly backtracks when multiple matchers could match the same entry.
func TestSetMatch_Unordered_Backtracking(t *testing.T) {
	// Three matchers: two Const, one Column
	// Three entries: two Const, one Column
	// The first Const matcher should not greedily consume both consts.
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkCol("t", "a", 1, 0), mkConst(2)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyUnordered)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)
	// Verify we got 2 consts and 1 column
	types := make(map[ET]int)
	for _, b := range bindings {
		types[b.Typ]++
	}
	assert.Equal(t, 2, types[ET_Const])
	assert.Equal(t, 1, types[ET_Column])
}

// TestSetMatch_Unordered_BacktrackingHard verifies a more complex
// backtracking scenario where the first few entries do not match early matchers.
func TestSetMatch_Unordered_BacktrackingHard(t *testing.T) {
	matchers := []ExprMatcher{
		&ColumnExprMatcher{},
		&ConstExprMatcher{},
		&ColumnExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkCol("t", "a", 1, 0), mkCol("t", "b", 1, 1)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicyUnordered)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)
	// Verify 2 columns and 1 const
	types := make(map[ET]int)
	for _, b := range bindings {
		types[b.Typ]++
	}
	assert.Equal(t, 2, types[ET_Column])
	assert.Equal(t, 1, types[ET_Const])
}

// TestArithmeticSimplificationPattern_DuckDB replicates DuckDB's
// ArithmeticSimplificationRule matcher pattern:
//   arithmetic_func(const, any) with SOME policy and integer type.
func TestArithmeticSimplificationPattern_DuckDB(t *testing.T) {
	// Pattern: +, -, *, / with (const_integer, any_integer) using SOME
	m := &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncAdd, FuncSubtract, FuncMultiply, FuncDivide),
		DataTypeMatcher: &IntegerDataTypeMatcher{},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&AnyExprMatcher{},
		},
		Policy: PolicySome,
	}

	// Build expression manually with integer type to satisfy DataTypeMatcher.
	addExpr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{
			mkConst(0),
			mkCol("t", "a", 1, 0),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncAdd}},
	}
	ok, bindings := m.Match(addExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// col + const — also matches because SOME
	addExpr.Children = []*Expr{mkCol("t", "a", 1, 0), mkConst(0)}
	ok, bindings = m.Match(addExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 3)

	// non-integer return type — should fail DataTypeMatcher
	addExpr.DataTyp = common.VarcharType()
	ok, bindings = m.Match(addExpr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestMoveConstantsPattern_DuckDB replicates DuckDB's MoveConstantsRule
// matcher pattern:
//   comparison( const, arithmetic(const, expr) ) with UNORDERED/SOME.
func TestMoveConstantsPattern_DuckDB(t *testing.T) {
	// Outer comparison: (const, arithmetic) with UNORDERED
	outer := &ComparisonExprMatcher{
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&FuncExprMatcher{
				FuncNameMatcher: NewManyFuncNameMatcher(FuncAdd, FuncSubtract, FuncMultiply),
				DataTypeMatcher: &IntegerDataTypeMatcher{},
				Children: []ExprMatcher{
					&ConstExprMatcher{},
					&AnyExprMatcher{},
				},
				Policy: PolicySome,
			},
		},
		Policy: PolicyUnordered,
	}

	// Build: col + 1 = 10
	arith := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{
			mkCol("t", "a", 1, 0),
			mkConst(1),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncAdd}},
	}
	comp := &Expr{
		Typ:     ET_Func,
		DataTyp: common.BooleanType(),
		Children: []*Expr{
			arith,
			mkConst(10),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncEqual}},
	}

	ok, bindings := outer.Match(comp, nil)
	assert.True(t, ok)
	// bindings: comparison, const(10), arith, const(1), col
	// (order may vary because outer uses UNORDERED)
	assert.Len(t, bindings, 5)

	// Verify the arithmetic expression is captured
	foundArith := false
	for _, b := range bindings {
		if b.Typ == ET_Func && b.Info != nil {
			if fi := b.GetFuncInfo(); fi != nil && fi.FunImpl != nil {
				if fi.FunImpl._name == FuncAdd {
					foundArith = true
				}
			}
		}
	}
	assert.True(t, foundArith, "arithmetic expression should be in bindings")
}

// TestNestedMatcher_FailRollback verifies that when a deeply nested
// matcher fails, no partial bindings leak out.
func TestNestedMatcher_FailRollback(t *testing.T) {
	// Pattern: +(const, *(const, col))
	innerMul := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncMultiply},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			&ColumnExprMatcher{},
		},
		Policy: PolicyOrdered,
	}
	outerAdd := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&ConstExprMatcher{},
			innerMul,
		},
		Policy: PolicyOrdered,
	}

	// Build: +(1, *(2, const)) — inner column matcher fails
	mulExpr := mkFunc(FuncMultiply, mkConst(2), mkConst(3))
	addExpr := mkFunc(FuncAdd, mkConst(1), mulExpr)

	ok, bindings := outerAdd.Match(addExpr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestFuncExprMatcher_ChildTypeMatcher verifies that child matchers
// with their own type matchers work correctly inside FuncExprMatcher.
func TestFuncExprMatcher_ChildTypeMatcher(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&FuncExprMatcher{
				FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncMultiply},
				Children: []ExprMatcher{
					&ConstExprMatcher{},
					&ColumnExprMatcher{},
				},
				Policy: PolicyOrdered,
			},
			&ConstExprMatcher{},
		},
		Policy: PolicyOrdered,
	}

	// +( *(2, col), 1 ) — matches
	mulExpr := mkFunc(FuncMultiply, mkConst(2), mkCol("t", "a", 1, 0))
	addExpr := mkFunc(FuncAdd, mulExpr, mkConst(1))
	ok, bindings := m.Match(addExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 5) // add, mul, const(2), col, const(1)

	// +( col, 1 ) — first child is not multiply, fails
	addExpr2 := mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkConst(1))
	ok, bindings = m.Match(addExpr2, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestFuncExprMatcher_DataTypeMatcherOnChildren verifies that child
// matchers with DataTypeMatcher constraints are respected.
func TestFuncExprMatcher_DataTypeMatcherOnChildren(t *testing.T) {
	m := &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAdd},
		Children: []ExprMatcher{
			&FuncExprMatcher{
				DataTypeMatcher: &IntegerDataTypeMatcher{},
				Children:        []ExprMatcher{&AnyExprMatcher{}},
			},
			&ConstExprMatcher{},
		},
		Policy: PolicyOrdered,
	}

	// First child is an integer-typed function expression
	childFunc := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{
			mkCol("t", "a", 1, 0),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncMultiply}},
	}
	addExpr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{
			childFunc,
			mkConst(1),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncAdd}},
	}
	ok, bindings := m.Match(addExpr, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 4)

	// First child has non-integer type — fails
	childFunc.DataTyp = common.VarcharType()
	ok, bindings = m.Match(addExpr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestComparisonExprMatcher_MismatchNotComparison verifies that
// ComparisonExprMatcher rejects non-comparison function expressions.
func TestComparisonExprMatcher_MismatchNotComparison(t *testing.T) {
	m := &ComparisonExprMatcher{
		Children: []ExprMatcher{&AnyExprMatcher{}, &AnyExprMatcher{}},
		Policy:   PolicyOrdered,
	}
	// Add is arithmetic, not comparison
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)

	// And is logical, not comparison
	andExpr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.BooleanType(),
		Children: []*Expr{
			mkConst(1),
			mkConst(0),
		},
		Info: &FunctionInfo{FunImpl: &Function{_name: FuncAnd}},
	}
	ok, bindings = m.Match(andExpr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestAggregateExprMatcher_MismatchNonAggregate verifies that
// AggregateExprMatcher rejects scalar function expressions.
func TestAggregateExprMatcher_MismatchNonAggregate(t *testing.T) {
	m := &AggregateExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: "sum"},
	}
	// mkFunc binds scalar functions, not aggregates
	ok, bindings := m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestAggregateExprMatcher_MismatchFuncName verifies that
// AggregateExprMatcher rejects aggregates with wrong name.
func TestAggregateExprMatcher_MismatchFuncName(t *testing.T) {
	m := &AggregateExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: "sum"},
	}
	expr := &Expr{
		Typ:     ET_Func,
		DataTyp: common.IntegerType(),
		Children: []*Expr{
			mkCol("t", "a", 1, 0),
		},
		Info: &FunctionInfo{
			FunImpl: &Function{_name: "avg", _funcTyp: AggregateFuncType},
		},
	}
	ok, bindings := m.Match(expr, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestExpressionEqualityMatcher_PointerEquality verifies that
// ExpressionEqualityMatcher uses pointer equality, not deep equality.
func TestExpressionEqualityMatcher_PointerEquality(t *testing.T) {
	target := mkConst(42)
	m := &ExpressionEqualityMatcher{Expression: target}

	// Same pointer — matches
	ok, bindings := m.Match(target, nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Different pointer, same value — does NOT match (pointer equality)
	other := mkConst(42)
	ok, bindings = m.Match(other, nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestStableExpressionMatcher_NonVolatileFunctions verifies that
// StableExpressionMatcher accepts function expressions (non-subquery).
func TestStableExpressionMatcher_NonVolatileFunctions(t *testing.T) {
	m := &StableExpressionMatcher{}

	// Column is stable
	ok, bindings := m.Match(mkCol("t", "a", 1, 0), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Const is stable
	ok, bindings = m.Match(mkConst(42), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Function expression is stable (not subquery)
	ok, bindings = m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)
}

// TestFoldableConstantMatcher_OnlyConst verifies that
// FoldableConstantMatcher currently matches only ET_Const.
func TestFoldableConstantMatcher_OnlyConst(t *testing.T) {
	m := &FoldableConstantMatcher{}

	ok, bindings := m.Match(mkConst(42), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Function of constants is foldable
	ok, bindings = m.Match(mkFunc(FuncAdd, mkConst(1), mkConst(2)), nil)
	assert.True(t, ok)
	assert.Len(t, bindings, 1)

	// Function with column is not foldable
	ok, bindings = m.Match(mkFunc(FuncAdd, mkCol("t", "a", 1, 0), mkConst(2)), nil)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestSetMatch_SomeOrdered_MultipleMatchers verifies PolicySomeOrdered
// with multiple matchers matching a prefix of entries.
func TestSetMatch_SomeOrdered_MultipleMatchers(t *testing.T) {
	matchers := []ExprMatcher{
		&ConstExprMatcher{},
		&ConstExprMatcher{},
	}
	entries := []*Expr{mkConst(1), mkConst(2), mkCol("t", "a", 1, 0)}
	ok, bindings := SetMatch(matchers, entries, nil, PolicySomeOrdered)
	assert.True(t, ok)
	assert.Len(t, bindings, 2)

	// Wrong prefix — fails
	entries2 := []*Expr{mkCol("t", "a", 1, 0), mkConst(1), mkConst(2)}
	ok, bindings = SetMatch(matchers, entries2, nil, PolicySomeOrdered)
	assert.False(t, ok)
	assert.Empty(t, bindings)
}

// TestLogicalOpWithChildMatcher_OutOfBounds verifies that
// LogicalOpWithChildMatcher returns false when child index is out of bounds.
func TestLogicalOpWithChildMatcher_OutOfBounds(t *testing.T) {
	m := &LogicalOpWithChildMatcher{
		OpMatcher:    &SpecificLogicalOpMatcher{Typ: LOT_Filter},
		ChildMatcher: &AnyLogicalOpMatcher{},
		ChildIndex:   5,
	}
	filter := &LogicalOperator{Typ: LOT_Filter, Children: []*LogicalOperator{{Typ: LOT_Scan}}}
	assert.False(t, m.Match(filter))
}

// TestLogicalOpWithChildMatcher_Nested verifies nested logical operator matching.
func TestLogicalOpWithChildMatcher_Nested(t *testing.T) {
	m := &LogicalOpWithChildMatcher{
		OpMatcher:    &SpecificLogicalOpMatcher{Typ: LOT_Project},
		ChildMatcher: &LogicalOpWithChildMatcher{
			OpMatcher:    &SpecificLogicalOpMatcher{Typ: LOT_Filter},
			ChildMatcher: &SpecificLogicalOpMatcher{Typ: LOT_Scan},
			ChildIndex:   0,
		},
		ChildIndex: 0,
	}
	plan := &LogicalOperator{
		Typ: LOT_Project,
		Children: []*LogicalOperator{{
			Typ:      LOT_Filter,
			Children: []*LogicalOperator{{Typ: LOT_Scan}},
		}},
	}
	assert.True(t, m.Match(plan))

	// Change inner scan to project — fails
	plan.Children[0].Children[0].Typ = LOT_Project
	assert.False(t, m.Match(plan))
}
