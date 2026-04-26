package compute

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 1.5: Expr unit tests

func TestExpr_InfoAccessor_CorrectType(t *testing.T) {
	// When Info is nil, accessor panics (no more embedded fallback)
	e := &Expr{Typ: ET_Func, Info: &FunctionInfo{FunImpl: &Function{_name: "sum"}}}
	fi := e.GetFuncInfo()
	require.NotNil(t, fi)
	assert.Equal(t, "sum", fi.FunImpl._name)

	// When Info is set, accessor returns Info
	info := &FunctionInfo{FunImpl: &Function{_name: "count"}}
	e2 := &Expr{Typ: ET_Func, Info: info}
	fi2 := e2.GetFuncInfo()
	require.NotNil(t, fi2)
	assert.Equal(t, "count", fi2.FunImpl._name)
}

func TestExpr_InfoAccessor_AllTypes(t *testing.T) {
	t.Run("SubqueryInfo", func(t *testing.T) {
		info := &SubqueryInfo{SubqueryTyp: ET_SubqueryTypeExists}
		e := &Expr{Typ: ET_Subquery, Info: info}
		assert.Equal(t, ET_SubqueryTypeExists, e.GetSubqueryInfo().SubqueryTyp)
	})
	t.Run("JoinInfo", func(t *testing.T) {
		info := &JoinInfo{JoinTyp: ET_JoinTypeLeft}
		e := &Expr{Typ: ET_Join, Info: info}
		assert.Equal(t, ET_JoinTypeLeft, e.GetJoinInfo().JoinTyp)
	})
	t.Run("TableInfo", func(t *testing.T) {
		info := &TableInfo{ColName2Idx: map[string]int{"a": 0}}
		e := &Expr{Typ: ET_TABLE, Info: info}
		assert.Equal(t, 0, e.GetTableInfo().ColName2Idx["a"])
	})
	t.Run("ValuesListInfo", func(t *testing.T) {
		info := &ValuesListInfo{Names: []string{"x"}}
		e := &Expr{Typ: ET_ValuesList, Info: info}
		assert.Equal(t, "x", e.GetValuesListInfo().Names[0])
	})
	t.Run("OrderByInfo", func(t *testing.T) {
		info := &OrderByInfo{Desc: true}
		e := &Expr{Typ: ET_Orderby, Info: info}
		assert.True(t, e.GetOrderByInfo().Desc)
	})
	t.Run("CTEInfo", func(t *testing.T) {
		info := &CTEInfo{CTEIndex: 7}
		e := &Expr{Typ: ET_CTE, Info: info}
		assert.Equal(t, uint64(7), e.GetCTEInfo().CTEIndex)
	})
}

func TestExpr_InfoAccessor_WrongType_Panics(t *testing.T) {
	e := &Expr{Typ: ET_Const, Info: &OrderByInfo{Desc: true}}
	assert.Panics(t, func() { e.GetFuncInfo() })
}

func TestExpr_Copy_ByType(t *testing.T) {
	tests := []struct {
		name string
		expr *Expr
	}{
		{"Column", colExpr("t", "c", 1, 0, common.IntegerType())},
		{"Const_Int", intConst(42)},
		{"Const_String", strConst("hello")},
		{"Const_Null", &Expr{Typ: ET_Const, ConstValue: NewNullConst(), DataTyp: common.IntegerType()}},
		{"Func", cmpExpr("=", intConst(1), intConst(2))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := tt.expr.copy()
			assert.True(t, tt.expr.equal(cp), "copy should be equal to original")
			// Modify copy should not affect original
			origTyp := tt.expr.DataTyp
			cp.DataTyp = common.DoubleType()
			assert.Equal(t, origTyp, tt.expr.DataTyp, "modifying copy should not affect original")
		})
	}
}

func TestExpr_Equal_DifferentTypes(t *testing.T) {
	a := intConst(1)
	b := colExpr("t", "c", 1, 0, common.IntegerType())
	assert.False(t, a.equal(b))
}

func TestExpr_Equal_SameTypeDifferentValue(t *testing.T) {
	a := intConst(1)
	b := intConst(2)
	assert.False(t, a.equal(b))
}

func TestConstValue_Copy(t *testing.T) {
	c := NewIntervalConst(10, "day")
	cp := c.copy()
	assert.True(t, c.equal(cp))
	cp.Interval.Value = 20
	assert.False(t, c.equal(cp), "copy should be independent")
}

func TestConstValue_Equal_AllTypes(t *testing.T) {
	pairs := []struct {
		name  string
		a, b  ConstValue
		equal bool
	}{
		{"int_eq", NewIntegerConst(1), NewIntegerConst(1), true},
		{"int_ne", NewIntegerConst(1), NewIntegerConst(2), false},
		{"str_eq", NewStringConst("a"), NewStringConst("a"), true},
		{"str_ne", NewStringConst("a"), NewStringConst("b"), false},
		{"bool_eq", NewBooleanConst(true), NewBooleanConst(true), true},
		{"bool_ne", NewBooleanConst(true), NewBooleanConst(false), false},
		{"null_eq", NewNullConst(), NewNullConst(), true},
		{"date_eq", NewDateConst("2024-01-01"), NewDateConst("2024-01-01"), true},
		{"date_ne", NewDateConst("2024-01-01"), NewDateConst("2024-01-02"), false},
		{"float_eq", NewFloatConst(1.5), NewFloatConst(1.5), true},
		{"float_ne", NewFloatConst(1.5), NewFloatConst(2.5), false},
		{"interval_eq", NewIntervalConst(1, "day"), NewIntervalConst(1, "day"), true},
		{"interval_ne", NewIntervalConst(1, "day"), NewIntervalConst(2, "day"), false},
		{"decimal_eq", NewDecimalConst("1.23"), NewDecimalConst("1.23"), true},
		{"decimal_ne", NewDecimalConst("1.23"), NewDecimalConst("4.56"), false},
		{"diff_type", NewIntegerConst(1), NewStringConst("1"), false},
	}
	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			assert.Equal(t, p.equal, p.a.equal(p.b))
		})
	}
}
