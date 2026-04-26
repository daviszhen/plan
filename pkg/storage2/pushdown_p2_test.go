// Copyright 2024 - Licensed under the Apache License
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// ============================================================================
// P2-3: pushdown.go CanPushdown tests
// ============================================================================

// TestCanPushdownColumnPredicate tests CanPushdown for ColumnPredicate.
func TestCanPushdownColumnPredicate(t *testing.T) {
	pred := &ColumnPredicate{
		ColumnIndex: 0,
		Op:          Eq,
		Value:       &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 42},
	}

	if !pred.CanPushdown() {
		t.Error("ColumnPredicate should be pushdown-able")
	}
}

// TestCanPushdownAndPredicate tests CanPushdown for AndPredicate.
func TestCanPushdownAndPredicate(t *testing.T) {
	left := &ColumnPredicate{ColumnIndex: 0, Op: Gt, Value: &chunk.Value{I64: 10}}
	right := &ColumnPredicate{ColumnIndex: 1, Op: Lt, Value: &chunk.Value{I64: 100}}

	and := &AndPredicate{Left: left, Right: right}

	if !and.CanPushdown() {
		t.Error("AndPredicate with two pushdownable children should be pushdown-able")
	}

	// Verify String()
	s := and.String()
	if s == "" {
		t.Error("AndPredicate.String() should not be empty")
	}
}

// TestCanPushdownOrPredicate tests CanPushdown for OrPredicate.
func TestCanPushdownOrPredicate(t *testing.T) {
	left := &ColumnPredicate{ColumnIndex: 0, Op: Eq, Value: &chunk.Value{I64: 1}}
	right := &ColumnPredicate{ColumnIndex: 0, Op: Eq, Value: &chunk.Value{I64: 2}}

	or := &OrPredicate{Left: left, Right: right}

	if !or.CanPushdown() {
		t.Error("OrPredicate with two pushdownable children should be pushdown-able")
	}

	// Verify String()
	s := or.String()
	if s == "" {
		t.Error("OrPredicate.String() should not be empty")
	}
}

// TestCanPushdownNotPredicate tests CanPushdown for NotPredicate.
func TestCanPushdownNotPredicate(t *testing.T) {
	inner := &ColumnPredicate{ColumnIndex: 0, Op: Eq, Value: &chunk.Value{I64: 42}}
	not := &NotPredicate{Inner: inner}

	if !not.CanPushdown() {
		t.Error("NotPredicate with pushdownable inner should be pushdown-able")
	}

	// Verify String()
	s := not.String()
	if s == "" {
		t.Error("NotPredicate.String() should not be empty")
	}
}

// TestCanPushdownNestedPredicates tests CanPushdown for deeply nested predicates.
func TestCanPushdownNestedPredicates(t *testing.T) {
	// Build: NOT ((col0 > 10 AND col1 < 20) OR col2 = 30)
	left := &AndPredicate{
		Left:  &ColumnPredicate{ColumnIndex: 0, Op: Gt, Value: &chunk.Value{I64: 10}},
		Right: &ColumnPredicate{ColumnIndex: 1, Op: Lt, Value: &chunk.Value{I64: 20}},
	}
	right := &ColumnPredicate{ColumnIndex: 2, Op: Eq, Value: &chunk.Value{I64: 30}}
	or := &OrPredicate{Left: left, Right: right}
	not := &NotPredicate{Inner: or}

	if !not.CanPushdown() {
		t.Error("Deeply nested predicate should be pushdown-able")
	}

	// Test String representation
	s := not.String()
	t.Logf("Nested predicate string: %s", s)
}

// TestComparisonOpString tests ComparisonOp.String() method.
func TestComparisonOpString(t *testing.T) {
	tests := []struct {
		op   ComparisonOp
		want string
	}{
		{Eq, "="},
		{Ne, "!="},
		{Lt, "<"},
		{Le, "<="},
		{Gt, ">"},
		{Ge, ">="},
		{ComparisonOp(99), "?"}, // Unknown op
	}

	for _, tt := range tests {
		got := tt.op.String()
		if got != tt.want {
			t.Errorf("ComparisonOp(%d).String(): got %q want %q", tt.op, got, tt.want)
		}
	}
}

// TestCompareValuesIntegerTypes tests compareValues with various integer types.
func TestCompareValuesIntegerTypes(t *testing.T) {
	// INTEGER vs INTEGER
	intVal1 := &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 10}
	intVal2 := &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 20}

	tests := []struct {
		a, b   *chunk.Value
		op     ComparisonOp
		expect bool
	}{
		{intVal1, intVal2, Eq, false},
		{intVal1, intVal2, Ne, true},
		{intVal1, intVal2, Lt, true},
		{intVal1, intVal2, Le, true},
		{intVal1, intVal2, Gt, false},
		{intVal1, intVal2, Ge, false},
		{intVal1, intVal1, Eq, true},
		{intVal1, intVal1, Le, true},
		{intVal1, intVal1, Ge, true},
	}

	for _, tt := range tests {
		got := compareValues(tt.a, tt.b, tt.op)
		if got != tt.expect {
			t.Errorf("compareValues(%v, %v, %v): got %v want %v", tt.a.I64, tt.b.I64, tt.op, got, tt.expect)
		}
	}
}

// TestCompareValuesNilHandling tests compareValues with nil values.
func TestCompareValuesNilHandling(t *testing.T) {
	val := &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 10}

	// Both nil
	if compareValues(nil, nil, Eq) {
		t.Error("nil vs nil should return false")
	}

	// One nil
	if compareValues(nil, val, Eq) {
		t.Error("nil vs value should return false")
	}
	if compareValues(val, nil, Eq) {
		t.Error("value vs nil should return false")
	}
}

// TestCompareValuesBigint tests compareValues with INTEGER vs BIGINT compatibility.
func TestCompareValuesBigint(t *testing.T) {
	intVal := &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 100}
	bigintVal := &chunk.Value{Typ: common.MakeLType(common.LTID_BIGINT), I64: 100}

	// INTEGER and BIGINT should be comparable
	if !compareValues(intVal, bigintVal, Eq) {
		t.Error("INTEGER 100 should equal BIGINT 100")
	}
	if compareValues(intVal, bigintVal, Ne) {
		t.Error("INTEGER 100 should not be != BIGINT 100")
	}
}

// TestCompareValuesFloat tests compareValues with float types.
func TestCompareValuesFloat(t *testing.T) {
	floatVal1 := &chunk.Value{Typ: common.MakeLType(common.LTID_FLOAT), F64: 3.14}
	floatVal2 := &chunk.Value{Typ: common.MakeLType(common.LTID_FLOAT), F64: 2.71}

	if !compareValues(floatVal1, floatVal2, Gt) {
		t.Error("3.14 should be > 2.71")
	}
	if !compareValues(floatVal2, floatVal1, Lt) {
		t.Error("2.71 should be < 3.14")
	}
}

// TestCompareValuesString tests compareValues with string types.
func TestCompareValuesString(t *testing.T) {
	strVal1 := &chunk.Value{Typ: common.MakeLType(common.LTID_VARCHAR), Str: "apple"}
	strVal2 := &chunk.Value{Typ: common.MakeLType(common.LTID_VARCHAR), Str: "banana"}

	if !compareValues(strVal1, strVal2, Lt) {
		t.Error("apple should be < banana")
	}
	if compareValues(strVal1, strVal2, Gt) {
		t.Error("apple should not be > banana")
	}
	if !compareValues(strVal1, strVal1, Eq) {
		t.Error("apple should == apple")
	}
}

// TestCompareValuesBoolean tests compareValues behavior with boolean types.
// Note: compareValues currently doesn't have explicit BOOLEAN support,
// so boolean comparisons return false.
func TestCompareValuesBoolean(t *testing.T) {
	trueVal := &chunk.Value{Typ: common.MakeLType(common.LTID_BOOLEAN), I64: 1}
	falseVal := &chunk.Value{Typ: common.MakeLType(common.LTID_BOOLEAN), I64: 0}

	// Since compareValues doesn't support BOOLEAN, all comparisons return false
	// This test documents the current behavior
	result := compareValues(trueVal, trueVal, Eq)
	t.Logf("compareValues(true, true, Eq) = %v (BOOLEAN not fully supported)", result)

	result = compareValues(falseVal, falseVal, Eq)
	t.Logf("compareValues(false, false, Eq) = %v (BOOLEAN not fully supported)", result)

	result = compareValues(trueVal, falseVal, Ne)
	t.Logf("compareValues(true, false, Ne) = %v (BOOLEAN not fully supported)", result)
}

// ============================================================================
// Additional pushdown tests
// ============================================================================

// TestEvaluateAndPredicate tests AndPredicate.Evaluate.
func TestEvaluateAndPredicate(t *testing.T) {
	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	// Set values: [10, 20, 30, 40]
	col := c.Data[0]
	col.SetValue(0, &chunk.Value{Typ: typs[0], I64: 10})
	col.SetValue(1, &chunk.Value{Typ: typs[0], I64: 20})
	col.SetValue(2, &chunk.Value{Typ: typs[0], I64: 30})
	col.SetValue(3, &chunk.Value{Typ: typs[0], I64: 40})

	// Predicate: col0 > 15 AND col0 < 35
	left := &ColumnPredicate{ColumnIndex: 0, Op: Gt, Value: &chunk.Value{Typ: typs[0], I64: 15}}
	right := &ColumnPredicate{ColumnIndex: 0, Op: Lt, Value: &chunk.Value{Typ: typs[0], I64: 35}}
	and := &AndPredicate{Left: left, Right: right}

	result, err := and.Evaluate(c)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Expected: [false, true, true, false]
	expected := []bool{false, true, true, false}
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("row %d: got %v want %v", i, result[i], want)
		}
	}
}

// TestEvaluateOrPredicate tests OrPredicate.Evaluate.
func TestEvaluateOrPredicate(t *testing.T) {
	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	col := c.Data[0]
	col.SetValue(0, &chunk.Value{Typ: typs[0], I64: 10})
	col.SetValue(1, &chunk.Value{Typ: typs[0], I64: 20})
	col.SetValue(2, &chunk.Value{Typ: typs[0], I64: 30})
	col.SetValue(3, &chunk.Value{Typ: typs[0], I64: 40})

	// Predicate: col0 < 15 OR col0 > 35
	left := &ColumnPredicate{ColumnIndex: 0, Op: Lt, Value: &chunk.Value{Typ: typs[0], I64: 15}}
	right := &ColumnPredicate{ColumnIndex: 0, Op: Gt, Value: &chunk.Value{Typ: typs[0], I64: 35}}
	or := &OrPredicate{Left: left, Right: right}

	result, err := or.Evaluate(c)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Expected: [true, false, false, true]
	expected := []bool{true, false, false, true}
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("row %d: got %v want %v", i, result[i], want)
		}
	}
}

// TestEvaluateNotPredicate tests NotPredicate.Evaluate.
func TestEvaluateNotPredicate(t *testing.T) {
	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	col := c.Data[0]
	col.SetValue(0, &chunk.Value{Typ: typs[0], I64: 10})
	col.SetValue(1, &chunk.Value{Typ: typs[0], I64: 20})
	col.SetValue(2, &chunk.Value{Typ: typs[0], I64: 30})
	col.SetValue(3, &chunk.Value{Typ: typs[0], I64: 40})

	// Predicate: NOT (col0 = 20)
	inner := &ColumnPredicate{ColumnIndex: 0, Op: Eq, Value: &chunk.Value{Typ: typs[0], I64: 20}}
	not := &NotPredicate{Inner: inner}

	result, err := not.Evaluate(c)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	// Expected: [true, false, true, true]
	expected := []bool{true, false, true, true}
	for i, want := range expected {
		if result[i] != want {
			t.Errorf("row %d: got %v want %v", i, result[i], want)
		}
	}
}
