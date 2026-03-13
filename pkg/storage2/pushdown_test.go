package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

func TestColumnPredicate(t *testing.T) {
	// Create a test chunk
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_VARCHAR),
	}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	// Set values: [1, "a"], [2, "b"], [3, "c"], [4, "d"]
	for i := 0; i < 4; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 1)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], Str: string(rune('a' + i))})
	}

	// Test equality predicate on column 0
	pred := NewColumnPredicate(0, Eq, &chunk.Value{Typ: typs[0], I64: 2})
	mask, err := pred.Evaluate(c)
	if err != nil {
		t.Fatal(err)
	}

	expected := []bool{false, true, false, false}
	for i, exp := range expected {
		if mask[i] != exp {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], exp)
		}
	}

	// Test greater than predicate
	pred = NewColumnPredicate(0, Gt, &chunk.Value{Typ: typs[0], I64: 2})
	mask, err = pred.Evaluate(c)
	if err != nil {
		t.Fatal(err)
	}

	expected = []bool{false, false, true, true}
	for i, exp := range expected {
		if mask[i] != exp {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], exp)
		}
	}
}

func TestAndPredicate(t *testing.T) {
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	for i := 0; i < 4; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 1)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
	}

	// col[0] > 1 AND col[0] < 4
	left := NewColumnPredicate(0, Gt, &chunk.Value{Typ: typs[0], I64: 1})
	right := NewColumnPredicate(0, Lt, &chunk.Value{Typ: typs[0], I64: 4})
	pred := NewAndPredicate(left, right)

	mask, err := pred.Evaluate(c)
	if err != nil {
		t.Fatal(err)
	}

	expected := []bool{false, true, true, false}
	for i, exp := range expected {
		if mask[i] != exp {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], exp)
		}
	}
}

func TestOrPredicate(t *testing.T) {
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	for i := 0; i < 4; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 1)})
	}

	// col[0] = 1 OR col[0] = 4
	left := NewColumnPredicate(0, Eq, &chunk.Value{Typ: typs[0], I64: 1})
	right := NewColumnPredicate(0, Eq, &chunk.Value{Typ: typs[0], I64: 4})
	pred := NewOrPredicate(left, right)

	mask, err := pred.Evaluate(c)
	if err != nil {
		t.Fatal(err)
	}

	expected := []bool{true, false, false, true}
	for i, exp := range expected {
		if mask[i] != exp {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], exp)
		}
	}
}

func TestNotPredicate(t *testing.T) {
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, 4)
	c.SetCard(4)

	for i := 0; i < 4; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 1)})
	}

	// NOT (col[0] = 2)
	inner := NewColumnPredicate(0, Eq, &chunk.Value{Typ: typs[0], I64: 2})
	pred := NewNotPredicate(inner)

	mask, err := pred.Evaluate(c)
	if err != nil {
		t.Fatal(err)
	}

	expected := []bool{true, false, true, true}
	for i, exp := range expected {
		if mask[i] != exp {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], exp)
		}
	}
}

func TestPushdownScanner(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create test data
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{
		NewDataFragment(0, []*DataFile{
			NewDataFile("data/0.dat", []int32{0}, 1, 0),
		}),
	}

	// Write test chunk
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_VARCHAR),
	}
	testChunk := &chunk.Chunk{}
	testChunk.Init(typs, 4)
	testChunk.SetCard(4)
	for i := 0; i < 4; i++ {
		testChunk.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 1)})
		testChunk.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], Str: string(rune('a' + i))})
	}

	if err := WriteChunkToFile(basePath+"/data/0.dat", testChunk); err != nil {
		t.Fatal(err)
	}

	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// Test scan without filter
	scanner := NewPushdownScanner(basePath, handler, 0, nil)
	results, err := scanner.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("len(results) = %d, want 1", len(results))
	}
	if results[0].Card() != 4 {
		t.Errorf("cardinality = %d, want 4", results[0].Card())
	}

	// Test scan with filter (col[0] > 2)
	config := DefaultScanConfig()
	config.Filter = NewColumnPredicate(0, Gt, &chunk.Value{Typ: typs[0], I64: 2})
	scanner = NewPushdownScanner(basePath, handler, 0, config)
	results, err = scanner.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("len(results) = %d, want 1", len(results))
	}
	if results[0].Card() != 2 {
		t.Errorf("cardinality after filter = %d, want 2", results[0].Card())
	}

	// Test scan with projection (only column 1)
	config = DefaultScanConfig()
	config.Columns = []int{1}
	scanner = NewPushdownScanner(basePath, handler, 0, config)
	results, err = scanner.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].ColumnCount() != 1 {
		t.Errorf("column count = %d, want 1", results[0].ColumnCount())
	}

	// Test scan with limit
	config = DefaultScanConfig()
	config.Limit = 2
	scanner = NewPushdownScanner(basePath, handler, 0, config)
	results, err = scanner.Scan(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Card() != 2 {
		t.Errorf("cardinality with limit = %d, want 2", results[0].Card())
	}
}

func TestComparisonOps(t *testing.T) {
	tests := []struct {
		op       ComparisonOp
		a, b     int64
		expected bool
	}{
		{Eq, 1, 1, true},
		{Eq, 1, 2, false},
		{Ne, 1, 2, true},
		{Ne, 1, 1, false},
		{Lt, 1, 2, true},
		{Lt, 2, 1, false},
		{Le, 1, 1, true},
		{Le, 1, 2, true},
		{Le, 2, 1, false},
		{Gt, 2, 1, true},
		{Gt, 1, 2, false},
		{Ge, 1, 1, true},
		{Ge, 2, 1, true},
		{Ge, 1, 2, false},
	}

	for _, tt := range tests {
		result := compareValues(
			&chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: tt.a},
			&chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: tt.b},
			tt.op,
		)
		if result != tt.expected {
			t.Errorf("%d %s %d = %v, want %v", tt.a, tt.op, tt.b, result, tt.expected)
		}
	}
}

func TestPredicateString(t *testing.T) {
	pred := NewColumnPredicate(0, Eq, &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 42})
	str := pred.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	andPred := NewAndPredicate(pred, pred)
	str = andPred.String()
	if str == "" {
		t.Error("AND String() returned empty string")
	}

	orPred := NewOrPredicate(pred, pred)
	str = orPred.String()
	if str == "" {
		t.Error("OR String() returned empty string")
	}

	notPred := NewNotPredicate(pred)
	str = notPred.String()
	if str == "" {
		t.Error("NOT String() returned empty string")
	}
}
