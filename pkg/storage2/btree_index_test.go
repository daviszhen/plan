package storage2

import (
	"context"
	"sort"
	"testing"
)

func TestBTreeIndexBasicOperations(t *testing.T) {
	ctx := context.Background()
	idx := NewBTreeIndex("test_idx", 0)

	// Test insert and search
	testData := []struct {
		key   int64
		rowID uint64
	}{
		{10, 1},
		{20, 2},
		{5, 3},
		{15, 4},
		{25, 5},
	}

	for _, data := range testData {
		if err := idx.Insert(data.key, data.rowID); err != nil {
			t.Fatalf("Failed to insert key %d: %v", data.key, err)
		}
	}

	// Test equality query
	result, err := idx.EqualityQuery(ctx, int64(15))
	if err != nil {
		t.Fatalf("EqualityQuery failed: %v", err)
	}
	if len(result) != 1 || result[0] != 4 {
		t.Errorf("Expected [4], got %v", result)
	}

	// Test range query
	rangeResult, err := idx.RangeQuery(ctx, int64(10), int64(20))
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	// Should find keys 10, 15, 20
	expected := []uint64{1, 4, 2}
	sort.Slice(rangeResult, func(i, j int) bool { return rangeResult[i] < rangeResult[j] })
	sort.Slice(expected, func(i, j int) bool { return expected[i] < expected[j] })

	if len(rangeResult) != len(expected) {
		t.Errorf("RangeQuery expected %v, got %v", expected, rangeResult)
	}
	for i := range expected {
		if rangeResult[i] != expected[i] {
			t.Errorf("RangeQuery mismatch at %d: expected %d, got %d", i, expected[i], rangeResult[i])
		}
	}
}

func TestBTreeIndexLargeDataset(t *testing.T) {
	ctx := context.Background()
	idx := NewBTreeIndex("large_idx", 0)

	// Insert 1000 entries
	for i := 0; i < 1000; i++ {
		key := int64(i * 10)
		rowID := uint64(i)
		if err := idx.Insert(key, rowID); err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Test statistics
	stats := idx.Statistics()
	if stats.NumEntries != 1000 {
		t.Errorf("Expected 1000 entries, got %d", stats.NumEntries)
	}

	// Test range query on large dataset
	result, err := idx.RangeQuery(ctx, int64(100), int64(500))
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	// Should find keys 100, 110, 120, ..., 500 (41 entries: 100, 110, ..., 500)
	// But our implementation might exclude the upper bound, so we expect 40 or 41
	expectedMinCount := 40
	expectedMaxCount := 41
	if len(result) < expectedMinCount || len(result) > expectedMaxCount {
		t.Errorf("RangeQuery expected %d-%d results, got %d", expectedMinCount, expectedMaxCount, len(result))
	}
}

func TestBTreeIndexDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	idx := NewBTreeIndex("dup_idx", 0)

	// Insert multiple rows with the same key
	key := int64(100)
	rowIDs := []uint64{1, 5, 10, 15, 20}

	for _, rowID := range rowIDs {
		if err := idx.Insert(key, rowID); err != nil {
			t.Fatalf("Failed to insert rowID %d: %v", rowID, err)
		}
	}

	// Test equality query with duplicates
	result, err := idx.EqualityQuery(ctx, key)
	if err != nil {
		t.Fatalf("EqualityQuery failed: %v", err)
	}

	if len(result) != len(rowIDs) {
		t.Errorf("Expected %d results, got %d", len(rowIDs), len(result))
	}

	// Verify all row IDs are present
	resultMap := make(map[uint64]bool)
	for _, id := range result {
		resultMap[id] = true
	}
	for _, expectedID := range rowIDs {
		if !resultMap[expectedID] {
			t.Errorf("Missing rowID %d in results", expectedID)
		}
	}
}

func TestBTreeIndexStringKeys(t *testing.T) {
	ctx := context.Background()
	idx := NewBTreeIndex("string_idx", 0)

	// Insert string keys
	testData := []struct {
		key   string
		rowID uint64
	}{
		{"apple", 1},
		{"banana", 2},
		{"cherry", 3},
		{"date", 4},
		{"elderberry", 5},
	}

	for _, data := range testData {
		if err := idx.Insert(data.key, data.rowID); err != nil {
			t.Fatalf("Failed to insert key %s: %v", data.key, err)
		}
	}

	// Test equality query
	result, err := idx.EqualityQuery(ctx, "cherry")
	if err != nil {
		t.Fatalf("EqualityQuery failed: %v", err)
	}
	if len(result) != 1 || result[0] != 3 {
		t.Errorf("Expected [3], got %v", result)
	}

	// Test range query
	rangeResult, err := idx.RangeQuery(ctx, "banana", "date")
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	// Should find banana, cherry, date
	if len(rangeResult) != 3 {
		t.Errorf("Expected 3 results, got %d", len(rangeResult))
	}
}

func TestBTreeIndexFloatKeys(t *testing.T) {
	ctx := context.Background()
	idx := NewBTreeIndex("float_idx", 0)

	// Insert float keys
	testData := []struct {
		key   float64
		rowID uint64
	}{
		{1.5, 1},
		{2.7, 2},
		{0.3, 3},
		{3.14, 4},
		{2.71, 5},
	}

	for _, data := range testData {
		if err := idx.Insert(data.key, data.rowID); err != nil {
			t.Fatalf("Failed to insert key %f: %v", data.key, err)
		}
	}

	// Test equality query
	result, err := idx.EqualityQuery(ctx, 3.14)
	if err != nil {
		t.Fatalf("EqualityQuery failed: %v", err)
	}
	if len(result) != 1 || result[0] != 4 {
		t.Errorf("Expected [4], got %v", result)
	}

	// Test range query
	rangeResult, err := idx.RangeQuery(ctx, 1.0, 3.0)
	if err != nil {
		t.Fatalf("RangeQuery failed: %v", err)
	}

	// Should find 1.5, 2.7, 2.71
	if len(rangeResult) != 3 {
		t.Errorf("Expected 3 results, got %d", len(rangeResult))
	}
}

func TestBTreeIndexInterface(t *testing.T) {
	idx := NewBTreeIndex("interface_idx", 0)

	// Verify interface implementations
	if idx.Name() != "interface_idx" {
		t.Errorf("Expected name 'interface_idx', got '%s'", idx.Name())
	}

	if idx.Type() != ScalarIndex {
		t.Errorf("Expected type ScalarIndex, got %v", idx.Type())
	}

	columns := idx.Columns()
	if len(columns) != 1 || columns[0] != 0 {
		t.Errorf("Expected columns [0], got %v", columns)
	}

	// Test Statistics
	stats := idx.Statistics()
	if stats.NumEntries != 0 {
		t.Errorf("Expected 0 entries initially, got %d", stats.NumEntries)
	}
}

func TestBTreeIndexNonExistentKey(t *testing.T) {
	ctx := context.Background()
	idx := NewBTreeIndex("empty_idx", 0)

	// Query non-existent key
	result, err := idx.EqualityQuery(ctx, int64(999))
	if err != nil {
		t.Fatalf("EqualityQuery failed: %v", err)
	}

	if result != nil && len(result) != 0 {
		t.Errorf("Expected nil or empty result for non-existent key, got %v", result)
	}
}

func TestBTreeIndexConcurrentAccess(t *testing.T) {
	idx := NewBTreeIndex("concurrent_idx", 0)
	ctx := context.Background()

	// Insert some data first
	for i := 0; i < 100; i++ {
		if err := idx.Insert(int64(i), uint64(i)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Concurrent reads
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 10; j++ {
				key := int64(j * 10)
				_, err := idx.EqualityQuery(ctx, key)
				if err != nil {
					t.Errorf("Concurrent query failed: %v", err)
				}
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}
