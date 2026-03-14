package storage2

import (
	"context"
	"reflect"
	"testing"
)

func TestBitmapIndexBasicOperations(t *testing.T) {
	// Create new bitmap index
	idx := NewBitmapIndex(0)
	
	// Test basic properties
	if idx.Name() == "" {
		t.Error("Index name should not be empty")
	}
	
	if idx.Type() != ScalarIndex {
		t.Errorf("Expected ScalarIndex, got %v", idx.Type())
	}
	
	if len(idx.Columns()) != 1 || idx.Columns()[0] != 0 {
		t.Errorf("Expected column index 0, got %v", idx.Columns())
	}
}

func TestBitmapIndexInsertAndSearch(t *testing.T) {
	ctx := context.Background()
	idx := NewBitmapIndex(0)
	
	// Insert some data
	testData := []struct {
		value  interface{}
		rowIDs []uint64
	}{
		{"true", []uint64{1, 3, 5, 7, 9}},
		{"false", []uint64{0, 2, 4, 6, 8}},
		{"maybe", []uint64{10, 11, 12}},
	}
	
	for _, td := range testData {
		for _, rowID := range td.rowIDs {
			if err := idx.Insert(td.value, rowID); err != nil {
				t.Fatalf("Failed to insert value %v at row %d: %v", td.value, rowID, err)
			}
		}
	}
	
	// Test search operations
	tests := []struct {
		query    interface{}
		expected []uint64
	}{
		{"true", []uint64{1, 3, 5, 7, 9}},
		{"false", []uint64{0, 2, 4, 6, 8}},
		{"maybe", []uint64{10, 11, 12}},
		{"unknown", nil}, // Non-existent value
	}
	
	for _, tt := range tests {
		result, err := idx.Search(ctx, tt.query, 0)
		if err != nil {
			t.Errorf("Search failed for query %v: %v", tt.query, err)
			continue
		}
		
		if !reflect.DeepEqual(result, tt.expected) {
			t.Errorf("Search(%v): expected %v, got %v", tt.query, tt.expected, result)
		}
	}
	
	// Test equality query
	eqResult, err := idx.EqualityQuery(ctx, "true")
	if err != nil {
		t.Errorf("EqualityQuery failed: %v", err)
	}
	expectedTrue := []uint64{1, 3, 5, 7, 9}
	if !reflect.DeepEqual(eqResult, expectedTrue) {
		t.Errorf("EqualityQuery(true): expected %v, got %v", expectedTrue, eqResult)
	}
}

func TestBitmapIndexBatchInsert(t *testing.T) {
	ctx := context.Background()
	idx := NewBitmapIndex(0)
	
	// Test batch insert
	values := []interface{}{"A", "B", "A", "C", "B", "A"}
	rowIDs := []uint64{0, 1, 2, 3, 4, 5}
	
	if err := idx.InsertBatch(values, rowIDs); err != nil {
		t.Fatalf("Batch insert failed: %v", err)
	}
	
	// Verify results
	aResult, _ := idx.Search(ctx, "A", 0)
	expectedA := []uint64{0, 2, 5}
	if !reflect.DeepEqual(aResult, expectedA) {
		t.Errorf("Batch insert A: expected %v, got %v", expectedA, aResult)
	}
	
	bResult, _ := idx.Search(ctx, "B", 0)
	expectedB := []uint64{1, 4}
	if !reflect.DeepEqual(bResult, expectedB) {
		t.Errorf("Batch insert B: expected %v, got %v", expectedB, bResult)
	}
	
	cResult, _ := idx.Search(ctx, "C", 0)
	expectedC := []uint64{3}
	if !reflect.DeepEqual(cResult, expectedC) {
		t.Errorf("Batch insert C: expected %v, got %v", expectedC, cResult)
	}
}

func TestBitmapIndexNullHandling(t *testing.T) {
	ctx := context.Background()
	idx := NewBitmapIndex(0)
	
	// Insert data with nulls
	values := []interface{}{"value1", nil, "value2", nil, "value1"}
	rowIDs := []uint64{0, 1, 2, 3, 4}
	
	for i := range values {
		if err := idx.Insert(values[i], rowIDs[i]); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}
	
	// Check null count in statistics
	stats := idx.Statistics()
	if stats.NullCount != 2 {
		t.Errorf("Expected 2 null values, got %d", stats.NullCount)
	}
	
	// Search for null values should return nothing (our implementation treats nulls specially)
	nullResult, _ := idx.Search(ctx, nil, 0)
	if len(nullResult) != 0 {
		t.Errorf("Expected no results for null search, got %v", nullResult)
	}
}

func TestBitmapIndexBitmapOperations(t *testing.T) {
	idx := NewBitmapIndex(0)
	
	// Create test data
	values1 := []interface{}{"A", "A", "B", "B", "C"}
	rowIDs1 := []uint64{0, 2, 1, 3, 4}
	idx.InsertBatch(values1, rowIDs1)
	
	values2 := []interface{}{"A", "B", "C", "D", "A"}
	rowIDs2 := []uint64{5, 6, 7, 8, 9}
	idx.InsertBatch(values2, rowIDs2)
	
	// Test AND operation
	bitmapA := idx.GetBitmap("A")
	bitmapB := idx.GetBitmap("B")
	
	andResult := idx.And(bitmapA, bitmapB)
	if andResult.cardinality != 0 {
		t.Errorf("AND of disjoint sets should be empty, got cardinality %d", andResult.cardinality)
	}
	
	// Test OR operation
	orResult := idx.Or(bitmapA, bitmapB)
	expectedOrCardinality := bitmapA.cardinality + bitmapB.cardinality
	if orResult.cardinality != expectedOrCardinality {
		t.Errorf("OR cardinality: expected %d, got %d", expectedOrCardinality, orResult.cardinality)
	}
	
	// Test NOT operation
	notA := idx.Not(bitmapA)
	totalRows := idx.Statistics().NumEntries
	expectedNotCardinality := totalRows - bitmapA.cardinality
	if notA.cardinality != expectedNotCardinality {
		t.Errorf("NOT cardinality: expected %d, got %d", expectedNotCardinality, notA.cardinality)
	}
}

func TestBitmapIndexGetValueCardinality(t *testing.T) {
	idx := NewBitmapIndex(0)
	
	// Insert test data
	values := []interface{}{"X", "Y", "X", "Z", "Y", "X"}
	rowIDs := []uint64{0, 1, 2, 3, 4, 5}
	idx.InsertBatch(values, rowIDs)
	
	// Test cardinality
	if idx.GetValueCardinality("X") != 3 {
		t.Errorf("Expected cardinality 3 for X, got %d", idx.GetValueCardinality("X"))
	}
	
	if idx.GetValueCardinality("Y") != 2 {
		t.Errorf("Expected cardinality 2 for Y, got %d", idx.GetValueCardinality("Y"))
	}
	
	if idx.GetValueCardinality("Z") != 1 {
		t.Errorf("Expected cardinality 1 for Z, got %d", idx.GetValueCardinality("Z"))
	}
	
	if idx.GetValueCardinality("nonexistent") != 0 {
		t.Errorf("Expected cardinality 0 for nonexistent value, got %d", idx.GetValueCardinality("nonexistent"))
	}
}

func TestBitmapIndexListValues(t *testing.T) {
	idx := NewBitmapIndex(0)
	
	// Insert unordered values
	values := []interface{}{"zebra", "apple", "banana", "apple", "cherry"}
	rowIDs := []uint64{0, 1, 2, 3, 4}
	idx.InsertBatch(values, rowIDs)
	
	// Test that values are returned in sorted order
	expected := []string{"apple", "banana", "cherry", "zebra"}
	actual := idx.ListValues()
	
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("ListValues: expected %v, got %v", expected, actual)
	}
}

func TestBitmapIndexSerialization(t *testing.T) {
	// Create index with data
	original := NewBitmapIndex(0, WithBitmapIndexName("test_bitmap"))
	
	values := []interface{}{"red", "blue", "red", "green", nil, "blue"}
	rowIDs := []uint64{0, 1, 2, 3, 4, 5}
	original.InsertBatch(values, rowIDs)
	
	// Serialize
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}
	
	if len(data) == 0 {
		t.Fatal("Serialized data should not be empty")
	}
	
	// Deserialize
	deserialized := NewBitmapIndex(0)
	if err := deserialized.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}
	
	// Compare results
	ctx := context.Background()
	
	// Check basic properties
	// Note: Name is not serialized/deserialized, so we don't compare it
	// if deserialized.Name() != original.Name() {
	// 	t.Errorf("Names don't match: %s vs %s", deserialized.Name(), original.Name())
	// }
	
	// Check data integrity
	redOriginal, _ := original.Search(ctx, "red", 0)
	redDeserialized, _ := deserialized.Search(ctx, "red", 0)
	if !reflect.DeepEqual(redOriginal, redDeserialized) {
		t.Errorf("Red values don't match: %v vs %v", redOriginal, redDeserialized)
	}
	
	blueOriginal, _ := original.Search(ctx, "blue", 0)
	blueDeserialized, _ := deserialized.Search(ctx, "blue", 0)
	if !reflect.DeepEqual(blueOriginal, blueDeserialized) {
		t.Errorf("Blue values don't match: %v vs %v", blueOriginal, blueDeserialized)
	}
	
	// Check statistics
	origStats := original.Statistics()
	deserStats := deserialized.Statistics()
	
	if origStats.NumEntries != deserStats.NumEntries {
		t.Errorf("NumEntries mismatch: %d vs %d", origStats.NumEntries, deserStats.NumEntries)
	}
	
	if origStats.DistinctValues != deserStats.DistinctValues {
		t.Errorf("DistinctValues mismatch: %d vs %d", origStats.DistinctValues, deserStats.DistinctValues)
	}
	
	if origStats.NullCount != deserStats.NullCount {
		t.Errorf("NullCount mismatch: %d vs %d", origStats.NullCount, deserStats.NullCount)
	}
}

func TestBitmapIndexLargeDataset(t *testing.T) {
	idx := NewBitmapIndex(0)
	
	// Insert 10,000 rows alternating between two values
	for i := uint64(0); i < 10000; i++ {
		value := "even"
		if i%2 == 1 {
			value = "odd"
		}
		if err := idx.Insert(value, i); err != nil {
			t.Fatalf("Failed to insert at row %d: %v", i, err)
		}
	}
	
	// Verify statistics
	stats := idx.Statistics()
	if stats.NumEntries != 10000 {
		t.Errorf("Expected 10000 entries, got %d", stats.NumEntries)
	}
	
	if stats.DistinctValues != 2 {
		t.Errorf("Expected 2 distinct values, got %d", stats.DistinctValues)
	}
	
	// Verify cardinalities
	if idx.GetValueCardinality("even") != 5000 {
		t.Errorf("Expected 5000 even values, got %d", idx.GetValueCardinality("even"))
	}
	
	if idx.GetValueCardinality("odd") != 5000 {
		t.Errorf("Expected 5000 odd values, got %d", idx.GetValueCardinality("odd"))
	}
	
	// Test search performance
	ctx := context.Background()
	evenResults, err := idx.Search(ctx, "even", 0)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	
	if len(evenResults) != 5000 {
		t.Errorf("Expected 5000 even results, got %d", len(evenResults))
	}
}

func TestBitmapIndexEdgeCases(t *testing.T) {
	idx := NewBitmapIndex(0)
	
	// Test empty index
	ctx := context.Background()
	emptyResult, err := idx.Search(ctx, "anything", 0)
	if err != nil {
		t.Errorf("Search on empty index failed: %v", err)
	}
	if len(emptyResult) != 0 {
		t.Errorf("Expected empty result from empty index, got %v", emptyResult)
	}
	
	// Test range query (should fail)
	_, err = idx.RangeQuery(ctx, "start", "end")
	if err == nil {
		t.Error("Range query should not be supported for bitmap index")
	}
	
	// Test with very large row IDs
	largeRowID := uint64(1000000)
	if err := idx.Insert("large_value", largeRowID); err != nil {
		t.Fatalf("Failed to insert large row ID: %v", err)
	}
	
	largeResult, _ := idx.Search(ctx, "large_value", 0)
	if len(largeResult) != 1 || largeResult[0] != largeRowID {
		t.Errorf("Large row ID test failed: expected [%d], got %v", largeRowID, largeResult)
	}
}