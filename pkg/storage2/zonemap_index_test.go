package storage2

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestZoneMapIndexBasicOperations(t *testing.T) {
	// Create new zone map index
	idx := NewZoneMapIndex()
	
	// Test basic properties
	if idx.Name() == "" {
		t.Error("Index name should not be empty")
	}
	
	if idx.Type() != ScalarIndex {
		t.Errorf("Expected ScalarIndex, got %v", idx.Type())
	}
}

func TestZoneMapIndexUpdateZoneMap(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Update zone map with some values
	values := []struct {
		columnIdx int
		value     interface{}
		isNull    bool
	}{
		{0, int64(10), false},
		{0, int64(20), false},
		{0, int64(5), false},
		{0, nil, true}, // null value
		{0, int64(30), false},
	}
	
	for _, v := range values {
		if err := idx.UpdateZoneMap(v.columnIdx, v.value, v.isNull); err != nil {
			t.Fatalf("Failed to update zone map: %v", err)
		}
	}
	
	// Check zone map
	zm := idx.GetZoneMap(0)
	if zm == nil {
		t.Fatal("Zone map should not be nil")
	}
	
	if zm.RowCount != 5 {
		t.Errorf("Expected row count 5, got %d", zm.RowCount)
	}
	
	if zm.NullCount != 1 {
		t.Errorf("Expected null count 1, got %d", zm.NullCount)
	}
	
	if !zm.Initialized {
		t.Error("Zone map should be initialized")
	}
	
	// Check min/max
	minVal, ok := zm.MinValue.(int64)
	if !ok {
		t.Fatalf("Expected int64 min value, got %T", zm.MinValue)
	}
	if minVal != 5 {
		t.Errorf("Expected min value 5, got %d", minVal)
	}
	
	maxVal, ok := zm.MaxValue.(int64)
	if !ok {
		t.Fatalf("Expected int64 max value, got %T", zm.MaxValue)
	}
	if maxVal != 30 {
		t.Errorf("Expected max value 30, got %d", maxVal)
	}
}

func TestZoneMapIndexUpdateZoneMapBatch(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Batch update
	values := []interface{}{int64(100), int64(200), int64(50), int64(150)}
	nullCount := uint64(2)
	
	if err := idx.UpdateZoneMapBatch(0, values, nullCount); err != nil {
		t.Fatalf("Failed to batch update zone map: %v", err)
	}
	
	zm := idx.GetZoneMap(0)
	if zm == nil {
		t.Fatal("Zone map should not be nil")
	}
	
	if zm.RowCount != 6 { // 4 values + 2 nulls
		t.Errorf("Expected row count 6, got %d", zm.RowCount)
	}
	
	if zm.NullCount != 2 {
		t.Errorf("Expected null count 2, got %d", zm.NullCount)
	}
	
	// Check min/max
	if zm.MinValue.(int64) != 50 {
		t.Errorf("Expected min 50, got %v", zm.MinValue)
	}
	
	if zm.MaxValue.(int64) != 200 {
		t.Errorf("Expected max 200, got %v", zm.MaxValue)
	}
}

func TestZoneMapIndexFragmentZoneMaps(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Update fragment zone maps
	fragment1Values := []interface{}{int64(1), int64(2), int64(3)}
	fragment2Values := []interface{}{int64(10), int64(20), int64(30)}
	
	// Fragment 1
	for _, v := range fragment1Values {
		if err := idx.UpdateFragmentZoneMap(1, 0, v, false); err != nil {
			t.Fatalf("Failed to update fragment zone map: %v", err)
		}
	}
	
	// Fragment 2
	for _, v := range fragment2Values {
		if err := idx.UpdateFragmentZoneMap(2, 0, v, false); err != nil {
			t.Fatalf("Failed to update fragment zone map: %v", err)
		}
	}
	
	// Check fragment zone maps
	zm1 := idx.GetFragmentZoneMap(1, 0)
	if zm1 == nil {
		t.Fatal("Fragment 1 zone map should not be nil")
	}
	
	if zm1.MinValue.(int64) != 1 || zm1.MaxValue.(int64) != 3 {
		t.Errorf("Fragment 1: expected [1, 3], got [%v, %v]", zm1.MinValue, zm1.MaxValue)
	}
	
	zm2 := idx.GetFragmentZoneMap(2, 0)
	if zm2 == nil {
		t.Fatal("Fragment 2 zone map should not be nil")
	}
	
	if zm2.MinValue.(int64) != 10 || zm2.MaxValue.(int64) != 30 {
		t.Errorf("Fragment 2: expected [10, 30], got [%v, %v]", zm2.MinValue, zm2.MaxValue)
	}
}

func TestZoneMapIndexPruning(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Set up zone map with range [10, 100]
	values := []interface{}{int64(10), int64(50), int64(100)}
	for _, v := range values {
		if err := idx.UpdateZoneMap(0, v, false); err != nil {
			t.Fatalf("Failed to update zone map: %v", err)
		}
	}
	
	tests := []struct {
		op       string
		value    interface{}
		canPrune bool
		desc     string
	}{
		{"=", int64(5), true, "value below min"},
		{"=", int64(150), true, "value above max"},
		{"=", int64(50), false, "value in range"},
		{"<", int64(10), true, "less than min"},
		{"<", int64(50), false, "less than mid"},
		{">", int64(100), true, "greater than max"},
		{">", int64(50), false, "greater than mid"},
		{"<=", int64(9), true, "less than or equal below min"},
		{">=", int64(101), true, "greater than or equal above max"},
	}
	
	for _, tt := range tests {
		result := idx.CanPrune(0, tt.op, tt.value)
		if result != tt.canPrune {
			t.Errorf("%s: CanPrune(%s, %v) = %v, expected %v", 
				tt.desc, tt.op, tt.value, result, tt.canPrune)
		}
	}
}

func TestZoneMapIndexRangePruning(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Set up zone map with range [10, 100]
	values := []interface{}{int64(10), int64(50), int64(100)}
	for _, v := range values {
		if err := idx.UpdateZoneMap(0, v, false); err != nil {
			t.Fatalf("Failed to update zone map: %v", err)
		}
	}
	
	tests := []struct {
		start    interface{}
		end      interface{}
		canPrune bool
		desc     string
	}{
		{int64(0), int64(5), true, "range completely below"},
		{int64(150), int64(200), true, "range completely above"},
		{int64(5), int64(15), false, "range overlaps at start"},
		{int64(90), int64(110), false, "range overlaps at end"},
		{int64(20), int64(80), false, "range completely inside"},
		{int64(0), int64(200), false, "range completely covers"},
	}
	
	for _, tt := range tests {
		result := idx.CanPruneRange(0, tt.start, tt.end)
		if result != tt.canPrune {
			t.Errorf("%s: CanPruneRange(%v, %v) = %v, expected %v",
				tt.desc, tt.start, tt.end, result, tt.canPrune)
		}
	}
}

func TestZoneMapIndexFragmentPruning(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Fragment 1: [1, 10]
	for i := 1; i <= 10; i++ {
		if err := idx.UpdateFragmentZoneMap(1, 0, int64(i), false); err != nil {
			t.Fatalf("Failed to update fragment zone map: %v", err)
		}
	}
	
	// Fragment 2: [100, 200]
	for i := 100; i <= 200; i += 10 {
		if err := idx.UpdateFragmentZoneMap(2, 0, int64(i), false); err != nil {
			t.Fatalf("Failed to update fragment zone map: %v", err)
		}
	}
	
	// Fragment 3: [1000, 2000]
	for i := 1000; i <= 2000; i += 100 {
		if err := idx.UpdateFragmentZoneMap(3, 0, int64(i), false); err != nil {
			t.Fatalf("Failed to update fragment zone map: %v", err)
		}
	}
	
	// Test pruning for value = 50
	// Fragment 1: [1, 10] - 50 > 10, can prune
	// Fragment 2: [100, 200] - 50 < 100, can prune
	// Fragment 3: [1000, 2000] - 50 < 1000, can prune
	// All fragments can be pruned for value 50
	prunable := idx.GetPrunableFragments(0, "=", int64(50))
	expectedPrunable := []uint64{1, 2, 3} // All fragments don't contain 50
	
	if len(prunable) != len(expectedPrunable) {
		t.Errorf("Expected %d prunable fragments, got %d", len(expectedPrunable), len(prunable))
	}
	
	for _, fragID := range expectedPrunable {
		found := false
		for _, p := range prunable {
			if p == fragID {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected fragment %d to be prunable", fragID)
		}
	}
}

func TestZoneMapIndexDifferentTypes(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Test with different data types
	tests := []struct {
		columnIdx int
		values    []interface{}
		expected  struct {
			min interface{}
			max interface{}
		}
	}{
		{0, []interface{}{int32(10), int32(20), int32(5)}, struct{ min, max interface{} }{int32(5), int32(20)}},
		{1, []interface{}{float64(1.5), float64(3.5), float64(2.5)}, struct{ min, max interface{} }{float64(1.5), float64(3.5)}},
		{2, []interface{}{"zebra", "apple", "banana"}, struct{ min, max interface{} }{"apple", "zebra"}},
		{3, []interface{}{true, false, true}, struct{ min, max interface{} }{false, true}},
	}
	
	for _, tt := range tests {
		for _, v := range tt.values {
			if err := idx.UpdateZoneMap(tt.columnIdx, v, false); err != nil {
				t.Fatalf("Failed to update zone map for column %d: %v", tt.columnIdx, err)
			}
		}
		
		zm := idx.GetZoneMap(tt.columnIdx)
		if zm == nil {
			t.Fatalf("Zone map for column %d should not be nil", tt.columnIdx)
		}
		
		if !reflect.DeepEqual(zm.MinValue, tt.expected.min) {
			t.Errorf("Column %d: expected min %v, got %v", tt.columnIdx, tt.expected.min, zm.MinValue)
		}
		
		if !reflect.DeepEqual(zm.MaxValue, tt.expected.max) {
			t.Errorf("Column %d: expected max %v, got %v", tt.columnIdx, tt.expected.max, zm.MaxValue)
		}
	}
}

func TestZoneMapIndexSerialization(t *testing.T) {
	// Create index with data
	original := NewZoneMapIndex(WithZoneMapIndexName("test_zonemap"))
	
	// Add column-level zone maps
	for i := 0; i < 3; i++ {
		for j := 0; j < 10; j++ {
			if err := original.UpdateZoneMap(i, int64(j*10), false); err != nil {
				t.Fatalf("Failed to update zone map: %v", err)
			}
		}
	}
	
	// Add fragment-level zone maps
	for fragID := uint64(1); fragID <= 3; fragID++ {
		for col := 0; col < 2; col++ {
			for j := 0; j < 5; j++ {
				if err := original.UpdateFragmentZoneMap(fragID, col, int64(fragID*100+uint64(j)), false); err != nil {
					t.Fatalf("Failed to update fragment zone map: %v", err)
				}
			}
		}
	}
	
	// Serialize
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}
	
	if len(data) == 0 {
		t.Fatal("Serialized data should not be empty")
	}
	
	// Deserialize
	deserialized := NewZoneMapIndex()
	if err := deserialized.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}
	
	// Compare zone maps
	for col := 0; col < 3; col++ {
		origZM := original.GetZoneMap(col)
		deserZM := deserialized.GetZoneMap(col)
		
		if origZM.RowCount != deserZM.RowCount {
			t.Errorf("Column %d: RowCount mismatch: %d vs %d", col, origZM.RowCount, deserZM.RowCount)
		}
		
		if origZM.MinValue.(int64) != deserZM.MinValue.(int64) {
			t.Errorf("Column %d: MinValue mismatch: %v vs %v", col, origZM.MinValue, deserZM.MinValue)
		}
		
		if origZM.MaxValue.(int64) != deserZM.MaxValue.(int64) {
			t.Errorf("Column %d: MaxValue mismatch: %v vs %v", col, origZM.MaxValue, deserZM.MaxValue)
		}
	}
	
	// Compare fragment zone maps
	for fragID := uint64(1); fragID <= 3; fragID++ {
		for col := 0; col < 2; col++ {
			origZM := original.GetFragmentZoneMap(fragID, col)
			deserZM := deserialized.GetFragmentZoneMap(fragID, col)
			
			if origZM == nil || deserZM == nil {
				t.Errorf("Fragment %d, Column %d: zone map should not be nil", fragID, col)
				continue
			}
			
			if origZM.MinValue.(int64) != deserZM.MinValue.(int64) {
				t.Errorf("Fragment %d, Column %d: MinValue mismatch", fragID, col)
			}
		}
	}
}

func TestMergeZoneMaps(t *testing.T) {
	// Create multiple zone maps
	zm1 := &ZoneMap{
		MinValue:   int64(10),
		MaxValue:   int64(50),
		RowCount:   100,
		NullCount:  5,
		Initialized: true,
	}
	
	zm2 := &ZoneMap{
		MinValue:   int64(30),
		MaxValue:   int64(80),
		RowCount:   150,
		NullCount:  10,
		Initialized: true,
	}
	
	zm3 := &ZoneMap{
		MinValue:   int64(5),
		MaxValue:   int64(60),
		RowCount:   200,
		NullCount:  15,
		Initialized: true,
	}
	
	// Merge
	merged := MergeZoneMaps(zm1, zm2, zm3)
	
	if merged == nil {
		t.Fatal("Merged zone map should not be nil")
	}
	
	// Check results
	if merged.MinValue.(int64) != 5 {
		t.Errorf("Expected min 5, got %v", merged.MinValue)
	}
	
	if merged.MaxValue.(int64) != 80 {
		t.Errorf("Expected max 80, got %v", merged.MaxValue)
	}
	
	if merged.RowCount != 450 {
		t.Errorf("Expected row count 450, got %d", merged.RowCount)
	}
	
	if merged.NullCount != 30 {
		t.Errorf("Expected null count 30, got %d", merged.NullCount)
	}
}

func TestZoneMapIndexColumns(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Add zone maps for columns 0, 2, 4
	for col := 0; col <= 4; col += 2 {
		if err := idx.UpdateZoneMap(col, int64(col*10), false); err != nil {
			t.Fatalf("Failed to update zone map: %v", err)
		}
	}
	
	cols := idx.Columns()
	expected := []int{0, 2, 4}
	
	if !reflect.DeepEqual(cols, expected) {
		t.Errorf("Expected columns %v, got %v", expected, cols)
	}
}

func TestCompareValues(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		expected int
	}{
		{int64(10), int64(20), -1},
		{int64(20), int64(10), 1},
		{int64(10), int64(10), 0},
		{float64(1.5), float64(2.5), -1},
		{"apple", "banana", -1},
		{"banana", "apple", 1},
		{true, false, 1},
		{false, true, -1},
		{nil, int64(10), -1},
		{int64(10), nil, 1},
		{nil, nil, 0},
	}
	
	for _, tt := range tests {
		result := zoneMapCompareValues(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("zoneMapCompareValues(%v, %v) = %d, expected %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestZoneMapIndexTimeValues(t *testing.T) {
	idx := NewZoneMapIndex()
	
	// Test with time values
	now := time.Now()
	t1 := now.Add(-24 * time.Hour)
	t2 := now
	t3 := now.Add(24 * time.Hour)
	
	values := []interface{}{t1, t2, t3}
	for _, v := range values {
		if err := idx.UpdateZoneMap(0, v, false); err != nil {
			t.Fatalf("Failed to update zone map with time: %v", err)
		}
	}
	
	zm := idx.GetZoneMap(0)
	if zm == nil {
		t.Fatal("Zone map should not be nil")
	}
	
	// Check min/max
	minTime, ok := zm.MinValue.(time.Time)
	if !ok {
		t.Fatalf("Expected time.Time, got %T", zm.MinValue)
	}
	
	if !minTime.Equal(t1) {
		t.Errorf("Expected min time %v, got %v", t1, minTime)
	}
	
	maxTime, ok := zm.MaxValue.(time.Time)
	if !ok {
		t.Fatalf("Expected time.Time, got %T", zm.MaxValue)
	}
	
	if !maxTime.Equal(t3) {
		t.Errorf("Expected max time %v, got %v", t3, maxTime)
	}
}

func TestZoneMapIndexUnsupportedOperations(t *testing.T) {
	idx := NewZoneMapIndex()
	ctx := context.Background()
	
	// Search should return error
	_, err := idx.Search(ctx, "value", 10)
	if err == nil {
		t.Error("Search should return error for zone map index")
	}
	
	// RangeQuery should return error
	_, err = idx.RangeQuery(ctx, int64(10), int64(20))
	if err == nil {
		t.Error("RangeQuery should return error for zone map index")
	}
	
	// EqualityQuery should return error
	_, err = idx.EqualityQuery(ctx, int64(10))
	if err == nil {
		t.Error("EqualityQuery should return error for zone map index")
	}
}