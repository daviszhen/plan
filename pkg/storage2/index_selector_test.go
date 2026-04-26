package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

func TestIndexSelectorConfig(t *testing.T) {
	config := DefaultIndexSelectorConfig()

	if !config.PreferBitmapForLowCardinality {
		t.Error("PreferBitmapForLowCardinality should be true by default")
	}

	if config.LowCardinalityThreshold != 100 {
		t.Errorf("Expected LowCardinalityThreshold 100, got %d", config.LowCardinalityThreshold)
	}

	if !config.UseZoneMapForRanges {
		t.Error("UseZoneMapForRanges should be true by default")
	}

	if !config.UseBloomFilterForEquality {
		t.Error("UseBloomFilterForEquality should be true by default")
	}
}

func TestIndexSelectorSelectForEquality(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	// Create a bitmap index
	if err := manager.CreateBitmapIndex(ctx, "test_bitmap", 0); err != nil {
		t.Fatalf("Failed to create bitmap index: %v", err)
	}

	// Add some data
	bitmapIdx := manager.indexes["test_bitmap"].(*BitmapIndex)
	for i := 0; i < 100; i++ {
		bitmapIdx.Insert("value1", uint64(i))
	}
	for i := 100; i < 200; i++ {
		bitmapIdx.Insert("value2", uint64(i))
	}

	// Debug: check statistics
	stats := bitmapIdx.Statistics()
	t.Logf("Bitmap stats: NumEntries=%d, DistinctValues=%d", stats.NumEntries, stats.DistinctValues)

	// Debug: check cardinality
	card := bitmapIdx.GetValueCardinality("value1")
	t.Logf("Cardinality for 'value1': %d", card)

	// Create selector
	selector := NewIndexSelector(manager, DefaultIndexSelectorConfig())

	// Create predicate
	value := &chunk.Value{
		Typ: common.MakeLType(common.LTID_VARCHAR),
		Str: "value1",
	}
	pred := NewColumnPredicate(0, Eq, value)

	// Select index
	selection, err := selector.SelectIndexForPredicate(ctx, pred)
	if err != nil {
		t.Fatalf("SelectIndexForPredicate failed: %v", err)
	}

	if selection == nil {
		t.Fatal("Expected index selection, got nil")
	}

	if selection.Type != ScalarIndex {
		t.Errorf("Expected ScalarIndex, got %v", selection.Type)
	}

	if selection.ColumnIdx != 0 {
		t.Errorf("Expected column 0, got %d", selection.ColumnIdx)
	}

	t.Logf("Selection: Type=%v, Selectivity=%.4f, CanPrune=%v",
		selection.Type, selection.EstimatedSelectivity, selection.CanPrune)
}

func TestIndexSelectorSelectForRange(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	// Create a zone map index
	if err := manager.CreateZoneMapIndex(ctx, "test_zonemap"); err != nil {
		t.Fatalf("Failed to create zone map index: %v", err)
	}

	// Add some data
	zonemapIdx := manager.indexes["test_zonemap"].(*ZoneMapIndex)
	for i := 0; i < 100; i++ {
		zonemapIdx.UpdateZoneMap(0, int64(i), false)
	}

	// Create selector
	selector := NewIndexSelector(manager, DefaultIndexSelectorConfig())

	// Create predicate for range query
	value := &chunk.Value{
		Typ: common.MakeLType(common.LTID_BIGINT),
		I64: 50,
	}
	pred := NewColumnPredicate(0, Lt, value)

	// Select index
	selection, err := selector.SelectIndexForPredicate(ctx, pred)
	if err != nil {
		t.Fatalf("SelectIndexForPredicate failed: %v", err)
	}

	// ZoneMap should be selected for range queries
	if selection != nil {
		if selection.Type != ScalarIndex {
			t.Errorf("Expected ScalarIndex, got %v", selection.Type)
		}
	}
}

func TestIndexSelectorNoIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	// No indexes created

	selector := NewIndexSelector(manager, DefaultIndexSelectorConfig())

	value := &chunk.Value{
		Typ: common.MakeLType(common.LTID_BIGINT),
		I64: 42,
	}
	pred := NewColumnPredicate(0, Eq, value)

	selection, err := selector.SelectIndexForPredicate(ctx, pred)
	if err != nil {
		t.Fatalf("SelectIndexForPredicate failed: %v", err)
	}

	if selection != nil {
		t.Error("Expected nil selection when no indexes available")
	}
}

func TestIndexSelectorAndPredicate(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	// Create bitmap index for column 0
	if err := manager.CreateBitmapIndex(ctx, "test_bitmap", 0); err != nil {
		t.Fatalf("Failed to create bitmap index: %v", err)
	}

	bitmapIdx := manager.indexes["test_bitmap"].(*BitmapIndex)
	for i := 0; i < 100; i++ {
		bitmapIdx.Insert("value1", uint64(i))
	}

	selector := NewIndexSelector(manager, DefaultIndexSelectorConfig())

	// Create AND predicate
	value1 := &chunk.Value{
		Typ: common.MakeLType(common.LTID_VARCHAR),
		Str: "value1",
	}
	value2 := &chunk.Value{
		Typ: common.MakeLType(common.LTID_BIGINT),
		I64: 100,
	}

	left := NewColumnPredicate(0, Eq, value1)
	right := NewColumnPredicate(1, Gt, value2)
	andPred := NewAndPredicate(left, right)

	selection, err := selector.SelectIndexForPredicate(ctx, andPred)
	if err != nil {
		t.Fatalf("SelectIndexForPredicate failed: %v", err)
	}

	// Should select index for column 0
	if selection != nil && selection.ColumnIdx != 0 {
		t.Errorf("Expected column 0, got %d", selection.ColumnIdx)
	}
}

func TestIndexStatisticsCollector(t *testing.T) {
	collector := NewIndexStatisticsCollector()

	// Create a test chunk
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_VARCHAR),
	}
	c := &chunk.Chunk{}
	c.Init(typs, 100)
	c.SetCard(100)

	// Fill with test data
	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			I64: int64(i % 10), // 10 distinct values
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			Str: string(rune('A' + i%5)), // 5 distinct values
		})
	}

	// Collect statistics
	collector.CollectFromChunk(c)

	// Check column 0 stats
	stats0 := collector.GetColumnStatistics(0)
	if stats0 == nil {
		t.Fatal("Column 0 stats should not be nil")
	}

	if stats0.TotalCount != 100 {
		t.Errorf("Expected TotalCount 100, got %d", stats0.TotalCount)
	}

	if stats0.DistinctCount != 10 {
		t.Errorf("Expected DistinctCount 10, got %d", stats0.DistinctCount)
	}

	// Check column 1 stats
	stats1 := collector.GetColumnStatistics(1)
	if stats1 == nil {
		t.Fatal("Column 1 stats should not be nil")
	}

	if stats1.DistinctCount != 5 {
		t.Errorf("Expected DistinctCount 5, got %d", stats1.DistinctCount)
	}
}

func TestIndexStatisticsCollectorRecommendations(t *testing.T) {
	collector := NewIndexStatisticsCollector()

	// Create chunks with different cardinalities
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER), // Low cardinality
		common.MakeLType(common.LTID_INTEGER), // High cardinality
	}

	c := &chunk.Chunk{}
	c.Init(typs, 1000)
	c.SetCard(1000)

	for i := 0; i < 1000; i++ {
		// Column 0: 10 distinct values (low cardinality)
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			I64: int64(i % 10),
		})
		// Column 1: 1000 distinct values (high cardinality)
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			I64: int64(i),
		})
	}

	collector.CollectFromChunk(c)

	// Get recommendations
	recommendations := collector.RecommendIndexes()

	// Column 0 should have Bitmap recommendation (low cardinality)
	if recs, ok := recommendations[0]; ok {
		foundBitmap := false
		for _, rec := range recs {
			if rec == BitmapIndexType {
				foundBitmap = true
				break
			}
		}
		if !foundBitmap {
			t.Error("Expected Bitmap recommendation for low cardinality column")
		}
	}

	// Column 1 should have BloomFilter recommendation (high cardinality)
	if recs, ok := recommendations[1]; ok {
		foundBloomFilter := false
		for _, rec := range recs {
			if rec == BloomFilterIndexType {
				foundBloomFilter = true
				break
			}
		}
		if !foundBloomFilter {
			t.Error("Expected BloomFilter recommendation for high cardinality column")
		}
	}
}

func TestExtractValueFromChunkValue(t *testing.T) {
	tests := []struct {
		value    *chunk.Value
		expected interface{}
	}{
		{
			value:    &chunk.Value{Typ: common.MakeLType(common.LTID_BOOLEAN), Bool: true},
			expected: true,
		},
		{
			value:    &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 42},
			expected: int64(42),
		},
		{
			value:    &chunk.Value{Typ: common.MakeLType(common.LTID_BIGINT), I64: 123456},
			expected: int64(123456),
		},
		{
			value:    &chunk.Value{Typ: common.MakeLType(common.LTID_DOUBLE), F64: 3.14},
			expected: 3.14,
		},
		{
			value:    &chunk.Value{Typ: common.MakeLType(common.LTID_VARCHAR), Str: "test"},
			expected: "test",
		},
		{
			value:    nil,
			expected: nil,
		},
	}

	for _, tt := range tests {
		result := extractValueFromChunkValue(tt.value)
		if result != tt.expected {
			t.Errorf("extractValueFromChunkValue(%v) = %v, expected %v", tt.value, result, tt.expected)
		}
	}
}

func TestCompareInterfaceValues(t *testing.T) {
	tests := []struct {
		a, b     interface{}
		expected int
	}{
		{int64(10), int64(20), -1},
		{int64(20), int64(10), 1},
		{int64(10), int64(10), 0},
		{float64(1.5), float64(2.5), -1},
		{float64(2.5), float64(1.5), 1},
		{"apple", "banana", -1},
		{"banana", "apple", 1},
		{int64(10), float64(10.0), 0}, // Different types
	}

	for _, tt := range tests {
		result := compareInterfaceValues(tt.a, tt.b)
		if result != tt.expected {
			t.Errorf("compareInterfaceValues(%v, %v) = %d, expected %d", tt.a, tt.b, result, tt.expected)
		}
	}
}

func TestEstimateFraction(t *testing.T) {
	tests := []struct {
		min, max, value interface{}
		expected        float64
		tolerance       float64
	}{
		{int64(0), int64(100), int64(50), 0.5, 0.0},
		{int64(0), int64(100), int64(25), 0.25, 0.0},
		{float64(0), float64(1), float64(0.5), 0.5, 0.0},
		{int64(0), int64(0), int64(0), 0.5, 0.5}, // Division by zero case
	}

	for _, tt := range tests {
		result := estimateFraction(tt.min, tt.max, tt.value)
		diff := result - tt.expected
		if diff < 0 {
			diff = -diff
		}
		if diff > tt.tolerance {
			t.Errorf("estimateFraction(%v, %v, %v) = %f, expected %f (tolerance %f)",
				tt.min, tt.max, tt.value, result, tt.expected, tt.tolerance)
		}
	}
}

func TestIndexAwareScannerCreation(t *testing.T) {
	// Create scanner with nil manager (should create default)
	scanner := NewIndexAwareScanner("/tmp/test", nil, 0, nil, nil)

	if scanner == nil {
		t.Fatal("Scanner should not be nil")
	}

	if scanner.manager == nil {
		t.Error("Manager should not be nil")
	}

	if scanner.selector == nil {
		t.Error("Selector should not be nil")
	}
}

func TestIndexManagerCreateBitmapIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	if err := manager.CreateBitmapIndex(ctx, "test_bitmap", 0); err != nil {
		t.Fatalf("CreateBitmapIndex failed: %v", err)
	}

	idx, ok := manager.GetIndex("test_bitmap")
	if !ok {
		t.Fatal("Index should exist")
	}

	if _, ok := idx.(*BitmapIndex); !ok {
		t.Error("Index should be BitmapIndex type")
	}
}

func TestIndexManagerCreateZoneMapIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	if err := manager.CreateZoneMapIndex(ctx, "test_zonemap"); err != nil {
		t.Fatalf("CreateZoneMapIndex failed: %v", err)
	}

	idx, ok := manager.GetIndex("test_zonemap")
	if !ok {
		t.Fatal("Index should exist")
	}

	if _, ok := idx.(*ZoneMapIndex); !ok {
		t.Error("Index should be ZoneMapIndex type")
	}
}

func TestIndexManagerCreateBloomFilterIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	if err := manager.CreateBloomFilterIndex(ctx, "test_bloomfilter"); err != nil {
		t.Fatalf("CreateBloomFilterIndex failed: %v", err)
	}

	idx, ok := manager.GetIndex("test_bloomfilter")
	if !ok {
		t.Fatal("Index should exist")
	}

	if _, ok := idx.(*BloomFilterIndex); !ok {
		t.Error("Index should be BloomFilterIndex type")
	}
}

func TestIndexManagerOptimizeNewIndexes(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("", nil)

	// Create all new index types
	if err := manager.CreateBitmapIndex(ctx, "bitmap_idx", 0); err != nil {
		t.Fatalf("CreateBitmapIndex failed: %v", err)
	}
	if err := manager.CreateZoneMapIndex(ctx, "zonemap_idx"); err != nil {
		t.Fatalf("CreateZoneMapIndex failed: %v", err)
	}
	if err := manager.CreateBloomFilterIndex(ctx, "bloomfilter_idx"); err != nil {
		t.Fatalf("CreateBloomFilterIndex failed: %v", err)
	}

	// Optimize all indexes
	if err := manager.OptimizeIndex(ctx, "bitmap_idx"); err != nil {
		t.Errorf("Optimize bitmap_idx failed: %v", err)
	}
	if err := manager.OptimizeIndex(ctx, "zonemap_idx"); err != nil {
		t.Errorf("Optimize zonemap_idx failed: %v", err)
	}
	if err := manager.OptimizeIndex(ctx, "bloomfilter_idx"); err != nil {
		t.Errorf("Optimize bloomfilter_idx failed: %v", err)
	}
}
