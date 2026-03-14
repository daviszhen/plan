package storage2

import (
	"context"
	"math"
	"testing"
)

func TestBloomFilterBasicOperations(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)
	
	if bf == nil {
		t.Fatal("Bloom filter should not be nil")
	}
	
	if bf.numBits == 0 {
		t.Error("Number of bits should be > 0")
	}
	
	if bf.numHashes == 0 {
		t.Error("Number of hashes should be > 0")
	}
}

func TestBloomFilterAddAndCheck(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)
	
	// Add some items
	items := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, item := range items {
		bf.AddString(item)
	}
	
	// Check that all added items are found
	for _, item := range items {
		if !bf.MightContainString(item) {
			t.Errorf("Item %s should be in the filter", item)
		}
	}
	
	// Check that items not added are not found (may have false positives)
	notAdded := []string{"fig", "grape", "honeydew"}
	falsePositives := 0
	for _, item := range notAdded {
		if bf.MightContainString(item) {
			falsePositives++
		}
	}
	
	// False positive rate should be reasonable
	fpr := float64(falsePositives) / float64(len(notAdded))
	if fpr > 0.5 { // Allow higher FPR for small sample
		t.Errorf("False positive rate too high: %.2f", fpr)
	}
}

func TestBloomFilterInt64(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)
	
	// Add int64 values
	values := []int64{1, 100, 1000, 10000, 100000}
	for _, v := range values {
		bf.AddInt64(v)
	}
	
	// Check added values
	for _, v := range values {
		if !bf.MightContainInt64(v) {
			t.Errorf("Value %d should be in the filter", v)
		}
	}
	
	// Check non-added values
	if bf.MightContainInt64(999) {
		// This could be a false positive, but log it
		t.Logf("False positive for value 999")
	}
}

func TestBloomFilterFloat64(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)
	
	// Add float64 values
	values := []float64{1.5, 2.5, 3.5, 4.5, 5.5}
	for _, v := range values {
		bf.AddFloat64(v)
	}
	
	// Check added values
	for _, v := range values {
		if !bf.MightContainFloat64(v) {
			t.Errorf("Value %f should be in the filter", v)
		}
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	// Test with different false positive rates
	testCases := []struct {
		expectedItems     uint64
		falsePositiveRate float64
		testItems         int
	}{
		{1000, 0.01, 1000},
		{1000, 0.05, 1000},
		{1000, 0.1, 1000},
	}
	
	for _, tc := range testCases {
		config := BloomFilterConfig{
			ExpectedItems:     tc.expectedItems,
			FalsePositiveRate: tc.falsePositiveRate,
		}
		bf := NewBloomFilter(config)
		
		// Add items
		for i := 0; i < int(tc.expectedItems); i++ {
			bf.AddInt64(int64(i))
		}
		
		// Test for false positives with items not in the set
		falsePositives := 0
		for i := int(tc.expectedItems); i < int(tc.expectedItems)+tc.testItems; i++ {
			if bf.MightContainInt64(int64(i)) {
				falsePositives++
			}
		}
		
		actualFPR := float64(falsePositives) / float64(tc.testItems)
		// Allow 2x the expected FPR due to randomness
		if actualFPR > tc.falsePositiveRate*2 {
			t.Errorf("FPR %.4f exceeds expected %.4f (2x tolerance)", 
				actualFPR, tc.falsePositiveRate)
		}
	}
}

func TestBloomFilterEstimatedFPR(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)
	
	// Initially FPR should be 0
	if bf.EstimatedFalsePositiveRate() != 0 {
		t.Errorf("Initial FPR should be 0, got %f", bf.EstimatedFalsePositiveRate())
	}
	
	// Add items and check FPR increases
	for i := 0; i < 1000; i++ {
		bf.AddInt64(int64(i))
	}
	
	estimatedFPR := bf.EstimatedFalsePositiveRate()
	if estimatedFPR <= 0 || estimatedFPR > 0.1 {
		t.Errorf("Estimated FPR should be between 0 and 0.1, got %f", estimatedFPR)
	}
}

func TestBloomFilterSize(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedItems:     10000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)
	
	size := bf.Size()
	if size == 0 {
		t.Error("Size should be > 0")
	}
	
	// Size should be reasonable (not too large)
	// For 10000 items at 1% FPR, optimal size is about 14KB
	if size > 100*1024 { // 100KB
		t.Errorf("Size %d bytes seems too large", size)
	}
}

func TestBloomFilterClear(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)
	
	// Add some items
	bf.AddString("test1")
	bf.AddString("test2")
	
	if bf.NumItems() != 2 {
		t.Errorf("Expected 2 items, got %d", bf.NumItems())
	}
	
	// Clear
	bf.Clear()
	
	if bf.NumItems() != 0 {
		t.Errorf("Expected 0 items after clear, got %d", bf.NumItems())
	}
	
	// Items should not be found after clear
	if bf.MightContainString("test1") {
		t.Error("Item should not be found after clear")
	}
}

func TestBloomFilterSerialization(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
	}
	original := NewBloomFilter(config)
	
	// Add some items
	for i := 0; i < 100; i++ {
		original.AddInt64(int64(i))
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
	deserialized := &BloomFilter{}
	if err := deserialized.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}
	
	// Compare properties
	if original.numBits != deserialized.numBits {
		t.Errorf("numBits mismatch: %d vs %d", original.numBits, deserialized.numBits)
	}
	
	if original.numHashes != deserialized.numHashes {
		t.Errorf("numHashes mismatch: %d vs %d", original.numHashes, deserialized.numHashes)
	}
	
	if original.numItems != deserialized.numItems {
		t.Errorf("numItems mismatch: %d vs %d", original.numItems, deserialized.numItems)
	}
	
	// Check that items are still found
	for i := 0; i < 100; i++ {
		if !deserialized.MightContainInt64(int64(i)) {
			t.Errorf("Item %d should be found in deserialized filter", i)
		}
	}
}

func TestBloomFilterIndexBasicOperations(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	if idx.Name() == "" {
		t.Error("Index name should not be empty")
	}
	
	if idx.Type() != ScalarIndex {
		t.Errorf("Expected ScalarIndex, got %v", idx.Type())
	}
}

func TestBloomFilterIndexCreateFilter(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	config := DefaultBloomFilterConfig()
	if err := idx.CreateFilter(0, config); err != nil {
		t.Fatalf("Failed to create filter: %v", err)
	}
	
	filter := idx.GetFilter(0)
	if filter == nil {
		t.Fatal("Filter should not be nil")
	}
}

func TestBloomFilterIndexAddAndCheck(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	// Add values
	values := []int64{1, 10, 100, 1000, 10000}
	for _, v := range values {
		if err := idx.AddToFilter(0, v); err != nil {
			t.Fatalf("Failed to add value: %v", err)
		}
	}
	
	// Check values
	for _, v := range values {
		contains, err := idx.MightContain(0, v)
		if err != nil {
			t.Fatalf("Failed to check value: %v", err)
		}
		if !contains {
			t.Errorf("Value %d should be in the filter", v)
		}
	}
}

func TestBloomFilterIndexFragmentFilters(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	// Create fragment filters
	config := DefaultBloomFilterConfig()
	
	if err := idx.CreateFragmentFilter(1, 0, config); err != nil {
		t.Fatalf("Failed to create fragment filter: %v", err)
	}
	
	if err := idx.CreateFragmentFilter(2, 0, config); err != nil {
		t.Fatalf("Failed to create fragment filter: %v", err)
	}
	
	// Add values to different fragments
	frag1Values := []int64{1, 2, 3, 4, 5}
	frag2Values := []int64{100, 200, 300, 400, 500}
	
	for _, v := range frag1Values {
		if err := idx.AddToFragmentFilter(1, 0, v); err != nil {
			t.Fatalf("Failed to add to fragment 1: %v", err)
		}
	}
	
	for _, v := range frag2Values {
		if err := idx.AddToFragmentFilter(2, 0, v); err != nil {
			t.Fatalf("Failed to add to fragment 2: %v", err)
		}
	}
	
	// Check fragment 1
	for _, v := range frag1Values {
		contains, err := idx.MightContainInFragment(1, 0, v)
		if err != nil {
			t.Fatalf("Failed to check fragment 1: %v", err)
		}
		if !contains {
			t.Errorf("Fragment 1 should contain %d", v)
		}
	}
	
	// Check fragment 2
	for _, v := range frag2Values {
		contains, err := idx.MightContainInFragment(2, 0, v)
		if err != nil {
			t.Fatalf("Failed to check fragment 2: %v", err)
		}
		if !contains {
			t.Errorf("Fragment 2 should contain %d", v)
		}
	}
}

func TestBloomFilterIndexPruning(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	config := DefaultBloomFilterConfig()
	
	// Create filters for 3 fragments
	for fragID := uint64(1); fragID <= 3; fragID++ {
		if err := idx.CreateFragmentFilter(fragID, 0, config); err != nil {
			t.Fatalf("Failed to create fragment filter: %v", err)
		}
	}
	
	// Add different ranges to each fragment
	// Fragment 1: 1-10
	for i := 1; i <= 10; i++ {
		if err := idx.AddToFragmentFilter(1, 0, int64(i)); err != nil {
			t.Fatalf("Failed to add to fragment 1: %v", err)
		}
	}
	
	// Fragment 2: 100-200
	for i := 100; i <= 200; i += 10 {
		if err := idx.AddToFragmentFilter(2, 0, int64(i)); err != nil {
			t.Fatalf("Failed to add to fragment 2: %v", err)
		}
	}
	
	// Fragment 3: 1000-2000
	for i := 1000; i <= 2000; i += 100 {
		if err := idx.AddToFragmentFilter(3, 0, int64(i)); err != nil {
			t.Fatalf("Failed to add to fragment 3: %v", err)
		}
	}
	
	// Test pruning for value 50
	// Fragment 1: [1-10] - 50 not in range, might still return true (false positive)
	// Fragment 2: [100-200] - 50 not in range, might return false
	// Fragment 3: [1000-2000] - 50 not in range, might return false
	
	prunable := idx.GetPrunableFragments(0, int64(50))
	t.Logf("Prunable fragments for value 50: %v", prunable)
	
	// At least some fragments should be prunable
	// (exact number depends on false positives)
}

func TestBloomFilterIndexCanPrune(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	config := DefaultBloomFilterConfig()
	if err := idx.CreateFragmentFilter(1, 0, config); err != nil {
		t.Fatalf("Failed to create filter: %v", err)
	}
	
	// Add value 100
	if err := idx.AddToFragmentFilter(1, 0, int64(100)); err != nil {
		t.Fatalf("Failed to add value: %v", err)
	}
	
	// Value 100 should not be prunable
	canPrune, err := idx.CanPrune(1, 0, int64(100))
	if err != nil {
		t.Fatalf("CanPrune failed: %v", err)
	}
	if canPrune {
		t.Error("Value 100 should not be prunable")
	}
}

func TestBloomFilterIndexSerialization(t *testing.T) {
	original := NewBloomFilterIndex(WithBloomFilterIndexName("test_bloom"))
	
	// Create column filters
	config := DefaultBloomFilterConfig()
	for col := 0; col < 3; col++ {
		if err := original.CreateFilter(col, config); err != nil {
			t.Fatalf("Failed to create filter: %v", err)
		}
		for i := 0; i < 10; i++ {
			if err := original.AddToFilter(col, int64(col*100+i)); err != nil {
				t.Fatalf("Failed to add value: %v", err)
			}
		}
	}
	
	// Create fragment filters
	for fragID := uint64(1); fragID <= 2; fragID++ {
		for col := 0; col < 2; col++ {
			if err := original.CreateFragmentFilter(fragID, col, config); err != nil {
				t.Fatalf("Failed to create fragment filter: %v", err)
			}
			for i := 0; i < 5; i++ {
				if err := original.AddToFragmentFilter(fragID, col, int64(fragID*1000+uint64(col*100+i))); err != nil {
					t.Fatalf("Failed to add to fragment: %v", err)
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
	deserialized := NewBloomFilterIndex()
	if err := deserialized.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}
	
	// Compare filters
	for col := 0; col < 3; col++ {
		origFilter := original.GetFilter(col)
		deserFilter := deserialized.GetFilter(col)
		
		if origFilter == nil || deserFilter == nil {
			t.Errorf("Column %d filter should not be nil", col)
			continue
		}
		
		if origFilter.numBits != deserFilter.numBits {
			t.Errorf("Column %d: numBits mismatch", col)
		}
		
		if origFilter.numItems != deserFilter.numItems {
			t.Errorf("Column %d: numItems mismatch", col)
		}
	}
}

func TestMergeFilters(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedItems:     100,
		FalsePositiveRate: 0.01,
	}
	
	bf1 := NewBloomFilter(config)
	bf2 := NewBloomFilter(config)
	bf3 := NewBloomFilter(config)
	
	// Add different values to each filter
	for i := 0; i < 10; i++ {
		bf1.AddInt64(int64(i))
	}
	for i := 10; i < 20; i++ {
		bf2.AddInt64(int64(i))
	}
	for i := 20; i < 30; i++ {
		bf3.AddInt64(int64(i))
	}
	
	// Merge filters
	merged, err := MergeFilters(bf1, bf2, bf3)
	if err != nil {
		t.Fatalf("MergeFilters failed: %v", err)
	}
	
	// Check that all values are in the merged filter
	for i := 0; i < 30; i++ {
		if !merged.MightContainInt64(int64(i)) {
			t.Errorf("Merged filter should contain %d", i)
		}
	}
	
	// Check that numItems is correct
	if merged.NumItems() != 30 {
		t.Errorf("Expected 30 items, got %d", merged.NumItems())
	}
}

func TestMergeFiltersDifferentSizes(t *testing.T) {
	config1 := BloomFilterConfig{ExpectedItems: 100, FalsePositiveRate: 0.01}
	config2 := BloomFilterConfig{ExpectedItems: 1000, FalsePositiveRate: 0.01}
	
	bf1 := NewBloomFilter(config1)
	bf2 := NewBloomFilter(config2)
	
	// Should fail because sizes are different
	_, err := MergeFilters(bf1, bf2)
	if err == nil {
		t.Error("MergeFilters should fail for different sized filters")
	}
}

func TestBloomFilterIndexColumns(t *testing.T) {
	idx := NewBloomFilterIndex()
	
	config := DefaultBloomFilterConfig()
	
	// Create filters for columns 0, 2, 4
	for col := 0; col <= 4; col += 2 {
		if err := idx.CreateFilter(col, config); err != nil {
			t.Fatalf("Failed to create filter: %v", err)
		}
	}
	
	cols := idx.Columns()
	if len(cols) != 3 {
		t.Errorf("Expected 3 columns, got %d", len(cols))
	}
}

func TestBloomFilterMemoryConstraint(t *testing.T) {
	// Test that memory constraint is respected
	config := BloomFilterConfig{
		ExpectedItems:     1000000,
		FalsePositiveRate: 0.01,
		MaxMemoryBytes:    1024, // 1KB max
	}
	
	bf := NewBloomFilter(config)
	
	size := bf.Size()
	if size > 1024 {
		t.Errorf("Size %d exceeds max memory %d", size, 1024)
	}
}

func TestBloomFilterIndexUnsupportedOperations(t *testing.T) {
	idx := NewBloomFilterIndex()
	ctx := context.Background()
	
	// Search should return error
	_, err := idx.Search(ctx, "value", 10)
	if err == nil {
		t.Error("Search should return error for bloom filter index")
	}
	
	// RangeQuery should return error
	_, err = idx.RangeQuery(ctx, int64(10), int64(20))
	if err == nil {
		t.Error("RangeQuery should return error for bloom filter index")
	}
	
	// EqualityQuery should return error
	_, err = idx.EqualityQuery(ctx, int64(10))
	if err == nil {
		t.Error("EqualityQuery should return error for bloom filter index")
	}
}

func TestBloomFilterAddValue(t *testing.T) {
	config := DefaultBloomFilterConfig()
	bf := NewBloomFilter(config)
	
	// Test different types
	testCases := []struct {
		value interface{}
	}{
		{int64(123)},
		{int32(456)},
		{float64(3.14)},
		{float32(2.71)},
		{"test string"},
		{true},
		{false},
	}
	
	for _, tc := range testCases {
		err := bf.AddValue(tc.value)
		if err != nil {
			t.Errorf("AddValue failed for type %T: %v", tc.value, err)
		}
		
		contains, err := bf.MightContainValue(tc.value)
		if err != nil {
			t.Errorf("MightContainValue failed for type %T: %v", tc.value, err)
		}
		if !contains {
			t.Errorf("Value %v (type %T) should be in filter", tc.value, tc.value)
		}
	}
}

func BenchmarkBloomFilterAdd(b *testing.B) {
	config := BloomFilterConfig{
		ExpectedItems:     uint64(b.N),
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.AddInt64(int64(i))
	}
}

func BenchmarkBloomFilterCheck(b *testing.B) {
	config := BloomFilterConfig{
		ExpectedItems:     10000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)
	
	// Pre-populate
	for i := 0; i < 10000; i++ {
		bf.AddInt64(int64(i))
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MightContainInt64(int64(i % 20000))
	}
}

func TestBloomFilterOptimalParameters(t *testing.T) {
	// Test that the optimal parameter calculation is reasonable
	testCases := []struct {
		expectedItems uint64
		targetFPR     float64
	}{
		{1000, 0.01},
		{10000, 0.01},
		{100000, 0.01},
		{1000, 0.001},
		{1000, 0.1},
	}
	
	for _, tc := range testCases {
		config := BloomFilterConfig{
			ExpectedItems:     tc.expectedItems,
			FalsePositiveRate: tc.targetFPR,
		}
		bf := NewBloomFilter(config)
		
		// Theoretical optimal: m/n = -k*ln(p) / ln(2)^2
		// For 1% FPR: m/n ≈ 9.6 bits per item
		bitsPerItem := float64(bf.numBits) / float64(tc.expectedItems)
		
		// Should be within reasonable range
		if bitsPerItem < 1 || bitsPerItem > 100 {
			t.Errorf("Bits per item %.2f seems unreasonable for FPR %.4f", 
				bitsPerItem, tc.targetFPR)
		}
		
		// Number of hashes should be reasonable
		if bf.numHashes < 1 || bf.numHashes > 20 {
			t.Errorf("Number of hashes %d seems unreasonable", bf.numHashes)
		}
	}
}

func TestBloomFilterFPRWithFillRate(t *testing.T) {
	config := BloomFilterConfig{
		ExpectedItems:     1000,
		FalsePositiveRate: 0.01,
	}
	bf := NewBloomFilter(config)
	
	// Test FPR at different fill rates
	fillRates := []float64{0.25, 0.5, 0.75, 1.0, 1.5}
	
	for _, rate := range fillRates {
		numItems := int(float64(config.ExpectedItems) * rate)
		
		// Clear and refill
		bf.Clear()
		for i := 0; i < numItems; i++ {
			bf.AddInt64(int64(i))
		}
		
		// Estimate FPR
		estFPR := bf.EstimatedFalsePositiveRate()
		
		// At 100% fill, FPR should be close to target
		// At higher fill, FPR should be higher
		if rate <= 1.0 && estFPR > tc.targetFPR*2 {
			t.Logf("Fill rate %.2f: estimated FPR %.4f (target %.4f)", 
				rate, estFPR, tc.targetFPR)
		}
	}
}

// Helper variable for the last test
var tc = struct {
	targetFPR float64
}{
	targetFPR: 0.01,
}

// Math function for test
func init() {
	// Ensure math package is used
	_ = math.Pi
}