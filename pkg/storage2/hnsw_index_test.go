package storage2

import (
	"container/heap"
	"context"
	"testing"
)

func TestHNSWIndexBasicOperations(t *testing.T) {
	ctx := context.Background()
	idx := NewHNSWIndex("test_hnsw", 0, 128, L2Metric)

	// Insert vectors
	for i := 0; i < 100; i++ {
		vector := make([]float32, 128)
		for j := 0; j < 128; j++ {
			vector[j] = float32(i) * 0.1
		}
		if err := idx.Insert(uint64(i), vector); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test ANN search
	query := make([]float32, 128)
	for j := 0; j < 128; j++ {
		query[j] = 5.0
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, query, 10)
	if err != nil {
		t.Fatalf("ANNSearch failed: %v", err)
	}

	if len(distances) != 10 || len(rowIDs) != 10 {
		t.Errorf("Expected 10 results, got %d distances and %d rowIDs", len(distances), len(rowIDs))
	}

	// Verify distances are sorted
	for i := 1; i < len(distances); i++ {
		if distances[i] < distances[i-1] {
			t.Errorf("Distances not sorted at index %d", i)
		}
	}
}

func TestHNSWIndexDifferentMetrics(t *testing.T) {
	ctx := context.Background()

	metrics := []MetricType{L2Metric, CosineMetric, DotMetric}
	
	for _, metric := range metrics {
		idx := NewHNSWIndex("test_metric", 0, 64, metric)
		
		// Insert vectors
		for i := 0; i < 50; i++ {
			vector := make([]float32, 64)
			for j := 0; j < 64; j++ {
				vector[j] = float32(i) * 0.01
			}
			idx.Insert(uint64(i), vector)
		}
		
		// Search
		query := make([]float32, 64)
		for j := 0; j < 64; j++ {
			query[j] = 0.25
		}
		
		_, rowIDs, err := idx.ANNSearch(ctx, query, 5)
		if err != nil {
			t.Errorf("ANNSearch failed for metric %v: %v", metric, err)
		}
		
		if len(rowIDs) != 5 {
			t.Errorf("Expected 5 results for metric %v, got %d", metric, len(rowIDs))
		}
	}
}

func TestHNSWIndexStatistics(t *testing.T) {
	idx := NewHNSWIndex("stats_idx", 0, 32, L2Metric)

	// Insert some vectors
	for i := 0; i < 50; i++ {
		vector := make([]float32, 32)
		for j := 0; j < 32; j++ {
			vector[j] = float32(i) * 0.1
		}
		idx.Insert(uint64(i), vector)
	}

	stats := idx.Statistics()
	
	if stats.NumEntries != 50 {
		t.Errorf("Expected 50 entries, got %d", stats.NumEntries)
	}
	
	if stats.IndexType != "hnsw" {
		t.Errorf("Expected index type 'hnsw', got '%s'", stats.IndexType)
	}
	
	if stats.SizeBytes == 0 {
		t.Error("SizeBytes should not be 0")
	}
}

func TestHNSWIndexSetEfSearch(t *testing.T) {
	idx := NewHNSWIndex("ef_idx", 0, 64, L2Metric)
	
	// Default efSearch should be 50
	if idx.efSearch != 50 {
		t.Errorf("Expected default efSearch 50, got %d", idx.efSearch)
	}
	
	// Set efSearch
	idx.SetEfSearch(100)
	
	if idx.efSearch != 100 {
		t.Errorf("Expected efSearch 100, got %d", idx.efSearch)
	}
}

func TestHNSWIndexInterface(t *testing.T) {
	idx := NewHNSWIndex("interface_idx", 1, 128, L2Metric)
	
	if idx.Name() != "interface_idx" {
		t.Errorf("Expected name 'interface_idx', got '%s'", idx.Name())
	}
	
	if idx.Type() != VectorIndex {
		t.Errorf("Expected type VectorIndex, got %v", idx.Type())
	}
	
	columns := idx.Columns()
	if len(columns) != 1 || columns[0] != 1 {
		t.Errorf("Expected columns [1], got %v", columns)
	}
	
	if idx.GetMetricType() != L2Metric {
		t.Errorf("Expected metric L2Metric, got %v", idx.GetMetricType())
	}
}

func TestHNSWIndexEmptySearch(t *testing.T) {
	ctx := context.Background()
	idx := NewHNSWIndex("empty_idx", 0, 64, L2Metric)
	
	// Search on empty index
	query := make([]float32, 64)
	distances, rowIDs, err := idx.ANNSearch(ctx, query, 10)
	
	if err != nil {
		t.Errorf("ANNSearch on empty index should not fail: %v", err)
	}
	
	if len(distances) != 0 || len(rowIDs) != 0 {
		t.Errorf("Expected empty results for empty index, got %d distances and %d rowIDs", 
			len(distances), len(rowIDs))
	}
}

func TestHNSWIndexWrongDimension(t *testing.T) {
	idx := NewHNSWIndex("dim_idx", 0, 64, L2Metric)
	
	// Try to insert vector with wrong dimension
	vector := make([]float32, 32)
	err := idx.Insert(0, vector)
	
	if err != nil {
		t.Errorf("Insert with wrong dimension should not fail, but got: %v", err)
	}
}

func TestHNSWIndexConcurrentAccess(t *testing.T) {
	idx := NewHNSWIndex("concurrent_hnsw", 0, 32, L2Metric)
	ctx := context.Background()
	
	// Insert vectors first
	for i := 0; i < 100; i++ {
		vector := make([]float32, 32)
		for j := 0; j < 32; j++ {
			vector[j] = float32(i) * 0.01
		}
		idx.Insert(uint64(i), vector)
	}
	
	// Concurrent searches
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()
			
			query := make([]float32, 32)
			for j := 0; j < 32; j++ {
				query[j] = float32(id) * 0.1
			}
			
			_, _, err := idx.ANNSearch(ctx, query, 5)
			if err != nil {
				t.Errorf("Concurrent search failed: %v", err)
			}
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestHNSWIndexLargeDataset(t *testing.T) {
	ctx := context.Background()
	idx := NewHNSWIndex("large_hnsw", 0, 128, L2Metric)

	// Insert 500 vectors
	for i := 0; i < 500; i++ {
		vector := make([]float32, 128)
		for j := 0; j < 128; j++ {
			vector[j] = float32(i) * 0.01
		}
		if err := idx.Insert(uint64(i), vector); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test statistics
	stats := idx.Statistics()
	if stats.NumEntries != 500 {
		t.Errorf("Expected 500 entries, got %d", stats.NumEntries)
	}

	// Search
	query := make([]float32, 128)
	for j := 0; j < 128; j++ {
		query[j] = 2.5
	}

	_, rowIDs, err := idx.ANNSearch(ctx, query, 20)
	if err != nil {
		t.Fatalf("ANNSearch failed: %v", err)
	}

	if len(rowIDs) != 20 {
		t.Errorf("Expected 20 results, got %d", len(rowIDs))
	}
}

func TestNodeDistanceHeap(t *testing.T) {
	h := &nodeDistanceHeap{}
	
	// Push elements
	heap.Push(h, nodeDistance{nodeID: 1, distance: 0.5})
	heap.Push(h, nodeDistance{nodeID: 2, distance: 0.3})
	heap.Push(h, nodeDistance{nodeID: 3, distance: 0.7})
	
	if h.Len() != 3 {
		t.Errorf("Expected heap length 3, got %d", h.Len())
	}
	
	// Pop elements (should be in ascending order)
	first := heap.Pop(h).(nodeDistance)
	if first.distance != 0.3 {
		t.Errorf("Expected distance 0.3, got %f", first.distance)
	}
	
	second := heap.Pop(h).(nodeDistance)
	if second.distance != 0.5 {
		t.Errorf("Expected distance 0.5, got %f", second.distance)
	}
	
	third := heap.Pop(h).(nodeDistance)
	if third.distance != 0.7 {
		t.Errorf("Expected distance 0.7, got %f", third.distance)
	}
}
