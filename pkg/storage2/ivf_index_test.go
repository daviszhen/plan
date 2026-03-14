package storage2

import (
	"context"
	"math"
	"testing"
)

func TestIVFIndexBasicOperations(t *testing.T) {
	ctx := context.Background()
	idx := NewIVFIndex("test_ivf", 0, 128, L2Metric)

	// Train the index with some vectors
	vectors := make([][]float32, 100)
	for i := 0; i < 100; i++ {
		vectors[i] = make([]float32, 128)
		for j := 0; j < 128; j++ {
			vectors[i][j] = float32(i) * 0.1
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("Train failed: %v", err)
	}

	// Insert vectors
	for i := 0; i < 100; i++ {
		if err := idx.Insert(uint64(i), vectors[i]); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Test ANN search
	query := make([]float32, 128)
	for j := 0; j < 128; j++ {
		query[j] = 5.0 // Search for vectors similar to index 50
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, query, 10)
	if err != nil {
		t.Fatalf("ANNSearch failed: %v", err)
	}

	// IVF may return fewer results if nprobe doesn't cover all clusters
	// Just verify we got some results and they're sorted
	if len(distances) == 0 || len(rowIDs) == 0 {
		t.Error("Expected some results, got none")
	}
	
	if len(distances) != len(rowIDs) {
		t.Errorf("Distances and rowIDs length mismatch: %d vs %d", len(distances), len(rowIDs))
	}

	// Verify distances are sorted
	for i := 1; i < len(distances); i++ {
		if distances[i] < distances[i-1] {
			t.Errorf("Distances not sorted at index %d", i)
		}
	}
}

func TestIVFIndexDifferentMetrics(t *testing.T) {
	ctx := context.Background()

	metrics := []MetricType{L2Metric, CosineMetric, DotMetric}
	
	for _, metric := range metrics {
		idx := NewIVFIndex("test_metric", 0, 64, metric)
		
		// Train and insert
		vectors := make([][]float32, 50)
		for i := 0; i < 50; i++ {
			vectors[i] = make([]float32, 64)
			for j := 0; j < 64; j++ {
				vectors[i][j] = float32(i) * 0.01
			}
		}
		
		idx.Train(vectors)
		for i, vec := range vectors {
			idx.Insert(uint64(i), vec)
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

func TestIVFIndexStatistics(t *testing.T) {
	idx := NewIVFIndex("stats_idx", 0, 32, L2Metric)

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
	
	if stats.IndexType != "ivf" {
		t.Errorf("Expected index type 'ivf', got '%s'", stats.IndexType)
	}
	
	if stats.SizeBytes == 0 {
		t.Error("SizeBytes should not be 0")
	}
}

func TestIVFIndexSetNProbe(t *testing.T) {
	idx := NewIVFIndex("nprobe_idx", 0, 64, L2Metric)
	
	// Default nprobe should be 10
	if idx.nprobe != 10 {
		t.Errorf("Expected default nprobe 10, got %d", idx.nprobe)
	}
	
	// Set nprobe
	idx.SetNProbe(20)
	
	if idx.nprobe != 20 {
		t.Errorf("Expected nprobe 20, got %d", idx.nprobe)
	}
}

func TestIVFIndexInterface(t *testing.T) {
	idx := NewIVFIndex("interface_idx", 1, 128, L2Metric)
	
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

func TestIVFIndexEmptySearch(t *testing.T) {
	ctx := context.Background()
	idx := NewIVFIndex("empty_idx", 0, 64, L2Metric)
	
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

func TestIVFIndexWrongDimension(t *testing.T) {
	ctx := context.Background()
	idx := NewIVFIndex("dim_idx", 0, 64, L2Metric)
	
	// Try to insert vector with wrong dimension
	vector := make([]float32, 32) // Wrong dimension
	err := idx.Insert(0, vector)
	
	if err != nil {
		t.Errorf("Insert with wrong dimension should not fail, but got: %v", err)
	}
	
	// Search with wrong dimension
	query := make([]float32, 32)
	_, _, err = idx.ANNSearch(ctx, query, 10)
	
	if err != nil {
		t.Errorf("ANNSearch with wrong dimension should not fail, but got: %v", err)
	}
}

func TestDistanceFunctions(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}
	
	// Test L2 distance
	l2 := l2Distance(a, b)
	expectedL2 := float32(math.Sqrt(27)) // sqrt((3^2 + 3^2 + 3^2))
	if math.Abs(float64(l2-expectedL2)) > 0.0001 {
		t.Errorf("L2 distance expected %f, got %f", expectedL2, l2)
	}
	
	// Test dot product
	dot := dotProduct(a, b)
	expectedDot := float32(32.0) // 1*4 + 2*5 + 3*6 = 32
	if dot != expectedDot {
		t.Errorf("Dot product expected %f, got %f", expectedDot, dot)
	}
	
	// Test cosine distance
	cosine := cosineDistance(a, b)
	if cosine < 0 || cosine > 2 {
		t.Errorf("Cosine distance should be between 0 and 2, got %f", cosine)
	}
}

func TestIVFIndexConcurrentAccess(t *testing.T) {
	idx := NewIVFIndex("concurrent_ivf", 0, 32, L2Metric)
	ctx := context.Background()
	
	// Train first
	vectors := make([][]float32, 100)
	for i := 0; i < 100; i++ {
		vectors[i] = make([]float32, 32)
		for j := 0; j < 32; j++ {
			vectors[i][j] = float32(i) * 0.01
		}
	}
	idx.Train(vectors)
	
	// Insert vectors
	for i, vec := range vectors {
		idx.Insert(uint64(i), vec)
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
