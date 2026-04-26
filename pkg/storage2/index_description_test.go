package storage2

import (
	"testing"
)

func TestDescribeBTreeIndex(t *testing.T) {
	manager := NewIndexManager("/tmp/test", NewLocalRenameCommitHandler())

	// Create B-tree index
	idx := NewBTreeIndex("btree_idx", 0)
	manager.indexes["btree_idx"] = idx

	// Insert some data
	for i := 0; i < 100; i++ {
		idx.Insert(int64(i), uint64(i))
	}

	// Describe the index
	desc, err := manager.DescribeIndex("btree_idx")
	if err != nil {
		t.Fatalf("DescribeIndex failed: %v", err)
	}

	// Verify basic info
	if desc.Name != "btree_idx" {
		t.Errorf("Expected name 'btree_idx', got '%s'", desc.Name)
	}

	if desc.Type != ScalarIndex {
		t.Errorf("Expected type ScalarIndex, got %v", desc.Type)
	}

	if desc.ColumnIdx != 0 {
		t.Errorf("Expected column index 0, got %d", desc.ColumnIdx)
	}

	// Verify parameters
	if degree, ok := desc.Parameters["degree"].(int); !ok || degree != BTreeDegree {
		t.Errorf("Expected degree %d, got %v", BTreeDegree, desc.Parameters["degree"])
	}

	if impl, ok := desc.Parameters["implementation"].(string); !ok || impl != "btree" {
		t.Errorf("Expected implementation 'btree', got %v", desc.Parameters["implementation"])
	}

	// Verify statistics
	if desc.Statistics.NumEntries != 100 {
		t.Errorf("Expected 100 entries, got %d", desc.Statistics.NumEntries)
	}
}

func TestDescribeIVFIndex(t *testing.T) {
	manager := NewIndexManager("/tmp/test2", NewLocalRenameCommitHandler())

	// Create IVF index
	idx := NewIVFIndex("ivf_idx", 1, 128, CosineMetric)
	manager.indexes["ivf_idx"] = idx

	// Train and insert vectors
	vectors := make([][]float32, 50)
	for i := 0; i < 50; i++ {
		vectors[i] = make([]float32, 128)
		for j := 0; j < 128; j++ {
			vectors[i][j] = float32(i) * 0.01
		}
	}
	idx.Train(vectors)
	for i, vec := range vectors {
		idx.Insert(uint64(i), vec)
	}

	// Describe the index
	desc, err := manager.DescribeIndex("ivf_idx")
	if err != nil {
		t.Fatalf("DescribeIndex failed: %v", err)
	}

	// Verify basic info
	if desc.Name != "ivf_idx" {
		t.Errorf("Expected name 'ivf_idx', got '%s'", desc.Name)
	}

	if desc.Type != VectorIndex {
		t.Errorf("Expected type VectorIndex, got %v", desc.Type)
	}

	if desc.ColumnIdx != 1 {
		t.Errorf("Expected column index 1, got %d", desc.ColumnIdx)
	}

	// Verify parameters
	if nlist, ok := desc.Parameters["nlist"].(int); !ok || nlist != 100 {
		t.Errorf("Expected nlist 100, got %v", desc.Parameters["nlist"])
	}

	if metric, ok := desc.Parameters["metric"].(string); !ok || metric != "cosine" {
		t.Errorf("Expected metric 'cosine', got %v", desc.Parameters["metric"])
	}

	if impl, ok := desc.Parameters["implementation"].(string); !ok || impl != "ivf" {
		t.Errorf("Expected implementation 'ivf', got %v", desc.Parameters["implementation"])
	}
}

func TestDescribeHNSWIndex(t *testing.T) {
	manager := NewIndexManager("/tmp/test3", NewLocalRenameCommitHandler())

	// Create HNSW index
	idx := NewHNSWIndex("hnsw_idx", 2, 64, L2Metric)
	manager.indexes["hnsw_idx"] = idx

	// Insert vectors
	for i := 0; i < 50; i++ {
		vector := make([]float32, 64)
		for j := 0; j < 64; j++ {
			vector[j] = float32(i) * 0.01
		}
		idx.Insert(uint64(i), vector)
	}

	// Describe the index
	desc, err := manager.DescribeIndex("hnsw_idx")
	if err != nil {
		t.Fatalf("DescribeIndex failed: %v", err)
	}

	// Verify basic info
	if desc.Name != "hnsw_idx" {
		t.Errorf("Expected name 'hnsw_idx', got '%s'", desc.Name)
	}

	if desc.Type != VectorIndex {
		t.Errorf("Expected type VectorIndex, got %v", desc.Type)
	}

	if desc.ColumnIdx != 2 {
		t.Errorf("Expected column index 2, got %d", desc.ColumnIdx)
	}

	// Verify parameters
	if M, ok := desc.Parameters["M"].(int); !ok || M != 16 {
		t.Errorf("Expected M 16, got %v", desc.Parameters["M"])
	}

	if efConstruction, ok := desc.Parameters["ef_construction"].(int); !ok || efConstruction != 200 {
		t.Errorf("Expected ef_construction 200, got %v", desc.Parameters["ef_construction"])
	}

	if metric, ok := desc.Parameters["metric"].(string); !ok || metric != "l2" {
		t.Errorf("Expected metric 'l2', got %v", desc.Parameters["metric"])
	}

	if impl, ok := desc.Parameters["implementation"].(string); !ok || impl != "hnsw" {
		t.Errorf("Expected implementation 'hnsw', got %v", desc.Parameters["implementation"])
	}
}

func TestDescribeNonExistentIndex(t *testing.T) {
	manager := NewIndexManager("/tmp/test4", NewLocalRenameCommitHandler())

	_, err := manager.DescribeIndex("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestDescribeIndexesByName(t *testing.T) {
	manager := NewIndexManager("/tmp/test5", NewLocalRenameCommitHandler())

	// Create multiple indexes with similar names
	idx1 := NewBTreeIndex("user_idx_1", 0)
	idx2 := NewBTreeIndex("user_idx_2", 1)
	idx3 := NewIVFIndex("product_idx", 2, 64, L2Metric)

	manager.indexes["user_idx_1"] = idx1
	manager.indexes["user_idx_2"] = idx2
	manager.indexes["product_idx"] = idx3

	// Describe indexes with prefix "user"
	descs, err := manager.DescribeIndexesByName("user")
	if err != nil {
		t.Fatalf("DescribeIndexesByName failed: %v", err)
	}

	if len(descs) != 2 {
		t.Errorf("Expected 2 indexes with prefix 'user', got %d", len(descs))
	}

	// Describe exact match
	descs2, err := manager.DescribeIndexesByName("product_idx")
	if err != nil {
		t.Fatalf("DescribeIndexesByName failed: %v", err)
	}

	if len(descs2) != 1 {
		t.Errorf("Expected 1 index with name 'product_idx', got %d", len(descs2))
	}
}

func TestMetricTypeString(t *testing.T) {
	tests := []struct {
		metric   MetricType
		expected string
	}{
		{L2Metric, "l2"},
		{CosineMetric, "cosine"},
		{DotMetric, "dot"},
		{MetricType(999), "unknown"},
	}

	for _, test := range tests {
		result := test.metric.String()
		if result != test.expected {
			t.Errorf("MetricType %v String() = '%s', expected '%s'",
				test.metric, result, test.expected)
		}
	}
}

func TestIndexDescriptionStatus(t *testing.T) {
	manager := NewIndexManager("/tmp/test6", NewLocalRenameCommitHandler())

	idx := NewBTreeIndex("status_idx", 0)
	manager.indexes["status_idx"] = idx

	desc, err := manager.DescribeIndex("status_idx")
	if err != nil {
		t.Fatalf("DescribeIndex failed: %v", err)
	}

	if desc.Status != "active" {
		t.Errorf("Expected status 'active', got '%s'", desc.Status)
	}

	if desc.IsOptimized {
		t.Error("Expected IsOptimized to be false for new index")
	}
}
