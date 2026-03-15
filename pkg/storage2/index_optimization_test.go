package storage2

import (
	"context"
	"testing"
)

func TestOptimizeIVFIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test", NewLocalRenameCommitHandler())

	// Create and populate IVF index
	idx := NewIVFIndex("test_ivf", 0, 64, L2Metric)
	manager.indexes["test_ivf"] = idx

	// Insert some vectors
	vectors := make([][]float32, 100)
	for i := 0; i < 100; i++ {
		vectors[i] = make([]float32, 64)
		for j := 0; j < 64; j++ {
			vectors[i][j] = float32(i) * 0.01
		}
		idx.Insert(uint64(i), vectors[i])
	}

	// Get initial stats
	initialStats := idx.Statistics()

	// Optimize the index
	if err := manager.OptimizeIndex(ctx, "test_ivf"); err != nil {
		t.Fatalf("OptimizeIndex failed: %v", err)
	}

	// Verify index still works after optimization
	query := make([]float32, 64)
	for j := 0; j < 64; j++ {
		query[j] = 0.5
	}

	_, rowIDs, err := idx.ANNSearch(ctx, query, 10)
	if err != nil {
		t.Fatalf("ANNSearch after optimization failed: %v", err)
	}

	if len(rowIDs) != 10 {
		t.Errorf("Expected 10 results after optimization, got %d", len(rowIDs))
	}

	// Stats should be updated
	finalStats := idx.Statistics()
	if finalStats.NumEntries != initialStats.NumEntries {
		t.Errorf("Entry count changed after optimization: %d -> %d",
			initialStats.NumEntries, finalStats.NumEntries)
	}
}

func TestOptimizeHNSWIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test2", NewLocalRenameCommitHandler())

	// Create and populate HNSW index
	idx := NewHNSWIndex("test_hnsw", 0, 64, L2Metric)
	manager.indexes["test_hnsw"] = idx

	// Insert some vectors
	for i := 0; i < 100; i++ {
		vector := make([]float32, 64)
		for j := 0; j < 64; j++ {
			vector[j] = float32(i) * 0.01
		}
		idx.Insert(uint64(i), vector)
	}

	// Optimize the index
	if err := manager.OptimizeIndex(ctx, "test_hnsw"); err != nil {
		t.Fatalf("OptimizeIndex failed: %v", err)
	}

	// Verify index still works after optimization
	query := make([]float32, 64)
	for j := 0; j < 64; j++ {
		query[j] = 0.5
	}

	_, rowIDs, err := idx.ANNSearch(ctx, query, 10)
	if err != nil {
		t.Fatalf("ANNSearch after optimization failed: %v", err)
	}

	if len(rowIDs) != 10 {
		t.Errorf("Expected 10 results after optimization, got %d", len(rowIDs))
	}
}

func TestOptimizeAllIndexes(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test3", NewLocalRenameCommitHandler())

	// Create multiple indexes
	ivfIdx := NewIVFIndex("ivf_idx", 0, 32, L2Metric)
	hnswIdx := NewHNSWIndex("hnsw_idx", 1, 32, L2Metric)
	manager.indexes["ivf_idx"] = ivfIdx
	manager.indexes["hnsw_idx"] = hnswIdx

	// Insert vectors
	for i := 0; i < 50; i++ {
		vector := make([]float32, 32)
		for j := 0; j < 32; j++ {
			vector[j] = float32(i) * 0.01
		}
		ivfIdx.Insert(uint64(i), vector)
		hnswIdx.Insert(uint64(i), vector)
	}

	// Optimize all indexes
	if err := manager.OptimizeAllIndexes(ctx); err != nil {
		t.Fatalf("OptimizeAllIndexes failed: %v", err)
	}

	// Verify both indexes still work
	query := make([]float32, 32)
	for j := 0; j < 32; j++ {
		query[j] = 0.25
	}

	_, ivfRowIDs, _ := ivfIdx.ANNSearch(ctx, query, 5)
	_, hnswRowIDs, _ := hnswIdx.ANNSearch(ctx, query, 5)

	if len(ivfRowIDs) != 5 {
		t.Errorf("Expected 5 results from IVF index, got %d", len(ivfRowIDs))
	}

	if len(hnswRowIDs) != 5 {
		t.Errorf("Expected 5 results from HNSW index, got %d", len(hnswRowIDs))
	}
}

func TestRebuildIVFIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test4", NewLocalRenameCommitHandler())

	// Create IVF index
	idx := NewIVFIndex("rebuild_ivf", 0, 64, L2Metric)
	manager.indexes["rebuild_ivf"] = idx

	// Insert initial vectors
	for i := 0; i < 50; i++ {
		vector := make([]float32, 64)
		for j := 0; j < 64; j++ {
			vector[j] = float32(i) * 0.01
		}
		idx.Insert(uint64(i), vector)
	}

	// Create new vectors for rebuild
	newVectors := make(map[uint64][]float32)
	for i := 0; i < 30; i++ {
		vector := make([]float32, 64)
		for j := 0; j < 64; j++ {
			vector[j] = float32(i) * 0.02
		}
		newVectors[uint64(i)] = vector
	}

	// Rebuild the index
	if err := manager.RebuildIndex(ctx, "rebuild_ivf", newVectors); err != nil {
		t.Fatalf("RebuildIndex failed: %v", err)
	}

	// Verify index has new data
	if idx.Statistics().NumEntries != 30 {
		t.Errorf("Expected 30 entries after rebuild, got %d", idx.Statistics().NumEntries)
	}

	// Verify search works
	query := make([]float32, 64)
	for j := 0; j < 64; j++ {
		query[j] = 0.3
	}

	_, rowIDs, err := idx.ANNSearch(ctx, query, 5)
	if err != nil {
		t.Fatalf("ANNSearch after rebuild failed: %v", err)
	}

	if len(rowIDs) != 5 {
		t.Errorf("Expected 5 results after rebuild, got %d", len(rowIDs))
	}
}

func TestRebuildHNSWIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test5", NewLocalRenameCommitHandler())

	// Create HNSW index
	idx := NewHNSWIndex("rebuild_hnsw", 0, 64, L2Metric)
	manager.indexes["rebuild_hnsw"] = idx

	// Insert initial vectors
	for i := 0; i < 50; i++ {
		vector := make([]float32, 64)
		for j := 0; j < 64; j++ {
			vector[j] = float32(i) * 0.01
		}
		idx.Insert(uint64(i), vector)
	}

	// Create new vectors for rebuild
	newVectors := make(map[uint64][]float32)
	for i := 0; i < 30; i++ {
		vector := make([]float32, 64)
		for j := 0; j < 64; j++ {
			vector[j] = float32(i) * 0.02
		}
		newVectors[uint64(i)] = vector
	}

	// Rebuild the index
	if err := manager.RebuildIndex(ctx, "rebuild_hnsw", newVectors); err != nil {
		t.Fatalf("RebuildIndex failed: %v", err)
	}

	// Verify index has new data
	if idx.Statistics().NumEntries != 30 {
		t.Errorf("Expected 30 entries after rebuild, got %d", idx.Statistics().NumEntries)
	}

	// Verify search works
	query := make([]float32, 64)
	for j := 0; j < 64; j++ {
		query[j] = 0.3
	}

	_, rowIDs, err := idx.ANNSearch(ctx, query, 5)
	if err != nil {
		t.Fatalf("ANNSearch after rebuild failed: %v", err)
	}

	if len(rowIDs) != 5 {
		t.Errorf("Expected 5 results after rebuild, got %d", len(rowIDs))
	}
}

func TestOptimizeNonExistentIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test6", NewLocalRenameCommitHandler())

	err := manager.OptimizeIndex(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

func TestRebuildNonExistentIndex(t *testing.T) {
	ctx := context.Background()
	manager := NewIndexManager("/tmp/test7", NewLocalRenameCommitHandler())

	vectors := make(map[uint64][]float32)
	err := manager.RebuildIndex(ctx, "nonexistent", vectors)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}
