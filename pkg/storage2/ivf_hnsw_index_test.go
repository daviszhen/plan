// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

func TestIVFHNSWIndexBasic(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 4
	cfg.Nprobe = 2

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	if idx.Name() != "test_ivf_hnsw" {
		t.Errorf("expected name 'test_ivf_hnsw', got '%s'", idx.Name())
	}

	if idx.Type() != VectorIndex {
		t.Errorf("expected type VectorIndex, got %v", idx.Type())
	}

	if idx.Dimension() != 4 {
		t.Errorf("expected dimension 4, got %d", idx.Dimension())
	}

	if idx.IsTrained() {
		t.Error("index should not be trained initially")
	}
}

func TestIVFHNSWIndexTrain(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 4
	cfg.Nprobe = 2

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Generate training vectors
	trainingVectors := make([][]float32, 100)
	for i := range trainingVectors {
		trainingVectors[i] = []float32{
			rand.Float32() * 10,
			rand.Float32() * 10,
			rand.Float32() * 10,
			rand.Float32() * 10,
		}
	}

	// Train the index
	err := idx.Train(trainingVectors)
	if err != nil {
		t.Fatalf("failed to train index: %v", err)
	}

	if !idx.IsTrained() {
		t.Error("index should be trained after Train()")
	}

	// Verify centroids were created
	if idx.NumPartitions() != 4 {
		t.Errorf("expected 4 partitions, got %d", idx.NumPartitions())
	}
}

func TestIVFHNSWIndexInsertAndSearch(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 4
	cfg.Nprobe = 4 // Search all partitions for accuracy

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Generate training vectors
	trainingVectors := make([][]float32, 50)
	for i := range trainingVectors {
		trainingVectors[i] = []float32{
			rand.Float32() * 10,
			rand.Float32() * 10,
			rand.Float32() * 10,
			rand.Float32() * 10,
		}
	}

	// Train
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// Insert vectors
	vectors := [][]float32{
		{1.0, 2.0, 3.0, 4.0},
		{2.0, 3.0, 4.0, 5.0},
		{5.0, 6.0, 7.0, 8.0},
		{9.0, 8.0, 7.0, 6.0},
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert vector %d: %v", i, err)
		}
	}

	// Verify statistics
	stats := idx.Statistics()
	if stats.NumEntries != 4 {
		t.Errorf("expected 4 entries, got %d", stats.NumEntries)
	}

	// Search for nearest neighbors to [1.0, 2.0, 3.0, 4.0]
	ctx := context.Background()
	query := []float32{1.0, 2.0, 3.0, 4.0}

	distances, rowIDs, err := idx.ANNSearch(ctx, query, 2)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Fatal("expected at least one result")
	}

	// The closest should be the exact match (rowID 0)
	if rowIDs[0] != 0 {
		t.Errorf("expected rowID 0 to be closest, got %d", rowIDs[0])
	}

	// Distance to exact match should be 0
	if distances[0] > 0.001 {
		t.Errorf("expected distance ~0 for exact match, got %f", distances[0])
	}
}

func TestIVFHNSWIndexWithPrefilter(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 2
	cfg.Nprobe = 2

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Train
	trainingVectors := make([][]float32, 20)
	for i := range trainingVectors {
		trainingVectors[i] = []float32{
			float32(i),
			float32(i),
			float32(i),
			float32(i),
		}
	}
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// Insert
	for i := 0; i < 10; i++ {
		vec := []float32{float32(i), float32(i), float32(i), float32(i)}
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Create a filter that only allows even row IDs
	filter := NewBitmapPreFilter(10)
	for i := 0; i < 10; i += 2 {
		filter.AddValidRow(uint64(i))
	}

	// Search with filter
	ctx := context.Background()
	query := []float32{3.0, 3.0, 3.0, 3.0}

	_, rowIDs, err := idx.ANNSearchWithFilter(ctx, query, 5, filter)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	// All results should be even
	for _, rowID := range rowIDs {
		if rowID%2 != 0 {
			t.Errorf("expected only even rowIDs, got %d", rowID)
		}
	}
}

func TestIVFHNSWIndexTrainWithFewVectors(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 10 // More clusters than vectors

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Only 5 training vectors, but nlist = 10
	trainingVectors := [][]float32{
		{1, 2, 3, 4},
		{2, 3, 4, 5},
		{3, 4, 5, 6},
		{4, 5, 6, 7},
		{5, 6, 7, 8},
	}

	// Should adjust nlist to number of vectors
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// nlist should be adjusted
	if idx.NumPartitions() != 5 {
		t.Errorf("expected 5 partitions (adjusted), got %d", idx.NumPartitions())
	}
}

func TestIVFHNSWIndexInsertWithoutTraining(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Try to insert without training
	err := idx.Insert(0, []float32{1, 2, 3, 4})
	if err == nil {
		t.Error("expected error when inserting without training")
	}
}

func TestIVFHNSWIndexDimensionMismatch(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Train with correct dimension
	trainingVectors := [][]float32{
		{1, 2, 3, 4},
		{2, 3, 4, 5},
	}
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// Try to insert with wrong dimension
	err := idx.Insert(0, []float32{1, 2, 3}) // Only 3 dimensions
	if err == nil {
		t.Error("expected error for dimension mismatch")
	}
}

func TestIVFHNSWIndexSetNprobe(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 10
	cfg.Nprobe = 2

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	if idx.GetNprobe() != 2 {
		t.Errorf("expected initial nprobe 2, got %d", idx.GetNprobe())
	}

	idx.SetNprobe(5)
	if idx.GetNprobe() != 5 {
		t.Errorf("expected nprobe 5 after SetNprobe, got %d", idx.GetNprobe())
	}

	// Should not allow nprobe > nlist
	idx.SetNprobe(100)
	if idx.GetNprobe() != 5 {
		t.Errorf("nprobe should remain 5 when trying to set > nlist, got %d", idx.GetNprobe())
	}
}

func TestIVFHNSWIndexGetPartitionStats(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 4
	cfg.Nprobe = 4

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Train
	trainingVectors := make([][]float32, 40)
	for i := range trainingVectors {
		trainingVectors[i] = []float32{
			float32(i % 10),
			float32(i / 10),
			float32(i % 5),
			float32(i / 5),
		}
	}
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// Insert vectors
	for i := 0; i < 20; i++ {
		vec := []float32{float32(i % 10), float32(i / 10), float32(i % 5), float32(i / 5)}
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Get partition stats
	stats := idx.GetPartitionStats()
	if len(stats) != 4 {
		t.Errorf("expected 4 partition stats, got %d", len(stats))
	}

	// Total should equal number of inserted vectors
	total := 0
	for _, count := range stats {
		total += count
	}
	if total != 20 {
		t.Errorf("expected total 20 vectors across partitions, got %d", total)
	}
}

func TestIVFHNSWIndexClear(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 4

	idx := NewIVFHNSWIndex("test_ivf_hnsw", 0, cfg)

	// Train and insert
	trainingVectors := make([][]float32, 20)
	for i := range trainingVectors {
		trainingVectors[i] = []float32{float32(i), float32(i), float32(i), float32(i)}
	}
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i := 0; i < 10; i++ {
		vec := []float32{float32(i), float32(i), float32(i), float32(i)}
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Verify data exists
	stats := idx.Statistics()
	if stats.NumEntries != 10 {
		t.Errorf("expected 10 entries before clear, got %d", stats.NumEntries)
	}

	// Clear
	idx.Clear()

	// Verify cleared
	stats = idx.Statistics()
	if stats.NumEntries != 0 {
		t.Errorf("expected 0 entries after clear, got %d", stats.NumEntries)
	}
}

func TestKmeansIVFHNSW(t *testing.T) {
	// Simple test data: 4 clusters in 2D
	vectors := [][]float32{
		// Cluster 1 (around 0,0)
		{0.1, 0.1}, {0.2, 0.2}, {-0.1, 0.1}, {0.1, -0.1},
		// Cluster 2 (around 10,0)
		{10.1, 0.1}, {9.9, 0.2}, {10.2, -0.1},
		// Cluster 3 (around 0,10)
		{0.1, 10.1}, {-0.1, 9.9}, {0.2, 10.2},
		// Cluster 4 (around 10,10)
		{10.1, 10.1}, {9.9, 9.9}, {10.2, 10.2},
	}

	centroids, err := kmeansIVFHNSW(vectors, 4, 100, L2Metric)
	if err != nil {
		t.Fatalf("kmeans failed: %v", err)
	}

	if len(centroids) != 4 {
		t.Errorf("expected 4 centroids, got %d", len(centroids))
	}

	// Check that centroids are roughly where expected
	expectedCenters := [][]float32{
		{0, 0}, {10, 0}, {0, 10}, {10, 10},
	}

	for _, expected := range expectedCenters {
		found := false
		for _, centroid := range centroids {
			dist := computeDistanceIVFHNSW(centroid, expected, L2Metric)
			if dist < 1.0 { // Within 1.0 of expected
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no centroid found near expected position %v", expected)
		}
	}
}

func TestComputeDistanceIVFHNSW(t *testing.T) {
	a := []float32{1, 2, 3, 4}
	b := []float32{5, 6, 7, 8}

	// Test L2 distance
	l2Dist := computeDistanceIVFHNSW(a, b, L2Metric)
	expectedL2 := float32(math.Sqrt(16 + 16 + 16 + 16)) // sqrt(64) = 8
	if math.Abs(float64(l2Dist-expectedL2)) > 0.001 {
		t.Errorf("L2 distance: expected %f, got %f", expectedL2, l2Dist)
	}

	// Test with identical vectors
	sameL2 := computeDistanceIVFHNSW(a, a, L2Metric)
	if sameL2 != 0 {
		t.Errorf("L2 distance of identical vectors should be 0, got %f", sameL2)
	}

	// Test cosine distance
	cosineDist := computeDistanceIVFHNSW(a, a, CosineMetric)
	if math.Abs(float64(cosineDist)) > 0.001 {
		t.Errorf("cosine distance of identical vectors should be 0, got %f", cosineDist)
	}

	// Test dot product distance
	dotDist := computeDistanceIVFHNSW(a, b, DotMetric)
	expectedDot := float32(-(1*5 + 2*6 + 3*7 + 4*8)) // -(5+12+21+32) = -70
	if dotDist != expectedDot {
		t.Errorf("dot product distance: expected %f, got %f", expectedDot, dotDist)
	}
}

func TestIVFHNSWIndexCosineMetric(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(4)
	cfg.Nlist = 2
	cfg.Nprobe = 2
	cfg.MetricType = CosineMetric

	idx := NewIVFHNSWIndex("test_cosine", 0, cfg)

	// Train
	trainingVectors := [][]float32{
		{1, 0, 0, 0},
		{0, 1, 0, 0},
		{0, 0, 1, 0},
		{0, 0, 0, 1},
	}
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// Insert
	for i, vec := range trainingVectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Search for a vector similar to the first one
	ctx := context.Background()
	query := []float32{1, 0.1, 0, 0} // Close to {1, 0, 0, 0}

	distances, rowIDs, err := idx.ANNSearch(ctx, query, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Fatal("expected at least one result")
	}

	if rowIDs[0] != 0 {
		t.Errorf("expected rowID 0 to be closest for cosine similarity, got %d", rowIDs[0])
	}

	// Cosine distance should be small but > 0 since query is not exactly parallel
	if distances[0] >= 0.5 {
		t.Errorf("expected small cosine distance, got %f", distances[0])
	}
}

func TestIVFHNSWIndexWithQuantization(t *testing.T) {
	cfg := DefaultIVFHNSWConfig(8) // 8 dimensions for PQ divisibility
	cfg.Nlist = 4
	cfg.Nprobe = 4
	cfg.QuantizerType = QuantizationSQ // Use SQ for simpler test

	idx := NewIVFHNSWIndex("test_quantized", 0, cfg)

	// Generate training vectors
	trainingVectors := make([][]float32, 50)
	for i := range trainingVectors {
		trainingVectors[i] = make([]float32, 8)
		for j := 0; j < 8; j++ {
			trainingVectors[i][j] = rand.Float32() * 10
		}
	}

	// Train - this should also train the quantizer
	if err := idx.Train(trainingVectors); err != nil {
		t.Fatalf("failed to train with quantization: %v", err)
	}

	if !idx.IsTrained() {
		t.Error("index should be trained")
	}

	// Insert and search should still work
	for i := 0; i < 10; i++ {
		vec := make([]float32, 8)
		for j := 0; j < 8; j++ {
			vec[j] = float32(i) + rand.Float32()
		}
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	ctx := context.Background()
	query := make([]float32, 8)
	for j := 0; j < 8; j++ {
		query[j] = 5.0
	}

	_, rowIDs, err := idx.ANNSearch(ctx, query, 3)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected at least one result")
	}
}
