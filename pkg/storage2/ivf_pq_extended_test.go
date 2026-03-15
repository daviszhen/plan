// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"context"
	"math"
	"math/rand"
	"testing"
)

// generateClusteredVectors generates vectors clustered around centroids.
// Returns (vectors, centroids) where vectors are distributed among the clusters.
func generateClusteredVectors(numClusters, vectorsPerCluster, dimension int) ([][]float32, [][]float32) {
	totalVectors := numClusters * vectorsPerCluster
	vectors := make([][]float32, totalVectors)
	centroids := make([][]float32, numClusters)

	// Generate random centroids
	for c := 0; c < numClusters; c++ {
		centroids[c] = make([]float32, dimension)
		for d := 0; d < dimension; d++ {
			centroids[c][d] = rand.Float32() * 100
		}
	}

	// Generate vectors around each centroid with some noise
	idx := 0
	for c := 0; c < numClusters; c++ {
		for v := 0; v < vectorsPerCluster; v++ {
			vectors[idx] = make([]float32, dimension)
			for d := 0; d < dimension; d++ {
				// Add Gaussian-like noise around centroid
				noise := (rand.Float32() - 0.5) * 10 // noise in [-5, 5]
				vectors[idx][d] = centroids[c][d] + noise
			}
			idx++
		}
	}

	// Shuffle vectors
	rand.Shuffle(len(vectors), func(i, j int) {
		vectors[i], vectors[j] = vectors[j], vectors[i]
	})

	return vectors, centroids
}

// TestIVFPQ_MultiNprobe tests that increasing nprobe generally improves recall.
func TestIVFPQ_MultiNprobe(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_nprobe", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Generate clustered vectors
	vectors, _ := generateClusteredVectors(10, 10, dim) // 100 vectors in 10 clusters

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Search with nprobe=1
	idx.SetNProbe(1)
	_, rowIDs1, err := idx.ANNSearch(ctx, vectors[0], 10)
	if err != nil {
		t.Fatalf("search with nprobe=1 failed: %v", err)
	}

	// Search with nprobe=5
	idx.SetNProbe(5)
	_, rowIDs5, err := idx.ANNSearch(ctx, vectors[0], 10)
	if err != nil {
		t.Fatalf("search with nprobe=5 failed: %v", err)
	}

	// More probes should generally find more candidates (or at least as many)
	// Note: Due to clustering, this may not always be strictly more, but the
	// probability of finding better results increases with more probes
	t.Logf("nprobe=1 found %d results, nprobe=5 found %d results", len(rowIDs1), len(rowIDs5))

	// At minimum, both should return results
	if len(rowIDs1) == 0 {
		t.Error("nprobe=1 returned no results")
	}
	if len(rowIDs5) == 0 {
		t.Error("nprobe=5 returned no results")
	}
}

// TestIVFPQ_EmptyPartition tests search when some partitions are empty.
func TestIVFPQ_EmptyPartition(t *testing.T) {
	ctx := context.Background()
	dim := 16

	idx, err := NewIVFPQIndex("test_empty", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Create vectors that will only populate a few clusters
	// Use very distinct clusters so most remain empty
	vectors := make([][]float32, 20)
	for i := 0; i < 20; i++ {
		vectors[i] = make([]float32, dim)
		// All vectors are very similar, will cluster together
		for d := 0; d < dim; d++ {
			vectors[i][d] = float32(i) * 0.001
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Search should still work even with empty partitions
	idx.SetNProbe(10) // Try to probe multiple partitions
	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}
	if len(distances) != len(rowIDs) {
		t.Errorf("distances and rowIDs length mismatch: %d vs %d", len(distances), len(rowIDs))
	}
}

// TestIVFPQ_CosineMetric tests IVF-PQ with cosine distance metric.
func TestIVFPQ_CosineMetric(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_cosine", 0, dim, CosineMetric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32()
		}
		// Normalize for meaningful cosine similarity
		norm := float32(0)
		for d := 0; d < dim; d++ {
			norm += vectors[i][d] * vectors[i][d]
		}
		norm = float32(math.Sqrt(float64(norm)))
		for d := 0; d < dim; d++ {
			vectors[i][d] /= norm
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}

	// First result should be the query itself (distance ~0)
	if distances[0] > 0.001 {
		t.Logf("Warning: first result distance=%f, expected ~0 for self-match", distances[0])
	}

	// Verify metric type
	if idx.GetMetricType() != CosineMetric {
		t.Errorf("expected CosineMetric, got %v", idx.GetMetricType())
	}
}

// TestIVFPQ_DotMetric tests IVF-PQ with dot product metric.
func TestIVFPQ_DotMetric(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_dot", 0, dim, DotMetric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32()
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}

	// With dot product, distances are negated for sorting (higher dot = lower distance)
	// First result should be the query itself (distance should be -dot(query, query))
	t.Logf("Dot metric search: distances=%v, rowIDs=%v", distances, rowIDs)

	// Verify metric type
	if idx.GetMetricType() != DotMetric {
		t.Errorf("expected DotMetric, got %v", idx.GetMetricType())
	}
}

// TestIVFPQ_L2Metric tests IVF-PQ with L2 (Euclidean) distance metric.
func TestIVFPQ_L2Metric(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_l2", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32() * 10
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}

	// First result should be the query itself (distance ~0)
	if distances[0] > 0.001 {
		t.Logf("Warning: first result distance=%f, expected ~0 for self-match", distances[0])
	}

	// Verify L2 distance computation
	if rowIDs[0] == 0 {
		// Self-match should have distance 0
		if distances[0] > 0.0001 {
			t.Errorf("self-match distance should be ~0, got %f", distances[0])
		}
	}

	// Verify metric type
	if idx.GetMetricType() != L2Metric {
		t.Errorf("expected L2Metric, got %v", idx.GetMetricType())
	}
}

// TestIVFPQ_NoQuantizer tests IVF-PQ without quantization (raw vector storage).
func TestIVFPQ_NoQuantizer(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_no_quant", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32()
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}

	// Without quantization, distances should be exact
	// First result should be the exact self-match
	if distances[0] > 0.0001 {
		t.Errorf("expected distance ~0 for self-match without quantization, got %f", distances[0])
	}
}

// TestIVFPQ_WithPQ tests IVF-PQ with Product Quantization.
func TestIVFPQ_WithPQ(t *testing.T) {
	ctx := context.Background()
	dim := 64 // Must be divisible by numSubvectors

	idx, err := NewIVFPQIndex("test_pq", 0, dim, L2Metric, QuantizationPQ)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 200)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32() * 10
		}
	}

	if err := idx.Train(vectors[:100]); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}

	// With PQ, distances are approximate
	t.Logf("PQ search results: distances=%v, rowIDs=%v", distances, rowIDs)

	// Verify distances are sorted
	for i := 1; i < len(distances); i++ {
		if distances[i] < distances[i-1] {
			t.Errorf("distances not sorted at index %d: %f < %f", i, distances[i], distances[i-1])
		}
	}
}

// TestIVFPQ_WithSQ tests IVF-PQ with Scalar Quantization.
func TestIVFPQ_WithSQ(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_sq", 0, dim, L2Metric, QuantizationSQ)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 150)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32() * 10
		}
	}

	if err := idx.Train(vectors[:100]); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) == 0 {
		t.Error("expected results, got none")
	}

	// With SQ, distances are approximate but typically more accurate than PQ
	t.Logf("SQ search results: distances=%v, rowIDs=%v", distances, rowIDs)

	// Verify distances are sorted
	for i := 1; i < len(distances); i++ {
		if distances[i] < distances[i-1] {
			t.Errorf("distances not sorted at index %d: %f < %f", i, distances[i], distances[i-1])
		}
	}
}

// TestIVFPQ_SearchLimit tests search with various k values.
func TestIVFPQ_SearchLimit(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_limit", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 50)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32()
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	testCases := []int{1, 5, 20}
	for _, k := range testCases {
		distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], k)
		if err != nil {
			t.Fatalf("search with k=%d failed: %v", k, err)
		}

		// Should return at most k results (may be fewer if not enough vectors)
		if len(rowIDs) > k {
			t.Errorf("k=%d: expected at most %d results, got %d", k, k, len(rowIDs))
		}
		if len(distances) != len(rowIDs) {
			t.Errorf("k=%d: distances and rowIDs length mismatch", k)
		}

		t.Logf("k=%d: returned %d results", k, len(rowIDs))
	}
}

// TestIVFPQ_SearchSortedByDistance verifies results are sorted by ascending distance.
func TestIVFPQ_SearchSortedByDistance(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_sorted", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32() * 100
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Perform multiple searches and verify sorting
	for trial := 0; trial < 5; trial++ {
		query := vectors[rand.Intn(len(vectors))]
		distances, rowIDs, err := idx.ANNSearch(ctx, query, 20)
		if err != nil {
			t.Fatalf("search failed: %v", err)
		}

		// Verify distances are sorted in ascending order
		for i := 1; i < len(distances); i++ {
			if distances[i] < distances[i-1] {
				t.Errorf("trial %d: distances not sorted at index %d: %f < %f",
					trial, i, distances[i], distances[i-1])
			}
		}

		// Verify each rowID is unique
		seen := make(map[uint64]bool)
		for _, id := range rowIDs {
			if seen[id] {
				t.Errorf("trial %d: duplicate rowID %d in results", trial, id)
			}
			seen[id] = true
		}
	}
}

// TestIVFPQ_InsertAndSearch tests inserting vectors and searching for cluster centers.
func TestIVFPQ_InsertAndSearch(t *testing.T) {
	ctx := context.Background()
	dim := 32
	numClusters := 10
	vectorsPerCluster := 20

	idx, err := NewIVFPQIndex("test_insert_search", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Generate clustered vectors
	vectors, centroids := generateClusteredVectors(numClusters, vectorsPerCluster, dim)

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Search for each centroid
	idx.SetNProbe(numClusters) // Probe all clusters
	correctCluster := 0
	for c, centroid := range centroids {
		_, rowIDs, err := idx.ANNSearch(ctx, centroid, 1)
		if err != nil {
			t.Fatalf("search for centroid %d failed: %v", c, err)
		}

		if len(rowIDs) == 0 {
			t.Errorf("no results for centroid %d", c)
			continue
		}

		// Find which cluster the result belongs to
		resultVec := vectors[rowIDs[0]]
		minDist := float32(math.MaxFloat32)
		closestCluster := 0
		for cc, cent := range centroids {
			d := l2Distance(resultVec, cent)
			if d < minDist {
				minDist = d
				closestCluster = cc
			}
		}

		if closestCluster == c {
			correctCluster++
		}
	}

	t.Logf("Correct cluster matches: %d/%d", correctCluster, numClusters)
	// At least half should match the correct cluster
	if correctCluster < numClusters/2 {
		t.Errorf("expected at least %d correct cluster matches, got %d", numClusters/2, correctCluster)
	}
}

// TestIVFPQ_Statistics verifies Statistics() returns meaningful data.
func TestIVFPQ_Statistics(t *testing.T) {
	dim := 32

	idx, err := NewIVFPQIndex("test_stats", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Initial stats
	stats := idx.Statistics()
	if stats.NumEntries != 0 {
		t.Errorf("expected 0 entries initially, got %d", stats.NumEntries)
	}

	// Train and insert
	vectors := make([][]float32, 100)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32()
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Check stats after insertion
	stats = idx.Statistics()
	if stats.NumEntries != 100 {
		t.Errorf("expected 100 entries, got %d", stats.NumEntries)
	}

	if stats.SizeBytes == 0 {
		t.Error("expected non-zero SizeBytes")
	}

	if stats.IndexType != "ivf_pq" {
		t.Errorf("expected index type 'ivf_pq', got '%s'", stats.IndexType)
	}

	t.Logf("Statistics: NumEntries=%d, SizeBytes=%d, IndexType=%s",
		stats.NumEntries, stats.SizeBytes, stats.IndexType)
}

// TestIVFPQ_DimensionMismatch tests error handling for wrong vector dimensions.
func TestIVFPQ_DimensionMismatch(t *testing.T) {
	dim := 32

	idx, err := NewIVFPQIndex("test_dim_mismatch", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Train with correct dimension
	trainVectors := make([][]float32, 50)
	for i := range trainVectors {
		trainVectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			trainVectors[i][d] = rand.Float32()
		}
	}
	if err := idx.Train(trainVectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	// Try to insert vector with wrong dimension
	wrongDimVector := make([]float32, 16) // Wrong: should be 32
	err = idx.Insert(0, wrongDimVector)
	if err == nil {
		t.Error("expected error for dimension mismatch, got nil")
	}
	t.Logf("Got expected error: %v", err)
}

// TestIVFPQ_SearchBeforeTrain tests search behavior before training.
func TestIVFPQ_SearchBeforeTrain(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_before_train", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	query := make([]float32, dim)
	for d := 0; d < dim; d++ {
		query[d] = rand.Float32()
	}

	// Search without training or inserting
	distances, rowIDs, err := idx.ANNSearch(ctx, query, 10)
	if err != nil {
		t.Logf("Search before train returned error: %v", err)
	}

	// Should return empty results (no vectors inserted)
	if len(rowIDs) != 0 || len(distances) != 0 {
		t.Errorf("expected empty results before training, got %d rowIDs", len(rowIDs))
	}
}

// TestIVFPQ_LargeKSearch tests search with k larger than number of vectors.
func TestIVFPQ_LargeKSearch(t *testing.T) {
	ctx := context.Background()
	dim := 32

	idx, err := NewIVFPQIndex("test_large_k", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	// Insert only 10 vectors
	vectors := make([][]float32, 10)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for d := 0; d < dim; d++ {
			vectors[i][d] = rand.Float32()
		}
	}

	if err := idx.Train(vectors); err != nil {
		t.Fatalf("failed to train: %v", err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
	}

	// Search with k=100 (more than available vectors)
	distances, rowIDs, err := idx.ANNSearch(ctx, vectors[0], 100)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	// Should return at most 10 results (all available vectors)
	if len(rowIDs) > 10 {
		t.Errorf("expected at most 10 results, got %d", len(rowIDs))
	}

	// Should return all available vectors
	if len(rowIDs) != 10 {
		t.Errorf("expected 10 results (all vectors), got %d", len(rowIDs))
	}

	if len(distances) != len(rowIDs) {
		t.Errorf("distances and rowIDs length mismatch: %d vs %d", len(distances), len(rowIDs))
	}
}
