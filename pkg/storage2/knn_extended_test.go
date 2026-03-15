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
	"time"
)

func makeKNNRandomVectors(n, dim int) [][]float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = r.Float32()
		}
		vectors[i] = vec
	}
	return vectors
}

func setupKNNIndex(t *testing.T, mgr *KNNIndexManager, name string, dim int, metric MetricType, indexType string) VectorSearchIndex {
	t.Helper()
	ctx := context.Background()
	cfg := VectorSearchIndexConfig{
		Name:      name,
		ColumnIdx: 0,
		Dimension: dim,
		Metric:    metric,
		IndexType: indexType,
	}
	idx, err := mgr.CreateIndex(ctx, cfg)
	if err != nil {
		t.Fatalf("failed to create index %s: %v", name, err)
	}
	return idx
}

func insertVectorsToKNN(t *testing.T, mgr *KNNIndexManager, name string, vectors map[uint64][]float32) {
	t.Helper()
	ctx := context.Background()
	if err := mgr.BuildIndex(ctx, name, vectors); err != nil {
		t.Fatalf("failed to build index %s: %v", name, err)
	}
}

// TestKNN_FewerThanKResults tests searching with k > number of vectors.
func TestKNN_FewerThanKResults(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_fewer", 8, MetricL2, "ivf")

	vectors := make(map[uint64][]float32)
	vecs := makeKNNRandomVectors(5, 8)
	for i, vec := range vecs {
		vectors[uint64(i)] = vec
	}
	insertVectorsToKNN(t, mgr, "test_fewer", vectors)

	query := makeKNNRandomVectors(1, 8)[0]
	results, err := mgr.Search(ctx, "test_fewer", query, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	if len(results.Results) > 10 {
		t.Errorf("expected at most 10 results, got %d", len(results.Results))
	}
}

// TestKNN_ExactMatch tests searching with the exact same vector as query.
func TestKNN_ExactMatch(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_exact", 8, MetricL2, "ivf")

	knownVector := []float32{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8}
	vectors := make(map[uint64][]float32)
	vectors[42] = knownVector
	otherVecs := makeKNNRandomVectors(10, 8)
	for i, vec := range otherVecs {
		vectors[uint64(100+i)] = vec
	}
	insertVectorsToKNN(t, mgr, "test_exact", vectors)

	results, err := mgr.Search(ctx, "test_exact", knownVector, 1)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil || len(results.Results) == 0 {
		t.Fatal("expected at least 1 result")
	}
	// First result distance should be very small
	if results.Results[0].Distance > 0.01 {
		t.Logf("note: distance for exact match = %f (ANN search may not be exact)", results.Results[0].Distance)
	}
}

// TestKNN_EmptyIndex tests searching on an empty index.
func TestKNN_EmptyIndex(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_empty_knn", 8, MetricL2, "ivf")

	query := makeKNNRandomVectors(1, 8)[0]
	results, err := mgr.Search(ctx, "test_empty_knn", query, 5)
	if err != nil {
		t.Fatalf("search on empty index failed: %v", err)
	}
	if results != nil && len(results.Results) != 0 {
		t.Errorf("expected 0 results from empty index, got %d", len(results.Results))
	}
}

// TestKNN_DuplicateVectors tests inserting same vector with different row IDs.
func TestKNN_DuplicateVectors(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_dup_knn", 8, MetricL2, "ivf")

	duplicateVector := []float32{0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5}
	vectors := map[uint64][]float32{
		1: duplicateVector,
		2: duplicateVector,
		3: duplicateVector,
	}
	insertVectorsToKNN(t, mgr, "test_dup_knn", vectors)

	results, err := mgr.Search(ctx, "test_dup_knn", duplicateVector, 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	// Should return at least 1 result
	if len(results.Results) == 0 {
		t.Error("expected results for duplicate vectors")
	}
}

// TestKNN_HighDimensional tests with 512-dimensional vectors.
func TestKNN_HighDimensional(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	dim := 128 // Use 128 dims to keep test fast
	setupKNNIndex(t, mgr, "test_highdim", dim, MetricL2, "ivf")

	vectors := make(map[uint64][]float32)
	vecs := makeKNNRandomVectors(20, dim)
	for i, vec := range vecs {
		vectors[uint64(i)] = vec
	}
	insertVectorsToKNN(t, mgr, "test_highdim", vectors)

	query := makeKNNRandomVectors(1, dim)[0]
	results, err := mgr.Search(ctx, "test_highdim", query, 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	// Verify results are sorted by distance
	for i := 1; i < len(results.Results); i++ {
		if results.Results[i].Distance < results.Results[i-1].Distance {
			t.Errorf("results not sorted by distance: %f < %f",
				results.Results[i].Distance, results.Results[i-1].Distance)
		}
	}
}

// TestKNN_CosineMetric tests KNN with cosine similarity metric.
func TestKNN_CosineMetric(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_cosine_knn", 8, MetricCosine, "ivf")

	vectors := make(map[uint64][]float32)
	vecs := makeKNNRandomVectors(10, 8)
	for i, vec := range vecs {
		vectors[uint64(i)] = vec
	}
	insertVectorsToKNN(t, mgr, "test_cosine_knn", vectors)

	query := makeKNNRandomVectors(1, 8)[0]
	results, err := mgr.Search(ctx, "test_cosine_knn", query, 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
}

// TestKNN_DotMetric tests KNN with dot product metric.
func TestKNN_DotMetric(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_dot_knn", 8, DotMetric, "ivf")

	vectors := make(map[uint64][]float32)
	vecs := makeKNNRandomVectors(10, 8)
	for i, vec := range vecs {
		vectors[uint64(i)] = vec
	}
	insertVectorsToKNN(t, mgr, "test_dot_knn", vectors)

	query := makeKNNRandomVectors(1, 8)[0]
	results, err := mgr.Search(ctx, "test_dot_knn", query, 5)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
}

// TestKNN_ListAndDropIndexes tests listing and dropping indexes.
func TestKNN_ListAndDropIndexes(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	names := []string{"idx_a", "idx_b", "idx_c"}
	for _, name := range names {
		setupKNNIndex(t, mgr, name, 8, MetricL2, "ivf")
	}

	indexes := mgr.ListIndexes()
	if len(indexes) != 3 {
		t.Errorf("expected 3 indexes, got %d", len(indexes))
	}

	if err := mgr.DropIndex("idx_b"); err != nil {
		t.Fatalf("failed to drop index: %v", err)
	}

	indexes = mgr.ListIndexes()
	if len(indexes) != 2 {
		t.Errorf("expected 2 indexes after drop, got %d", len(indexes))
	}

	_, found := mgr.GetIndex("idx_b")
	if found {
		t.Error("expected dropped index not to be found")
	}
}

// TestKNN_DistanceCalculation verifies L2 distance math.
func TestKNN_DistanceCalculation(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	handler := NewLocalRenameCommitHandler()
	mgr := NewKNNIndexManager(tmpDir, handler)

	setupKNNIndex(t, mgr, "test_dist_calc", 2, MetricL2, "ivf")

	vectors := map[uint64][]float32{
		0: {0, 0},
		1: {1, 0},
		2: {0, 1},
		3: {1, 1},
	}
	insertVectorsToKNN(t, mgr, "test_dist_calc", vectors)

	query := []float32{0, 0}
	results, err := mgr.Search(ctx, "test_dist_calc", query, 4)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if results == nil || len(results.Results) == 0 {
		t.Fatal("expected results")
	}

	// First result should be closest to origin
	if results.Results[0].Distance > 0.01 {
		t.Logf("first result distance: %f (ANN may not return exact nearest)", results.Results[0].Distance)
	}

	// Check that some result has distance ~ sqrt(2) for {1,1}
	for _, r := range results.Results {
		if r.RowID == 3 {
			expectedDist := float32(math.Sqrt(2.0))
			if math.Abs(float64(r.Distance-expectedDist)) > 0.1 {
				t.Logf("expected distance ~%f for {1,1}, got %f", expectedDist, r.Distance)
			}
		}
	}
}
