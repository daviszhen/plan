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
	"math/rand"
	"testing"
)

// --- IncrementalIVFIndex tests ---

func TestIncrementalIVFInsertBeforeTraining(t *testing.T) {
	dim := 32
	idx := NewIncrementalIVFIndex("inc_ivf", 0, dim, L2Metric, IncrementalIndexConfig{
		BufferSize:     100,
		MergeThreshold: 50,
	})

	// Insert before training — should go to pending
	for i := 0; i < 10; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		if err := idx.InsertIncremental(uint64(i), vec); err != nil {
			t.Fatal(err)
		}
	}

	if idx.PendingCount() != 10 {
		t.Fatalf("expected 10 pending, got %d", idx.PendingCount())
	}
}

func TestIncrementalIVFTrainAndInsert(t *testing.T) {
	dim := 32
	numVectors := 500
	idx := NewIncrementalIVFIndex("inc_ivf", 0, dim, L2Metric, IncrementalIndexConfig{
		BufferSize:     1000,
		MergeThreshold: 500,
	})

	vectors := make([][]float32, numVectors)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rand.Float32()
		}
	}

	// Train with initial batch
	if err := idx.Train(vectors[:200]); err != nil {
		t.Fatal(err)
	}

	// Incremental inserts after training
	for i := 200; i < numVectors; i++ {
		if err := idx.InsertIncremental(uint64(i), vectors[i]); err != nil {
			t.Fatal(err)
		}
	}

	// There should be no pending vectors (all inserted directly)
	if idx.PendingCount() != 0 {
		t.Fatalf("expected 0 pending, got %d", idx.PendingCount())
	}

	// Search should work
	dists, rowIDs, err := idx.ANNSearchWithPending(context.Background(), vectors[200], 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) == 0 {
		t.Fatal("no results")
	}
	t.Logf("Incremental IVF results: rowIDs=%v dists=%v", rowIDs, dists)
}

func TestIncrementalIVFRebuild(t *testing.T) {
	dim := 16
	idx := NewIncrementalIVFIndex("inc_ivf", 0, dim, L2Metric, IncrementalIndexConfig{
		BufferSize:     50,
		MergeThreshold: 20,
	})

	// Insert vectors without training — all go to pending
	vectors := make([][]float32, 30)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rand.Float32()
		}
		if err := idx.InsertIncremental(uint64(i), vectors[i]); err != nil {
			t.Fatal(err)
		}
	}

	if idx.PendingCount() != 30 {
		t.Fatalf("expected 30 pending, got %d", idx.PendingCount())
	}

	// Rebuild
	if err := idx.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}

	if idx.PendingCount() != 0 {
		t.Fatalf("expected 0 pending after rebuild, got %d", idx.PendingCount())
	}

	if idx.NeedsRebuild() {
		t.Fatal("should not need rebuild after rebuilding")
	}

	// Search should work
	_, rowIDs, err := idx.ANNSearchWithPending(context.Background(), vectors[0], 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) == 0 {
		t.Fatal("no results after rebuild")
	}
}

func TestIncrementalIVFNeedsRebuild(t *testing.T) {
	dim := 8
	idx := NewIncrementalIVFIndex("inc_ivf", 0, dim, L2Metric, IncrementalIndexConfig{
		BufferSize:     5,
		MergeThreshold: 3,
	})

	if idx.NeedsRebuild() {
		t.Fatal("should not need rebuild initially")
	}

	for i := 0; i < 5; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		idx.InsertIncremental(uint64(i), vec)
	}

	if !idx.NeedsRebuild() {
		t.Fatal("should need rebuild after reaching buffer size")
	}
}

func TestIncrementalIVFSearchWithPending(t *testing.T) {
	dim := 16
	idx := NewIncrementalIVFIndex("inc_ivf", 0, dim, L2Metric, IncrementalIndexConfig{
		BufferSize:     1000,
		MergeThreshold: 1000,
	})

	// Create a target vector (all zeros)
	target := make([]float32, dim)

	// Insert it into pending (no training)
	idx.InsertIncremental(0, target)

	// Insert random vectors
	for i := 1; i < 50; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		idx.InsertIncremental(uint64(i), vec)
	}

	// Searching for the target should find rowID=0 since it's an exact match
	dists, rowIDs, err := idx.ANNSearchWithPending(context.Background(), target, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) == 0 {
		t.Fatal("no results")
	}
	if rowIDs[0] != 0 {
		t.Fatalf("expected rowID 0 (exact match), got %d (dist=%f)", rowIDs[0], dists[0])
	}
	if dists[0] != 0 {
		t.Fatalf("expected distance 0, got %f", dists[0])
	}
}

// --- IncrementalHNSWIndex tests ---

func TestIncrementalHNSWBufferedInsert(t *testing.T) {
	dim := 16
	idx := NewIncrementalHNSWIndex("inc_hnsw", 0, dim, L2Metric, 100)

	for i := 0; i < 20; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		idx.InsertBuffered(uint64(i), vec)
	}

	if idx.PendingCount() != 20 {
		t.Fatalf("expected 20 pending, got %d", idx.PendingCount())
	}

	// Flush
	if err := idx.Flush(); err != nil {
		t.Fatal(err)
	}

	if idx.PendingCount() != 0 {
		t.Fatalf("expected 0 pending after flush, got %d", idx.PendingCount())
	}

	// Graph should have 20 vectors now
	stats := idx.Statistics()
	if stats.NumEntries != 20 {
		t.Fatalf("expected 20 entries, got %d", stats.NumEntries)
	}
}

func TestIncrementalHNSWSearchWithPending(t *testing.T) {
	dim := 16
	idx := NewIncrementalHNSWIndex("inc_hnsw", 0, dim, L2Metric, 1000)

	// Insert some vectors into graph
	for i := 0; i < 50; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatal(err)
		}
	}

	// Insert a special vector into buffer
	target := make([]float32, dim) // all zeros
	idx.InsertBuffered(999, target)

	// Search should find the buffered vector
	dists, rowIDs, err := idx.ANNSearchWithPending(context.Background(), target, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) == 0 {
		t.Fatal("no results")
	}
	if rowIDs[0] != 999 {
		t.Fatalf("expected rowID 999 (pending exact match), got %d (dist=%f)", rowIDs[0], dists[0])
	}
}

func TestIncrementalHNSWNeedsFlush(t *testing.T) {
	dim := 8
	idx := NewIncrementalHNSWIndex("inc_hnsw", 0, dim, L2Metric, 5)

	if idx.NeedsFlush() {
		t.Fatal("should not need flush initially")
	}

	for i := 0; i < 5; i++ {
		vec := make([]float32, dim)
		for j := range vec {
			vec[j] = rand.Float32()
		}
		idx.InsertBuffered(uint64(i), vec)
	}

	if !idx.NeedsFlush() {
		t.Fatal("should need flush after reaching buffer size")
	}
}
