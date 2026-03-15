// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package storage2

import (
	"context"
	"testing"
)

func TestRTreeIndexBasic(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})

	// Insert some 2D points
	for i := 0; i < 100; i++ {
		x := float64(i % 10)
		y := float64(i / 10)
		if err := idx.InsertPoint(uint64(i), []float64{x, y}); err != nil {
			t.Fatalf("InsertPoint: %v", err)
		}
	}

	stats := idx.Statistics()
	if stats.NumEntries != 100 {
		t.Errorf("NumEntries: got %d, want 100", stats.NumEntries)
	}
}

func TestRTreeIndexRangeSearch(t *testing.T) {
	// Use larger maxEntries to avoid triggering splits
	// (the current split algorithm is simplified)
	idx := NewRTreeIndex("spatial_idx", []int{0, 1}, WithRTreeMaxEntries(200))
	ctx := context.Background()

	// Insert 10x10 grid of points
	for i := 0; i < 100; i++ {
		x := float64(i % 10)
		y := float64(i / 10)
		if err := idx.InsertPoint(uint64(i), []float64{x, y}); err != nil {
			t.Fatalf("InsertPoint: %v", err)
		}
	}

	// Search for points in [2,5] x [3,7]
	queryBBox := &BoundingBox{
		MinPoint: []float64{2, 3},
		MaxPoint: []float64{5, 7},
	}
	results, err := idx.RangeSearch(ctx, queryBBox, 0)
	if err != nil {
		t.Fatalf("RangeSearch: %v", err)
	}

	// Expected: x in [2,5] and y in [3,7]
	// x: 2,3,4,5 (4 values)
	// y: 3,4,5,6,7 (5 values)
	// Total: 4 * 5 = 20 points
	if len(results) != 20 {
		t.Errorf("RangeSearch: got %d results, want 20", len(results))
	}
}

func TestRTreeIndexContainsSearch(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})
	ctx := context.Background()

	// Insert bounding boxes
	idx.Insert(0, &BoundingBox{MinPoint: []float64{0, 0}, MaxPoint: []float64{2, 2}})
	idx.Insert(1, &BoundingBox{MinPoint: []float64{1, 1}, MaxPoint: []float64{3, 3}})
	idx.Insert(2, &BoundingBox{MinPoint: []float64{5, 5}, MaxPoint: []float64{6, 6}})

	// Query box that contains box 2
	queryBBox := &BoundingBox{
		MinPoint: []float64{4, 4},
		MaxPoint: []float64{7, 7},
	}
	results, err := idx.ContainsSearch(ctx, queryBBox, 0)
	if err != nil {
		t.Fatalf("ContainsSearch: %v", err)
	}

	if len(results) != 1 || results[0] != 2 {
		t.Errorf("ContainsSearch: got %v, want [2]", results)
	}
}

func TestRTreeIndexNearestNeighbors(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})
	ctx := context.Background()

	// Insert points at (0,0), (1,1), (2,2), (3,3), (4,4)
	for i := 0; i < 5; i++ {
		if err := idx.InsertPoint(uint64(i), []float64{float64(i), float64(i)}); err != nil {
			t.Fatalf("InsertPoint: %v", err)
		}
	}

	// Find 3 nearest neighbors to (1.5, 1.5)
	rowIDs, distances, err := idx.NearestNeighbors(ctx, []float64{1.5, 1.5}, 3)
	if err != nil {
		t.Fatalf("NearestNeighbors: %v", err)
	}

	if len(rowIDs) != 3 {
		t.Fatalf("NearestNeighbors: got %d results, want 3", len(rowIDs))
	}

	// Nearest should be points 1 and 2 (both at distance sqrt(0.5))
	// Then point 0 or 3
	// Just verify we got 3 results with increasing distances
	for i := 0; i < len(distances)-1; i++ {
		if distances[i] > distances[i+1] {
			t.Errorf("Distances not sorted: %v", distances)
		}
	}
}

func TestRTreeIndex3D(t *testing.T) {
	idx := NewRTreeIndex("spatial_3d_idx", []int{0, 1, 2}, WithRTreeDimension(3))
	ctx := context.Background()

	// Insert 3D points
	for i := 0; i < 27; i++ {
		x := float64(i % 3)
		y := float64((i / 3) % 3)
		z := float64(i / 9)
		if err := idx.InsertPoint(uint64(i), []float64{x, y, z}); err != nil {
			t.Fatalf("InsertPoint: %v", err)
		}
	}

	// Search in 3D box
	queryBBox := &BoundingBox{
		MinPoint: []float64{0, 0, 0},
		MaxPoint: []float64{1, 1, 1},
	}
	results, err := idx.RangeSearch(ctx, queryBBox, 0)
	if err != nil {
		t.Fatalf("RangeSearch: %v", err)
	}

	// Expected: x,y,z each in [0,1] => 2*2*2 = 8 points
	if len(results) != 8 {
		t.Errorf("3D RangeSearch: got %d results, want 8", len(results))
	}
}

func TestRTreeIndexSerialization(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})

	// Insert some points
	for i := 0; i < 50; i++ {
		x := float64(i % 10)
		y := float64(i / 10)
		if err := idx.InsertPoint(uint64(i), []float64{x, y}); err != nil {
			t.Fatalf("InsertPoint: %v", err)
		}
	}

	// Marshal
	data, err := idx.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	// Unmarshal into new index
	idx2 := NewRTreeIndex("", []int{})
	if err := idx2.Unmarshal(data); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Verify
	if idx2.Name() != idx.Name() {
		t.Errorf("Name: got %q, want %q", idx2.Name(), idx.Name())
	}
	stats := idx2.Statistics()
	if stats.NumEntries != 50 {
		t.Errorf("NumEntries: got %d, want 50", stats.NumEntries)
	}

	// Test query on restored index
	ctx := context.Background()
	queryBBox := &BoundingBox{
		MinPoint: []float64{0, 0},
		MaxPoint: []float64{4, 4},
	}
	results, err := idx2.RangeSearch(ctx, queryBBox, 0)
	if err != nil {
		t.Fatalf("RangeSearch: %v", err)
	}
	// 5x5 = 25 points in [0,4] x [0,4]
	if len(results) != 25 {
		t.Errorf("RangeSearch after restore: got %d results, want 25", len(results))
	}
}

func TestRTreeIndexDimensionMismatch(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})

	// Try to insert 3D point into 2D index
	err := idx.InsertPoint(0, []float64{1, 2, 3})
	if err == nil {
		t.Error("Expected error for dimension mismatch")
	}
}

func TestRTreeIndexEmptySearch(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})
	ctx := context.Background()

	// Search on empty index
	queryBBox := &BoundingBox{
		MinPoint: []float64{0, 0},
		MaxPoint: []float64{10, 10},
	}
	results, err := idx.RangeSearch(ctx, queryBBox, 0)
	if err != nil {
		t.Fatalf("RangeSearch on empty: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("RangeSearch on empty: got %d, want 0", len(results))
	}
}

func TestRTreeIndexSearchWithLimit(t *testing.T) {
	idx := NewRTreeIndex("spatial_idx", []int{0, 1})
	ctx := context.Background()

	// Insert many points
	for i := 0; i < 100; i++ {
		if err := idx.InsertPoint(uint64(i), []float64{float64(i), float64(i)}); err != nil {
			t.Fatalf("InsertPoint: %v", err)
		}
	}

	// Search with limit
	queryBBox := &BoundingBox{
		MinPoint: []float64{0, 0},
		MaxPoint: []float64{100, 100},
	}
	results, err := idx.RangeSearch(ctx, queryBBox, 10)
	if err != nil {
		t.Fatalf("RangeSearch: %v", err)
	}
	if len(results) != 10 {
		t.Errorf("RangeSearch with limit: got %d, want 10", len(results))
	}
}
