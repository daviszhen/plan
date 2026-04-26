// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"math"
	"testing"
)

func TestFlatIndexBasic(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	if idx.Name() != "test" {
		t.Errorf("expected name 'test', got %s", idx.Name())
	}
	if idx.Dimension() != 3 {
		t.Errorf("expected dimension 3, got %d", idx.Dimension())
	}
	if idx.Size() != 0 {
		t.Errorf("expected size 0, got %d", idx.Size())
	}

	// Add vectors
	err := idx.Add(1, []float32{1.0, 0.0, 0.0})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	err = idx.Add(2, []float32{0.0, 1.0, 0.0})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	err = idx.Add(3, []float32{0.0, 0.0, 1.0})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	if idx.Size() != 3 {
		t.Errorf("expected size 3, got %d", idx.Size())
	}
}

func TestFlatIndexDimensionMismatch(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	// Wrong dimension should fail
	err := idx.Add(1, []float32{1.0, 0.0})
	if err != ErrDimensionMismatch {
		t.Errorf("expected ErrDimensionMismatch, got %v", err)
	}
}

func TestFlatIndexSearch(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	// Add unit vectors
	idx.Add(1, []float32{1.0, 0.0, 0.0})
	idx.Add(2, []float32{0.0, 1.0, 0.0})
	idx.Add(3, []float32{0.0, 0.0, 1.0})

	// Search for vector closest to [1,0,0]
	results, err := idx.Search([]float32{0.9, 0.1, 0.0}, 2, nil)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	// First result should be row 1 (closest to [1,0,0])
	if results[0].RowID != 1 {
		t.Errorf("expected first result to be row 1, got %d", results[0].RowID)
	}
}

func TestFlatIndexSearchWithFilter(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	idx.Add(1, []float32{1.0, 0.0, 0.0})
	idx.Add(2, []float32{0.0, 1.0, 0.0})
	idx.Add(3, []float32{0.0, 0.0, 1.0})

	// Create filter that excludes row 1
	filter := NewBitmapPreFilterFromValid([]uint64{2, 3}, 3)

	results, err := idx.Search([]float32{0.9, 0.1, 0.0}, 3, filter)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	// Should only get rows 2 and 3
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}

	for _, r := range results {
		if r.RowID == 1 {
			t.Error("row 1 should have been filtered out")
		}
	}
}

func TestFlatIndexSearchWithRadius(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	idx.Add(1, []float32{0.0, 0.0, 0.0})
	idx.Add(2, []float32{0.5, 0.0, 0.0})
	idx.Add(3, []float32{1.0, 0.0, 0.0})
	idx.Add(4, []float32{2.0, 0.0, 0.0})

	// Search within radius 0.6 from origin
	results, err := idx.SearchWithRadius([]float32{0.0, 0.0, 0.0}, 0.6, 0, nil)
	if err != nil {
		t.Fatalf("SearchWithRadius: %v", err)
	}

	// Should find rows 1 and 2 (distances 0 and 0.5)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestFlatIndexRemove(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	idx.Add(1, []float32{1.0, 0.0, 0.0})
	idx.Add(2, []float32{0.0, 1.0, 0.0})

	if idx.Size() != 2 {
		t.Errorf("expected size 2, got %d", idx.Size())
	}

	idx.Remove(1)

	if idx.Size() != 1 {
		t.Errorf("expected size 1, got %d", idx.Size())
	}

	// Search should not find row 1
	results, _ := idx.Search([]float32{1.0, 0.0, 0.0}, 10, nil)
	for _, r := range results {
		if r.RowID == 1 {
			t.Error("row 1 should have been removed")
		}
	}
}

func TestFlatIndexClear(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	idx.Add(1, []float32{1.0, 0.0, 0.0})
	idx.Add(2, []float32{0.0, 1.0, 0.0})

	idx.Clear()

	if idx.Size() != 0 {
		t.Errorf("expected size 0, got %d", idx.Size())
	}
}

func TestFlatIndexGetVector(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	idx.Add(1, []float32{1.0, 2.0, 3.0})

	v, ok := idx.GetVector(1)
	if !ok {
		t.Fatal("expected to find vector")
	}
	if len(v) != 3 || v[0] != 1.0 || v[1] != 2.0 || v[2] != 3.0 {
		t.Errorf("unexpected vector: %v", v)
	}

	// Non-existent vector
	_, ok = idx.GetVector(999)
	if ok {
		t.Error("expected not to find vector 999")
	}
}

func TestFlatIndexBatchSearch(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	idx.Add(1, []float32{1.0, 0.0, 0.0})
	idx.Add(2, []float32{0.0, 1.0, 0.0})
	idx.Add(3, []float32{0.0, 0.0, 1.0})

	queries := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
	}

	results, err := idx.BatchSearch(queries, 1, nil)
	if err != nil {
		t.Fatalf("BatchSearch: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 result sets, got %d", len(results))
	}

	// First query should match row 1
	if results[0][0].RowID != 1 {
		t.Errorf("query 1: expected row 1, got %d", results[0][0].RowID)
	}
	// Second query should match row 2
	if results[1][0].RowID != 2 {
		t.Errorf("query 2: expected row 2, got %d", results[1][0].RowID)
	}
}

func TestL2DistanceFlat(t *testing.T) {
	a := []float32{0.0, 0.0, 0.0}
	b := []float32{3.0, 4.0, 0.0}

	dist := l2DistanceFlat(a, b)
	if math.Abs(float64(dist)-5.0) > 0.001 {
		t.Errorf("expected distance 5.0, got %f", dist)
	}
}

func TestCosineDistanceFlat(t *testing.T) {
	// Same direction
	a := []float32{1.0, 0.0, 0.0}
	b := []float32{2.0, 0.0, 0.0}

	dist := cosineDistanceFlat(a, b)
	if math.Abs(float64(dist)) > 0.001 {
		t.Errorf("same direction: expected distance ~0, got %f", dist)
	}

	// Opposite direction
	c := []float32{-1.0, 0.0, 0.0}
	dist = cosineDistanceFlat(a, c)
	if math.Abs(float64(dist)-2.0) > 0.001 {
		t.Errorf("opposite direction: expected distance ~2, got %f", dist)
	}

	// Orthogonal
	d := []float32{0.0, 1.0, 0.0}
	dist = cosineDistanceFlat(a, d)
	if math.Abs(float64(dist)-1.0) > 0.001 {
		t.Errorf("orthogonal: expected distance ~1, got %f", dist)
	}
}

func TestDotProductDistanceFlat(t *testing.T) {
	a := []float32{1.0, 2.0, 3.0}
	b := []float32{4.0, 5.0, 6.0}

	// Dot product = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
	dist := dotProductDistanceFlat(a, b)
	if math.Abs(float64(dist)+32.0) > 0.001 {
		t.Errorf("expected distance -32, got %f", dist)
	}
}

func TestFlatIndexAddBatch(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricL2)

	rowIDs := []uint64{1, 2, 3}
	vectors := [][]float32{
		{1.0, 0.0, 0.0},
		{0.0, 1.0, 0.0},
		{0.0, 0.0, 1.0},
	}

	err := idx.AddBatch(rowIDs, vectors)
	if err != nil {
		t.Fatalf("AddBatch: %v", err)
	}

	if idx.Size() != 3 {
		t.Errorf("expected size 3, got %d", idx.Size())
	}
}

func TestFlatIndexCosineMetric(t *testing.T) {
	idx := NewFlatIndex("test", 0, 3, MetricCosine)

	// Add vectors with different magnitudes but same direction
	idx.Add(1, []float32{1.0, 0.0, 0.0})
	idx.Add(2, []float32{10.0, 0.0, 0.0})
	idx.Add(3, []float32{0.0, 1.0, 0.0})

	// Query in same direction - both 1 and 2 should have distance 0
	results, _ := idx.Search([]float32{5.0, 0.0, 0.0}, 3, nil)

	// First two results should have very small distance
	if results[0].Distance > 0.01 {
		t.Errorf("expected small distance for same direction, got %f", results[0].Distance)
	}
}
