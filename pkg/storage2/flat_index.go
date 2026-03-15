// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"container/heap"
	"errors"
	"math"
	"sort"
	"sync"
)

// Errors for vector index operations
var (
	ErrDimensionMismatch = errors.New("vector dimension mismatch")
	ErrInvalidInput      = errors.New("invalid input")
)

// FlatIndex is a brute-force vector index that performs exact nearest neighbor search.
// It's suitable for small datasets where building more complex indexes is not worth it.
type FlatIndex struct {
	name      string
	columnIdx int
	dimension int
	metric    MetricType

	mu      sync.RWMutex
	vectors map[uint64][]float32 // rowID -> vector
}

// NewFlatIndex creates a new FlatIndex
func NewFlatIndex(name string, columnIdx, dimension int, metric MetricType) *FlatIndex {
	return &FlatIndex{
		name:      name,
		columnIdx: columnIdx,
		dimension: dimension,
		metric:    metric,
		vectors:   make(map[uint64][]float32),
	}
}

// Name returns the index name
func (idx *FlatIndex) Name() string {
	return idx.name
}

// ColumnIndex returns the column index
func (idx *FlatIndex) ColumnIndex() int {
	return idx.columnIdx
}

// Dimension returns the vector dimension
func (idx *FlatIndex) Dimension() int {
	return idx.dimension
}

// Metric returns the distance metric type
func (idx *FlatIndex) Metric() MetricType {
	return idx.metric
}

// Size returns the number of vectors in the index
func (idx *FlatIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.vectors)
}

// Add adds a vector to the index
func (idx *FlatIndex) Add(rowID uint64, vector []float32) error {
	if len(vector) != idx.dimension {
		return ErrDimensionMismatch
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Copy the vector
	v := make([]float32, len(vector))
	copy(v, vector)
	idx.vectors[rowID] = v

	return nil
}

// AddBatch adds multiple vectors to the index
func (idx *FlatIndex) AddBatch(rowIDs []uint64, vectors [][]float32) error {
	if len(rowIDs) != len(vectors) {
		return ErrInvalidInput
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	for i, rowID := range rowIDs {
		if len(vectors[i]) != idx.dimension {
			return ErrDimensionMismatch
		}
		v := make([]float32, idx.dimension)
		copy(v, vectors[i])
		idx.vectors[rowID] = v
	}

	return nil
}

// Remove removes a vector from the index
func (idx *FlatIndex) Remove(rowID uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.vectors, rowID)
}

// Search performs k-nearest neighbor search
func (idx *FlatIndex) Search(query []float32, k int, filter PreFilter) ([]SearchResult, error) {
	if len(query) != idx.dimension {
		return nil, ErrDimensionMismatch
	}
	if k <= 0 {
		return nil, nil
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Calculate distances to all vectors
	results := make([]SearchResult, 0, len(idx.vectors))

	for rowID, vector := range idx.vectors {
		// Apply pre-filter
		if filter != nil && !filter.ShouldInclude(rowID) {
			continue
		}

		distance := idx.computeDistance(query, vector)
		results = append(results, SearchResult{
			RowID:    rowID,
			Distance: distance,
		})
	}

	// Sort by distance
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	// Return top-k
	if len(results) > k {
		results = results[:k]
	}

	return results, nil
}

// SearchWithRadius performs range search within a given radius
func (idx *FlatIndex) SearchWithRadius(query []float32, radius float32, maxResults int, filter PreFilter) ([]SearchResult, error) {
	if len(query) != idx.dimension {
		return nil, ErrDimensionMismatch
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	results := make([]SearchResult, 0)

	for rowID, vector := range idx.vectors {
		// Apply pre-filter
		if filter != nil && !filter.ShouldInclude(rowID) {
			continue
		}

		distance := idx.computeDistance(query, vector)
		if distance <= radius {
			results = append(results, SearchResult{
				RowID:    rowID,
				Distance: distance,
			})
		}
	}

	// Sort by distance
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})

	// Apply max results limit
	if maxResults > 0 && len(results) > maxResults {
		results = results[:maxResults]
	}

	return results, nil
}

// computeDistance calculates distance between two vectors based on the metric type
func (idx *FlatIndex) computeDistance(a, b []float32) float32 {
	switch idx.metric {
	case MetricL2:
		return l2DistanceFlat(a, b)
	case MetricCosine:
		return cosineDistanceFlat(a, b)
	case MetricDotProduct:
		return dotProductDistanceFlat(a, b)
	default:
		return l2DistanceFlat(a, b)
	}
}

// l2DistanceFlat calculates Euclidean (L2) distance between two vectors
func l2DistanceFlat(a, b []float32) float32 {
	var sum float32
	for i := range a {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// cosineDistanceFlat calculates cosine distance (1 - cosine similarity)
func cosineDistanceFlat(a, b []float32) float32 {
	var dot, normA, normB float32
	for i := range a {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	if normA == 0 || normB == 0 {
		return 1.0 // Maximum distance
	}

	similarity := dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
	return 1.0 - similarity
}

// dotProductDistanceFlat calculates negative dot product (for maximizing similarity)
func dotProductDistanceFlat(a, b []float32) float32 {
	var dot float32
	for i := range a {
		dot += a[i] * b[i]
	}
	return -dot // Negative because we want to minimize distance
}

// Metric type constants for FlatIndex (aliases for index.go constants)
const (
	MetricL2         = L2Metric
	MetricCosine     = CosineMetric
	MetricDotProduct = DotMetric
)

type searchResultHeap []SearchResult

func (h searchResultHeap) Len() int           { return len(h) }
func (h searchResultHeap) Less(i, j int) bool { return h[i].Distance > h[j].Distance } // Max heap
func (h searchResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *searchResultHeap) Push(x interface{}) {
	*h = append(*h, x.(SearchResult))
}

func (h *searchResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// FlatIndexBatchSearch performs batch search on multiple queries
func (idx *FlatIndex) BatchSearch(queries [][]float32, k int, filter PreFilter) ([][]SearchResult, error) {
	results := make([][]SearchResult, len(queries))

	for i, query := range queries {
		res, err := idx.Search(query, k, filter)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}

	return results, nil
}

// topKHeap maintains the top-k smallest distances using a max-heap
type topKHeap struct {
	h searchResultHeap
	k int
}

func newTopKHeap(k int) *topKHeap {
	return &topKHeap{
		h: make(searchResultHeap, 0, k),
		k: k,
	}
}

func (t *topKHeap) Push(result SearchResult) {
	if len(t.h) < t.k {
		heap.Push(&t.h, result)
	} else if result.Distance < t.h[0].Distance {
		heap.Pop(&t.h)
		heap.Push(&t.h, result)
	}
}

func (t *topKHeap) Results() []SearchResult {
	results := make([]SearchResult, len(t.h))
	copy(results, t.h)
	sort.Slice(results, func(i, j int) bool {
		return results[i].Distance < results[j].Distance
	})
	return results
}

// Clear removes all vectors from the index
func (idx *FlatIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.vectors = make(map[uint64][]float32)
}

// GetVector returns the vector for a given row ID
func (idx *FlatIndex) GetVector(rowID uint64) ([]float32, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	v, ok := idx.vectors[rowID]
	if !ok {
		return nil, false
	}
	// Return a copy
	result := make([]float32, len(v))
	copy(result, v)
	return result, true
}

// AllRowIDs returns all row IDs in the index
func (idx *FlatIndex) AllRowIDs() []uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	ids := make([]uint64, 0, len(idx.vectors))
	for id := range idx.vectors {
		ids = append(ids, id)
	}
	return ids
}
