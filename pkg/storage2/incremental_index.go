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
	"sort"
	"sync"
)

// IncrementalIndexConfig configures the incremental vector index wrapper.
type IncrementalIndexConfig struct {
	// BufferSize is the maximum number of pending vectors before triggering a rebuild.
	BufferSize int
	// MergeThreshold triggers a merge/rebuild when this many pending vectors exist.
	MergeThreshold int
}

// IncrementalIVFIndex wraps an IVFIndex with a write buffer that allows
// efficient incremental inserts without immediate full retraining.
// New vectors go into a pending buffer; searches check both the main
// index and the buffer. A rebuild merges everything.
type IncrementalIVFIndex struct {
	*IVFIndex

	pendingVectors map[uint64][]float32
	pendingCount   int
	bufferSize     int
	mergeThreshold int
	needsRebuild   bool
}

// NewIncrementalIVFIndex creates an incremental IVF index.
func NewIncrementalIVFIndex(name string, columnIdx int, dimension int, metric MetricType, cfg IncrementalIndexConfig) *IncrementalIVFIndex {
	ivf := NewIVFIndex(name, columnIdx, dimension, metric)

	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = 10000
	}
	mergeThreshold := cfg.MergeThreshold
	if mergeThreshold <= 0 {
		mergeThreshold = 5000
	}

	return &IncrementalIVFIndex{
		IVFIndex:       ivf,
		pendingVectors: make(map[uint64][]float32),
		bufferSize:     bufferSize,
		mergeThreshold: mergeThreshold,
	}
}

// InsertIncremental adds a vector. If the index is not yet trained, the vector
// goes into the pending buffer. Once trained, it is inserted directly into the
// closest cluster. The buffer is checked against thresholds.
func (idx *IncrementalIVFIndex) InsertIncremental(rowID uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// If centroids have not been trained, buffer the vector
	if len(idx.centroids) == 0 || idx.centroids[0] == nil {
		v := make([]float32, len(vector))
		copy(v, vector)
		idx.pendingVectors[rowID] = v
		idx.pendingCount++
		if idx.pendingCount >= idx.bufferSize {
			idx.needsRebuild = true
		}
		return nil
	}

	// Index is trained — insert directly into the closest cluster
	v := make([]float32, len(vector))
	copy(v, vector)
	idx.vectors[rowID] = v

	minDist := float32(math.MaxFloat32)
	closest := 0
	for i, c := range idx.centroids {
		if c != nil {
			d := l2Distance(vector, c)
			if d < minDist {
				minDist = d
				closest = i
			}
		}
	}
	idx.invertedLists[closest] = append(idx.invertedLists[closest], rowID)
	idx.stats.NumEntries++

	idx.pendingCount++
	if idx.pendingCount >= idx.mergeThreshold {
		idx.needsRebuild = true
	}

	return nil
}

// NeedsRebuild returns true when the buffer has accumulated enough vectors.
func (idx *IncrementalIVFIndex) NeedsRebuild() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.needsRebuild
}

// PendingCount returns the number of pending (not yet indexed) vectors.
func (idx *IncrementalIVFIndex) PendingCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.pendingVectors)
}

// Rebuild retrains the centroids and re-inserts all vectors (main + pending).
func (idx *IncrementalIVFIndex) Rebuild(ctx context.Context) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Collect all vectors
	allVectors := make([][]float32, 0, len(idx.vectors)+len(idx.pendingVectors))
	allRowIDs := make([]uint64, 0, len(idx.vectors)+len(idx.pendingVectors))

	for rowID, vec := range idx.vectors {
		allVectors = append(allVectors, vec)
		allRowIDs = append(allRowIDs, rowID)
	}
	for rowID, vec := range idx.pendingVectors {
		allVectors = append(allVectors, vec)
		allRowIDs = append(allRowIDs, rowID)
	}

	if len(allVectors) == 0 {
		return nil
	}

	// Retrain centroids (we already hold the lock)
	if err := idx.IVFIndex.trainInternal(allVectors); err != nil {
		return err
	}

	// Clear old data
	idx.vectors = make(map[uint64][]float32)
	for i := range idx.invertedLists {
		idx.invertedLists[i] = nil
	}

	// Re-insert all vectors
	for i, vec := range allVectors {
		rowID := allRowIDs[i]
		idx.vectors[rowID] = vec

		minDist := float32(math.MaxFloat32)
		closest := 0
		for j, c := range idx.centroids {
			if c != nil {
				d := l2Distance(vec, c)
				if d < minDist {
					minDist = d
					closest = j
				}
			}
		}
		idx.invertedLists[closest] = append(idx.invertedLists[closest], rowID)
	}

	// Clear pending buffer
	idx.pendingVectors = make(map[uint64][]float32)
	idx.pendingCount = 0
	idx.needsRebuild = false
	idx.stats.NumEntries = uint64(len(allVectors))

	return nil
}

// ANNSearchWithPending searches both the main index and the pending buffer,
// merging results by distance.
func (idx *IncrementalIVFIndex) ANNSearchWithPending(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	type result struct {
		rowID    uint64
		distance float32
	}

	var results []result

	// Search main index (unlocked version since we already hold the lock)
	if len(idx.centroids) > 0 && idx.centroids[0] != nil {
		dists, rowIDs, err := idx.IVFIndex.ANNSearch(ctx, queryVector, limit)
		if err != nil {
			return nil, nil, err
		}
		for i := range dists {
			results = append(results, result{rowID: rowIDs[i], distance: dists[i]})
		}
	}

	// Brute-force search pending vectors
	for rowID, vec := range idx.pendingVectors {
		d := l2Distance(queryVector, vec)
		results = append(results, result{rowID: rowID, distance: d})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	if limit > len(results) {
		limit = len(results)
	}

	distances := make([]float32, limit)
	rowIDs := make([]uint64, limit)
	for i := 0; i < limit; i++ {
		distances[i] = results[i].distance
		rowIDs[i] = results[i].rowID
	}
	return distances, rowIDs, nil
}

// --- IncrementalHNSWIndex ---

// IncrementalHNSWIndex wraps an HNSWIndex with a pending buffer for
// incremental batch insertions. HNSW supports online insertion natively,
// so the buffer is mainly used for batching small inserts and deferring
// graph construction until Flush is called.
type IncrementalHNSWIndex struct {
	*HNSWIndex

	pendingVectors map[uint64][]float32
	mu2            sync.Mutex
	bufferSize     int
}

// NewIncrementalHNSWIndex creates an incremental HNSW index.
func NewIncrementalHNSWIndex(name string, columnIdx int, dimension int, metric MetricType, bufferSize int) *IncrementalHNSWIndex {
	if bufferSize <= 0 {
		bufferSize = 5000
	}
	return &IncrementalHNSWIndex{
		HNSWIndex:      NewHNSWIndex(name, columnIdx, dimension, metric),
		pendingVectors: make(map[uint64][]float32),
		bufferSize:     bufferSize,
	}
}

// InsertBuffered adds a vector to the pending buffer. Call Flush to insert
// buffered vectors into the HNSW graph.
func (idx *IncrementalHNSWIndex) InsertBuffered(rowID uint64, vector []float32) {
	idx.mu2.Lock()
	defer idx.mu2.Unlock()
	v := make([]float32, len(vector))
	copy(v, vector)
	idx.pendingVectors[rowID] = v
}

// NeedsFlush returns true when the pending buffer exceeds bufferSize.
func (idx *IncrementalHNSWIndex) NeedsFlush() bool {
	idx.mu2.Lock()
	defer idx.mu2.Unlock()
	return len(idx.pendingVectors) >= idx.bufferSize
}

// PendingCount returns the number of buffered but not yet indexed vectors.
func (idx *IncrementalHNSWIndex) PendingCount() int {
	idx.mu2.Lock()
	defer idx.mu2.Unlock()
	return len(idx.pendingVectors)
}

// Flush inserts all pending vectors into the HNSW graph.
func (idx *IncrementalHNSWIndex) Flush() error {
	idx.mu2.Lock()
	pending := idx.pendingVectors
	idx.pendingVectors = make(map[uint64][]float32)
	idx.mu2.Unlock()

	for rowID, vec := range pending {
		if err := idx.HNSWIndex.Insert(rowID, vec); err != nil {
			return err
		}
	}
	return nil
}

// ANNSearchWithPending searches both the graph and the pending buffer.
func (idx *IncrementalHNSWIndex) ANNSearchWithPending(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
	// Search the graph
	dists, rowIDs, err := idx.HNSWIndex.ANNSearch(ctx, queryVector, limit)
	if err != nil {
		return nil, nil, err
	}

	type result struct {
		rowID    uint64
		distance float32
	}
	results := make([]result, len(dists))
	for i := range dists {
		results[i] = result{rowID: rowIDs[i], distance: dists[i]}
	}

	// Brute-force pending
	idx.mu2.Lock()
	for rowID, vec := range idx.pendingVectors {
		d := l2Distance(queryVector, vec)
		results = append(results, result{rowID: rowID, distance: d})
	}
	idx.mu2.Unlock()

	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

	if limit > len(results) {
		limit = len(results)
	}

	outDists := make([]float32, limit)
	outIDs := make([]uint64, limit)
	for i := 0; i < limit; i++ {
		outDists[i] = results[i].distance
		outIDs[i] = results[i].rowID
	}
	return outDists, outIDs, nil
}
