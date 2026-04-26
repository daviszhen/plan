// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// IVFHNSWConfig holds configuration for IVF-HNSW index
type IVFHNSWConfig struct {
	Dimension     int              // Vector dimension
	Nlist         int              // Number of IVF clusters (default: sqrt(n) or 100)
	Nprobe        int              // Number of clusters to search (default: nlist/10)
	MetricType    MetricType       // Distance metric
	QuantizerType QuantizationType // Optional quantization (None, PQ, SQ)

	// HNSW sub-index parameters
	HNSWM              int // Max connections per node (default: 16)
	HNSWEfConstruction int // Size of dynamic candidate list during construction (default: 200)
	HNSWEfSearch       int // Size of dynamic candidate list during search (default: 50)
}

// DefaultIVFHNSWConfig returns default configuration
func DefaultIVFHNSWConfig(dimension int) IVFHNSWConfig {
	return IVFHNSWConfig{
		Dimension:          dimension,
		Nlist:              100,
		Nprobe:             10,
		MetricType:         L2Metric,
		QuantizerType:      QuantizationNone,
		HNSWM:              16,
		HNSWEfConstruction: 200,
		HNSWEfSearch:       50,
	}
}

// IVFHNSWIndex is an IVF + HNSW hybrid index for approximate nearest neighbor search.
// It combines IVF clustering for coarse partitioning with HNSW sub-indexes for
// efficient search within each partition.
type IVFHNSWIndex struct {
	name       string
	columnIdx  int
	indexType  IndexType
	metricType MetricType
	dimension  int

	// IVF layer
	nlist     int         // Number of clusters
	centroids [][]float32 // Cluster centroids

	// HNSW sub-indexes (one per partition)
	subIndexes []*HNSWIndex

	// Partition to row IDs mapping
	partitionRowIDs [][]uint64

	// Quantizer (optional)
	quantizer Quantizer
	quantType QuantizationType

	// Search parameters
	nprobe int // Number of partitions to search

	// HNSW parameters for sub-indexes
	hnswM              int
	hnswEfConstruction int
	hnswEfSearch       int

	// Training state
	trained bool

	mu    sync.RWMutex
	stats IndexStats
}

// NewIVFHNSWIndex creates a new IVF-HNSW hybrid index
func NewIVFHNSWIndex(name string, columnIdx int, cfg IVFHNSWConfig) *IVFHNSWIndex {
	nlist := cfg.Nlist
	if nlist <= 0 {
		nlist = 100
	}

	nprobe := cfg.Nprobe
	if nprobe <= 0 {
		nprobe = maxInt(1, nlist/10)
	}

	hnswM := cfg.HNSWM
	if hnswM <= 0 {
		hnswM = 16
	}

	efConstruction := cfg.HNSWEfConstruction
	if efConstruction <= 0 {
		efConstruction = 200
	}

	efSearch := cfg.HNSWEfSearch
	if efSearch <= 0 {
		efSearch = 50
	}

	return &IVFHNSWIndex{
		name:               name,
		columnIdx:          columnIdx,
		indexType:          VectorIndex,
		metricType:         cfg.MetricType,
		dimension:          cfg.Dimension,
		nlist:              nlist,
		nprobe:             nprobe,
		centroids:          make([][]float32, nlist),
		subIndexes:         make([]*HNSWIndex, nlist),
		partitionRowIDs:    make([][]uint64, nlist),
		quantType:          cfg.QuantizerType,
		hnswM:              hnswM,
		hnswEfConstruction: efConstruction,
		hnswEfSearch:       efSearch,
		stats: IndexStats{
			NumEntries: 0,
			SizeBytes:  0,
			IndexType:  "ivf_hnsw",
		},
	}
}

// Name returns the index name
func (idx *IVFHNSWIndex) Name() string {
	return idx.name
}

// Type returns the index type
func (idx *IVFHNSWIndex) Type() IndexType {
	return idx.indexType
}

// Columns returns the column indices this index covers
func (idx *IVFHNSWIndex) Columns() []int {
	return []int{idx.columnIdx}
}

// Search implements the Index interface
func (idx *IVFHNSWIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	queryVector, ok := query.([]float32)
	if !ok {
		return nil, fmt.Errorf("query must be []float32")
	}

	_, rowIDs, err := idx.ANNSearch(ctx, queryVector, limit)
	return rowIDs, err
}

// Statistics returns statistics about the index
func (idx *IVFHNSWIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// GetMetricType returns the distance metric used by the index
func (idx *IVFHNSWIndex) GetMetricType() MetricType {
	return idx.metricType
}

// IsTrained returns whether the index has been trained
func (idx *IVFHNSWIndex) IsTrained() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.trained
}

// Train trains the IVF clustering centroids using k-means
func (idx *IVFHNSWIndex) Train(vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vectors) == 0 {
		return fmt.Errorf("no training vectors provided")
	}

	// Validate dimension
	for i, v := range vectors {
		if len(v) != idx.dimension {
			return fmt.Errorf("vector %d has dimension %d, expected %d", i, len(v), idx.dimension)
		}
	}

	// Adjust nlist if we have fewer vectors than clusters
	actualNlist := idx.nlist
	if len(vectors) < actualNlist {
		actualNlist = len(vectors)
	}

	// 1. Train k-means centroids
	centroids, err := kmeansIVFHNSW(vectors, actualNlist, 20, idx.metricType)
	if err != nil {
		return fmt.Errorf("k-means training failed: %w", err)
	}

	idx.centroids = centroids
	idx.nlist = actualNlist

	// Resize arrays if nlist changed
	if len(idx.subIndexes) != actualNlist {
		idx.subIndexes = make([]*HNSWIndex, actualNlist)
		idx.partitionRowIDs = make([][]uint64, actualNlist)
	}

	// 2. Create HNSW sub-index for each partition
	for i := 0; i < actualNlist; i++ {
		subIdx := NewHNSWIndex(
			fmt.Sprintf("%s_part_%d", idx.name, i),
			idx.columnIdx,
			idx.dimension,
			idx.metricType,
		)
		subIdx.M = idx.hnswM
		subIdx.Mmax = idx.hnswM
		subIdx.Mmax0 = idx.hnswM * 2
		subIdx.efConstruction = idx.hnswEfConstruction
		subIdx.efSearch = idx.hnswEfSearch
		idx.subIndexes[i] = subIdx
		idx.partitionRowIDs[i] = make([]uint64, 0)
	}

	// 3. Train quantizer if specified
	if idx.quantType != QuantizationNone && len(vectors) > 0 {
		residuals := idx.computeResiduals(vectors)

		switch idx.quantType {
		case QuantizationPQ:
			numSubvectors := idx.dimension / 8
			if numSubvectors < 1 {
				numSubvectors = 1
			}
			// Ensure dimension is divisible
			if idx.dimension%numSubvectors != 0 {
				numSubvectors = 1
			}
			pq, err := NewPQQuantizer(PQConfig{
				Dimension:     idx.dimension,
				NumSubvectors: numSubvectors,
				BitsPerCode:   8,
				MetricType:    idx.metricType,
			})
			if err != nil {
				return fmt.Errorf("failed to create PQ quantizer: %w", err)
			}
			if err := pq.Train(residuals); err != nil {
				return fmt.Errorf("failed to train PQ quantizer: %w", err)
			}
			idx.quantizer = pq

		case QuantizationSQ:
			sq := NewSQQuantizer(idx.dimension, idx.metricType)
			if err := sq.Train(residuals); err != nil {
				return fmt.Errorf("failed to train SQ quantizer: %w", err)
			}
			idx.quantizer = sq
		}
	}

	idx.trained = true
	return nil
}

// computeResiduals computes residual vectors (vector - nearest centroid)
func (idx *IVFHNSWIndex) computeResiduals(vectors [][]float32) [][]float32 {
	residuals := make([][]float32, len(vectors))

	for i, vec := range vectors {
		// Find nearest centroid
		minDist := float32(math.MaxFloat32)
		minCentroid := 0

		for j, c := range idx.centroids {
			if c != nil {
				dist := computeDistanceIVFHNSW(vec, c, idx.metricType)
				if dist < minDist {
					minDist = dist
					minCentroid = j
				}
			}
		}

		// Compute residual
		residuals[i] = make([]float32, idx.dimension)
		for d := 0; d < idx.dimension; d++ {
			residuals[i][d] = vec[d] - idx.centroids[minCentroid][d]
		}
	}

	return residuals
}

// Insert adds a vector to the index
func (idx *IVFHNSWIndex) Insert(rowID uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if !idx.trained {
		return fmt.Errorf("index not trained, call Train() first")
	}

	if len(vector) != idx.dimension {
		return fmt.Errorf("vector dimension %d != index dimension %d", len(vector), idx.dimension)
	}

	// Find nearest partition
	partition := idx.findNearestPartitionLocked(vector)

	// Insert into sub-index
	if idx.subIndexes[partition] == nil {
		return fmt.Errorf("sub-index for partition %d not initialized", partition)
	}

	if err := idx.subIndexes[partition].Insert(rowID, vector); err != nil {
		return fmt.Errorf("failed to insert into sub-index: %w", err)
	}

	// Track row ID in partition
	idx.partitionRowIDs[partition] = append(idx.partitionRowIDs[partition], rowID)

	idx.stats.NumEntries++
	idx.updateStatsLocked()

	return nil
}

// InsertBatch adds multiple vectors to the index
func (idx *IVFHNSWIndex) InsertBatch(rowIDs []uint64, vectors [][]float32) error {
	if len(rowIDs) != len(vectors) {
		return fmt.Errorf("rowIDs length %d != vectors length %d", len(rowIDs), len(vectors))
	}

	for i, rowID := range rowIDs {
		if err := idx.Insert(rowID, vectors[i]); err != nil {
			return fmt.Errorf("failed to insert vector %d: %w", i, err)
		}
	}

	return nil
}

// findNearestPartitionLocked finds the nearest partition (must hold lock)
func (idx *IVFHNSWIndex) findNearestPartitionLocked(vector []float32) int {
	minDist := float32(math.MaxFloat32)
	minPartition := 0

	for i, c := range idx.centroids {
		if c != nil {
			dist := computeDistanceIVFHNSW(vector, c, idx.metricType)
			if dist < minDist {
				minDist = dist
				minPartition = i
			}
		}
	}

	return minPartition
}

// ANNSearch performs approximate nearest neighbor search
func (idx *IVFHNSWIndex) ANNSearch(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
	return idx.ANNSearchWithFilter(ctx, queryVector, limit, nil)
}

// ANNSearchWithFilter performs ANN search with optional pre-filtering
func (idx *IVFHNSWIndex) ANNSearchWithFilter(ctx context.Context, queryVector []float32, limit int, prefilter PreFilter) ([]float32, []uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if !idx.trained {
		return nil, nil, fmt.Errorf("index not trained")
	}

	if len(queryVector) != idx.dimension {
		return nil, nil, fmt.Errorf("query dimension %d != index dimension %d", len(queryVector), idx.dimension)
	}

	// Wait for prefilter if provided
	if prefilter != nil && !prefilter.IsEmpty() {
		if err := prefilter.WaitForReady(ctx); err != nil {
			return nil, nil, fmt.Errorf("prefilter not ready: %w", err)
		}
	}

	// 1. Find nprobe nearest partitions
	partitions := idx.findNearestPartitionsLocked(queryVector, idx.nprobe)

	// 2. Search in each partition's HNSW sub-index
	type searchResult struct {
		rowID    uint64
		distance float32
	}
	var allResults []searchResult
	seen := make(map[uint64]struct{}) // Deduplicate results

	for _, p := range partitions {
		if idx.subIndexes[p] == nil {
			continue
		}

		// Search sub-index
		distances, rowIDs, err := idx.subIndexes[p].ANNSearch(ctx, queryVector, limit)
		if err != nil {
			continue
		}

		for i := range rowIDs {
			rowID := rowIDs[i]

			// Skip duplicates
			if _, exists := seen[rowID]; exists {
				continue
			}
			seen[rowID] = struct{}{}

			// Apply prefilter
			if prefilter != nil && !prefilter.ShouldInclude(rowID) {
				continue
			}

			allResults = append(allResults, searchResult{
				rowID:    rowID,
				distance: distances[i],
			})
		}
	}

	// 3. Sort by distance
	sort.Slice(allResults, func(i, j int) bool {
		return allResults[i].distance < allResults[j].distance
	})

	// 4. Return top-k
	if limit > len(allResults) {
		limit = len(allResults)
	}

	distances := make([]float32, limit)
	rowIDs := make([]uint64, limit)
	for i := 0; i < limit; i++ {
		distances[i] = allResults[i].distance
		rowIDs[i] = allResults[i].rowID
	}

	return distances, rowIDs, nil
}

// findNearestPartitionsLocked finds the n nearest partitions (must hold read lock)
func (idx *IVFHNSWIndex) findNearestPartitionsLocked(vector []float32, n int) []int {
	type partDist struct {
		partition int
		distance  float32
	}

	dists := make([]partDist, 0, len(idx.centroids))
	for i, c := range idx.centroids {
		if c != nil {
			dists = append(dists, partDist{
				partition: i,
				distance:  computeDistanceIVFHNSW(vector, c, idx.metricType),
			})
		}
	}

	sort.Slice(dists, func(i, j int) bool {
		return dists[i].distance < dists[j].distance
	})

	if n > len(dists) {
		n = len(dists)
	}

	result := make([]int, n)
	for i := 0; i < n; i++ {
		result[i] = dists[i].partition
	}

	return result
}

// SetNprobe sets the number of partitions to search
func (idx *IVFHNSWIndex) SetNprobe(nprobe int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if nprobe > 0 && nprobe <= idx.nlist {
		idx.nprobe = nprobe
	}
}

// GetNprobe returns the current nprobe value
func (idx *IVFHNSWIndex) GetNprobe() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.nprobe
}

// SetEfSearch sets the efSearch parameter for all sub-indexes
func (idx *IVFHNSWIndex) SetEfSearch(ef int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.hnswEfSearch = ef
	for _, subIdx := range idx.subIndexes {
		if subIdx != nil {
			subIdx.SetEfSearch(ef)
		}
	}
}

// GetPartitionStats returns statistics for each partition
func (idx *IVFHNSWIndex) GetPartitionStats() []int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := make([]int, len(idx.partitionRowIDs))
	for i, rowIDs := range idx.partitionRowIDs {
		stats[i] = len(rowIDs)
	}
	return stats
}

// NumPartitions returns the number of partitions (nlist)
func (idx *IVFHNSWIndex) NumPartitions() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.nlist
}

// Dimension returns the vector dimension
func (idx *IVFHNSWIndex) Dimension() int {
	return idx.dimension
}

// updateStatsLocked updates index statistics (must hold lock)
func (idx *IVFHNSWIndex) updateStatsLocked() {
	// Calculate approximate size
	// Centroids: nlist * dimension * 4 bytes
	centroidSize := uint64(idx.nlist) * uint64(idx.dimension) * 4

	// Sub-indexes: sum of all sub-index sizes
	var subIndexSize uint64
	for _, subIdx := range idx.subIndexes {
		if subIdx != nil {
			subIndexSize += subIdx.Statistics().SizeBytes
		}
	}

	idx.stats.SizeBytes = centroidSize + subIndexSize
	idx.stats.IndexType = "ivf_hnsw"
}

// Remove removes a vector from the index by row ID
func (idx *IVFHNSWIndex) Remove(rowID uint64) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Find which partition contains this row ID
	for p, rowIDs := range idx.partitionRowIDs {
		for i, id := range rowIDs {
			if id == rowID {
				// Remove from partition row IDs
				idx.partitionRowIDs[p] = append(rowIDs[:i], rowIDs[i+1:]...)

				// Remove from sub-index (HNSW doesn't have a Remove method by default)
				// For now, we just track the removal in partitionRowIDs
				// Full removal would require rebuilding the HNSW sub-index

				idx.stats.NumEntries--
				return true
			}
		}
	}

	return false
}

// Clear removes all vectors from the index
func (idx *IVFHNSWIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Reset sub-indexes
	for i := range idx.subIndexes {
		if idx.trained {
			idx.subIndexes[i] = NewHNSWIndex(
				fmt.Sprintf("%s_part_%d", idx.name, i),
				idx.columnIdx,
				idx.dimension,
				idx.metricType,
			)
		} else {
			idx.subIndexes[i] = nil
		}
	}

	// Clear partition row IDs
	for i := range idx.partitionRowIDs {
		idx.partitionRowIDs[i] = make([]uint64, 0)
	}

	idx.stats.NumEntries = 0
	idx.updateStatsLocked()
}

// kmeansIVFHNSW performs k-means clustering
func kmeansIVFHNSW(vectors [][]float32, k int, maxIterations int, metric MetricType) ([][]float32, error) {
	if len(vectors) == 0 {
		return nil, fmt.Errorf("no vectors provided")
	}
	if k <= 0 {
		return nil, fmt.Errorf("k must be positive")
	}
	if k > len(vectors) {
		k = len(vectors)
	}

	dimension := len(vectors[0])
	centroids := make([][]float32, k)

	// Initialize centroids using k-means++ style initialization
	// First centroid: random selection
	centroids[0] = make([]float32, dimension)
	copy(centroids[0], vectors[rand.Intn(len(vectors))])

	// Remaining centroids: weighted random selection based on distance
	for i := 1; i < k; i++ {
		// Calculate distances to nearest centroid for each vector
		distances := make([]float64, len(vectors))
		var totalDist float64

		for j, vec := range vectors {
			minDist := float32(math.MaxFloat32)
			for c := 0; c < i; c++ {
				dist := computeDistanceIVFHNSW(vec, centroids[c], metric)
				if dist < minDist {
					minDist = dist
				}
			}
			distances[j] = float64(minDist * minDist) // Square for probability
			totalDist += distances[j]
		}

		// Weighted random selection
		if totalDist > 0 {
			target := rand.Float64() * totalDist
			var cumSum float64
			selected := 0
			for j := range distances {
				cumSum += distances[j]
				if cumSum >= target {
					selected = j
					break
				}
			}
			centroids[i] = make([]float32, dimension)
			copy(centroids[i], vectors[selected])
		} else {
			// Fallback to random selection
			centroids[i] = make([]float32, dimension)
			copy(centroids[i], vectors[rand.Intn(len(vectors))])
		}
	}

	// K-means iterations
	assignments := make([]int, len(vectors))

	for iter := 0; iter < maxIterations; iter++ {
		// Assignment step: assign each vector to nearest centroid
		changed := false
		for i, vec := range vectors {
			minDist := float32(math.MaxFloat32)
			minCluster := 0
			for j, c := range centroids {
				if c != nil {
					dist := computeDistanceIVFHNSW(vec, c, metric)
					if dist < minDist {
						minDist = dist
						minCluster = j
					}
				}
			}
			if assignments[i] != minCluster {
				assignments[i] = minCluster
				changed = true
			}
		}

		// Update step: recompute centroids
		newCentroids := make([][]float32, k)
		counts := make([]int, k)

		for i := range newCentroids {
			newCentroids[i] = make([]float32, dimension)
		}

		for i, vec := range vectors {
			cluster := assignments[i]
			counts[cluster]++
			for d := 0; d < dimension; d++ {
				newCentroids[cluster][d] += vec[d]
			}
		}

		for i := 0; i < k; i++ {
			if counts[i] > 0 {
				for d := 0; d < dimension; d++ {
					newCentroids[i][d] /= float32(counts[i])
				}
				centroids[i] = newCentroids[i]
			}
		}

		// Check for convergence
		if !changed {
			break
		}
	}

	return centroids, nil
}

// computeDistanceIVFHNSW computes distance between two vectors
func computeDistanceIVFHNSW(a, b []float32, metric MetricType) float32 {
	switch metric {
	case L2Metric:
		var sum float32
		for i := range a {
			diff := a[i] - b[i]
			sum += diff * diff
		}
		return float32(math.Sqrt(float64(sum)))

	case CosineMetric:
		var dot, normA, normB float32
		for i := range a {
			dot += a[i] * b[i]
			normA += a[i] * a[i]
			normB += b[i] * b[i]
		}
		if normA == 0 || normB == 0 {
			return 1.0
		}
		similarity := dot / (float32(math.Sqrt(float64(normA))) * float32(math.Sqrt(float64(normB))))
		return 1.0 - similarity

	case DotMetric:
		var dot float32
		for i := range a {
			dot += a[i] * b[i]
		}
		return -dot // Negative for min-heap

	default:
		return computeDistanceIVFHNSW(a, b, L2Metric)
	}
}

// maxInt returns the maximum of two integers
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Ensure IVFHNSWIndex implements VectorIndexImpl
var _ VectorIndexImpl = (*IVFHNSWIndex)(nil)
