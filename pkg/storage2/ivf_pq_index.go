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
	"fmt"
	"math"
	"sort"
	"sync"
)

// IVFPQIndex combines IVF clustering with vector quantization for memory-efficient ANN search.
// During search, residual vectors (vector - centroid) are quantized, and distances are
// computed using precomputed ADC tables for fast approximate search.
type IVFPQIndex struct {
	name       string
	columnIdx  int
	indexType  IndexType
	metricType MetricType
	dimension  int

	// IVF parameters
	nlist  int
	nprobe int

	// Clustering
	centroids     [][]float32
	invertedLists [][]uint64 // rowIDs per cluster

	// Quantization
	quantizer    Quantizer
	codes        map[uint64][]byte // rowID -> quantized residual code
	useQuantizer bool

	// Fallback: raw vectors when quantizer is not used
	vectors map[uint64][]float32

	mu    sync.RWMutex
	stats IndexStats
}

// NewIVFPQIndex creates an IVF index with optional quantization.
// If quantizerType is QuantizationNone, it behaves like a plain IVF index storing raw vectors.
func NewIVFPQIndex(name string, columnIdx int, dimension int, metric MetricType, quantizerType QuantizationType) (*IVFPQIndex, error) {
	nlist := 100
	if nlist > dimension {
		nlist = dimension
	}

	var quantizer Quantizer
	var err error

	switch quantizerType {
	case QuantizationPQ:
		numSub := dimension / 8
		if numSub < 1 {
			numSub = 1
		}
		quantizer, err = NewPQQuantizer(PQConfig{
			Dimension:     dimension,
			NumSubvectors: numSub,
			BitsPerCode:   8,
			MetricType:    metric,
		})
		if err != nil {
			return nil, fmt.Errorf("create PQ quantizer: %w", err)
		}
	case QuantizationSQ:
		quantizer = NewSQQuantizer(dimension, metric)
	case QuantizationNone:
		// No quantizer
	default:
		return nil, fmt.Errorf("unsupported quantizer type: %d", quantizerType)
	}

	return &IVFPQIndex{
		name:          name,
		columnIdx:     columnIdx,
		indexType:     VectorIndex,
		metricType:    metric,
		dimension:     dimension,
		nlist:         nlist,
		nprobe:        10,
		centroids:     make([][]float32, nlist),
		invertedLists: make([][]uint64, nlist),
		quantizer:     quantizer,
		codes:         make(map[uint64][]byte),
		useQuantizer:  quantizer != nil,
		vectors:       make(map[uint64][]float32),
	}, nil
}

// Name returns the index name.
func (idx *IVFPQIndex) Name() string { return idx.name }

// Type returns VectorIndex.
func (idx *IVFPQIndex) Type() IndexType { return idx.indexType }

// Columns returns the indexed column.
func (idx *IVFPQIndex) Columns() []int { return []int{idx.columnIdx} }

// GetMetricType returns the distance metric.
func (idx *IVFPQIndex) GetMetricType() MetricType { return idx.metricType }

// Statistics returns index stats.
func (idx *IVFPQIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// Search performs ANN search (implements Index interface).
func (idx *IVFPQIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	qv, ok := query.([]float32)
	if !ok {
		return nil, nil
	}
	_, rowIDs, err := idx.ANNSearch(ctx, qv, limit)
	return rowIDs, err
}

// SetNProbe sets the number of clusters to probe during search.
func (idx *IVFPQIndex) SetNProbe(nprobe int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.nprobe = nprobe
}

// Train trains IVF centroids and (optionally) the quantizer on residual vectors.
func (idx *IVFPQIndex) Train(vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vectors) == 0 {
		return nil
	}

	// 1. Train IVF centroids via K-means
	nlist := idx.nlist
	if nlist > len(vectors) {
		nlist = len(vectors)
	}

	centroids, err := kmeans(vectors, nlist, 10, idx.dimension)
	if err != nil {
		return fmt.Errorf("train IVF centroids: %w", err)
	}
	// Pad to nlist if fewer centroids
	for len(centroids) < idx.nlist {
		centroids = append(centroids, nil)
	}
	idx.centroids = centroids

	// 2. Train quantizer on residual vectors
	if idx.useQuantizer && idx.quantizer != nil {
		residuals := make([][]float32, len(vectors))
		for i, vec := range vectors {
			closestCluster := idx.findClosestCentroid(vec)
			residuals[i] = make([]float32, idx.dimension)
			for d := 0; d < idx.dimension; d++ {
				residuals[i][d] = vec[d] - idx.centroids[closestCluster][d]
			}
		}
		if err := idx.quantizer.Train(residuals); err != nil {
			return fmt.Errorf("train quantizer: %w", err)
		}
	}

	return nil
}

// Insert adds a vector to the index.
func (idx *IVFPQIndex) Insert(rowID uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vector) != idx.dimension {
		return fmt.Errorf("vector dimension %d != expected %d", len(vector), idx.dimension)
	}

	closestCluster := idx.findClosestCentroid(vector)
	idx.invertedLists[closestCluster] = append(idx.invertedLists[closestCluster], rowID)

	if idx.useQuantizer && idx.quantizer != nil && idx.quantizer.IsTrained() {
		// Store quantized residual
		residual := make([]float32, idx.dimension)
		for d := 0; d < idx.dimension; d++ {
			residual[d] = vector[d] - idx.centroids[closestCluster][d]
		}
		code, err := idx.quantizer.Encode(residual)
		if err != nil {
			return fmt.Errorf("encode residual: %w", err)
		}
		idx.codes[rowID] = code
	} else {
		// Fallback: store raw vector
		v := make([]float32, len(vector))
		copy(v, vector)
		idx.vectors[rowID] = v
	}

	idx.stats.NumEntries++
	idx.updateStats()
	return nil
}

// ANNSearch performs approximate nearest neighbor search.
func (idx *IVFPQIndex) ANNSearch(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(queryVector) != idx.dimension {
		return nil, nil, nil
	}

	// Find nprobe closest centroids
	type centroidDist struct {
		idx      int
		distance float32
	}
	cds := make([]centroidDist, 0, idx.nlist)
	for i, c := range idx.centroids {
		if c != nil {
			cds = append(cds, centroidDist{idx: i, distance: idx.computeDistance(queryVector, c)})
		}
	}
	sort.Slice(cds, func(i, j int) bool {
		return cds[i].distance < cds[j].distance
	})

	nprobe := idx.nprobe
	if nprobe > len(cds) {
		nprobe = len(cds)
	}

	// Search within the closest clusters
	type result struct {
		rowID    uint64
		distance float32
	}
	var results []result

	if idx.useQuantizer && idx.quantizer != nil && idx.quantizer.IsTrained() {
		// ADC search: precompute distance table per cluster
		for i := 0; i < nprobe; i++ {
			clusterIdx := cds[i].idx
			centroid := idx.centroids[clusterIdx]

			// Query residual relative to this centroid
			queryResidual := make([]float32, idx.dimension)
			for d := 0; d < idx.dimension; d++ {
				queryResidual[d] = queryVector[d] - centroid[d]
			}

			distTable, err := idx.quantizer.ComputeDistanceTable(queryResidual)
			if err != nil {
				continue
			}

			for _, rowID := range idx.invertedLists[clusterIdx] {
				if code, ok := idx.codes[rowID]; ok {
					dist := idx.quantizer.ComputeDistanceWithTable(distTable, code)
					results = append(results, result{rowID: rowID, distance: dist})
				}
			}
		}
	} else {
		// Exact search using raw vectors
		for i := 0; i < nprobe; i++ {
			clusterIdx := cds[i].idx
			for _, rowID := range idx.invertedLists[clusterIdx] {
				if vec, ok := idx.vectors[rowID]; ok {
					dist := idx.computeDistance(queryVector, vec)
					results = append(results, result{rowID: rowID, distance: dist})
				}
			}
		}
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

// --- internal helpers ---

func (idx *IVFPQIndex) findClosestCentroid(vector []float32) int {
	minDist := float32(math.MaxFloat32)
	closest := 0
	for i, c := range idx.centroids {
		if c != nil {
			d := idx.computeDistance(vector, c)
			if d < minDist {
				minDist = d
				closest = i
			}
		}
	}
	return closest
}

func (idx *IVFPQIndex) computeDistance(a, b []float32) float32 {
	switch idx.metricType {
	case CosineMetric:
		return cosineDistance(a, b)
	case DotMetric:
		return -dotProduct(a, b)
	default:
		return l2Distance(a, b)
	}
}

func (idx *IVFPQIndex) updateStats() {
	if idx.useQuantizer {
		idx.stats.SizeBytes = uint64(len(idx.codes)) * uint64(idx.quantizer.CodeSize())
	} else {
		idx.stats.SizeBytes = uint64(len(idx.vectors)) * uint64(idx.dimension) * 4
	}
	idx.stats.IndexType = "ivf_pq"
}

// Verify interface compliance.
var _ VectorIndexImpl = (*IVFPQIndex)(nil)
