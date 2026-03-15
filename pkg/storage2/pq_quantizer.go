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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
)

// PQConfig holds configuration for Product Quantization.
type PQConfig struct {
	Dimension     int        // Original vector dimension
	NumSubvectors int        // Number of sub-vectors (M), defaults to dimension/8
	BitsPerCode   int        // Bits per sub-quantizer code (default 8 = 256 centroids)
	MetricType    MetricType // Distance metric
	KMeansIters   int        // K-means iterations (default 20)
}

// PQQuantizer implements Product Quantization.
// It splits each vector into M sub-vectors and independently quantizes each
// sub-vector using a small codebook trained via K-means.
type PQQuantizer struct {
	dimension     int
	numSubvectors int
	bitsPerCode   int
	numCentroids  int
	subDimension  int
	kmeansIters   int
	metricType    MetricType
	codebooks     [][][]float32 // [M][K][subDim]
	trained       bool
}

// NewPQQuantizer creates a new PQ quantizer.
func NewPQQuantizer(cfg PQConfig) (*PQQuantizer, error) {
	if cfg.Dimension <= 0 {
		return nil, fmt.Errorf("dimension must be positive, got %d", cfg.Dimension)
	}

	numSub := cfg.NumSubvectors
	if numSub <= 0 {
		numSub = cfg.Dimension / 8
		if numSub < 1 {
			numSub = 1
		}
	}

	if cfg.Dimension%numSub != 0 {
		return nil, fmt.Errorf("dimension %d must be divisible by numSubvectors %d", cfg.Dimension, numSub)
	}

	bits := cfg.BitsPerCode
	if bits <= 0 {
		bits = 8
	}
	if bits > 8 {
		return nil, fmt.Errorf("bitsPerCode must be <= 8, got %d", bits)
	}

	iters := cfg.KMeansIters
	if iters <= 0 {
		iters = 20
	}

	return &PQQuantizer{
		dimension:     cfg.Dimension,
		numSubvectors: numSub,
		bitsPerCode:   bits,
		numCentroids:  1 << bits,
		subDimension:  cfg.Dimension / numSub,
		kmeansIters:   iters,
		metricType:    cfg.MetricType,
	}, nil
}

// Train trains the PQ codebooks using K-means on each sub-vector space.
func (pq *PQQuantizer) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return fmt.Errorf("no training vectors provided")
	}
	for _, v := range vectors {
		if len(v) != pq.dimension {
			return fmt.Errorf("training vector dimension %d != expected %d", len(v), pq.dimension)
		}
	}

	pq.codebooks = make([][][]float32, pq.numSubvectors)

	for m := 0; m < pq.numSubvectors; m++ {
		// Extract sub-vectors for this subspace
		subVectors := make([][]float32, len(vectors))
		start := m * pq.subDimension
		for i, vec := range vectors {
			subVectors[i] = vec[start : start+pq.subDimension]
		}

		centroids, err := kmeans(subVectors, pq.numCentroids, pq.kmeansIters, pq.subDimension)
		if err != nil {
			return fmt.Errorf("kmeans for subvector %d: %w", m, err)
		}
		pq.codebooks[m] = centroids
	}

	pq.trained = true
	return nil
}

// Encode encodes a vector into a PQ code (one byte per sub-vector).
func (pq *PQQuantizer) Encode(vector []float32) ([]byte, error) {
	if !pq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	if len(vector) != pq.dimension {
		return nil, fmt.Errorf("vector dimension %d != expected %d", len(vector), pq.dimension)
	}

	code := make([]byte, pq.numSubvectors)
	for m := 0; m < pq.numSubvectors; m++ {
		start := m * pq.subDimension
		subVec := vector[start : start+pq.subDimension]

		minDist := float32(math.MaxFloat32)
		minIdx := 0
		for k := 0; k < pq.numCentroids; k++ {
			dist := l2DistanceSquared(subVec, pq.codebooks[m][k])
			if dist < minDist {
				minDist = dist
				minIdx = k
			}
		}
		code[m] = byte(minIdx)
	}
	return code, nil
}

// Decode reconstructs an approximate vector from a PQ code.
func (pq *PQQuantizer) Decode(code []byte) ([]float32, error) {
	if !pq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	if len(code) != pq.numSubvectors {
		return nil, fmt.Errorf("code length %d != expected %d", len(code), pq.numSubvectors)
	}

	vector := make([]float32, pq.dimension)
	for m := 0; m < pq.numSubvectors; m++ {
		centroidIdx := int(code[m])
		start := m * pq.subDimension
		copy(vector[start:start+pq.subDimension], pq.codebooks[m][centroidIdx])
	}
	return vector, nil
}

// ComputeDistanceTable precomputes a distance lookup table (ADC table).
// Returns a [][]float32 of shape [M][K] where entry [m][k] is the squared
// distance from the m-th sub-query to the k-th centroid.
func (pq *PQQuantizer) ComputeDistanceTable(query []float32) (interface{}, error) {
	if !pq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	if len(query) != pq.dimension {
		return nil, fmt.Errorf("query dimension %d != expected %d", len(query), pq.dimension)
	}

	table := make([][]float32, pq.numSubvectors)
	for m := 0; m < pq.numSubvectors; m++ {
		table[m] = make([]float32, pq.numCentroids)
		start := m * pq.subDimension
		subQuery := query[start : start+pq.subDimension]
		for k := 0; k < pq.numCentroids; k++ {
			table[m][k] = l2DistanceSquared(subQuery, pq.codebooks[m][k])
		}
	}
	return table, nil
}

// ComputeDistanceWithTable computes the approximate L2 distance using a precomputed ADC table.
func (pq *PQQuantizer) ComputeDistanceWithTable(table interface{}, code []byte) float32 {
	distTable := table.([][]float32)
	var sumSq float32
	for m := 0; m < pq.numSubvectors; m++ {
		sumSq += distTable[m][int(code[m])]
	}
	return float32(math.Sqrt(float64(sumSq)))
}

// ComputeDistance computes the approximate distance between a query and a code.
func (pq *PQQuantizer) ComputeDistance(query []float32, code []byte) (float32, error) {
	table, err := pq.ComputeDistanceTable(query)
	if err != nil {
		return 0, err
	}
	return pq.ComputeDistanceWithTable(table, code), nil
}

// CodeSize returns bytes per code (one byte per sub-vector).
func (pq *PQQuantizer) CodeSize() int {
	return pq.numSubvectors
}

// Type returns QuantizationPQ.
func (pq *PQQuantizer) Type() QuantizationType {
	return QuantizationPQ
}

// IsTrained returns whether the codebooks have been trained.
func (pq *PQQuantizer) IsTrained() bool {
	return pq.trained
}

// Marshal serializes PQ configuration and codebooks.
// Format: [4]dim [4]numSub [4]bits [4]metric [codebook float32 data]
func (pq *PQQuantizer) Marshal() ([]byte, error) {
	if !pq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}

	headerSize := 16
	codebookSize := pq.numSubvectors * pq.numCentroids * pq.subDimension * 4
	data := make([]byte, headerSize+codebookSize)

	binary.LittleEndian.PutUint32(data[0:4], uint32(pq.dimension))
	binary.LittleEndian.PutUint32(data[4:8], uint32(pq.numSubvectors))
	binary.LittleEndian.PutUint32(data[8:12], uint32(pq.bitsPerCode))
	binary.LittleEndian.PutUint32(data[12:16], uint32(pq.metricType))

	offset := headerSize
	for m := 0; m < pq.numSubvectors; m++ {
		for k := 0; k < pq.numCentroids; k++ {
			for d := 0; d < pq.subDimension; d++ {
				binary.LittleEndian.PutUint32(data[offset:], math.Float32bits(pq.codebooks[m][k][d]))
				offset += 4
			}
		}
	}
	return data, nil
}

// Unmarshal deserializes PQ configuration and codebooks.
func (pq *PQQuantizer) Unmarshal(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("PQ data too short: %d bytes", len(data))
	}

	pq.dimension = int(binary.LittleEndian.Uint32(data[0:4]))
	pq.numSubvectors = int(binary.LittleEndian.Uint32(data[4:8]))
	pq.bitsPerCode = int(binary.LittleEndian.Uint32(data[8:12]))
	pq.metricType = MetricType(binary.LittleEndian.Uint32(data[12:16]))
	pq.numCentroids = 1 << pq.bitsPerCode
	pq.subDimension = pq.dimension / pq.numSubvectors

	expected := 16 + pq.numSubvectors*pq.numCentroids*pq.subDimension*4
	if len(data) < expected {
		return fmt.Errorf("PQ data truncated: got %d, expected %d", len(data), expected)
	}

	pq.codebooks = make([][][]float32, pq.numSubvectors)
	offset := 16
	for m := 0; m < pq.numSubvectors; m++ {
		pq.codebooks[m] = make([][]float32, pq.numCentroids)
		for k := 0; k < pq.numCentroids; k++ {
			pq.codebooks[m][k] = make([]float32, pq.subDimension)
			for d := 0; d < pq.subDimension; d++ {
				pq.codebooks[m][k][d] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
				offset += 4
			}
		}
	}

	pq.trained = true
	return nil
}

// --- Helper functions ---

// l2DistanceSquared computes squared L2 distance (no sqrt).
func l2DistanceSquared(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a) && i < len(b); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

// kmeans performs K-means clustering and returns centroids.
func kmeans(vectors [][]float32, k int, maxIters int, dim int) ([][]float32, error) {
	if len(vectors) == 0 {
		return nil, fmt.Errorf("no vectors for kmeans")
	}

	// Initialize centroids: if we have fewer vectors than k, duplicate some
	centroids := make([][]float32, k)
	perm := rand.Perm(len(vectors))
	for i := 0; i < k; i++ {
		centroids[i] = make([]float32, dim)
		copy(centroids[i], vectors[perm[i%len(perm)]])
	}

	assignments := make([]int, len(vectors))

	for iter := 0; iter < maxIters; iter++ {
		// Assignment step
		for i, vec := range vectors {
			minDist := float32(math.MaxFloat32)
			minIdx := 0
			for j, c := range centroids {
				d := l2DistanceSquared(vec, c)
				if d < minDist {
					minDist = d
					minIdx = j
				}
			}
			assignments[i] = minIdx
		}

		// Update step
		newCentroids := make([][]float32, k)
		counts := make([]int, k)
		for i := 0; i < k; i++ {
			newCentroids[i] = make([]float32, dim)
		}

		for i, vec := range vectors {
			c := assignments[i]
			counts[c]++
			for j := 0; j < dim; j++ {
				newCentroids[c][j] += vec[j]
			}
		}

		for i := 0; i < k; i++ {
			if counts[i] > 0 {
				for j := 0; j < dim; j++ {
					newCentroids[i][j] /= float32(counts[i])
				}
				centroids[i] = newCentroids[i]
			}
		}
	}

	return centroids, nil
}

// Verify interface compliance.
var _ Quantizer = (*PQQuantizer)(nil)
