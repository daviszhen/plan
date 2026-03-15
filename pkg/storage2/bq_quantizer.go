// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"fmt"
	"math/bits"
)

// BQQuantizer implements Binary Quantization (1-bit per dimension).
// It achieves 32x compression ratio compared to float32 vectors.
// Best used with Hamming distance for similarity computation.
type BQQuantizer struct {
	dimension  int
	metricType MetricType
	trained    bool
}

// NewBQQuantizer creates a new Binary Quantizer
func NewBQQuantizer(dimension int) *BQQuantizer {
	return &BQQuantizer{
		dimension:  dimension,
		metricType: HammingMetric,
	}
}

// Train prepares the quantizer. BQ doesn't require training.
func (bq *BQQuantizer) Train(vectors [][]float32) error {
	if len(vectors) > 0 && len(vectors[0]) != bq.dimension {
		return fmt.Errorf("vector dimension %d does not match quantizer dimension %d",
			len(vectors[0]), bq.dimension)
	}
	bq.trained = true
	return nil
}

// Encode encodes a float32 vector to binary representation.
// Each dimension is quantized to 1 bit: value > 0 -> 1, value <= 0 -> 0.
// 8 dimensions are packed into 1 byte.
func (bq *BQQuantizer) Encode(vector []float32) ([]byte, error) {
	if len(vector) != bq.dimension {
		return nil, fmt.Errorf("vector dimension %d does not match quantizer dimension %d",
			len(vector), bq.dimension)
	}

	// Calculate number of bytes needed
	numBytes := (bq.dimension + 7) / 8
	code := make([]byte, numBytes)

	for i := 0; i < bq.dimension; i++ {
		if vector[i] > 0 {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			code[byteIdx] |= 1 << bitIdx
		}
	}

	return code, nil
}

// Decode reconstructs an approximate vector from binary code.
// Returns a vector with values of -1.0 or +1.0.
func (bq *BQQuantizer) Decode(code []byte) ([]float32, error) {
	expectedSize := (bq.dimension + 7) / 8
	if len(code) != expectedSize {
		return nil, fmt.Errorf("code size %d does not match expected size %d",
			len(code), expectedSize)
	}

	vector := make([]float32, bq.dimension)

	for i := 0; i < bq.dimension; i++ {
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		if (code[byteIdx]>>bitIdx)&1 == 1 {
			vector[i] = 1.0
		} else {
			vector[i] = -1.0
		}
	}

	return vector, nil
}

// ComputeDistance computes Hamming distance between a query vector and a code.
func (bq *BQQuantizer) ComputeDistance(query []float32, code []byte) (float32, error) {
	queryCode, err := bq.Encode(query)
	if err != nil {
		return 0, err
	}
	return bq.hammingDistanceBinary(queryCode, code), nil
}

// hammingDistanceBinary computes Hamming distance between two binary codes
func (bq *BQQuantizer) hammingDistanceBinary(a, b []byte) float32 {
	var dist int
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		dist += bits.OnesCount8(a[i] ^ b[i])
	}
	return float32(dist)
}

// ComputeDistanceTable precomputes a lookup table for fast distance computation.
// For BQ, this encodes the query vector.
func (bq *BQQuantizer) ComputeDistanceTable(query []float32) (interface{}, error) {
	return bq.Encode(query)
}

// ComputeDistanceWithTable computes distance using a precomputed table.
func (bq *BQQuantizer) ComputeDistanceWithTable(table interface{}, code []byte) float32 {
	queryCode, ok := table.([]byte)
	if !ok {
		return float32(bq.dimension) // Maximum distance
	}
	return bq.hammingDistanceBinary(queryCode, code)
}

// CodeSize returns the number of bytes per encoded vector.
func (bq *BQQuantizer) CodeSize() int {
	return (bq.dimension + 7) / 8
}

// Type returns the quantization type.
func (bq *BQQuantizer) Type() QuantizationType {
	return QuantizationBQ
}

// IsTrained returns whether the quantizer has been trained.
func (bq *BQQuantizer) IsTrained() bool {
	return bq.trained
}

// Dimension returns the dimension of vectors this quantizer handles.
func (bq *BQQuantizer) Dimension() int {
	return bq.dimension
}

// CompressionRatio returns the compression ratio achieved (float32 -> 1bit).
func (bq *BQQuantizer) CompressionRatio() float64 {
	// float32 = 32 bits, BQ = 1 bit per dimension
	return 32.0
}

// EncodeBatch encodes multiple vectors
func (bq *BQQuantizer) EncodeBatch(vectors [][]float32) ([][]byte, error) {
	codes := make([][]byte, len(vectors))
	for i, vec := range vectors {
		code, err := bq.Encode(vec)
		if err != nil {
			return nil, fmt.Errorf("failed to encode vector %d: %w", i, err)
		}
		codes[i] = code
	}
	return codes, nil
}

// BatchComputeDistances computes distances from a query to multiple codes
func (bq *BQQuantizer) BatchComputeDistances(query []float32, codes [][]byte) ([]float32, error) {
	queryCode, err := bq.Encode(query)
	if err != nil {
		return nil, err
	}

	distances := make([]float32, len(codes))
	for i, code := range codes {
		distances[i] = bq.hammingDistanceBinary(queryCode, code)
	}
	return distances, nil
}

// BQIndex wraps BQQuantizer for use as a vector index with Hamming distance
type BQIndex struct {
	name      string
	columnIdx int
	dimension int
	quantizer *BQQuantizer

	// Storage
	codes    [][]byte       // Encoded vectors
	rowIDs   []uint64       // Row IDs
	rowIDMap map[uint64]int // rowID -> index in codes/rowIDs
}

// NewBQIndex creates a new Binary Quantization index
func NewBQIndex(name string, columnIdx int, dimension int) *BQIndex {
	return &BQIndex{
		name:      name,
		columnIdx: columnIdx,
		dimension: dimension,
		quantizer: NewBQQuantizer(dimension),
		codes:     make([][]byte, 0),
		rowIDs:    make([]uint64, 0),
		rowIDMap:  make(map[uint64]int),
	}
}

// Insert adds a vector to the index
func (idx *BQIndex) Insert(rowID uint64, vector []float32) error {
	if !idx.quantizer.IsTrained() {
		idx.quantizer.Train(nil) // BQ doesn't need actual training
	}

	code, err := idx.quantizer.Encode(vector)
	if err != nil {
		return err
	}

	if _, exists := idx.rowIDMap[rowID]; exists {
		// Update existing entry
		i := idx.rowIDMap[rowID]
		idx.codes[i] = code
	} else {
		// Add new entry
		idx.rowIDMap[rowID] = len(idx.codes)
		idx.codes = append(idx.codes, code)
		idx.rowIDs = append(idx.rowIDs, rowID)
	}

	return nil
}

// Search performs approximate nearest neighbor search using Hamming distance
func (idx *BQIndex) Search(query []float32, k int) ([]uint64, []float32, error) {
	if len(idx.codes) == 0 {
		return nil, nil, nil
	}

	distances, err := idx.quantizer.BatchComputeDistances(query, idx.codes)
	if err != nil {
		return nil, nil, err
	}

	// Find top-k by sorting
	type result struct {
		rowID    uint64
		distance float32
	}
	results := make([]result, len(distances))
	for i, dist := range distances {
		results[i] = result{rowID: idx.rowIDs[i], distance: dist}
	}

	// Partial sort to get top-k
	for i := 0; i < k && i < len(results); i++ {
		minIdx := i
		for j := i + 1; j < len(results); j++ {
			if results[j].distance < results[minIdx].distance {
				minIdx = j
			}
		}
		results[i], results[minIdx] = results[minIdx], results[i]
	}

	if k > len(results) {
		k = len(results)
	}

	rowIDs := make([]uint64, k)
	dists := make([]float32, k)
	for i := 0; i < k; i++ {
		rowIDs[i] = results[i].rowID
		dists[i] = results[i].distance
	}

	return rowIDs, dists, nil
}

// Remove removes a vector from the index
func (idx *BQIndex) Remove(rowID uint64) bool {
	i, exists := idx.rowIDMap[rowID]
	if !exists {
		return false
	}

	// Remove by swapping with last element
	lastIdx := len(idx.codes) - 1
	if i != lastIdx {
		idx.codes[i] = idx.codes[lastIdx]
		idx.rowIDs[i] = idx.rowIDs[lastIdx]
		idx.rowIDMap[idx.rowIDs[i]] = i
	}

	idx.codes = idx.codes[:lastIdx]
	idx.rowIDs = idx.rowIDs[:lastIdx]
	delete(idx.rowIDMap, rowID)

	return true
}

// Size returns the number of vectors in the index
func (idx *BQIndex) Size() int {
	return len(idx.codes)
}

// MemoryUsage returns approximate memory usage in bytes
func (idx *BQIndex) MemoryUsage() int64 {
	codeSize := int64(idx.quantizer.CodeSize())
	return int64(len(idx.codes)) * (codeSize + 8) // code + rowID
}

// Ensure BQQuantizer implements Quantizer interface
var _ Quantizer = (*BQQuantizer)(nil)
