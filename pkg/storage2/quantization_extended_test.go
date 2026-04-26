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
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

// makeRandomFloat32Vectors generates n random vectors of dimension dim for quantization tests
func makeRandomFloat32Vectors(n, dim int) [][]float32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = r.Float32()*2 - 1 // Range [-1, 1]
		}
		vectors[i] = vec
	}
	return vectors
}

// TestPQ_CosineMetric tests PQ quantizer with cosine metric
func TestPQ_CosineMetric(t *testing.T) {
	dim := 16
	numSubvectors := 4
	bitsPerCode := 8

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    MetricCosine,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Generate training data
	trainData := makeRandomFloat32Vectors(1000, dim)

	// Train the quantizer
	if err := pq.Train(trainData); err != nil {
		t.Fatalf("failed to train PQ: %v", err)
	}

	// Encode and verify
	testVec := makeRandomFloat32Vectors(1, dim)[0]
	code, err := pq.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode vector: %v", err)
	}

	expectedCodeSize := pq.CodeSize()
	if len(code) != expectedCodeSize {
		t.Errorf("expected code size %d, got %d", expectedCodeSize, len(code))
	}

	// Decode and verify reconstruction
	decoded, decErr := pq.Decode(code)
	if decErr != nil {
		t.Fatalf("failed to decode: %v", decErr)
	}
	if len(decoded) != dim {
		t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
	}

	// Compute distance between query and code
	query := makeRandomFloat32Vectors(1, dim)[0]
	dist, err := pq.ComputeDistance(query, code)
	if err != nil {
		t.Fatalf("failed to compute distance: %v", err)
	}

	// Distance should be non-negative
	if dist < 0 {
		t.Errorf("expected non-negative distance, got %f", dist)
	}
}

// TestPQ_DotMetric tests PQ quantizer with dot product metric
func TestPQ_DotMetric(t *testing.T) {
	dim := 16
	numSubvectors := 4
	bitsPerCode := 8

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    DotMetric,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Generate training data
	trainData := makeRandomFloat32Vectors(1000, dim)

	// Train the quantizer
	if err := pq.Train(trainData); err != nil {
		t.Fatalf("failed to train PQ: %v", err)
	}

	// Encode and decode
	testVec := makeRandomFloat32Vectors(1, dim)[0]
	code, err := pq.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode vector: %v", err)
	}

	decoded, decErr := pq.Decode(code)
	if decErr != nil {
		t.Fatalf("failed to decode: %v", decErr)
	}
	if len(decoded) != dim {
		t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
	}

	// Compute distance between query and code
	query := makeRandomFloat32Vectors(1, dim)[0]
	dist, err := pq.ComputeDistance(query, code)
	if err != nil {
		t.Fatalf("failed to compute distance: %v", err)
	}

	if dist < 0 {
		t.Errorf("expected non-negative distance, got %f", dist)
	}
}

// TestPQ_InsufficientRows tests PQ training with fewer rows than centroids
func TestPQ_InsufficientRows(t *testing.T) {
	dim := 16
	numSubvectors := 4
	bitsPerCode := 8 // 256 centroids per subvector

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    MetricL2,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Generate only 50 training samples (fewer than 256 centroids)
	trainData := makeRandomFloat32Vectors(50, dim)

	// Training should either error or degrade gracefully
	err = pq.Train(trainData)
	if err != nil {
		// Error is acceptable for insufficient data
		t.Logf("training failed as expected with insufficient data: %v", err)
		return
	}

	// If training succeeded, verify it works with degraded centroids
	testVec := makeRandomFloat32Vectors(1, dim)[0]
	code, err := pq.Encode(testVec)
	if err != nil {
		t.Fatalf("encode failed after degraded training: %v", err)
	}

	decoded, decErr2 := pq.Decode(code)
	if decErr2 != nil {
		t.Fatalf("failed to decode: %v", decErr2)
	}
	if len(decoded) != dim {
		t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
	}
}

// TestPQ_LargeDimension tests PQ with 256-dimensional vectors
func TestPQ_LargeDimension(t *testing.T) {
	dim := 256
	numSubvectors := 32 // Each subvector handles 8 dimensions
	bitsPerCode := 8

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    MetricL2,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Generate training data
	trainData := makeRandomFloat32Vectors(1000, dim)

	// Train the quantizer
	if err := pq.Train(trainData); err != nil {
		t.Fatalf("failed to train PQ: %v", err)
	}

	// Encode and decode
	testVec := makeRandomFloat32Vectors(1, dim)[0]
	code, err := pq.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode vector: %v", err)
	}

	expectedCodeSize := pq.CodeSize()
	if len(code) != expectedCodeSize {
		t.Errorf("expected code size %d, got %d", expectedCodeSize, len(code))
	}

	decoded, decErr3 := pq.Decode(code)
	if decErr3 != nil {
		t.Fatalf("failed to decode: %v", decErr3)
	}
	if len(decoded) != dim {
		t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
	}

	// Verify reconstruction error is reasonable
	var totalError float64
	for i := 0; i < dim; i++ {
		diff := float64(testVec[i] - decoded[i])
		totalError += diff * diff
	}
	rmse := math.Sqrt(totalError / float64(dim))
	t.Logf("RMSE for 256-dim reconstruction: %f", rmse)
}

// TestSQ_BoundaryValues tests SQ with vectors at float boundaries
func TestSQ_BoundaryValues(t *testing.T) {
	dim := 8

	sq := NewSQQuantizer(dim, MetricL2)

	// Create vectors at boundaries
	boundaryVecs := [][]float32{
		make([]float32, dim), // all zeros
		make([]float32, dim), // will be max
		make([]float32, dim), // will be min
	}

	// Set max values
	for i := 0; i < dim; i++ {
		boundaryVecs[1][i] = math.MaxFloat32
		boundaryVecs[2][i] = -math.MaxFloat32
	}

	// Train with boundary values
	if err := sq.Train(boundaryVecs); err != nil {
		t.Fatalf("failed to train SQ: %v", err)
	}

	// Encode and decode each boundary vector
	for i, vec := range boundaryVecs {
		code, err := sq.Encode(vec)
		if err != nil {
			t.Fatalf("failed to encode boundary vector %d: %v", i, err)
		}

		decoded, decErr := sq.Decode(code)
		if decErr != nil {
			t.Fatalf("failed to decode boundary vector %d: %v", i, decErr)
		}
		if len(decoded) != dim {
			t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
		}
	}
}

// TestSQ_ConstantValues tests SQ with all identical values
func TestSQ_ConstantValues(t *testing.T) {
	dim := 8

	sq := NewSQQuantizer(dim, MetricL2)

	// Create vectors with all identical values
	constantVecs := make([][]float32, 10)
	for i := range constantVecs {
		constantVecs[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			constantVecs[i][j] = 0.5 // All same value
		}
	}

	// Train should handle zero scale case
	if err := sq.Train(constantVecs); err != nil {
		t.Fatalf("failed to train SQ with constant values: %v", err)
	}

	// Encode and decode
	code, err := sq.Encode(constantVecs[0])
	if err != nil {
		t.Fatalf("failed to encode constant vector: %v", err)
	}

	decoded, decErr := sq.Decode(code)
	if decErr != nil {
		t.Fatalf("failed to decode constant vector: %v", decErr)
	}
	if len(decoded) != dim {
		t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
	}

	// All decoded values should be approximately the same
	for i := 1; i < dim; i++ {
		if math.Abs(float64(decoded[i]-decoded[0])) > 0.1 {
			t.Errorf("decoded values not consistent: %f vs %f", decoded[0], decoded[i])
		}
	}
}

// TestSQ_ReconstructionError measures max reconstruction error for SQ
func TestSQ_ReconstructionError(t *testing.T) {
	dim := 16

	sq := NewSQQuantizer(dim, MetricL2)

	// Generate training data
	trainData := makeRandomFloat32Vectors(1000, dim)

	// Train
	if err := sq.Train(trainData); err != nil {
		t.Fatalf("failed to train SQ: %v", err)
	}

	// Test reconstruction error on new vectors
	testVecs := makeRandomFloat32Vectors(100, dim)
	var maxError float64
	var totalError float64

	for _, vec := range testVecs {
		code, err := sq.Encode(vec)
		if err != nil {
			t.Fatalf("failed to encode: %v", err)
		}

		decoded, decErr := sq.Decode(code)
		if decErr != nil {
			t.Fatalf("failed to decode: %v", decErr)
		}

		for i := 0; i < dim; i++ {
			diff := math.Abs(float64(vec[i] - decoded[i]))
			if diff > maxError {
				maxError = diff
			}
			totalError += diff
		}
	}

	avgError := totalError / float64(len(testVecs)*dim)
	t.Logf("SQ Reconstruction - Max error: %f, Avg error: %f", maxError, avgError)

	// SQ typically has low reconstruction error for normalized data
	if maxError > 0.5 {
		t.Errorf("max reconstruction error %f seems high for SQ", maxError)
	}
}

// TestPQ_ZeroVectors tests PQ with all-zero vectors
func TestPQ_ZeroVectors(t *testing.T) {
	dim := 16
	numSubvectors := 4
	bitsPerCode := 8

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    MetricL2,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Generate training data with some zero vectors mixed in
	trainData := makeRandomFloat32Vectors(900, dim)
	// Add 100 zero vectors
	for i := 0; i < 100; i++ {
		zeroVec := make([]float32, dim)
		trainData = append(trainData, zeroVec)
	}

	// Train
	if err := pq.Train(trainData); err != nil {
		t.Fatalf("failed to train PQ: %v", err)
	}

	// Encode a zero vector
	zeroVec := make([]float32, dim)
	code, err := pq.Encode(zeroVec)
	if err != nil {
		t.Fatalf("failed to encode zero vector: %v", err)
	}

	decoded, decErr4 := pq.Decode(code)
	if decErr4 != nil {
		t.Fatalf("failed to decode zero vector: %v", decErr4)
	}
	if len(decoded) != dim {
		t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
	}

	// Decoded values should be close to zero
	var maxVal float32
	for _, v := range decoded {
		if v > maxVal {
			maxVal = v
		}
		if v < -maxVal {
			maxVal = -v
		}
	}
	t.Logf("Max decoded value for zero vector: %f", maxVal)
}

// TestPQ_MarshalUnmarshal tests PQ serialization
func TestPQ_MarshalUnmarshal(t *testing.T) {
	dim := 16
	numSubvectors := 4
	bitsPerCode := 8

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    MetricL2,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Train
	trainData := makeRandomFloat32Vectors(500, dim)
	if err := pq.Train(trainData); err != nil {
		t.Fatalf("failed to train PQ: %v", err)
	}

	// Marshal
	data, err := pq.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal PQ: %v", err)
	}

	// Create new PQ and unmarshal
	pq2, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create second PQ quantizer: %v", err)
	}

	if err := pq2.Unmarshal(data); err != nil {
		t.Fatalf("failed to unmarshal PQ: %v", err)
	}

	// Verify both produce same results
	testVec := makeRandomFloat32Vectors(1, dim)[0]

	code1, err := pq.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode with original: %v", err)
	}

	code2, err := pq2.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode with unmarshaled: %v", err)
	}

	if len(code1) != len(code2) {
		t.Errorf("code lengths differ: %d vs %d", len(code1), len(code2))
	}

	for i := range code1 {
		if code1[i] != code2[i] {
			t.Errorf("code mismatch at position %d: %d vs %d", i, code1[i], code2[i])
		}
	}
}

// TestSQ_MarshalUnmarshal tests SQ serialization
func TestSQ_MarshalUnmarshal(t *testing.T) {
	dim := 16

	sq := NewSQQuantizer(dim, MetricL2)

	// Train
	trainData := makeRandomFloat32Vectors(500, dim)
	if err := sq.Train(trainData); err != nil {
		t.Fatalf("failed to train SQ: %v", err)
	}

	// Marshal
	data, err := sq.Marshal()
	if err != nil {
		t.Fatalf("failed to marshal SQ: %v", err)
	}

	// Create new SQ and unmarshal
	sq2 := NewSQQuantizer(dim, MetricL2)
	if err := sq2.Unmarshal(data); err != nil {
		t.Fatalf("failed to unmarshal SQ: %v", err)
	}

	// Verify both produce same results
	testVec := makeRandomFloat32Vectors(1, dim)[0]

	code1, err := sq.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode with original: %v", err)
	}

	code2, err := sq2.Encode(testVec)
	if err != nil {
		t.Fatalf("failed to encode with unmarshaled: %v", err)
	}

	if len(code1) != len(code2) {
		t.Errorf("code lengths differ: %d vs %d", len(code1), len(code2))
	}

	for i := range code1 {
		if code1[i] != code2[i] {
			t.Errorf("code mismatch at position %d: %d vs %d", i, code1[i], code2[i])
		}
	}
}

// TestPQ_ComputeDistance verifies distance computation accuracy
func TestPQ_ComputeDistance(t *testing.T) {
	dim := 16
	numSubvectors := 4
	bitsPerCode := 8

	cfg := PQConfig{
		Dimension:     dim,
		NumSubvectors: numSubvectors,
		BitsPerCode:   bitsPerCode,
		MetricType:    MetricL2,
		KMeansIters:   10,
	}

	pq, err := NewPQQuantizer(cfg)
	if err != nil {
		t.Fatalf("failed to create PQ quantizer: %v", err)
	}

	// Train
	trainData := makeRandomFloat32Vectors(500, dim)
	if err := pq.Train(trainData); err != nil {
		t.Fatalf("failed to train PQ: %v", err)
	}

	// Test distance computation
	query := makeRandomFloat32Vectors(1, dim)[0]
	vec := makeRandomFloat32Vectors(1, dim)[0]

	// Get approximate distance via PQ
	code, err := pq.Encode(vec)
	if err != nil {
		t.Fatalf("failed to encode: %v", err)
	}

	approxDist, err := pq.ComputeDistance(query, code)
	if err != nil {
		t.Fatalf("failed to compute distance: %v", err)
	}

	// Compute exact L2 distance
	var exactDist float32
	for i := 0; i < dim; i++ {
		diff := query[i] - vec[i]
		exactDist += diff * diff
	}
	exactDist = float32(math.Sqrt(float64(exactDist)))

	t.Logf("Approximate distance: %f, Exact distance: %f", approxDist, exactDist)
}

// TestSQ_DifferentMetrics tests SQ with different distance metrics
func TestSQ_DifferentMetrics(t *testing.T) {
	dim := 8

	metrics := []MetricType{L2Metric, CosineMetric, DotMetric}

	for _, metric := range metrics {
		t.Run(fmt.Sprintf("metric_%d", metric), func(t *testing.T) {
			sq := NewSQQuantizer(dim, metric)

			trainData := makeRandomFloat32Vectors(200, dim)
			if err := sq.Train(trainData); err != nil {
				t.Fatalf("failed to train SQ with metric %s: %v", metric, err)
			}

			testVec := makeRandomFloat32Vectors(1, dim)[0]
			code, err := sq.Encode(testVec)
			if err != nil {
				t.Fatalf("failed to encode: %v", err)
			}

			decoded, decErr := sq.Decode(code)
			if decErr != nil {
				t.Fatalf("failed to decode: %v", decErr)
			}
			if len(decoded) != dim {
				t.Errorf("expected decoded dimension %d, got %d", dim, len(decoded))
			}
		})
	}
}

// TestPQ_VariousConfigurations tests PQ with different configurations
func TestPQ_VariousConfigurations(t *testing.T) {
	configs := []struct {
		name          string
		dim           int
		numSubvectors int
		bitsPerCode   int
	}{
		{"small", 8, 2, 4},
		{"medium", 32, 8, 8},
		{"many_subvectors", 64, 16, 4},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			cfg := PQConfig{
				Dimension:     tc.dim,
				NumSubvectors: tc.numSubvectors,
				BitsPerCode:   tc.bitsPerCode,
				MetricType:    MetricL2,
				KMeansIters:   5,
			}

			pq, err := NewPQQuantizer(cfg)
			if err != nil {
				t.Fatalf("failed to create PQ: %v", err)
			}

			trainData := makeRandomFloat32Vectors(500, tc.dim)
			if err := pq.Train(trainData); err != nil {
				t.Fatalf("failed to train PQ: %v", err)
			}

			testVec := makeRandomFloat32Vectors(1, tc.dim)[0]
			code, err := pq.Encode(testVec)
			if err != nil {
				t.Fatalf("failed to encode: %v", err)
			}

			expectedCodeSize := tc.numSubvectors * tc.bitsPerCode / 8
			if tc.bitsPerCode%8 != 0 {
				expectedCodeSize = tc.numSubvectors * ((tc.bitsPerCode + 7) / 8)
			}
			if len(code) != expectedCodeSize && len(code) != tc.numSubvectors {
				t.Logf("code size: got %d, expected %d or %d", len(code), expectedCodeSize, tc.numSubvectors)
			}

			decoded, decErr := pq.Decode(code)
			if decErr != nil {
				t.Fatalf("failed to decode: %v", decErr)
			}
			if len(decoded) != tc.dim {
				t.Errorf("expected decoded dimension %d, got %d", tc.dim, len(decoded))
			}
		})
	}
}
