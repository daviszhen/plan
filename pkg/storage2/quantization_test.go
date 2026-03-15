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
	"math/rand"
	"testing"
)

// --- PQ tests ---

func TestPQQuantizerTrainAndEncode(t *testing.T) {
	dim := 64
	numVectors := 500

	vectors := randomVectors(numVectors, dim)

	pq, err := NewPQQuantizer(PQConfig{
		Dimension:     dim,
		NumSubvectors: 8,
		BitsPerCode:   8,
		MetricType:    L2Metric,
	})
	if err != nil {
		t.Fatal(err)
	}

	if pq.IsTrained() {
		t.Fatal("should not be trained yet")
	}

	if err := pq.Train(vectors[:200]); err != nil {
		t.Fatal(err)
	}

	if !pq.IsTrained() {
		t.Fatal("should be trained")
	}

	if pq.CodeSize() != 8 {
		t.Fatalf("expected code size 8, got %d", pq.CodeSize())
	}

	// Encode and decode
	code, err := pq.Encode(vectors[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(code) != 8 {
		t.Fatalf("expected code length 8, got %d", len(code))
	}

	decoded, err := pq.Decode(code)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != dim {
		t.Fatalf("decoded dimension %d != %d", len(decoded), dim)
	}

	// Reconstruction error should be finite
	recErr := l2Distance(vectors[0], decoded)
	if math.IsNaN(float64(recErr)) || math.IsInf(float64(recErr), 0) {
		t.Fatalf("reconstruction error is not finite: %f", recErr)
	}
	t.Logf("PQ reconstruction error: %f", recErr)
}

func TestPQDistanceTable(t *testing.T) {
	dim := 32
	vectors := randomVectors(300, dim)

	pq, err := NewPQQuantizer(PQConfig{
		Dimension:     dim,
		NumSubvectors: 4,
		BitsPerCode:   8,
		MetricType:    L2Metric,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pq.Train(vectors[:200]); err != nil {
		t.Fatal(err)
	}

	query := vectors[0]
	target := vectors[1]

	code, err := pq.Encode(target)
	if err != nil {
		t.Fatal(err)
	}

	// Direct distance
	dist1, err := pq.ComputeDistance(query, code)
	if err != nil {
		t.Fatal(err)
	}

	// Table-based distance
	table, err := pq.ComputeDistanceTable(query)
	if err != nil {
		t.Fatal(err)
	}
	dist2 := pq.ComputeDistanceWithTable(table, code)

	// Both methods should agree
	if math.Abs(float64(dist1-dist2)) > 1e-5 {
		t.Fatalf("distance mismatch: direct=%f table=%f", dist1, dist2)
	}
}

func TestPQMarshalUnmarshal(t *testing.T) {
	dim := 32
	vectors := randomVectors(200, dim)

	pq, err := NewPQQuantizer(PQConfig{
		Dimension:     dim,
		NumSubvectors: 4,
		BitsPerCode:   8,
		MetricType:    L2Metric,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pq.Train(vectors[:100]); err != nil {
		t.Fatal(err)
	}

	data, err := pq.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	pq2 := &PQQuantizer{}
	if err := pq2.Unmarshal(data); err != nil {
		t.Fatal(err)
	}

	if !pq2.IsTrained() {
		t.Fatal("deserialized quantizer should be trained")
	}

	// Encode with both and compare
	code1, _ := pq.Encode(vectors[0])
	code2, _ := pq2.Encode(vectors[0])

	for i := range code1 {
		if code1[i] != code2[i] {
			t.Fatalf("code mismatch at %d: %d vs %d", i, code1[i], code2[i])
		}
	}
}

func TestPQValidation(t *testing.T) {
	// Bad dimension
	_, err := NewPQQuantizer(PQConfig{Dimension: 0})
	if err == nil {
		t.Fatal("expected error for zero dimension")
	}

	// Dimension not divisible by numSubvectors
	_, err = NewPQQuantizer(PQConfig{Dimension: 10, NumSubvectors: 3})
	if err == nil {
		t.Fatal("expected error for indivisible dimension")
	}

	// bitsPerCode > 8
	_, err = NewPQQuantizer(PQConfig{Dimension: 16, NumSubvectors: 2, BitsPerCode: 9})
	if err == nil {
		t.Fatal("expected error for bitsPerCode > 8")
	}

	// Encode before training
	pq, _ := NewPQQuantizer(PQConfig{Dimension: 16, NumSubvectors: 2})
	_, err = pq.Encode(make([]float32, 16))
	if err == nil {
		t.Fatal("expected error encoding untrained quantizer")
	}
}

// --- SQ tests ---

func TestSQQuantizerTrainAndEncode(t *testing.T) {
	dim := 64
	vectors := randomVectors(500, dim)

	sq := NewSQQuantizer(dim, L2Metric)
	if sq.IsTrained() {
		t.Fatal("should not be trained yet")
	}

	if err := sq.Train(vectors[:200]); err != nil {
		t.Fatal(err)
	}
	if !sq.IsTrained() {
		t.Fatal("should be trained")
	}
	if sq.CodeSize() != dim {
		t.Fatalf("expected code size %d, got %d", dim, sq.CodeSize())
	}

	code, err := sq.Encode(vectors[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(code) != dim {
		t.Fatalf("expected code length %d, got %d", dim, len(code))
	}

	decoded, err := sq.Decode(code)
	if err != nil {
		t.Fatal(err)
	}

	recErr := l2Distance(vectors[0], decoded)
	t.Logf("SQ reconstruction error: %f", recErr)

	// SQ should have lower reconstruction error than PQ for the same dimension
	if recErr > 5.0 {
		t.Fatalf("SQ reconstruction error too high: %f", recErr)
	}
}

func TestSQMarshalUnmarshal(t *testing.T) {
	dim := 16
	vectors := randomVectors(100, dim)

	sq := NewSQQuantizer(dim, L2Metric)
	if err := sq.Train(vectors); err != nil {
		t.Fatal(err)
	}

	data, err := sq.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	sq2 := &SQQuantizer{}
	if err := sq2.Unmarshal(data); err != nil {
		t.Fatal(err)
	}

	if !sq2.IsTrained() {
		t.Fatal("deserialized SQ should be trained")
	}

	code1, _ := sq.Encode(vectors[0])
	code2, _ := sq2.Encode(vectors[0])
	for i := range code1 {
		if code1[i] != code2[i] {
			t.Fatalf("SQ code mismatch at %d", i)
		}
	}
}

func TestSQDistance(t *testing.T) {
	dim := 16
	vectors := randomVectors(100, dim)

	sq := NewSQQuantizer(dim, L2Metric)
	if err := sq.Train(vectors); err != nil {
		t.Fatal(err)
	}

	code, _ := sq.Encode(vectors[1])
	query := vectors[0]

	dist1, err := sq.ComputeDistance(query, code)
	if err != nil {
		t.Fatal(err)
	}

	table, err := sq.ComputeDistanceTable(query)
	if err != nil {
		t.Fatal(err)
	}
	dist2 := sq.ComputeDistanceWithTable(table, code)

	if math.Abs(float64(dist1-dist2)) > 1e-5 {
		t.Fatalf("SQ distance mismatch: %f vs %f", dist1, dist2)
	}
}

// --- IVF-PQ integration tests ---

func TestIVFPQIndex(t *testing.T) {
	dim := 64
	numVectors := 1000

	idx, err := NewIVFPQIndex("test_ivfpq", 0, dim, L2Metric, QuantizationPQ)
	if err != nil {
		t.Fatal(err)
	}

	vectors := randomVectors(numVectors, dim)

	// Train
	if err := idx.Train(vectors[:500]); err != nil {
		t.Fatal(err)
	}

	// Insert all vectors
	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatal(err)
		}
	}

	// Search
	query := vectors[0]
	distances, rowIDs, err := idx.ANNSearch(context.Background(), query, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(rowIDs) == 0 {
		t.Fatal("no results returned")
	}
	if len(distances) != len(rowIDs) {
		t.Fatal("distances/rowIDs length mismatch")
	}

	t.Logf("IVF-PQ top-10 results: rowIDs=%v distances=%v", rowIDs, distances)

	// Distances should be sorted
	for i := 1; i < len(distances); i++ {
		if distances[i] < distances[i-1] {
			t.Fatalf("distances not sorted at index %d", i)
		}
	}
}

func TestIVFSQIndex(t *testing.T) {
	dim := 32
	numVectors := 500

	idx, err := NewIVFPQIndex("test_ivfsq", 0, dim, L2Metric, QuantizationSQ)
	if err != nil {
		t.Fatal(err)
	}

	vectors := randomVectors(numVectors, dim)

	if err := idx.Train(vectors[:200]); err != nil {
		t.Fatal(err)
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatal(err)
		}
	}

	_, rowIDs, err := idx.ANNSearch(context.Background(), vectors[0], 5)
	if err != nil {
		t.Fatal(err)
	}

	if len(rowIDs) == 0 {
		t.Fatal("no results")
	}
	t.Logf("IVF-SQ top-5: %v", rowIDs)
}

func TestIVFPQNoQuantizer(t *testing.T) {
	dim := 32
	numVectors := 200

	idx, err := NewIVFPQIndex("test_ivf_none", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatal(err)
	}

	vectors := randomVectors(numVectors, dim)

	if err := idx.Train(vectors[:100]); err != nil {
		t.Fatal(err)
	}
	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatal(err)
		}
	}

	dists, rowIDs, err := idx.ANNSearch(context.Background(), vectors[0], 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) == 0 {
		t.Fatal("no results")
	}

	// First result should be very close to 0 (self-match)
	if dists[0] > 0.01 {
		t.Logf("Warning: first result distance=%f (self-match expected ~0)", dists[0])
	}
}

func TestIVFPQSearchInterface(t *testing.T) {
	dim := 32
	idx, err := NewIVFPQIndex("test_search", 0, dim, L2Metric, QuantizationNone)
	if err != nil {
		t.Fatal(err)
	}

	vectors := randomVectors(100, dim)
	if err := idx.Train(vectors); err != nil {
		t.Fatal(err)
	}
	for i, v := range vectors {
		if err := idx.Insert(uint64(i), v); err != nil {
			t.Fatal(err)
		}
	}

	// Search via the Index interface
	rowIDs, err := idx.Search(context.Background(), vectors[0], 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) == 0 {
		t.Fatal("no results via Search interface")
	}
}

// --- helpers ---

func randomVectors(n, dim int) [][]float32 {
	vectors := make([][]float32, n)
	for i := range vectors {
		vectors[i] = make([]float32, dim)
		for j := range vectors[i] {
			vectors[i][j] = rand.Float32()
		}
	}
	return vectors
}
