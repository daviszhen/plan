// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"testing"
)

func TestBQQuantizerEncode(t *testing.T) {
	bq := NewBQQuantizer(8)

	// Test encoding
	vector := []float32{1.0, -1.0, 0.5, -0.5, 2.0, 0.0, -2.0, 0.1}
	code, err := bq.Encode(vector)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Expected: bits 0,2,4,7 are 1 (positive values)
	// bit pattern: 10110101 = 0xB5
	// But bits are stored LSB first within byte
	// position 0: 1.0 > 0 -> bit 0 = 1
	// position 1: -1.0 <= 0 -> bit 1 = 0
	// position 2: 0.5 > 0 -> bit 2 = 1
	// position 3: -0.5 <= 0 -> bit 3 = 0
	// position 4: 2.0 > 0 -> bit 4 = 1
	// position 5: 0.0 <= 0 -> bit 5 = 0
	// position 6: -2.0 <= 0 -> bit 6 = 0
	// position 7: 0.1 > 0 -> bit 7 = 1
	// Result: 10010101 in LSB-first = 0x95
	expected := byte(0x95)
	if len(code) != 1 {
		t.Fatalf("expected 1 byte, got %d", len(code))
	}
	if code[0] != expected {
		t.Errorf("expected code 0x%02X, got 0x%02X", expected, code[0])
	}
}

func TestBQQuantizerDecode(t *testing.T) {
	bq := NewBQQuantizer(8)

	code := []byte{0xFF} // All 1s
	vector, err := bq.Decode(code)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if len(vector) != 8 {
		t.Fatalf("expected 8 dimensions, got %d", len(vector))
	}

	for i, val := range vector {
		if val != 1.0 {
			t.Errorf("vector[%d] = %f, expected 1.0", i, val)
		}
	}

	// Test all 0s
	code = []byte{0x00}
	vector, err = bq.Decode(code)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i, val := range vector {
		if val != -1.0 {
			t.Errorf("vector[%d] = %f, expected -1.0", i, val)
		}
	}
}

func TestBQQuantizerRoundtrip(t *testing.T) {
	bq := NewBQQuantizer(16)

	// Original vector
	original := []float32{1, -1, 0.5, -0.5, 2, 0, -2, 0.1, 1, 1, -1, -1, 0.1, -0.1, 0.5, 0}

	// Encode
	code, err := bq.Encode(original)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode
	decoded, err := bq.Decode(code)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify signs match
	for i := range original {
		originalPositive := original[i] > 0
		decodedPositive := decoded[i] > 0
		if originalPositive != decodedPositive {
			t.Errorf("position %d: sign mismatch (original %v, decoded %v)", i, original[i], decoded[i])
		}
	}
}

func TestBQQuantizerDistance(t *testing.T) {
	bq := NewBQQuantizer(8)
	bq.Train(nil)

	// Same vectors should have distance 0
	v1 := []float32{1, 1, 1, 1, 1, 1, 1, 1}
	c1, _ := bq.Encode(v1)

	dist, err := bq.ComputeDistance(v1, c1)
	if err != nil {
		t.Fatalf("distance computation failed: %v", err)
	}
	if dist != 0 {
		t.Errorf("same vector should have distance 0, got %f", dist)
	}

	// Opposite vectors should have max distance (8)
	v2 := []float32{-1, -1, -1, -1, -1, -1, -1, -1}
	dist, err = bq.ComputeDistance(v1, c1)
	if err != nil {
		t.Fatalf("distance computation failed: %v", err)
	}

	c2, _ := bq.Encode(v2)
	dist, err = bq.ComputeDistance(v1, c2)
	if err != nil {
		t.Fatalf("distance computation failed: %v", err)
	}
	if dist != 8 {
		t.Errorf("opposite vectors should have distance 8, got %f", dist)
	}
}

func TestBQQuantizerDistanceTable(t *testing.T) {
	bq := NewBQQuantizer(8)
	bq.Train(nil)

	query := []float32{1, 1, 1, 1, -1, -1, -1, -1}
	table, err := bq.ComputeDistanceTable(query)
	if err != nil {
		t.Fatalf("compute distance table failed: %v", err)
	}

	// Test with same vector
	code, _ := bq.Encode(query)
	dist := bq.ComputeDistanceWithTable(table, code)
	if dist != 0 {
		t.Errorf("same vector should have distance 0, got %f", dist)
	}
}

func TestBQQuantizerCodeSize(t *testing.T) {
	tests := []struct {
		dimension    int
		expectedSize int
	}{
		{1, 1},
		{8, 1},
		{9, 2},
		{16, 2},
		{17, 3},
		{64, 8},
		{128, 16},
	}

	for _, tc := range tests {
		bq := NewBQQuantizer(tc.dimension)
		if bq.CodeSize() != tc.expectedSize {
			t.Errorf("dimension %d: expected code size %d, got %d",
				tc.dimension, tc.expectedSize, bq.CodeSize())
		}
	}
}

func TestBQQuantizerCompressionRatio(t *testing.T) {
	bq := NewBQQuantizer(128)
	ratio := bq.CompressionRatio()
	if ratio != 32.0 {
		t.Errorf("expected compression ratio 32.0, got %f", ratio)
	}
}

func TestBQIndexBasic(t *testing.T) {
	idx := NewBQIndex("test_bq", 0, 8)

	// Insert vectors
	vectors := [][]float32{
		{1, 1, 1, 1, 1, 1, 1, 1},
		{-1, -1, -1, -1, -1, -1, -1, -1},
		{1, 1, 1, 1, -1, -1, -1, -1},
		{1, -1, 1, -1, 1, -1, 1, -1},
	}

	for i, vec := range vectors {
		if err := idx.Insert(uint64(i), vec); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	if idx.Size() != 4 {
		t.Errorf("expected size 4, got %d", idx.Size())
	}

	// Search for nearest to all-positive
	query := []float32{1, 1, 1, 1, 1, 1, 1, 1}
	rowIDs, dists, err := idx.Search(query, 2)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}

	if len(rowIDs) != 2 {
		t.Fatalf("expected 2 results, got %d", len(rowIDs))
	}

	// First result should be the exact match (rowID 0)
	if rowIDs[0] != 0 {
		t.Errorf("expected rowID 0 as first result, got %d", rowIDs[0])
	}
	if dists[0] != 0 {
		t.Errorf("expected distance 0 for exact match, got %f", dists[0])
	}
}

func TestBQIndexRemove(t *testing.T) {
	idx := NewBQIndex("test_bq", 0, 8)

	idx.Insert(0, []float32{1, 1, 1, 1, 1, 1, 1, 1})
	idx.Insert(1, []float32{-1, -1, -1, -1, -1, -1, -1, -1})
	idx.Insert(2, []float32{1, -1, 1, -1, 1, -1, 1, -1})

	if idx.Size() != 3 {
		t.Fatalf("expected size 3, got %d", idx.Size())
	}

	// Remove middle element
	if !idx.Remove(1) {
		t.Error("remove should return true for existing element")
	}

	if idx.Size() != 2 {
		t.Errorf("expected size 2 after remove, got %d", idx.Size())
	}

	// Remove non-existent
	if idx.Remove(1) {
		t.Error("remove should return false for non-existent element")
	}

	// Verify remaining elements are searchable
	_, _, err := idx.Search([]float32{1, 1, 1, 1, 1, 1, 1, 1}, 10)
	if err != nil {
		t.Fatalf("search after remove failed: %v", err)
	}
}

func TestBQIndexUpdate(t *testing.T) {
	idx := NewBQIndex("test_bq", 0, 8)

	// Insert
	idx.Insert(0, []float32{1, 1, 1, 1, 1, 1, 1, 1})

	// Update same rowID with different vector
	idx.Insert(0, []float32{-1, -1, -1, -1, -1, -1, -1, -1})

	if idx.Size() != 1 {
		t.Errorf("expected size 1 after update, got %d", idx.Size())
	}

	// Search should find the updated vector
	rowIDs, dists, _ := idx.Search([]float32{-1, -1, -1, -1, -1, -1, -1, -1}, 1)
	if rowIDs[0] != 0 {
		t.Errorf("expected rowID 0, got %d", rowIDs[0])
	}
	if dists[0] != 0 {
		t.Errorf("expected distance 0 for exact match, got %f", dists[0])
	}
}

func TestBQQuantizerDimensionMismatch(t *testing.T) {
	bq := NewBQQuantizer(8)

	// Wrong dimension
	_, err := bq.Encode([]float32{1, 2, 3}) // Only 3 dimensions
	if err == nil {
		t.Error("expected error for dimension mismatch")
	}
}

func TestBQQuantizerBatchEncode(t *testing.T) {
	bq := NewBQQuantizer(8)

	vectors := [][]float32{
		{1, 1, 1, 1, 1, 1, 1, 1},
		{-1, -1, -1, -1, -1, -1, -1, -1},
		{1, -1, 1, -1, 1, -1, 1, -1},
	}

	codes, err := bq.EncodeBatch(vectors)
	if err != nil {
		t.Fatalf("batch encode failed: %v", err)
	}

	if len(codes) != 3 {
		t.Errorf("expected 3 codes, got %d", len(codes))
	}

	// Verify each code
	for i, vec := range vectors {
		single, _ := bq.Encode(vec)
		if len(codes[i]) != len(single) {
			t.Errorf("code %d length mismatch", i)
		}
		for j := range single {
			if codes[i][j] != single[j] {
				t.Errorf("code %d byte %d mismatch", i, j)
			}
		}
	}
}

func BenchmarkBQEncode(b *testing.B) {
	bq := NewBQQuantizer(128)
	vector := make([]float32, 128)
	for i := range vector {
		vector[i] = float32(i%2)*2 - 1
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bq.Encode(vector)
	}
}

func BenchmarkBQDistance(b *testing.B) {
	bq := NewBQQuantizer(128)
	v1 := make([]float32, 128)
	v2 := make([]float32, 128)
	for i := range v1 {
		v1[i] = float32(i%2)*2 - 1
		v2[i] = float32((i+1)%2)*2 - 1
	}

	code2, _ := bq.Encode(v2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bq.ComputeDistance(v1, code2)
	}
}
