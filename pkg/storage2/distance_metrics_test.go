// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"math"
	"testing"
)

func TestL2Distance(t *testing.T) {
	tests := []struct {
		a, b     []float32
		expected float32
	}{
		{[]float32{0, 0}, []float32{0, 0}, 0},
		{[]float32{0, 0}, []float32{3, 4}, 5},
		{[]float32{1, 2, 3}, []float32{4, 5, 6}, float32(math.Sqrt(27))},
		{[]float32{1, 1, 1, 1}, []float32{2, 2, 2, 2}, 2},
	}

	for i, tc := range tests {
		result := L2Distance(tc.a, tc.b)
		if math.Abs(float64(result-tc.expected)) > 0.001 {
			t.Errorf("test %d: L2Distance(%v, %v) = %f, want %f", i, tc.a, tc.b, result, tc.expected)
		}
	}
}

func TestCosineDistance(t *testing.T) {
	tests := []struct {
		a, b     []float32
		expected float32
	}{
		// Same direction = 0 distance
		{[]float32{1, 0}, []float32{1, 0}, 0},
		{[]float32{1, 2, 3}, []float32{2, 4, 6}, 0},
		// Perpendicular = 1 distance
		{[]float32{1, 0}, []float32{0, 1}, 1},
		// Opposite direction = 2 distance
		{[]float32{1, 0}, []float32{-1, 0}, 2},
	}

	for i, tc := range tests {
		result := CosineDistance(tc.a, tc.b)
		if math.Abs(float64(result-tc.expected)) > 0.001 {
			t.Errorf("test %d: CosineDistance(%v, %v) = %f, want %f", i, tc.a, tc.b, result, tc.expected)
		}
	}
}

func TestDotProductDistance(t *testing.T) {
	tests := []struct {
		a, b     []float32
		expected float32
	}{
		{[]float32{1, 2, 3}, []float32{4, 5, 6}, -32}, // -(1*4 + 2*5 + 3*6) = -32
		{[]float32{1, 0}, []float32{0, 1}, 0},         // Perpendicular
		{[]float32{1, 1}, []float32{1, 1}, -2},        // -(1+1) = -2
	}

	for i, tc := range tests {
		result := DotProductDistance(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("test %d: DotProductDistance(%v, %v) = %f, want %f", i, tc.a, tc.b, result, tc.expected)
		}
	}
}

func TestHammingDistance(t *testing.T) {
	tests := []struct {
		a, b     []float32
		expected float32
	}{
		// Same signs = 0 distance
		{[]float32{1, 1, 1}, []float32{2, 3, 4}, 0},
		// All different = dimension distance
		{[]float32{1, 1, 1}, []float32{-1, -1, -1}, 3},
		// Mixed
		{[]float32{1, -1, 1, -1}, []float32{1, 1, -1, -1}, 2},
		// Zero values count as negative (not > 0)
		{[]float32{1, 0}, []float32{1, 1}, 1},
	}

	for i, tc := range tests {
		result := HammingDistance(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("test %d: HammingDistance(%v, %v) = %f, want %f", i, tc.a, tc.b, result, tc.expected)
		}
	}
}

func TestHammingDistanceBinary(t *testing.T) {
	tests := []struct {
		a, b     []uint64
		expected int
	}{
		{[]uint64{0}, []uint64{0}, 0},
		{[]uint64{0xFF}, []uint64{0}, 8},        // 8 ones
		{[]uint64{0xFFFFFFFF}, []uint64{0}, 32}, // 32 ones
		{[]uint64{0b1010}, []uint64{0b0101}, 4}, // All 4 bits differ
		{[]uint64{0b1111}, []uint64{0b1110}, 1}, // 1 bit differs
	}

	for i, tc := range tests {
		result := HammingDistanceBinary(tc.a, tc.b)
		if result != tc.expected {
			t.Errorf("test %d: HammingDistanceBinary = %d, want %d", i, result, tc.expected)
		}
	}
}

func TestNormalizeVector(t *testing.T) {
	v := []float32{3, 4}
	normalized := NormalizeVector(v)

	// L2 norm should be 1
	norm := L2Norm(normalized)
	if math.Abs(float64(norm-1.0)) > 0.001 {
		t.Errorf("normalized vector has norm %f, want 1.0", norm)
	}

	// Direction should be preserved
	expected := []float32{0.6, 0.8} // 3/5, 4/5
	for i, val := range normalized {
		if math.Abs(float64(val-expected[i])) > 0.001 {
			t.Errorf("normalized[%d] = %f, want %f", i, val, expected[i])
		}
	}
}

func TestNormalizeVectorInPlace(t *testing.T) {
	v := []float32{3, 4}
	NormalizeVectorInPlace(v)

	norm := L2Norm(v)
	if math.Abs(float64(norm-1.0)) > 0.001 {
		t.Errorf("normalized vector has norm %f, want 1.0", norm)
	}
}

func TestInnerProduct(t *testing.T) {
	a := []float32{1, 2, 3}
	b := []float32{4, 5, 6}
	expected := float32(32) // 1*4 + 2*5 + 3*6

	result := InnerProduct(a, b)
	if result != expected {
		t.Errorf("InnerProduct = %f, want %f", result, expected)
	}
}

func TestSquaredL2Distance(t *testing.T) {
	a := []float32{0, 0}
	b := []float32{3, 4}
	expected := float32(25) // 9 + 16

	result := SquaredL2Distance(a, b)
	if result != expected {
		t.Errorf("SquaredL2Distance = %f, want %f", result, expected)
	}
}

func TestComputeDistance(t *testing.T) {
	a := []float32{1, 0}
	b := []float32{0, 1}

	// L2: sqrt(2)
	l2 := ComputeDistance(a, b, L2Metric)
	if math.Abs(float64(l2-float32(math.Sqrt(2)))) > 0.001 {
		t.Errorf("L2 distance = %f, want %f", l2, math.Sqrt(2))
	}

	// Cosine: 1 (perpendicular)
	cosine := ComputeDistance(a, b, CosineMetric)
	if math.Abs(float64(cosine-1.0)) > 0.001 {
		t.Errorf("Cosine distance = %f, want 1.0", cosine)
	}

	// Dot: 0
	dot := ComputeDistance(a, b, DotMetric)
	if dot != 0 {
		t.Errorf("Dot distance = %f, want 0", dot)
	}

	// Hamming: 2 (both bits differ)
	hamming := ComputeDistance(a, b, HammingMetric)
	if hamming != 2 {
		t.Errorf("Hamming distance = %f, want 2", hamming)
	}
}

func TestBatchDistances(t *testing.T) {
	query := []float32{0, 0}
	vectors := [][]float32{
		{3, 4}, // distance 5
		{0, 1}, // distance 1
		{1, 0}, // distance 1
	}

	l2Distances := BatchL2Distance(query, vectors)
	if len(l2Distances) != 3 {
		t.Fatalf("expected 3 distances, got %d", len(l2Distances))
	}
	if math.Abs(float64(l2Distances[0]-5.0)) > 0.001 {
		t.Errorf("l2Distances[0] = %f, want 5.0", l2Distances[0])
	}
}

func TestMetricTypeFromString(t *testing.T) {
	tests := []struct {
		input    string
		expected MetricType
	}{
		{"l2", L2Metric},
		{"L2", L2Metric},
		{"euclidean", L2Metric},
		{"cosine", CosineMetric},
		{"Cosine", CosineMetric},
		{"dot", DotMetric},
		{"inner_product", DotMetric},
		{"ip", DotMetric},
		{"hamming", HammingMetric},
		{"Hamming", HammingMetric},
		{"unknown", L2Metric}, // Default
	}

	for _, tc := range tests {
		result := MetricTypeFromString(tc.input)
		if result != tc.expected {
			t.Errorf("MetricTypeFromString(%q) = %v, want %v", tc.input, result, tc.expected)
		}
	}
}

func TestMetricTypeStringDistanceMetrics(t *testing.T) {
	tests := []struct {
		metric   MetricType
		expected string
	}{
		{L2Metric, "l2"},
		{CosineMetric, "cosine"},
		{DotMetric, "dot"},
		{HammingMetric, "hamming"},
		{MetricType(100), "unknown"},
	}

	for _, tc := range tests {
		result := tc.metric.String()
		if result != tc.expected {
			t.Errorf("MetricType(%d).String() = %q, want %q", tc.metric, result, tc.expected)
		}
	}
}

func TestIsValidMetricType(t *testing.T) {
	if !IsValidMetricType(L2Metric) {
		t.Error("L2Metric should be valid")
	}
	if !IsValidMetricType(HammingMetric) {
		t.Error("HammingMetric should be valid")
	}
	if IsValidMetricType(MetricType(100)) {
		t.Error("MetricType(100) should be invalid")
	}
}

func TestDifferentLengthVectors(t *testing.T) {
	a := []float32{1, 2, 3, 4, 5}
	b := []float32{1, 2}

	// Should work with shorter length
	l2 := L2Distance(a, b)
	if l2 != 0 {
		t.Errorf("L2Distance with different lengths failed, got %f", l2)
	}

	cosine := CosineDistance(a, b)
	if math.Abs(float64(cosine)) > 0.001 {
		t.Errorf("CosineDistance with same prefix vectors should be 0, got %f", cosine)
	}
}

func BenchmarkL2Distance(b *testing.B) {
	v1 := make([]float32, 128)
	v2 := make([]float32, 128)
	for i := range v1 {
		v1[i] = float32(i)
		v2[i] = float32(i + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		L2Distance(v1, v2)
	}
}

func BenchmarkCosineDistance(b *testing.B) {
	v1 := make([]float32, 128)
	v2 := make([]float32, 128)
	for i := range v1 {
		v1[i] = float32(i)
		v2[i] = float32(i + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CosineDistance(v1, v2)
	}
}

func BenchmarkHammingDistanceBinary(b *testing.B) {
	// 128 dimensions packed into 2 uint64s
	v1 := []uint64{0xAAAAAAAAAAAAAAAA, 0x5555555555555555}
	v2 := []uint64{0x5555555555555555, 0xAAAAAAAAAAAAAAAA}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		HammingDistanceBinary(v1, v2)
	}
}
