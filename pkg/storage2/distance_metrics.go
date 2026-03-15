// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"math"
	"math/bits"
)

// ComputeDistance calculates distance between two vectors based on metric type
func ComputeDistance(a, b []float32, metric MetricType) float32 {
	switch metric {
	case L2Metric:
		return L2Distance(a, b)
	case CosineMetric:
		return CosineDistance(a, b)
	case DotMetric:
		return DotProductDistance(a, b)
	case HammingMetric:
		return HammingDistance(a, b)
	default:
		return L2Distance(a, b)
	}
}

// L2Distance computes Euclidean (L2) distance between two vectors
func L2Distance(a, b []float32) float32 {
	var sum float32
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// CosineDistance computes cosine distance (1 - cosine similarity)
func CosineDistance(a, b []float32) float32 {
	var dot, normA, normB float32
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
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

// DotProductDistance computes negative dot product
// Returns negative value so that larger dot products result in smaller distances
func DotProductDistance(a, b []float32) float32 {
	var dot float32
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		dot += a[i] * b[i]
	}
	return -dot
}

// HammingDistance computes Hamming distance for binary vectors
// Treats positive values as 1, non-positive values as 0
func HammingDistance(a, b []float32) float32 {
	var dist float32
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		aBit := a[i] > 0
		bBit := b[i] > 0
		if aBit != bBit {
			dist++
		}
	}
	return dist
}

// HammingDistanceBinary computes Hamming distance for packed binary data (uint64 slices)
// This is more efficient for binary quantized vectors
func HammingDistanceBinary(a, b []uint64) int {
	var dist int
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		dist += bits.OnesCount64(a[i] ^ b[i])
	}
	return dist
}

// NormalizeVector normalizes a vector to unit length (L2 norm = 1)
// Returns a new normalized vector
func NormalizeVector(v []float32) []float32 {
	var norm float32
	for _, val := range v {
		norm += val * val
	}

	if norm == 0 {
		result := make([]float32, len(v))
		return result
	}

	invNorm := float32(1.0 / math.Sqrt(float64(norm)))
	result := make([]float32, len(v))
	for i, val := range v {
		result[i] = val * invNorm
	}
	return result
}

// NormalizeVectorInPlace normalizes a vector in-place
func NormalizeVectorInPlace(v []float32) {
	var norm float32
	for _, val := range v {
		norm += val * val
	}

	if norm == 0 {
		return
	}

	invNorm := float32(1.0 / math.Sqrt(float64(norm)))
	for i := range v {
		v[i] *= invNorm
	}
}

// InnerProduct computes the inner (dot) product of two vectors
func InnerProduct(a, b []float32) float32 {
	var sum float32
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		sum += a[i] * b[i]
	}
	return sum
}

// L2Norm computes the L2 norm (magnitude) of a vector
func L2Norm(v []float32) float32 {
	var sum float32
	for _, val := range v {
		sum += val * val
	}
	return float32(math.Sqrt(float64(sum)))
}

// SquaredL2Distance computes squared Euclidean distance (avoids sqrt for efficiency)
func SquaredL2Distance(a, b []float32) float32 {
	var sum float32
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum
}

// BatchL2Distance computes L2 distances from a query to multiple vectors
func BatchL2Distance(query []float32, vectors [][]float32) []float32 {
	distances := make([]float32, len(vectors))
	for i, vec := range vectors {
		distances[i] = L2Distance(query, vec)
	}
	return distances
}

// BatchCosineDistance computes cosine distances from a query to multiple vectors
func BatchCosineDistance(query []float32, vectors [][]float32) []float32 {
	distances := make([]float32, len(vectors))
	for i, vec := range vectors {
		distances[i] = CosineDistance(query, vec)
	}
	return distances
}

// BatchDotProductDistance computes dot product distances from a query to multiple vectors
func BatchDotProductDistance(query []float32, vectors [][]float32) []float32 {
	distances := make([]float32, len(vectors))
	for i, vec := range vectors {
		distances[i] = DotProductDistance(query, vec)
	}
	return distances
}

// MetricTypeFromString parses a metric type from string
func MetricTypeFromString(s string) MetricType {
	switch s {
	case "l2", "L2", "euclidean":
		return L2Metric
	case "cosine", "Cosine":
		return CosineMetric
	case "dot", "Dot", "inner_product", "ip":
		return DotMetric
	case "hamming", "Hamming":
		return HammingMetric
	default:
		return L2Metric
	}
}

// IsValidMetricType checks if a metric type is valid
func IsValidMetricType(m MetricType) bool {
	return m >= L2Metric && m <= HammingMetric
}
