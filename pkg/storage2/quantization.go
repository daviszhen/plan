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

// QuantizationType identifies the vector quantization method.
type QuantizationType int

const (
	QuantizationNone QuantizationType = iota
	QuantizationPQ                    // Product Quantization
	QuantizationSQ                    // Scalar Quantization (int8)
	QuantizationBQ                    // Binary Quantization (1-bit)
)

// Quantizer compresses float32 vectors into compact codes for memory-efficient ANN search.
type Quantizer interface {
	// Train learns quantization parameters from a set of training vectors.
	Train(vectors [][]float32) error

	// Encode compresses a vector into a compact code.
	Encode(vector []float32) ([]byte, error)

	// Decode reconstructs an approximate vector from a code.
	Decode(code []byte) ([]float32, error)

	// ComputeDistance computes the approximate distance between a query vector and a code.
	ComputeDistance(query []float32, code []byte) (float32, error)

	// ComputeDistanceTable precomputes a lookup table for fast distance computation.
	// For PQ, this is the asymmetric distance table; for SQ, the query itself is returned.
	ComputeDistanceTable(query []float32) (interface{}, error)

	// ComputeDistanceWithTable computes distance using a precomputed table.
	ComputeDistanceWithTable(table interface{}, code []byte) float32

	// CodeSize returns the number of bytes per encoded vector.
	CodeSize() int

	// Type returns the quantization type.
	Type() QuantizationType

	// IsTrained returns whether the quantizer has been trained.
	IsTrained() bool
}
