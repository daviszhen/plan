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
)

// SQQuantizer implements Scalar Quantization (uint8).
// Each float32 dimension is linearly mapped to [0, 255] using per-dimension min/max,
// achieving 4x compression (float32 -> uint8).
type SQQuantizer struct {
	dimension  int
	mins       []float32
	scales     []float32 // (255.0) / (max - min) per dimension
	metricType MetricType
	trained    bool
}

// NewSQQuantizer creates a new scalar quantizer.
func NewSQQuantizer(dimension int, metric MetricType) *SQQuantizer {
	return &SQQuantizer{
		dimension:  dimension,
		metricType: metric,
	}
}

// Train computes per-dimension min and scale from the training set.
func (sq *SQQuantizer) Train(vectors [][]float32) error {
	if len(vectors) == 0 {
		return fmt.Errorf("no training vectors")
	}
	for _, v := range vectors {
		if len(v) != sq.dimension {
			return fmt.Errorf("vector dimension %d != expected %d", len(v), sq.dimension)
		}
	}

	sq.mins = make([]float32, sq.dimension)
	maxs := make([]float32, sq.dimension)
	sq.scales = make([]float32, sq.dimension)

	for d := 0; d < sq.dimension; d++ {
		sq.mins[d] = float32(math.MaxFloat32)
		maxs[d] = -float32(math.MaxFloat32)
	}

	for _, vec := range vectors {
		for d := 0; d < sq.dimension; d++ {
			if vec[d] < sq.mins[d] {
				sq.mins[d] = vec[d]
			}
			if vec[d] > maxs[d] {
				maxs[d] = vec[d]
			}
		}
	}

	for d := 0; d < sq.dimension; d++ {
		r := maxs[d] - sq.mins[d]
		if r > 0 {
			sq.scales[d] = 255.0 / r
		} else {
			sq.scales[d] = 1.0
		}
	}

	sq.trained = true
	return nil
}

// Encode maps a float32 vector to uint8 codes.
func (sq *SQQuantizer) Encode(vector []float32) ([]byte, error) {
	if !sq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	if len(vector) != sq.dimension {
		return nil, fmt.Errorf("vector dimension %d != expected %d", len(vector), sq.dimension)
	}

	code := make([]byte, sq.dimension)
	for d := 0; d < sq.dimension; d++ {
		val := (vector[d] - sq.mins[d]) * sq.scales[d]
		if val < 0 {
			val = 0
		}
		if val > 255 {
			val = 255
		}
		code[d] = byte(val + 0.5) // round
	}
	return code, nil
}

// Decode reconstructs an approximate float32 vector from uint8 codes.
func (sq *SQQuantizer) Decode(code []byte) ([]float32, error) {
	if !sq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	if len(code) != sq.dimension {
		return nil, fmt.Errorf("code length %d != expected %d", len(code), sq.dimension)
	}

	vector := make([]float32, sq.dimension)
	for d := 0; d < sq.dimension; d++ {
		vector[d] = float32(code[d])/sq.scales[d] + sq.mins[d]
	}
	return vector, nil
}

// ComputeDistance computes the approximate distance via decode.
func (sq *SQQuantizer) ComputeDistance(query []float32, code []byte) (float32, error) {
	decoded, err := sq.Decode(code)
	if err != nil {
		return 0, err
	}
	return l2Distance(query, decoded), nil
}

// ComputeDistanceTable returns the query vector itself (no table needed for SQ).
func (sq *SQQuantizer) ComputeDistanceTable(query []float32) (interface{}, error) {
	if !sq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	return query, nil
}

// ComputeDistanceWithTable computes distance using the stored query.
func (sq *SQQuantizer) ComputeDistanceWithTable(table interface{}, code []byte) float32 {
	query := table.([]float32)
	decoded, _ := sq.Decode(code)
	return l2Distance(query, decoded)
}

// CodeSize returns the number of bytes per code (== dimension).
func (sq *SQQuantizer) CodeSize() int {
	return sq.dimension
}

// Type returns QuantizationSQ.
func (sq *SQQuantizer) Type() QuantizationType {
	return QuantizationSQ
}

// IsTrained returns whether min/scale have been computed.
func (sq *SQQuantizer) IsTrained() bool {
	return sq.trained
}

// Marshal serializes SQ parameters (min/scale per dimension).
// Format: [4]dim [4]metric [dim*4]mins [dim*4]scales
func (sq *SQQuantizer) Marshal() ([]byte, error) {
	if !sq.trained {
		return nil, fmt.Errorf("quantizer not trained")
	}
	size := 8 + sq.dimension*4*2
	data := make([]byte, size)
	binary.LittleEndian.PutUint32(data[0:4], uint32(sq.dimension))
	binary.LittleEndian.PutUint32(data[4:8], uint32(sq.metricType))

	offset := 8
	for d := 0; d < sq.dimension; d++ {
		binary.LittleEndian.PutUint32(data[offset:], math.Float32bits(sq.mins[d]))
		offset += 4
	}
	for d := 0; d < sq.dimension; d++ {
		binary.LittleEndian.PutUint32(data[offset:], math.Float32bits(sq.scales[d]))
		offset += 4
	}
	return data, nil
}

// Unmarshal deserializes SQ parameters.
func (sq *SQQuantizer) Unmarshal(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("SQ data too short: %d bytes", len(data))
	}
	sq.dimension = int(binary.LittleEndian.Uint32(data[0:4]))
	sq.metricType = MetricType(binary.LittleEndian.Uint32(data[4:8]))

	expected := 8 + sq.dimension*4*2
	if len(data) < expected {
		return fmt.Errorf("SQ data truncated: got %d, expected %d", len(data), expected)
	}

	sq.mins = make([]float32, sq.dimension)
	sq.scales = make([]float32, sq.dimension)

	offset := 8
	for d := 0; d < sq.dimension; d++ {
		sq.mins[d] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
	}
	for d := 0; d < sq.dimension; d++ {
		sq.scales[d] = math.Float32frombits(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
	}

	sq.trained = true
	return nil
}

// Verify interface compliance.
var _ Quantizer = (*SQQuantizer)(nil)
