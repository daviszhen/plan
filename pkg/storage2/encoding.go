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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// ============================================================================
// Physical Encoding Types
// ============================================================================

// EncodingType identifies the physical encoding used for a data page.
type EncodingType uint8

const (
	// EncodingPlain stores values in their native fixed-width binary form.
	EncodingPlain EncodingType = iota
	// EncodingBitPacked packs integer values using the minimum number of bits.
	EncodingBitPacked
	// EncodingRLE uses run-length encoding for repeated values.
	EncodingRLE
	// EncodingDictionary replaces values with dictionary indices.
	EncodingDictionary
	// EncodingVarBinary stores variable-length binary with offset/data layout.
	EncodingVarBinary
)

func (e EncodingType) String() string {
	switch e {
	case EncodingPlain:
		return "plain"
	case EncodingBitPacked:
		return "bitpacked"
	case EncodingRLE:
		return "rle"
	case EncodingDictionary:
		return "dictionary"
	case EncodingVarBinary:
		return "varbinary"
	default:
		return "unknown"
	}
}

// ============================================================================
// Encoded Page — the output of any encoder
// ============================================================================

// EncodedPage is the result of encoding a column (or part of a column).
type EncodedPage struct {
	Encoding    EncodingType
	NumRows     int
	Data        []byte // main data buffer
	Validity    []byte // null bitmap (1 bit per row, LSB first)
	DictData    []byte // dictionary values (only for EncodingDictionary)
	DictEntries int    // number of dictionary entries
	BitWidth    int    // bits per value (for EncodingBitPacked)
}

// ============================================================================
// Physical Encoder Interface
// ============================================================================

// PhysicalEncoder encodes a slice of typed values into a compact binary page.
type PhysicalEncoder interface {
	// Encode encodes the values and returns an EncodedPage.
	Encode(values []int64, numRows int) *EncodedPage
	// EncodingType returns the encoding type this encoder produces.
	EncodingType() EncodingType
}

// ============================================================================
// Plain Encoder
// ============================================================================

// PlainEncoder writes values in their native fixed-width form.
type PlainEncoder struct {
	byteWidth int // bytes per value (1, 2, 4, or 8)
}

// NewPlainEncoder creates a plain encoder for values of the given byte width.
func NewPlainEncoder(byteWidth int) *PlainEncoder {
	return &PlainEncoder{byteWidth: byteWidth}
}

func (e *PlainEncoder) EncodingType() EncodingType { return EncodingPlain }

func (e *PlainEncoder) Encode(values []int64, numRows int) *EncodedPage {
	buf := make([]byte, numRows*e.byteWidth)
	for i := 0; i < numRows; i++ {
		v := values[i]
		off := i * e.byteWidth
		switch e.byteWidth {
		case 1:
			buf[off] = byte(v)
		case 2:
			binary.LittleEndian.PutUint16(buf[off:], uint16(v))
		case 4:
			binary.LittleEndian.PutUint32(buf[off:], uint32(v))
		case 8:
			binary.LittleEndian.PutUint64(buf[off:], uint64(v))
		}
	}
	return &EncodedPage{
		Encoding: EncodingPlain,
		NumRows:  numRows,
		Data:     buf,
	}
}

// ============================================================================
// BitPacking Encoder
// ============================================================================

// BitPackEncoder packs integer values using the minimum number of bits needed
// to represent the range [min, max]. Values are shifted by min so that the
// packed range starts at 0. This is especially effective for columns with a
// small value range (e.g. year, month, status codes).
type BitPackEncoder struct{}

// NewBitPackEncoder creates a new bit-packing encoder.
func NewBitPackEncoder() *BitPackEncoder { return &BitPackEncoder{} }

func (e *BitPackEncoder) EncodingType() EncodingType { return EncodingBitPacked }

func (e *BitPackEncoder) Encode(values []int64, numRows int) *EncodedPage {
	if numRows == 0 {
		return &EncodedPage{Encoding: EncodingBitPacked, NumRows: 0, BitWidth: 0}
	}

	// Find min/max to determine bit width.
	minVal, maxVal := values[0], values[0]
	for i := 1; i < numRows; i++ {
		if values[i] < minVal {
			minVal = values[i]
		}
		if values[i] > maxVal {
			maxVal = values[i]
		}
	}

	rangeVal := uint64(maxVal - minVal)
	bitWidth := 0
	if rangeVal > 0 {
		bitWidth = bits.Len64(rangeVal)
	}
	if bitWidth == 0 {
		bitWidth = 1 // need at least 1 bit even for constant columns
	}

	// Header: 8 bytes minVal + 1 byte bitWidth
	totalBits := numRows * bitWidth
	dataBytes := (totalBits + 7) / 8
	buf := make([]byte, 9+dataBytes)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(minVal))
	buf[8] = byte(bitWidth)

	// Pack values
	data := buf[9:]
	for i := 0; i < numRows; i++ {
		shifted := uint64(values[i] - minVal)
		bitPos := i * bitWidth
		for b := 0; b < bitWidth; b++ {
			if shifted&(1<<b) != 0 {
				byteIdx := (bitPos + b) / 8
				bitIdx := (bitPos + b) % 8
				data[byteIdx] |= 1 << bitIdx
			}
		}
	}

	return &EncodedPage{
		Encoding: EncodingBitPacked,
		NumRows:  numRows,
		Data:     buf,
		BitWidth: bitWidth,
	}
}

// DecodeBitPacked decodes a bit-packed page back to int64 values.
func DecodeBitPacked(page *EncodedPage) []int64 {
	if page.NumRows == 0 || len(page.Data) < 9 {
		return nil
	}

	minVal := int64(binary.LittleEndian.Uint64(page.Data[0:8]))
	bitWidth := int(page.Data[8])
	data := page.Data[9:]
	result := make([]int64, page.NumRows)

	for i := 0; i < page.NumRows; i++ {
		var shifted uint64
		bitPos := i * bitWidth
		for b := 0; b < bitWidth; b++ {
			byteIdx := (bitPos + b) / 8
			bitIdx := (bitPos + b) % 8
			if byteIdx < len(data) && data[byteIdx]&(1<<bitIdx) != 0 {
				shifted |= 1 << b
			}
		}
		result[i] = minVal + int64(shifted)
	}
	return result
}

// ============================================================================
// Run-Length Encoder
// ============================================================================

// RLEEncoder uses run-length encoding: sequences of identical values are stored
// as (count, value) pairs. Effective for sorted columns or columns with many
// repeated values.
type RLEEncoder struct {
	byteWidth int // bytes per value
}

// NewRLEEncoder creates a new RLE encoder.
func NewRLEEncoder(byteWidth int) *RLEEncoder {
	return &RLEEncoder{byteWidth: byteWidth}
}

func (e *RLEEncoder) EncodingType() EncodingType { return EncodingRLE }

func (e *RLEEncoder) Encode(values []int64, numRows int) *EncodedPage {
	if numRows == 0 {
		return &EncodedPage{Encoding: EncodingRLE, NumRows: 0}
	}

	// Build runs
	var buf bytes.Buffer
	// Header: 4 bytes byteWidth + 4 bytes numRuns (written at end)
	header := make([]byte, 8)
	binary.LittleEndian.PutUint32(header[0:4], uint32(e.byteWidth))
	buf.Write(header)

	numRuns := uint32(0)
	i := 0
	for i < numRows {
		runVal := values[i]
		runLen := uint32(1)
		for i+int(runLen) < numRows && values[i+int(runLen)] == runVal {
			runLen++
		}
		// Write run: count(4 bytes) + value(byteWidth bytes)
		var countBuf [4]byte
		binary.LittleEndian.PutUint32(countBuf[:], runLen)
		buf.Write(countBuf[:])

		valBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(valBuf, uint64(runVal))
		buf.Write(valBuf[:e.byteWidth])

		i += int(runLen)
		numRuns++
	}

	data := buf.Bytes()
	// Patch numRuns into header
	binary.LittleEndian.PutUint32(data[4:8], numRuns)

	return &EncodedPage{
		Encoding: EncodingRLE,
		NumRows:  numRows,
		Data:     data,
	}
}

// DecodeRLE decodes an RLE-encoded page back to int64 values.
func DecodeRLE(page *EncodedPage) []int64 {
	if page.NumRows == 0 || len(page.Data) < 8 {
		return nil
	}

	byteWidth := int(binary.LittleEndian.Uint32(page.Data[0:4]))
	numRuns := int(binary.LittleEndian.Uint32(page.Data[4:8]))
	result := make([]int64, 0, page.NumRows)

	offset := 8
	for r := 0; r < numRuns && offset+4+byteWidth <= len(page.Data); r++ {
		runLen := int(binary.LittleEndian.Uint32(page.Data[offset : offset+4]))
		offset += 4

		valBuf := make([]byte, 8)
		copy(valBuf, page.Data[offset:offset+byteWidth])
		val := int64(binary.LittleEndian.Uint64(valBuf))
		// Sign-extend for smaller widths
		switch byteWidth {
		case 1:
			val = int64(int8(valBuf[0]))
		case 2:
			val = int64(int16(binary.LittleEndian.Uint16(valBuf)))
		case 4:
			val = int64(int32(binary.LittleEndian.Uint32(valBuf)))
		}
		offset += byteWidth

		for j := 0; j < runLen; j++ {
			result = append(result, val)
		}
	}
	return result
}

// ============================================================================
// Dictionary Encoder
// ============================================================================

// DictEncoder replaces values with small integer indices into a dictionary.
// Effective when the number of distinct values is much smaller than the number
// of rows (e.g. country codes, status fields).
type DictEncoder struct {
	byteWidth int // bytes per original value
}

// NewDictEncoder creates a new dictionary encoder.
func NewDictEncoder(byteWidth int) *DictEncoder {
	return &DictEncoder{byteWidth: byteWidth}
}

func (e *DictEncoder) EncodingType() EncodingType { return EncodingDictionary }

func (e *DictEncoder) Encode(values []int64, numRows int) *EncodedPage {
	if numRows == 0 {
		return &EncodedPage{Encoding: EncodingDictionary, NumRows: 0}
	}

	// Build dictionary: value -> index
	dict := make(map[int64]int)
	var dictValues []int64
	indices := make([]int64, numRows)

	for i := 0; i < numRows; i++ {
		v := values[i]
		idx, ok := dict[v]
		if !ok {
			idx = len(dictValues)
			dict[v] = idx
			dictValues = append(dictValues, v)
		}
		indices[i] = int64(idx)
	}

	// Encode dictionary as byteWidth * numEntries
	dictBuf := make([]byte, len(dictValues)*e.byteWidth)
	for i, v := range dictValues {
		off := i * e.byteWidth
		valBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(valBuf, uint64(v))
		copy(dictBuf[off:off+e.byteWidth], valBuf[:e.byteWidth])
	}

	// Encode indices using bit-packing for compactness
	idxBitWidth := 1
	if len(dictValues) > 1 {
		idxBitWidth = bits.Len(uint(len(dictValues) - 1))
	}

	totalBits := numRows * idxBitWidth
	idxBytes := (totalBits + 7) / 8

	// Data layout: 4 bytes dictSize + 1 byte idxBitWidth + packed indices
	dataBuf := make([]byte, 5+idxBytes)
	binary.LittleEndian.PutUint32(dataBuf[0:4], uint32(len(dictValues)))
	dataBuf[4] = byte(idxBitWidth)

	idxData := dataBuf[5:]
	for i := 0; i < numRows; i++ {
		idx := uint64(indices[i])
		bitPos := i * idxBitWidth
		for b := 0; b < idxBitWidth; b++ {
			if idx&(1<<b) != 0 {
				byteIdx := (bitPos + b) / 8
				bitIdx := (bitPos + b) % 8
				idxData[byteIdx] |= 1 << bitIdx
			}
		}
	}

	return &EncodedPage{
		Encoding:    EncodingDictionary,
		NumRows:     numRows,
		Data:        dataBuf,
		DictData:    dictBuf,
		DictEntries: len(dictValues),
		BitWidth:    idxBitWidth,
	}
}

// DecodeDictionary decodes a dictionary-encoded page back to int64 values.
func DecodeDictionary(page *EncodedPage, byteWidth int) []int64 {
	if page.NumRows == 0 || len(page.Data) < 5 {
		return nil
	}

	dictSize := int(binary.LittleEndian.Uint32(page.Data[0:4]))
	idxBitWidth := int(page.Data[4])
	idxData := page.Data[5:]

	// Decode dictionary
	dictValues := make([]int64, dictSize)
	for i := 0; i < dictSize; i++ {
		off := i * byteWidth
		if off+byteWidth > len(page.DictData) {
			break
		}
		valBuf := make([]byte, 8)
		copy(valBuf, page.DictData[off:off+byteWidth])
		switch byteWidth {
		case 1:
			dictValues[i] = int64(int8(valBuf[0]))
		case 2:
			dictValues[i] = int64(int16(binary.LittleEndian.Uint16(valBuf)))
		case 4:
			dictValues[i] = int64(int32(binary.LittleEndian.Uint32(valBuf)))
		default:
			dictValues[i] = int64(binary.LittleEndian.Uint64(valBuf))
		}
	}

	// Decode indices
	result := make([]int64, page.NumRows)
	for i := 0; i < page.NumRows; i++ {
		var idx uint64
		bitPos := i * idxBitWidth
		for b := 0; b < idxBitWidth; b++ {
			byteIdx := (bitPos + b) / 8
			bitIdx := (bitPos + b) % 8
			if byteIdx < len(idxData) && idxData[byteIdx]&(1<<bitIdx) != 0 {
				idx |= 1 << b
			}
		}
		if int(idx) < dictSize {
			result[i] = dictValues[idx]
		}
	}
	return result
}

// ============================================================================
// VarBinary Encoder — for variable-length data (strings, blobs)
// ============================================================================

// VarBinaryEncoder encodes variable-length byte sequences as offsets + data.
type VarBinaryEncoder struct{}

// NewVarBinaryEncoder creates a new variable-length binary encoder.
func NewVarBinaryEncoder() *VarBinaryEncoder { return &VarBinaryEncoder{} }

// EncodeStrings encodes a slice of strings into an EncodedPage.
func (e *VarBinaryEncoder) EncodeStrings(values []string, validity []bool) *EncodedPage {
	numRows := len(values)
	if numRows == 0 {
		return &EncodedPage{Encoding: EncodingVarBinary, NumRows: 0}
	}

	// Build offsets and concatenated data
	offsets := make([]uint32, numRows+1)
	var dataBuf bytes.Buffer
	offset := uint32(0)

	for i := 0; i < numRows; i++ {
		offsets[i] = offset
		if i < len(validity) && validity[i] {
			dataBuf.WriteString(values[i])
			offset += uint32(len(values[i]))
		}
	}
	offsets[numRows] = offset

	// Encode: offsets array + data
	var buf bytes.Buffer
	for _, off := range offsets {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], off)
		buf.Write(b[:])
	}
	buf.Write(dataBuf.Bytes())

	// Build validity bitmap
	validityBitmap := makeValidityBitmap(validity, numRows)

	return &EncodedPage{
		Encoding: EncodingVarBinary,
		NumRows:  numRows,
		Data:     buf.Bytes(),
		Validity: validityBitmap,
	}
}

// DecodeVarBinary decodes a VarBinary-encoded page back to strings.
func DecodeVarBinary(page *EncodedPage) ([]string, []bool) {
	if page.NumRows == 0 {
		return nil, nil
	}

	numRows := page.NumRows
	offsetBytes := (numRows + 1) * 4
	if len(page.Data) < offsetBytes {
		return nil, nil
	}

	// Read offsets
	offsets := make([]uint32, numRows+1)
	for i := 0; i <= numRows; i++ {
		offsets[i] = binary.LittleEndian.Uint32(page.Data[i*4:])
	}

	strData := page.Data[offsetBytes:]
	values := make([]string, numRows)
	validity := decodeValidityBitmap(page.Validity, numRows)

	for i := 0; i < numRows; i++ {
		if validity[i] {
			start := offsets[i]
			end := offsets[i+1]
			if int(end) <= len(strData) {
				values[i] = string(strData[start:end])
			}
		}
	}

	return values, validity
}

// ============================================================================
// String Dictionary Encoder — dictionary encoding for string columns
// ============================================================================

// StringDictEncoder replaces string values with indices into a dictionary.
type StringDictEncoder struct{}

// NewStringDictEncoder creates a new string dictionary encoder.
func NewStringDictEncoder() *StringDictEncoder { return &StringDictEncoder{} }

// EncodeStrings encodes strings using dictionary encoding.
func (e *StringDictEncoder) EncodeStrings(values []string, validity []bool) *EncodedPage {
	numRows := len(values)
	if numRows == 0 {
		return &EncodedPage{Encoding: EncodingDictionary, NumRows: 0}
	}

	// Build dictionary
	dict := make(map[string]int)
	var dictValues []string
	indices := make([]int64, numRows)

	for i := 0; i < numRows; i++ {
		if i < len(validity) && !validity[i] {
			indices[i] = 0 // null values get index 0
			continue
		}
		v := values[i]
		idx, ok := dict[v]
		if !ok {
			idx = len(dictValues)
			dict[v] = idx
			dictValues = append(dictValues, v)
		}
		indices[i] = int64(idx)
	}

	// Encode dictionary values as VarBinary
	var dictBuf bytes.Buffer
	dictOffsets := make([]uint32, len(dictValues)+1)
	dictOffset := uint32(0)
	for i, v := range dictValues {
		dictOffsets[i] = dictOffset
		dictBuf.WriteString(v)
		dictOffset += uint32(len(v))
	}
	dictOffsets[len(dictValues)] = dictOffset

	// Write dict: numEntries(4) + offsets + data
	var dictData bytes.Buffer
	var entryCountBuf [4]byte
	binary.LittleEndian.PutUint32(entryCountBuf[:], uint32(len(dictValues)))
	dictData.Write(entryCountBuf[:])
	for _, off := range dictOffsets {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], off)
		dictData.Write(b[:])
	}
	dictData.Write(dictBuf.Bytes())

	// Encode indices with bit-packing
	idxBitWidth := 1
	if len(dictValues) > 1 {
		idxBitWidth = bits.Len(uint(len(dictValues) - 1))
	}
	totalBits := numRows * idxBitWidth
	idxBytes := (totalBits + 7) / 8

	dataBuf := make([]byte, 1+idxBytes) // 1 byte bitWidth + packed indices
	dataBuf[0] = byte(idxBitWidth)
	idxData := dataBuf[1:]
	for i := 0; i < numRows; i++ {
		idx := uint64(indices[i])
		bitPos := i * idxBitWidth
		for b := 0; b < idxBitWidth; b++ {
			if idx&(1<<b) != 0 {
				byteIdx := (bitPos + b) / 8
				bitIdx := (bitPos + b) % 8
				idxData[byteIdx] |= 1 << bitIdx
			}
		}
	}

	validityBitmap := makeValidityBitmap(validity, numRows)

	return &EncodedPage{
		Encoding:    EncodingDictionary,
		NumRows:     numRows,
		Data:        dataBuf,
		DictData:    dictData.Bytes(),
		DictEntries: len(dictValues),
		BitWidth:    idxBitWidth,
		Validity:    validityBitmap,
	}
}

// DecodeStringDictionary decodes a dictionary-encoded string page.
func DecodeStringDictionary(page *EncodedPage) ([]string, []bool) {
	if page.NumRows == 0 || len(page.Data) < 1 || len(page.DictData) < 4 {
		return nil, nil
	}

	// Decode dictionary
	numEntries := int(binary.LittleEndian.Uint32(page.DictData[0:4]))
	offsetStart := 4
	offsetEnd := offsetStart + (numEntries+1)*4
	if offsetEnd > len(page.DictData) {
		return nil, nil
	}

	dictOffsets := make([]uint32, numEntries+1)
	for i := 0; i <= numEntries; i++ {
		dictOffsets[i] = binary.LittleEndian.Uint32(page.DictData[offsetStart+i*4:])
	}
	dictStrData := page.DictData[offsetEnd:]

	dictValues := make([]string, numEntries)
	for i := 0; i < numEntries; i++ {
		start := dictOffsets[i]
		end := dictOffsets[i+1]
		if int(end) <= len(dictStrData) {
			dictValues[i] = string(dictStrData[start:end])
		}
	}

	// Decode indices
	idxBitWidth := int(page.Data[0])
	idxData := page.Data[1:]

	values := make([]string, page.NumRows)
	validity := decodeValidityBitmap(page.Validity, page.NumRows)

	for i := 0; i < page.NumRows; i++ {
		if !validity[i] {
			continue
		}
		var idx uint64
		bitPos := i * idxBitWidth
		for b := 0; b < idxBitWidth; b++ {
			byteIdx := (bitPos + b) / 8
			bitIdx := (bitPos + b) % 8
			if byteIdx < len(idxData) && idxData[byteIdx]&(1<<bitIdx) != 0 {
				idx |= 1 << b
			}
		}
		if int(idx) < numEntries {
			values[i] = dictValues[idx]
		}
	}

	return values, validity
}

// ============================================================================
// Validity Bitmap Helpers
// ============================================================================

func makeValidityBitmap(validity []bool, numRows int) []byte {
	bitmapSize := (numRows + 7) / 8
	bitmap := make([]byte, bitmapSize)
	for i := 0; i < numRows; i++ {
		if i < len(validity) && validity[i] {
			bitmap[i/8] |= 1 << (i % 8)
		} else if i >= len(validity) {
			// Default to valid if validity slice is shorter
			bitmap[i/8] |= 1 << (i % 8)
		}
	}
	return bitmap
}

func decodeValidityBitmap(bitmap []byte, numRows int) []bool {
	validity := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		if i/8 < len(bitmap) {
			validity[i] = bitmap[i/8]&(1<<(i%8)) != 0
		} else {
			validity[i] = true // default valid
		}
	}
	return validity
}

// ============================================================================
// Logical Encoder — type-aware column encoder that selects physical encoding
// ============================================================================

// ColumnEncodingStats holds statistics used for encoding selection.
type ColumnEncodingStats struct {
	NumRows       int
	NumNulls      int
	NumDistinct   int
	MinVal        int64
	MaxVal        int64
	NumRuns       int // number of runs for RLE analysis
	AvgStringLen  float64
	TotalDataSize int64
}

// AnalyzeIntColumn computes encoding stats for an integer column.
func AnalyzeIntColumn(vec *chunk.Vector, numRows int) ColumnEncodingStats {
	stats := ColumnEncodingStats{NumRows: numRows}
	distinct := make(map[int64]struct{})
	first := true

	runCount := 0
	var lastVal int64

	for i := 0; i < numRows; i++ {
		if !vec.Mask.RowIsValid(uint64(i)) {
			stats.NumNulls++
			continue
		}
		val := vec.GetValue(i)
		v := val.I64

		if first {
			stats.MinVal = v
			stats.MaxVal = v
			first = false
		} else {
			if v < stats.MinVal {
				stats.MinVal = v
			}
			if v > stats.MaxVal {
				stats.MaxVal = v
			}
		}

		distinct[v] = struct{}{}

		if i == 0 || v != lastVal {
			runCount++
		}
		lastVal = v
	}
	stats.NumDistinct = len(distinct)
	stats.NumRuns = runCount
	return stats
}

// AnalyzeStringColumn computes encoding stats for a string column.
func AnalyzeStringColumn(vec *chunk.Vector, numRows int) ColumnEncodingStats {
	stats := ColumnEncodingStats{NumRows: numRows}
	distinct := make(map[string]struct{})
	totalLen := 0

	for i := 0; i < numRows; i++ {
		if !vec.Mask.RowIsValid(uint64(i)) {
			stats.NumNulls++
			continue
		}
		val := vec.GetValue(i)
		distinct[val.Str] = struct{}{}
		totalLen += len(val.Str)
	}
	stats.NumDistinct = len(distinct)
	validRows := numRows - stats.NumNulls
	if validRows > 0 {
		stats.AvgStringLen = float64(totalLen) / float64(validRows)
	}
	stats.TotalDataSize = int64(totalLen)
	return stats
}

// SelectIntEncoding chooses the best physical encoding for an integer column.
func SelectIntEncoding(stats ColumnEncodingStats, byteWidth int) EncodingType {
	validRows := stats.NumRows - stats.NumNulls
	if validRows == 0 {
		return EncodingPlain
	}

	// RLE: if average run length > 4, RLE wins
	if stats.NumRuns > 0 {
		avgRunLen := float64(validRows) / float64(stats.NumRuns)
		if avgRunLen > 4.0 {
			return EncodingRLE
		}
	}

	// Dictionary: if distinct/total ratio is low (< 10%) and dictionary fits in reasonable size
	if stats.NumDistinct > 0 && stats.NumDistinct < 256 {
		dictRatio := float64(stats.NumDistinct) / float64(validRows)
		if dictRatio < 0.1 {
			return EncodingDictionary
		}
	}

	// BitPacking: if value range is small enough to save space
	rangeVal := uint64(stats.MaxVal - stats.MinVal)
	bitsNeeded := 1
	if rangeVal > 0 {
		bitsNeeded = bits.Len64(rangeVal)
	}
	plainBits := byteWidth * 8
	if bitsNeeded < plainBits/2 { // save at least 50% space
		return EncodingBitPacked
	}

	return EncodingPlain
}

// SelectStringEncoding chooses the best encoding for a string column.
func SelectStringEncoding(stats ColumnEncodingStats) EncodingType {
	validRows := stats.NumRows - stats.NumNulls
	if validRows == 0 {
		return EncodingVarBinary
	}

	// Dictionary: effective when few distinct values relative to row count
	if stats.NumDistinct > 0 && stats.NumDistinct < 4096 {
		dictRatio := float64(stats.NumDistinct) / float64(validRows)
		if dictRatio < 0.5 {
			return EncodingDictionary
		}
	}

	return EncodingVarBinary
}

// ============================================================================
// LogicalColumnEncoder — encodes a chunk.Vector using the best encoding
// ============================================================================

// LogicalColumnEncoder encodes a single column from a chunk.Vector,
// automatically selecting the best physical encoding.
type LogicalColumnEncoder struct {
	autoSelect bool // whether to auto-select encoding
	forceEnc   EncodingType
}

// NewLogicalColumnEncoder creates a logical encoder with automatic encoding selection.
func NewLogicalColumnEncoder() *LogicalColumnEncoder {
	return &LogicalColumnEncoder{autoSelect: true}
}

// NewLogicalColumnEncoderWithEncoding creates a logical encoder that forces a specific encoding.
func NewLogicalColumnEncoderWithEncoding(enc EncodingType) *LogicalColumnEncoder {
	return &LogicalColumnEncoder{autoSelect: false, forceEnc: enc}
}

// EncodeColumn encodes a column vector into an EncodedPage.
func (e *LogicalColumnEncoder) EncodeColumn(vec *chunk.Vector, numRows int) (*EncodedPage, error) {
	typ := vec.Typ()

	switch typ.Id {
	case common.LTID_BOOLEAN:
		return e.encodeBooleanColumn(vec, numRows)
	case common.LTID_TINYINT, common.LTID_UTINYINT:
		return e.encodeIntColumn(vec, numRows, 1)
	case common.LTID_SMALLINT, common.LTID_USMALLINT:
		return e.encodeIntColumn(vec, numRows, 2)
	case common.LTID_INTEGER, common.LTID_UINTEGER, common.LTID_DATE:
		return e.encodeIntColumn(vec, numRows, 4)
	case common.LTID_BIGINT, common.LTID_UBIGINT, common.LTID_TIMESTAMP:
		return e.encodeIntColumn(vec, numRows, 8)
	case common.LTID_FLOAT:
		return e.encodeFloatColumn(vec, numRows)
	case common.LTID_DOUBLE:
		return e.encodeDoubleColumn(vec, numRows)
	case common.LTID_VARCHAR, common.LTID_CHAR, common.LTID_BLOB:
		return e.encodeStringColumn(vec, numRows)
	case common.LTID_DECIMAL:
		return e.encodeIntColumn(vec, numRows, 8) // decimal stored as int64
	default:
		return e.encodeIntColumn(vec, numRows, 8) // fallback
	}
}

func (e *LogicalColumnEncoder) encodeBooleanColumn(vec *chunk.Vector, numRows int) (*EncodedPage, error) {
	// Boolean: pack as 1 bit per value
	values := make([]int64, numRows)
	validity := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		if vec.Mask.RowIsValid(uint64(i)) {
			validity[i] = true
			val := vec.GetValue(i)
			if val.Bool {
				values[i] = 1
			} else {
				values[i] = 0
			}
		}
	}

	// Always use bit-packing for booleans (1 bit per value)
	encoder := NewBitPackEncoder()
	page := encoder.Encode(values, numRows)
	page.Validity = makeValidityBitmap(validity, numRows)
	return page, nil
}

func (e *LogicalColumnEncoder) encodeIntColumn(vec *chunk.Vector, numRows int, byteWidth int) (*EncodedPage, error) {
	values := make([]int64, numRows)
	validity := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		if vec.Mask.RowIsValid(uint64(i)) {
			validity[i] = true
			val := vec.GetValue(i)
			values[i] = val.I64
		}
	}

	var enc EncodingType
	if e.autoSelect {
		stats := AnalyzeIntColumn(vec, numRows)
		enc = SelectIntEncoding(stats, byteWidth)
	} else {
		enc = e.forceEnc
	}

	var page *EncodedPage
	switch enc {
	case EncodingBitPacked:
		encoder := NewBitPackEncoder()
		page = encoder.Encode(values, numRows)
	case EncodingRLE:
		encoder := NewRLEEncoder(byteWidth)
		page = encoder.Encode(values, numRows)
	case EncodingDictionary:
		encoder := NewDictEncoder(byteWidth)
		page = encoder.Encode(values, numRows)
	default:
		encoder := NewPlainEncoder(byteWidth)
		page = encoder.Encode(values, numRows)
	}

	page.Validity = makeValidityBitmap(validity, numRows)
	return page, nil
}

func (e *LogicalColumnEncoder) encodeFloatColumn(vec *chunk.Vector, numRows int) (*EncodedPage, error) {
	// Float: always plain encoding (bit manipulation not useful for floats)
	buf := make([]byte, numRows*4)
	validity := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		if vec.Mask.RowIsValid(uint64(i)) {
			validity[i] = true
			val := vec.GetValue(i)
			bits := math.Float32bits(float32(val.F64))
			binary.LittleEndian.PutUint32(buf[i*4:], bits)
		}
	}
	page := &EncodedPage{
		Encoding: EncodingPlain,
		NumRows:  numRows,
		Data:     buf,
		Validity: makeValidityBitmap(validity, numRows),
	}
	return page, nil
}

func (e *LogicalColumnEncoder) encodeDoubleColumn(vec *chunk.Vector, numRows int) (*EncodedPage, error) {
	buf := make([]byte, numRows*8)
	validity := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		if vec.Mask.RowIsValid(uint64(i)) {
			validity[i] = true
			val := vec.GetValue(i)
			bits := math.Float64bits(val.F64)
			binary.LittleEndian.PutUint64(buf[i*8:], bits)
		}
	}
	page := &EncodedPage{
		Encoding: EncodingPlain,
		NumRows:  numRows,
		Data:     buf,
		Validity: makeValidityBitmap(validity, numRows),
	}
	return page, nil
}

func (e *LogicalColumnEncoder) encodeStringColumn(vec *chunk.Vector, numRows int) (*EncodedPage, error) {
	values := make([]string, numRows)
	validity := make([]bool, numRows)
	for i := 0; i < numRows; i++ {
		if vec.Mask.RowIsValid(uint64(i)) {
			validity[i] = true
			val := vec.GetValue(i)
			values[i] = val.Str
		}
	}

	var enc EncodingType
	if e.autoSelect {
		stats := AnalyzeStringColumn(vec, numRows)
		enc = SelectStringEncoding(stats)
	} else {
		enc = e.forceEnc
	}

	switch enc {
	case EncodingDictionary:
		encoder := NewStringDictEncoder()
		return encoder.EncodeStrings(values, validity), nil
	default:
		encoder := NewVarBinaryEncoder()
		return encoder.EncodeStrings(values, validity), nil
	}
}

// ============================================================================
// LogicalColumnDecoder — decodes an EncodedPage back to chunk.Vector
// ============================================================================

// LogicalColumnDecoder decodes an EncodedPage and populates a chunk.Vector.
type LogicalColumnDecoder struct{}

// NewLogicalColumnDecoder creates a new logical column decoder.
func NewLogicalColumnDecoder() *LogicalColumnDecoder {
	return &LogicalColumnDecoder{}
}

// DecodeColumn decodes an EncodedPage into a Vector.
func (d *LogicalColumnDecoder) DecodeColumn(page *EncodedPage, vec *chunk.Vector, typ common.LType) error {
	switch typ.Id {
	case common.LTID_BOOLEAN:
		return d.decodeBooleanColumn(page, vec)
	case common.LTID_TINYINT, common.LTID_UTINYINT:
		return d.decodeIntColumn(page, vec, typ, 1)
	case common.LTID_SMALLINT, common.LTID_USMALLINT:
		return d.decodeIntColumn(page, vec, typ, 2)
	case common.LTID_INTEGER, common.LTID_UINTEGER, common.LTID_DATE:
		return d.decodeIntColumn(page, vec, typ, 4)
	case common.LTID_BIGINT, common.LTID_UBIGINT, common.LTID_TIMESTAMP:
		return d.decodeIntColumn(page, vec, typ, 8)
	case common.LTID_FLOAT:
		return d.decodeFloatColumn(page, vec, typ)
	case common.LTID_DOUBLE:
		return d.decodeDoubleColumn(page, vec, typ)
	case common.LTID_VARCHAR, common.LTID_CHAR, common.LTID_BLOB:
		return d.decodeStringColumn(page, vec, typ)
	case common.LTID_DECIMAL:
		return d.decodeIntColumn(page, vec, typ, 8)
	default:
		return d.decodeIntColumn(page, vec, typ, 8)
	}
}

func (d *LogicalColumnDecoder) decodeBooleanColumn(page *EncodedPage, vec *chunk.Vector) error {
	values := DecodeBitPacked(page)
	validity := decodeValidityBitmap(page.Validity, page.NumRows)
	typ := vec.Typ()

	for i := 0; i < page.NumRows; i++ {
		if !validity[i] {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else if i < len(values) {
			vec.SetValue(i, &chunk.Value{Typ: typ, Bool: values[i] != 0})
		}
	}
	return nil
}

func (d *LogicalColumnDecoder) decodeIntColumn(page *EncodedPage, vec *chunk.Vector, typ common.LType, byteWidth int) error {
	var values []int64

	switch page.Encoding {
	case EncodingBitPacked:
		values = DecodeBitPacked(page)
	case EncodingRLE:
		values = DecodeRLE(page)
	case EncodingDictionary:
		values = DecodeDictionary(page, byteWidth)
	case EncodingPlain:
		values = decodePlain(page.Data, byteWidth, page.NumRows)
	default:
		return fmt.Errorf("unsupported encoding %s for int column", page.Encoding)
	}

	validity := decodeValidityBitmap(page.Validity, page.NumRows)
	for i := 0; i < page.NumRows; i++ {
		if !validity[i] {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else if i < len(values) {
			vec.SetValue(i, &chunk.Value{Typ: typ, I64: values[i]})
		}
	}
	return nil
}

func (d *LogicalColumnDecoder) decodeFloatColumn(page *EncodedPage, vec *chunk.Vector, typ common.LType) error {
	validity := decodeValidityBitmap(page.Validity, page.NumRows)
	for i := 0; i < page.NumRows; i++ {
		if !validity[i] {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else {
			off := i * 4
			if off+4 <= len(page.Data) {
				bits := binary.LittleEndian.Uint32(page.Data[off:])
				vec.SetValue(i, &chunk.Value{Typ: typ, F64: float64(math.Float32frombits(bits))})
			}
		}
	}
	return nil
}

func (d *LogicalColumnDecoder) decodeDoubleColumn(page *EncodedPage, vec *chunk.Vector, typ common.LType) error {
	validity := decodeValidityBitmap(page.Validity, page.NumRows)
	for i := 0; i < page.NumRows; i++ {
		if !validity[i] {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else {
			off := i * 8
			if off+8 <= len(page.Data) {
				bits := binary.LittleEndian.Uint64(page.Data[off:])
				vec.SetValue(i, &chunk.Value{Typ: typ, F64: math.Float64frombits(bits)})
			}
		}
	}
	return nil
}

func (d *LogicalColumnDecoder) decodeStringColumn(page *EncodedPage, vec *chunk.Vector, typ common.LType) error {
	switch page.Encoding {
	case EncodingDictionary:
		values, validity := DecodeStringDictionary(page)
		for i := 0; i < page.NumRows; i++ {
			if i < len(validity) && !validity[i] {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			} else if i < len(values) {
				vec.SetValue(i, &chunk.Value{Typ: typ, Str: values[i]})
			}
		}
	case EncodingVarBinary:
		values, validity := DecodeVarBinary(page)
		for i := 0; i < page.NumRows; i++ {
			if i < len(validity) && !validity[i] {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			} else if i < len(values) {
				vec.SetValue(i, &chunk.Value{Typ: typ, Str: values[i]})
			}
		}
	default:
		return fmt.Errorf("unsupported encoding %s for string column", page.Encoding)
	}
	return nil
}

// decodePlain decodes plain-encoded int64 values.
func decodePlain(data []byte, byteWidth, numRows int) []int64 {
	result := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		off := i * byteWidth
		if off+byteWidth > len(data) {
			break
		}
		switch byteWidth {
		case 1:
			result[i] = int64(int8(data[off]))
		case 2:
			result[i] = int64(int16(binary.LittleEndian.Uint16(data[off:])))
		case 4:
			result[i] = int64(int32(binary.LittleEndian.Uint32(data[off:])))
		case 8:
			result[i] = int64(binary.LittleEndian.Uint64(data[off:]))
		}
	}
	return result
}

// ============================================================================
// EncodingProfile — measures compression effectiveness
// ============================================================================

// EncodingProfile contains metrics about an encoding result.
type EncodingProfile struct {
	Encoding       EncodingType
	OriginalSize   int64 // uncompressed size in bytes
	CompressedSize int64 // compressed size in bytes
	Ratio          float64
	NumRows        int
}

// ProfileEncoding measures the compression ratio of encoding a column.
func ProfileEncoding(vec *chunk.Vector, numRows int) []EncodingProfile {
	typ := vec.Typ()
	var profiles []EncodingProfile

	encodings := []EncodingType{EncodingPlain}

	switch typ.Id {
	case common.LTID_TINYINT, common.LTID_UTINYINT,
		common.LTID_SMALLINT, common.LTID_USMALLINT,
		common.LTID_INTEGER, common.LTID_UINTEGER,
		common.LTID_BIGINT, common.LTID_UBIGINT,
		common.LTID_DATE, common.LTID_TIMESTAMP:
		encodings = append(encodings, EncodingBitPacked, EncodingRLE, EncodingDictionary)
	case common.LTID_VARCHAR, common.LTID_CHAR, common.LTID_BLOB:
		encodings = append(encodings, EncodingDictionary)
	}

	for _, enc := range encodings {
		encoder := NewLogicalColumnEncoderWithEncoding(enc)
		page, err := encoder.EncodeColumn(vec, numRows)
		if err != nil {
			continue
		}

		origSize := estimateOriginalSize(typ, numRows)
		compressedSize := int64(len(page.Data) + len(page.DictData) + len(page.Validity))
		ratio := 0.0
		if origSize > 0 {
			ratio = float64(compressedSize) / float64(origSize)
		}

		profiles = append(profiles, EncodingProfile{
			Encoding:       enc,
			OriginalSize:   origSize,
			CompressedSize: compressedSize,
			Ratio:          ratio,
			NumRows:        numRows,
		})
	}

	// Sort by compression ratio (best first)
	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].Ratio < profiles[j].Ratio
	})

	return profiles
}

func estimateOriginalSize(typ common.LType, numRows int) int64 {
	switch typ.Id {
	case common.LTID_BOOLEAN, common.LTID_TINYINT, common.LTID_UTINYINT:
		return int64(numRows)
	case common.LTID_SMALLINT, common.LTID_USMALLINT:
		return int64(numRows * 2)
	case common.LTID_INTEGER, common.LTID_UINTEGER, common.LTID_FLOAT, common.LTID_DATE:
		return int64(numRows * 4)
	case common.LTID_BIGINT, common.LTID_UBIGINT, common.LTID_DOUBLE, common.LTID_TIMESTAMP:
		return int64(numRows * 8)
	default:
		return int64(numRows * 8) // conservative estimate
	}
}
