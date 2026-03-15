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
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// ============================================================================
// PlainEncoder Tests
// ============================================================================

func TestPlainEncoderByteWidth1(t *testing.T) {
	values := []int64{0, 1, 127, -128, 50, -1}
	encoder := NewPlainEncoder(1)
	page := encoder.Encode(values, len(values))

	if page.Encoding != EncodingPlain {
		t.Errorf("expected EncodingPlain, got %v", page.Encoding)
	}
	if page.NumRows != len(values) {
		t.Errorf("expected NumRows=%d, got %d", len(values), page.NumRows)
	}
	if len(page.Data) != len(values)*1 {
		t.Errorf("expected data size %d, got %d", len(values)*1, len(page.Data))
	}

	// Verify roundtrip
	decoded := decodePlain(page.Data, 1, page.NumRows)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestPlainEncoderByteWidth2(t *testing.T) {
	values := []int64{0, 1000, 32767, -32768, -500}
	encoder := NewPlainEncoder(2)
	page := encoder.Encode(values, len(values))

	if len(page.Data) != len(values)*2 {
		t.Errorf("expected data size %d, got %d", len(values)*2, len(page.Data))
	}

	decoded := decodePlain(page.Data, 2, page.NumRows)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestPlainEncoderByteWidth4(t *testing.T) {
	values := []int64{0, 100000, 2147483647, -2147483648, -99999}
	encoder := NewPlainEncoder(4)
	page := encoder.Encode(values, len(values))

	if len(page.Data) != len(values)*4 {
		t.Errorf("expected data size %d, got %d", len(values)*4, len(page.Data))
	}

	decoded := decodePlain(page.Data, 4, page.NumRows)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestPlainEncoderByteWidth8(t *testing.T) {
	values := []int64{0, 9223372036854775807, -9223372036854775808, 123456789012345}
	encoder := NewPlainEncoder(8)
	page := encoder.Encode(values, len(values))

	if len(page.Data) != len(values)*8 {
		t.Errorf("expected data size %d, got %d", len(values)*8, len(page.Data))
	}

	decoded := decodePlain(page.Data, 8, page.NumRows)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

// ============================================================================
// BitPackEncoder Tests
// ============================================================================

func TestBitPackEncoderEmpty(t *testing.T) {
	encoder := NewBitPackEncoder()
	page := encoder.Encode([]int64{}, 0)

	if page.NumRows != 0 {
		t.Errorf("expected NumRows=0, got %d", page.NumRows)
	}
}

func TestBitPackEncoderConstant(t *testing.T) {
	values := []int64{42, 42, 42, 42, 42}
	encoder := NewBitPackEncoder()
	page := encoder.Encode(values, len(values))

	if page.BitWidth != 1 {
		t.Errorf("expected BitWidth=1 for constant, got %d", page.BitWidth)
	}

	decoded := DecodeBitPacked(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestBitPackEncoderSmallRange(t *testing.T) {
	// Values 0-15 need 4 bits
	values := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	encoder := NewBitPackEncoder()
	page := encoder.Encode(values, len(values))

	if page.BitWidth != 4 {
		t.Errorf("expected BitWidth=4, got %d", page.BitWidth)
	}

	decoded := DecodeBitPacked(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestBitPackEncoderWithOffset(t *testing.T) {
	// Values 1000-1015: range is 15, needs 4 bits
	values := []int64{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007,
		1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015}
	encoder := NewBitPackEncoder()
	page := encoder.Encode(values, len(values))

	if page.BitWidth != 4 {
		t.Errorf("expected BitWidth=4, got %d", page.BitWidth)
	}

	decoded := DecodeBitPacked(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestBitPackEncoderNegative(t *testing.T) {
	// Test with negative values
	values := []int64{-10, -5, 0, 5, 10}
	encoder := NewBitPackEncoder()
	page := encoder.Encode(values, len(values))

	decoded := DecodeBitPacked(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestBitPackEncoderCompressionRatio(t *testing.T) {
	// 1000 values in range 0-255 (8 bits each)
	numRows := 1000
	values := make([]int64, numRows)
	for i := 0; i < numRows; i++ {
		values[i] = int64(i % 256)
	}

	encoder := NewBitPackEncoder()
	page := encoder.Encode(values, numRows)

	// Plain encoding would be 8 bytes per value = 8000 bytes
	// Bit-packed should be 8 bits per value = 1000 bytes + 9 byte header
	plainSize := numRows * 8
	bitPackedSize := len(page.Data)

	if bitPackedSize >= plainSize {
		t.Errorf("bit-packed size %d should be less than plain size %d", bitPackedSize, plainSize)
	}

	// Verify roundtrip
	decoded := DecodeBitPacked(page)
	for i := 0; i < numRows; i++ {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
			break
		}
	}
}

// ============================================================================
// RLEEncoder Tests
// ============================================================================

func TestRLEEncoderEmpty(t *testing.T) {
	encoder := NewRLEEncoder(4)
	page := encoder.Encode([]int64{}, 0)

	if page.NumRows != 0 {
		t.Errorf("expected NumRows=0, got %d", page.NumRows)
	}
}

func TestRLEEncoderSingleRun(t *testing.T) {
	values := []int64{42, 42, 42, 42, 42}
	encoder := NewRLEEncoder(4)
	page := encoder.Encode(values, len(values))

	decoded := DecodeRLE(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestRLEEncoderMultipleRuns(t *testing.T) {
	// 5 runs of different lengths
	values := []int64{
		1, 1, 1, // run 1: 3x
		2, 2, // run 2: 2x
		3, 3, 3, 3, // run 3: 4x
		4,       // run 4: 1x
		5, 5, 5, // run 5: 3x
	}
	encoder := NewRLEEncoder(4)
	page := encoder.Encode(values, len(values))

	decoded := DecodeRLE(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestRLEEncoderAlternating(t *testing.T) {
	// Worst case for RLE: alternating values
	values := []int64{1, 2, 1, 2, 1, 2, 1, 2}
	encoder := NewRLEEncoder(4)
	page := encoder.Encode(values, len(values))

	decoded := DecodeRLE(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestRLEEncoderNegative(t *testing.T) {
	values := []int64{-1, -1, -1, -5, -5, 0, 0, 0, 0, 0}
	encoder := NewRLEEncoder(4)
	page := encoder.Encode(values, len(values))

	decoded := DecodeRLE(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestRLEEncoderByteWidth1(t *testing.T) {
	values := []int64{10, 10, 10, 20, 20, 30}
	encoder := NewRLEEncoder(1)
	page := encoder.Encode(values, len(values))

	decoded := DecodeRLE(page)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

// ============================================================================
// DictEncoder Tests
// ============================================================================

func TestDictEncoderEmpty(t *testing.T) {
	encoder := NewDictEncoder(4)
	page := encoder.Encode([]int64{}, 0)

	if page.NumRows != 0 {
		t.Errorf("expected NumRows=0, got %d", page.NumRows)
	}
}

func TestDictEncoderSingleValue(t *testing.T) {
	values := []int64{42, 42, 42, 42}
	encoder := NewDictEncoder(4)
	page := encoder.Encode(values, len(values))

	if page.DictEntries != 1 {
		t.Errorf("expected 1 dictionary entry, got %d", page.DictEntries)
	}

	decoded := DecodeDictionary(page, 4)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestDictEncoderFewDistinct(t *testing.T) {
	// 100 rows but only 4 distinct values
	values := make([]int64, 100)
	for i := 0; i < 100; i++ {
		values[i] = int64(i % 4)
	}

	encoder := NewDictEncoder(4)
	page := encoder.Encode(values, len(values))

	if page.DictEntries != 4 {
		t.Errorf("expected 4 dictionary entries, got %d", page.DictEntries)
	}

	decoded := DecodeDictionary(page, 4)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestDictEncoderAllDistinct(t *testing.T) {
	// Every value is distinct (worst case for dict encoding)
	values := []int64{1, 2, 3, 4, 5}
	encoder := NewDictEncoder(4)
	page := encoder.Encode(values, len(values))

	if page.DictEntries != 5 {
		t.Errorf("expected 5 dictionary entries, got %d", page.DictEntries)
	}

	decoded := DecodeDictionary(page, 4)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

func TestDictEncoderNegative(t *testing.T) {
	values := []int64{-100, -200, -100, -200, -100}
	encoder := NewDictEncoder(4)
	page := encoder.Encode(values, len(values))

	if page.DictEntries != 2 {
		t.Errorf("expected 2 dictionary entries, got %d", page.DictEntries)
	}

	decoded := DecodeDictionary(page, 4)
	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], decoded[i])
		}
	}
}

// ============================================================================
// VarBinaryEncoder Tests
// ============================================================================

func TestVarBinaryEncoderEmpty(t *testing.T) {
	encoder := NewVarBinaryEncoder()
	page := encoder.EncodeStrings([]string{}, []bool{})

	if page.NumRows != 0 {
		t.Errorf("expected NumRows=0, got %d", page.NumRows)
	}
}

func TestVarBinaryEncoderBasic(t *testing.T) {
	values := []string{"hello", "world", "test", ""}
	validity := []bool{true, true, true, true}

	encoder := NewVarBinaryEncoder()
	page := encoder.EncodeStrings(values, validity)

	decoded, decodedValidity := DecodeVarBinary(page)

	for i := range values {
		if decodedValidity[i] != validity[i] {
			t.Errorf("row %d validity: expected %v, got %v", i, validity[i], decodedValidity[i])
		}
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %q, got %q", i, values[i], decoded[i])
		}
	}
}

func TestVarBinaryEncoderWithNulls(t *testing.T) {
	values := []string{"a", "b", "c", "d"}
	validity := []bool{true, false, true, false}

	encoder := NewVarBinaryEncoder()
	page := encoder.EncodeStrings(values, validity)

	decoded, decodedValidity := DecodeVarBinary(page)

	if decodedValidity[1] != false || decodedValidity[3] != false {
		t.Errorf("null rows not preserved")
	}
	if decoded[0] != "a" || decoded[2] != "c" {
		t.Errorf("non-null values incorrect")
	}
}

func TestVarBinaryEncoderUnicode(t *testing.T) {
	values := []string{"你好", "世界", "🎉", "日本語"}
	validity := []bool{true, true, true, true}

	encoder := NewVarBinaryEncoder()
	page := encoder.EncodeStrings(values, validity)

	decoded, _ := DecodeVarBinary(page)

	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %q, got %q", i, values[i], decoded[i])
		}
	}
}

func TestVarBinaryEncoderLongStrings(t *testing.T) {
	// Test with longer strings
	values := []string{
		"short",
		"this is a much longer string with more characters",
		"medium length string",
	}
	validity := []bool{true, true, true}

	encoder := NewVarBinaryEncoder()
	page := encoder.EncodeStrings(values, validity)

	decoded, _ := DecodeVarBinary(page)

	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %q, got %q", i, values[i], decoded[i])
		}
	}
}

// ============================================================================
// StringDictEncoder Tests
// ============================================================================

func TestStringDictEncoderEmpty(t *testing.T) {
	encoder := NewStringDictEncoder()
	page := encoder.EncodeStrings([]string{}, []bool{})

	if page.NumRows != 0 {
		t.Errorf("expected NumRows=0, got %d", page.NumRows)
	}
}

func TestStringDictEncoderBasic(t *testing.T) {
	values := []string{"apple", "banana", "apple", "cherry", "banana", "apple"}
	validity := []bool{true, true, true, true, true, true}

	encoder := NewStringDictEncoder()
	page := encoder.EncodeStrings(values, validity)

	if page.DictEntries != 3 {
		t.Errorf("expected 3 dictionary entries, got %d", page.DictEntries)
	}

	decoded, decodedValidity := DecodeStringDictionary(page)

	for i := range values {
		if decodedValidity[i] != validity[i] {
			t.Errorf("row %d validity: expected %v, got %v", i, validity[i], decodedValidity[i])
		}
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %q, got %q", i, values[i], decoded[i])
		}
	}
}

func TestStringDictEncoderWithNulls(t *testing.T) {
	values := []string{"a", "b", "c", "d"}
	validity := []bool{true, false, true, false}

	encoder := NewStringDictEncoder()
	page := encoder.EncodeStrings(values, validity)

	decoded, decodedValidity := DecodeStringDictionary(page)

	if decodedValidity[1] != false || decodedValidity[3] != false {
		t.Errorf("null rows not preserved")
	}
	if decoded[0] != "a" || decoded[2] != "c" {
		t.Errorf("non-null values incorrect")
	}
}

func TestStringDictEncoderUnicode(t *testing.T) {
	values := []string{"你好", "世界", "你好", "你好"}
	validity := []bool{true, true, true, true}

	encoder := NewStringDictEncoder()
	page := encoder.EncodeStrings(values, validity)

	if page.DictEntries != 2 {
		t.Errorf("expected 2 dictionary entries, got %d", page.DictEntries)
	}

	decoded, _ := DecodeStringDictionary(page)

	for i := range values {
		if decoded[i] != values[i] {
			t.Errorf("row %d: expected %q, got %q", i, values[i], decoded[i])
		}
	}
}

// ============================================================================
// Validity Bitmap Tests
// ============================================================================

func TestValidityBitmapAllValid(t *testing.T) {
	numRows := 100
	validity := make([]bool, numRows)
	for i := range validity {
		validity[i] = true
	}

	bitmap := makeValidityBitmap(validity, numRows)
	decoded := decodeValidityBitmap(bitmap, numRows)

	for i := 0; i < numRows; i++ {
		if !decoded[i] {
			t.Errorf("row %d should be valid", i)
		}
	}
}

func TestValidityBitmapAllNull(t *testing.T) {
	numRows := 50
	validity := make([]bool, numRows) // all false

	bitmap := makeValidityBitmap(validity, numRows)
	decoded := decodeValidityBitmap(bitmap, numRows)

	for i := 0; i < numRows; i++ {
		if decoded[i] {
			t.Errorf("row %d should be null", i)
		}
	}
}

func TestValidityBitmapMixed(t *testing.T) {
	numRows := 16
	validity := []bool{
		true, false, true, false,
		true, false, true, false,
		true, false, true, false,
		true, false, true, false,
	}

	bitmap := makeValidityBitmap(validity, numRows)
	decoded := decodeValidityBitmap(bitmap, numRows)

	for i := 0; i < numRows; i++ {
		expected := (i % 2) == 0
		if decoded[i] != expected {
			t.Errorf("row %d: expected validity %v, got %v", i, expected, decoded[i])
		}
	}
}

// ============================================================================
// Encoding Selection Tests
// ============================================================================

func TestSelectIntEncodingRLE(t *testing.T) {
	// High run length should select RLE
	stats := ColumnEncodingStats{
		NumRows:     100,
		NumNulls:    0,
		NumDistinct: 5,
		NumRuns:     5, // avg run length = 20
		MinVal:      0,
		MaxVal:      4,
	}

	enc := SelectIntEncoding(stats, 4)
	if enc != EncodingRLE {
		t.Errorf("expected RLE for high run length, got %v", enc)
	}
}

func TestSelectIntEncodingDictionary(t *testing.T) {
	// Low distinct ratio should select Dictionary
	stats := ColumnEncodingStats{
		NumRows:     1000,
		NumNulls:    0,
		NumDistinct: 10,  // 1% distinct ratio
		NumRuns:     500, // avg run length = 2 (not RLE)
		MinVal:      0,
		MaxVal:      1000000, // large range
	}

	enc := SelectIntEncoding(stats, 4)
	if enc != EncodingDictionary {
		t.Errorf("expected Dictionary for low distinct ratio, got %v", enc)
	}
}

func TestSelectIntEncodingBitPacked(t *testing.T) {
	// Small value range should select BitPacked
	stats := ColumnEncodingStats{
		NumRows:     100,
		NumNulls:    0,
		NumDistinct: 100, // all distinct
		NumRuns:     100, // no runs
		MinVal:      0,
		MaxVal:      255, // 8 bits vs 32 bits for int32
	}

	enc := SelectIntEncoding(stats, 4)
	if enc != EncodingBitPacked {
		t.Errorf("expected BitPacked for small range, got %v", enc)
	}
}

func TestSelectIntEncodingPlain(t *testing.T) {
	// Large range, many distinct, no runs -> Plain
	stats := ColumnEncodingStats{
		NumRows:     100,
		NumNulls:    0,
		NumDistinct: 100,
		NumRuns:     100,
		MinVal:      0,
		MaxVal:      1000000000, // large range
	}

	enc := SelectIntEncoding(stats, 4)
	if enc != EncodingPlain {
		t.Errorf("expected Plain for large range, got %v", enc)
	}
}

func TestSelectStringEncodingDictionary(t *testing.T) {
	// Low distinct ratio should select Dictionary
	stats := ColumnEncodingStats{
		NumRows:     1000,
		NumNulls:    0,
		NumDistinct: 50, // 5% distinct ratio
	}

	enc := SelectStringEncoding(stats)
	if enc != EncodingDictionary {
		t.Errorf("expected Dictionary for low distinct ratio, got %v", enc)
	}
}

func TestSelectStringEncodingVarBinary(t *testing.T) {
	// High distinct ratio should select VarBinary
	stats := ColumnEncodingStats{
		NumRows:     1000,
		NumNulls:    0,
		NumDistinct: 900, // 90% distinct ratio
	}

	enc := SelectStringEncoding(stats)
	if enc != EncodingVarBinary {
		t.Errorf("expected VarBinary for high distinct ratio, got %v", enc)
	}
}

// ============================================================================
// LogicalColumnEncoder/Decoder Integration Tests
// ============================================================================

func TestLogicalColumnEncoderIntegerRoundtrip(t *testing.T) {
	typ := common.MakeLType(common.LTID_INTEGER)
	vec := chunk.NewFlatVector(typ, 100)

	values := make([]int64, 100)
	for i := 0; i < 100; i++ {
		values[i] = int64(i * 10)
		vec.SetValue(i, &chunk.Value{Typ: typ, I64: values[i]})
	}

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(vec, 100)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode back
	outVec := chunk.NewFlatVector(typ, 100)

	decoder := NewLogicalColumnDecoder()
	err = decoder.DecodeColumn(page, outVec, typ)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify values
	for i := 0; i < 100; i++ {
		got := outVec.GetValue(i)
		if got.I64 != values[i] {
			t.Errorf("row %d: expected %d, got %d", i, values[i], got.I64)
		}
	}
}

func TestLogicalColumnEncoderIntegerWithNulls(t *testing.T) {
	typ := common.MakeLType(common.LTID_INTEGER)
	vec := chunk.NewFlatVector(typ, 10)

	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(i)})
		} else {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		}
	}

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(vec, 10)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode back
	outVec := chunk.NewFlatVector(typ, 10)

	decoder := NewLogicalColumnDecoder()
	err = decoder.DecodeColumn(page, outVec, typ)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify values and nulls
	for i := 0; i < 10; i++ {
		got := outVec.GetValue(i)
		if i%2 == 0 {
			if got.IsNull {
				t.Errorf("row %d should not be null", i)
			}
			if got.I64 != int64(i) {
				t.Errorf("row %d: expected %d, got %d", i, i, got.I64)
			}
		} else {
			if !got.IsNull {
				t.Errorf("row %d should be null", i)
			}
		}
	}
}

func TestLogicalColumnEncoderBooleanRoundtrip(t *testing.T) {
	typ := common.MakeLType(common.LTID_BOOLEAN)
	vec := chunk.NewFlatVector(typ, 8)

	values := []bool{false, true, true, false, true, false, false, true}
	for i := 0; i < 8; i++ {
		vec.SetValue(i, &chunk.Value{Typ: typ, Bool: values[i]})
	}

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(vec, 8)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Boolean should use bit-packing
	if page.Encoding != EncodingBitPacked {
		t.Errorf("expected BitPacked for boolean, got %v", page.Encoding)
	}

	// Decode back
	outVec := chunk.NewFlatVector(typ, 8)

	decoder := NewLogicalColumnDecoder()
	err = decoder.DecodeColumn(page, outVec, typ)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i := 0; i < 8; i++ {
		got := outVec.GetValue(i)
		if got.Bool != values[i] {
			t.Errorf("row %d: expected %v, got %v", i, values[i], got.Bool)
		}
	}
}

func TestLogicalColumnEncoderFloatRoundtrip(t *testing.T) {
	typ := common.MakeLType(common.LTID_FLOAT)
	vec := chunk.NewFlatVector(typ, 10)

	values := []float64{0.0, 1.5, -2.5, 3.14159, 100.0, -0.001, 1e10, -1e10, 0.5, 99.99}
	for i := 0; i < 10; i++ {
		vec.SetValue(i, &chunk.Value{Typ: typ, F64: values[i]})
	}

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(vec, 10)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Float should use plain encoding
	if page.Encoding != EncodingPlain {
		t.Errorf("expected Plain for float, got %v", page.Encoding)
	}

	// Decode back
	outVec := chunk.NewFlatVector(typ, 10)

	decoder := NewLogicalColumnDecoder()
	err = decoder.DecodeColumn(page, outVec, typ)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		got := outVec.GetValue(i)
		// Use approximate comparison for floats
		diff := got.F64 - values[i]
		if diff < 0 {
			diff = -diff
		}
		if diff > 1e-5 {
			t.Errorf("row %d: expected %f, got %f", i, values[i], got.F64)
		}
	}
}

func TestLogicalColumnEncoderStringRoundtrip(t *testing.T) {
	typ := common.MakeLType(common.LTID_VARCHAR)
	vec := chunk.NewFlatVector(typ, util.DefaultVectorSize)

	values := []string{"hello", "world", "test", "encoding", "roundtrip"}
	for i := 0; i < len(values); i++ {
		vec.SetValue(i, &chunk.Value{Typ: typ, Str: values[i]})
	}

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(vec, len(values))
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode back
	outVec := chunk.NewFlatVector(typ, util.DefaultVectorSize)

	decoder := NewLogicalColumnDecoder()
	err = decoder.DecodeColumn(page, outVec, typ)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i := 0; i < len(values); i++ {
		got := outVec.GetValue(i)
		if got.Str != values[i] {
			t.Errorf("row %d: expected %q, got %q", i, values[i], got.Str)
		}
	}
}

func TestLogicalColumnEncoderStringWithNulls(t *testing.T) {
	typ := common.MakeLType(common.LTID_VARCHAR)
	vec := chunk.NewFlatVector(typ, 10)

	values := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for i := 0; i < 10; i++ {
		if i%3 == 0 {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else {
			vec.SetValue(i, &chunk.Value{Typ: typ, Str: values[i]})
		}
	}

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(vec, 10)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	// Decode back
	outVec := chunk.NewFlatVector(typ, 10)

	decoder := NewLogicalColumnDecoder()
	err = decoder.DecodeColumn(page, outVec, typ)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		got := outVec.GetValue(i)
		if i%3 == 0 {
			if !got.IsNull {
				t.Errorf("row %d should be null", i)
			}
		} else {
			if got.IsNull {
				t.Errorf("row %d should not be null", i)
			}
			if got.Str != values[i] {
				t.Errorf("row %d: expected %q, got %q", i, values[i], got.Str)
			}
		}
	}
}

func TestLogicalColumnEncoderForcedEncoding(t *testing.T) {
	typ := common.MakeLType(common.LTID_INTEGER)
	vec := chunk.NewFlatVector(typ, 100)

	for i := 0; i < 100; i++ {
		vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(i)})
	}

	// Force RLE encoding
	encoder := NewLogicalColumnEncoderWithEncoding(EncodingRLE)
	page, err := encoder.EncodeColumn(vec, 100)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	if page.Encoding != EncodingRLE {
		t.Errorf("expected forced RLE encoding, got %v", page.Encoding)
	}
}

// ============================================================================
// EncodingProfile Tests
// ============================================================================

func TestEncodingProfileInteger(t *testing.T) {
	typ := common.MakeLType(common.LTID_INTEGER)
	vec := chunk.NewFlatVector(typ, 1000)

	// Create data with small range (good for bit-packing)
	for i := 0; i < 1000; i++ {
		vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(i % 100)})
	}

	profiles := ProfileEncoding(vec, 1000)

	if len(profiles) == 0 {
		t.Fatal("expected at least one encoding profile")
	}

	// Profiles should be sorted by compression ratio (best first)
	for i := 1; i < len(profiles); i++ {
		if profiles[i].Ratio < profiles[i-1].Ratio {
			t.Errorf("profiles not sorted by ratio: %v", profiles)
			break
		}
	}

	// Bit-packed should be good for this data
	foundBitPacked := false
	for _, p := range profiles {
		if p.Encoding == EncodingBitPacked {
			foundBitPacked = true
			if p.Ratio > 0.5 {
				t.Errorf("bit-packed ratio %f should be < 0.5 for this data", p.Ratio)
			}
		}
	}
	if !foundBitPacked {
		t.Error("expected bit-packed profile")
	}
}

func TestEncodingProfileString(t *testing.T) {
	typ := common.MakeLType(common.LTID_VARCHAR)
	vec := chunk.NewFlatVector(typ, 100)

	// Create data with few distinct values (good for dictionary)
	values := []string{"apple", "banana", "cherry"}
	for i := 0; i < 100; i++ {
		vec.SetValue(i, &chunk.Value{Typ: typ, Str: values[i%3]})
	}

	profiles := ProfileEncoding(vec, 100)

	if len(profiles) == 0 {
		t.Fatal("expected at least one encoding profile")
	}

	// Dictionary should be good for this data
	foundDict := false
	for _, p := range profiles {
		if p.Encoding == EncodingDictionary {
			foundDict = true
		}
	}
	if !foundDict {
		t.Error("expected dictionary profile for string column")
	}
}

// ============================================================================
// EncodingType String Tests
// ============================================================================

func TestEncodingTypeString(t *testing.T) {
	tests := []struct {
		enc      EncodingType
		expected string
	}{
		{EncodingPlain, "plain"},
		{EncodingBitPacked, "bitpacked"},
		{EncodingRLE, "rle"},
		{EncodingDictionary, "dictionary"},
		{EncodingVarBinary, "varbinary"},
		{EncodingType(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.enc.String()
		if got != tt.expected {
			t.Errorf("EncodingType(%d).String() = %q, want %q", tt.enc, got, tt.expected)
		}
	}
}

// TestBitpackedRoundtrip verifies bitpacked encode -> decode roundtrip for various value ranges.
func TestBitpackedRoundtrip(t *testing.T) {
	tests := []struct {
		name   string
		values []int64
	}{
		{"SmallRange", []int64{0, 1, 2, 3, 4, 5, 6, 7}},
		{"SingleValue", []int64{42, 42, 42, 42}},
		{"PowerOfTwo", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		{"NegativeValues", []int64{-10, -5, 0, 5, 10, 15, 20}},
		{"LargeRange", []int64{0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}},
		{"AllZeros", []int64{0, 0, 0, 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewBitPackEncoder()
			page := enc.Encode(tt.values, len(tt.values))
			if page.Encoding != EncodingBitPacked {
				t.Errorf("encoding: got %v want bitpacked", page.Encoding)
			}
			if page.NumRows != len(tt.values) {
				t.Errorf("NumRows: got %d want %d", page.NumRows, len(tt.values))
			}
			decoded := DecodeBitPacked(page)
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: got %d want %d", len(decoded), len(tt.values))
			}
			for i := range tt.values {
				if decoded[i] != tt.values[i] {
					t.Errorf("row %d: got %d want %d", i, decoded[i], tt.values[i])
				}
			}
		})
	}
}

// TestDictRoundtrip verifies dictionary encode -> decode roundtrip.
func TestDictRoundtrip(t *testing.T) {
	tests := []struct {
		name      string
		values    []int64
		byteWidth int
	}{
		{"FewDistinct4", []int64{10, 20, 30, 10, 20, 30, 10}, 4},
		{"SingleDistinct4", []int64{99, 99, 99, 99, 99}, 4},
		{"AllDistinct4", []int64{1, 2, 3, 4, 5}, 4},
		{"NegativeValues4", []int64{-5, -3, -1, -5, -3, -1}, 4},
		{"FewDistinct8", []int64{1000000, 2000000, 1000000, 2000000}, 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewDictEncoder(tt.byteWidth)
			page := enc.Encode(tt.values, len(tt.values))
			if page.Encoding != EncodingDictionary {
				t.Errorf("encoding: got %v want dictionary", page.Encoding)
			}
			if page.NumRows != len(tt.values) {
				t.Errorf("NumRows: got %d want %d", page.NumRows, len(tt.values))
			}
			decoded := DecodeDictionary(page, tt.byteWidth)
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: got %d want %d", len(decoded), len(tt.values))
			}
			for i := range tt.values {
				if decoded[i] != tt.values[i] {
					t.Errorf("row %d: got %d want %d", i, decoded[i], tt.values[i])
				}
			}
		})
	}
}

// TestRleRoundtrip verifies RLE encode -> decode roundtrip for various patterns.
func TestRleRoundtrip(t *testing.T) {
	tests := []struct {
		name      string
		values    []int64
		byteWidth int
	}{
		{"Repeated4", []int64{5, 5, 5, 5, 5, 10, 10, 10, 20}, 4},
		{"AllSame4", []int64{42, 42, 42, 42, 42, 42}, 4},
		{"NoRepeats4", []int64{1, 2, 3, 4, 5}, 4},
		{"Mixed8", []int64{100, 100, 200, 200, 200, 300, 100, 100}, 8},
		{"SingleValue4", []int64{7}, 4},
		{"LongRun2", []int64{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewRLEEncoder(tt.byteWidth)
			page := enc.Encode(tt.values, len(tt.values))
			if page.Encoding != EncodingRLE {
				t.Errorf("encoding: got %v want rle", page.Encoding)
			}
			if page.NumRows != len(tt.values) {
				t.Errorf("NumRows: got %d want %d", page.NumRows, len(tt.values))
			}
			decoded := DecodeRLE(page)
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: got %d want %d", len(decoded), len(tt.values))
			}
			for i := range tt.values {
				if decoded[i] != tt.values[i] {
					t.Errorf("row %d: got %d want %d", i, decoded[i], tt.values[i])
				}
			}
		})
	}
}

// TestBinaryRoundtrip verifies VarBinary encode -> decode roundtrip for string data.
func TestBinaryRoundtrip(t *testing.T) {
	// VarBinary validity convention: true = value present, false = null
	tests := []struct {
		name     string
		values   []string
		validity []bool // true = present, false = null
	}{
		{"Simple", []string{"hello", "world", "test"}, []bool{true, true, true}},
		{"WithNulls", []string{"a", "", "c", "", "e"}, []bool{true, false, true, false, true}},
		{"Empty", []string{""}, []bool{true}},
		{"LongStrings", []string{
			"the quick brown fox jumps over the lazy dog",
			"lorem ipsum dolor sit amet",
			"abcdefghijklmnopqrstuvwxyz",
		}, []bool{true, true, true}},
		{"AllNulls", []string{"", "", ""}, []bool{false, false, false}},
		{"Unicode", []string{"hello", "world", "test"}, []bool{true, true, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewVarBinaryEncoder()
			page := enc.EncodeStrings(tt.values, tt.validity)
			if page.Encoding != EncodingVarBinary {
				t.Errorf("encoding: got %v want varbinary", page.Encoding)
			}
			if page.NumRows != len(tt.values) {
				t.Errorf("NumRows: got %d want %d", page.NumRows, len(tt.values))
			}
			decoded, decodedValidity := DecodeVarBinary(page)
			if len(decoded) != len(tt.values) {
				t.Fatalf("decoded len: got %d want %d", len(decoded), len(tt.values))
			}
			for i := range tt.values {
				if decodedValidity[i] != tt.validity[i] {
					t.Errorf("row %d validity: got %v want %v", i, decodedValidity[i], tt.validity[i])
				}
				if tt.validity[i] && decoded[i] != tt.values[i] {
					t.Errorf("row %d: got %q want %q", i, decoded[i], tt.values[i])
				}
			}
		})
	}
}
