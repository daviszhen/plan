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
	"fmt"
	"sync"
	"testing"

	"github.com/daviszhen/plan/pkg/common"
)

// TestV2FileHeaderRoundtrip tests header serialization.
func TestV2FileHeaderRoundtrip(t *testing.T) {
	header := &V2FileHeader{
		Magic:          [4]byte{'L', 'N', 'C', '2'},
		MajorVersion:   V2MajorVersion,
		MinorVersion:   V2MinorVersion,
		Flags:          0,
		NumColumns:     5,
		NumRows:        1000,
		MetadataOffset: 12345,
	}

	// Write
	buf := new(bytes.Buffer)
	if err := header.Write(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Read back
	readHeader := &V2FileHeader{}
	if err := readHeader.Read(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	// Verify
	if readHeader.MajorVersion != header.MajorVersion {
		t.Errorf("major version mismatch: got %d, want %d", readHeader.MajorVersion, header.MajorVersion)
	}
	if readHeader.MinorVersion != header.MinorVersion {
		t.Errorf("minor version mismatch: got %d, want %d", readHeader.MinorVersion, header.MinorVersion)
	}
	if readHeader.NumColumns != header.NumColumns {
		t.Errorf("num columns mismatch: got %d, want %d", readHeader.NumColumns, header.NumColumns)
	}
	if readHeader.NumRows != header.NumRows {
		t.Errorf("num rows mismatch: got %d, want %d", readHeader.NumRows, header.NumRows)
	}
	if readHeader.MetadataOffset != header.MetadataOffset {
		t.Errorf("metadata offset mismatch: got %d, want %d", readHeader.MetadataOffset, header.MetadataOffset)
	}
}

// TestV2PageHeaderRoundtrip tests page header serialization.
func TestV2PageHeaderRoundtrip(t *testing.T) {
	header := &V2PageHeader{
		PageType:         V2PageData,
		Compression:      V2CompressionZstd,
		Encoding:         EncodingBitPacked,
		Reserved:         0,
		ColumnIndex:      3,
		NumValues:        500,
		UncompressedSize: 4000,
		CompressedSize:   2500,
		Checksum:         0x12345678,
	}

	// Write
	buf := new(bytes.Buffer)
	if err := header.Write(buf); err != nil {
		t.Fatalf("failed to write page header: %v", err)
	}

	// Read back
	readHeader := &V2PageHeader{}
	if err := readHeader.Read(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("failed to read page header: %v", err)
	}

	// Verify
	if readHeader.PageType != header.PageType {
		t.Errorf("page type mismatch")
	}
	if readHeader.Compression != header.Compression {
		t.Errorf("compression mismatch")
	}
	if readHeader.Encoding != header.Encoding {
		t.Errorf("encoding mismatch")
	}
	if readHeader.ColumnIndex != header.ColumnIndex {
		t.Errorf("column index mismatch")
	}
	if readHeader.NumValues != header.NumValues {
		t.Errorf("num values mismatch")
	}
	if readHeader.UncompressedSize != header.UncompressedSize {
		t.Errorf("uncompressed size mismatch")
	}
	if readHeader.CompressedSize != header.CompressedSize {
		t.Errorf("compressed size mismatch")
	}
	if readHeader.Checksum != header.Checksum {
		t.Errorf("checksum mismatch")
	}
}

// TestV2PageIndexEntryRoundtrip tests page index entry serialization.
func TestV2PageIndexEntryRoundtrip(t *testing.T) {
	entry := V2PageIndexEntry{
		Offset:      100000,
		Length:      5000,
		ColumnIndex: 2,
		FirstRow:    1000,
		NumRows:     500,
	}

	// Write
	buf := new(bytes.Buffer)
	if err := entry.Write(buf); err != nil {
		t.Fatalf("failed to write page index entry: %v", err)
	}

	// Read back
	readEntry := V2PageIndexEntry{}
	if err := readEntry.Read(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("failed to read page index entry: %v", err)
	}

	// Verify
	if readEntry.Offset != entry.Offset {
		t.Errorf("offset mismatch: got %d, want %d", readEntry.Offset, entry.Offset)
	}
	if readEntry.Length != entry.Length {
		t.Errorf("length mismatch: got %d, want %d", readEntry.Length, entry.Length)
	}
	if readEntry.ColumnIndex != entry.ColumnIndex {
		t.Errorf("column index mismatch")
	}
	if readEntry.FirstRow != entry.FirstRow {
		t.Errorf("first row mismatch")
	}
	if readEntry.NumRows != entry.NumRows {
		t.Errorf("num rows mismatch")
	}
}

// TestChecksum tests CRC32 checksum.
func TestChecksum(t *testing.T) {
	data := []byte("hello world, this is a test for checksum")

	checksum := ComputeChecksum(data)
	if checksum == 0 {
		t.Error("checksum should not be zero")
	}

	if !VerifyChecksum(data, checksum) {
		t.Error("checksum verification failed")
	}

	// Modified data should fail verification
	data[0] = 'H'
	if VerifyChecksum(data, checksum) {
		t.Error("modified data should fail verification")
	}
}

// TestV2CompressionTypeString tests compression type string conversion.
func TestV2CompressionTypeString(t *testing.T) {
	tests := []struct {
		compression V2CompressionType
		expected    string
	}{
		{V2CompressionNone, "none"},
		{V2CompressionLZ4, "lz4"},
		{V2CompressionZstd, "zstd"},
		{V2CompressionSnappy, "snappy"},
		{V2CompressionType(99), "unknown"},
	}

	for _, test := range tests {
		if got := test.compression.String(); got != test.expected {
			t.Errorf("CompressionType(%d).String() = %s, want %s",
				test.compression, got, test.expected)
		}
	}
}

// TestV2ColumnWriterConfig tests default config.
func TestV2ColumnWriterConfig(t *testing.T) {
	cfg := DefaultV2ColumnWriterConfig()

	if cfg.PageSize != V2DefaultPageSize {
		t.Errorf("expected default page size %d, got %d", V2DefaultPageSize, cfg.PageSize)
	}

	if cfg.Compression != V2CompressionNone {
		t.Errorf("expected default compression None, got %v", cfg.Compression)
	}

	if !cfg.ComputeStats {
		t.Error("expected ComputeStats to be true by default")
	}
}

// TestV2FileWriterConfig tests default file writer config.
func TestV2FileWriterConfig(t *testing.T) {
	cfg := DefaultV2FileWriterConfig()

	if cfg.PageSize != V2DefaultPageSize {
		t.Errorf("expected default page size %d, got %d", V2DefaultPageSize, cfg.PageSize)
	}
}

// TestV2ColumnWriterBasic tests basic column writer operations.
func TestV2ColumnWriterBasic(t *testing.T) {
	buf := new(bytes.Buffer)
	cfg := DefaultV2ColumnWriterConfig()
	cfg.PageSize = 1024 // Small page size for testing

	writer := NewV2ColumnWriter(buf, 0, common.IntegerType(), cfg)

	// Flush empty writer should succeed
	if err := writer.Flush(); err != nil {
		t.Errorf("flush empty writer failed: %v", err)
	}

	// Check initial state
	if writer.TotalBytes() != 0 {
		t.Errorf("expected 0 total bytes, got %d", writer.TotalBytes())
	}

	pages := writer.Pages()
	if len(pages) != 0 {
		t.Errorf("expected 0 pages, got %d", len(pages))
	}

	stats := writer.Stats()
	if stats == nil {
		t.Error("expected non-nil stats")
	}
}

// TestLanceLogicalTypeConversion tests type conversion.
func TestLanceLogicalTypeConversion(t *testing.T) {
	tests := []struct {
		logicalType string
		expectedID  common.LTypeId
	}{
		{"bool", common.LTID_BOOLEAN},
		{"int8", common.LTID_TINYINT},
		{"int16", common.LTID_SMALLINT},
		{"int32", common.LTID_INTEGER},
		{"int64", common.LTID_BIGINT},
		{"float", common.LTID_FLOAT},
		{"double", common.LTID_DOUBLE},
		{"string", common.LTID_VARCHAR},
	}

	for _, test := range tests {
		typ := lanceLogicalTypeToLType(test.logicalType)
		if typ.Id != test.expectedID {
			t.Errorf("lanceLogicalTypeToLType(%s) = %v, want %v",
				test.logicalType, typ.Id, test.expectedID)
		}
	}
}

// TestV2Constants tests format constants.
func TestV2Constants(t *testing.T) {
	if V2Magic != "LNC2" {
		t.Errorf("unexpected magic: %s", V2Magic)
	}

	if V2MajorVersion != 2 {
		t.Errorf("unexpected major version: %d", V2MajorVersion)
	}

	if V2HeaderSize != 32 {
		t.Errorf("unexpected header size: %d", V2HeaderSize)
	}

	if V2PageHeaderSize != 24 {
		t.Errorf("unexpected page header size: %d", V2PageHeaderSize)
	}

	if V2DefaultPageSize != 64*1024 {
		t.Errorf("unexpected default page size: %d", V2DefaultPageSize)
	}
}

// TestV2InvalidMagic tests reading with invalid magic.
func TestV2InvalidMagic(t *testing.T) {
	header := &V2FileHeader{}
	data := []byte("BADM\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

	err := header.Read(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

// TestLZ4CompressionRoundtrip tests LZ4 compression and decompression.
func TestLZ4CompressionRoundtrip(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello world")},
		{"repetitive", bytes.Repeat([]byte("abcd"), 1000)},
		{"large-repetitive", bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 1000)},
		{"random-like", func() []byte {
			data := make([]byte, 10000)
			for i := range data {
				data[i] = byte((i*17 + 31) % 256)
			}
			return data
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := compressLZ4(tc.data)
			if err != nil {
				t.Fatalf("compression failed: %v", err)
			}

			// LZ4 block mode returns original data copy if incompressible
			// In that case, len(compressed) == len(data) and data is not LZ4 format
			if len(compressed) == len(tc.data) {
				// Data was not compressed (incompressible), verify it's a copy
				if !bytes.Equal(tc.data, compressed) {
					t.Errorf("incompressible data copy mismatch")
				}
				t.Logf("%s: incompressible, original=%d", tc.name, len(tc.data))
				return
			}

			// Data was compressed, verify roundtrip
			decompressed, err := decompressLZ4(compressed, len(tc.data))
			if err != nil {
				t.Fatalf("decompression failed: %v", err)
			}

			if !bytes.Equal(tc.data, decompressed) {
				t.Errorf("roundtrip failed: got %d bytes, want %d bytes", len(decompressed), len(tc.data))
			}

			ratio := float64(len(compressed)) / float64(len(tc.data))
			t.Logf("%s: original=%d, compressed=%d, ratio=%.2f", tc.name, len(tc.data), len(compressed), ratio)
		})
	}
}

// TestZstdCompressionRoundtrip tests Zstd compression and decompression.
func TestZstdCompressionRoundtrip(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello world")},
		{"repetitive", bytes.Repeat([]byte("abcd"), 1000)},
		{"large-repetitive", bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog "), 10000)},
		{"random-like", func() []byte {
			data := make([]byte, 10000)
			for i := range data {
				data[i] = byte((i*17 + 31) % 256)
			}
			return data
		}()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := compressZstd(tc.data)
			if err != nil {
				t.Fatalf("compression failed: %v", err)
			}

			decompressed, err := decompressZstd(compressed, len(tc.data))
			if err != nil {
				t.Fatalf("decompression failed: %v", err)
			}

			if !bytes.Equal(tc.data, decompressed) {
				t.Errorf("roundtrip failed: got %d bytes, want %d bytes", len(decompressed), len(tc.data))
			}

			// Log compression ratio for informational purposes
			if len(tc.data) > 0 {
				ratio := float64(len(compressed)) / float64(len(tc.data))
				t.Logf("%s: original=%d, compressed=%d, ratio=%.2f", tc.name, len(tc.data), len(compressed), ratio)
			}
		})
	}
}

// TestCompressWithType tests the unified compression interface.
func TestCompressWithType(t *testing.T) {
	data := bytes.Repeat([]byte("test data for compression "), 100)

	testCases := []struct {
		name        string
		compression V2CompressionType
	}{
		{"none", V2CompressionNone},
		{"lz4", V2CompressionLZ4},
		{"zstd", V2CompressionZstd},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressed, err := CompressWithType(data, tc.compression)
			if err != nil {
				t.Fatalf("compression failed: %v", err)
			}

			decompressed, err := DecompressWithType(compressed, len(data), tc.compression)
			if err != nil {
				t.Fatalf("decompression failed: %v", err)
			}

			if !bytes.Equal(data, decompressed) {
				t.Errorf("roundtrip failed for %s", tc.name)
			}
		})
	}
}

// TestCompressionUnsupportedType tests handling of unsupported compression types.
func TestCompressionUnsupportedType(t *testing.T) {
	data := []byte("test")

	_, err := CompressWithType(data, V2CompressionType(99))
	if err == nil {
		t.Error("expected error for unsupported compression type")
	}

	_, err = DecompressWithType(data, len(data), V2CompressionType(99))
	if err == nil {
		t.Error("expected error for unsupported decompression type")
	}
}

// TestZstdConcurrentUsage tests that Zstd encoder/decoder pools work correctly under concurrent access.
func TestZstdConcurrentUsage(t *testing.T) {
	data := bytes.Repeat([]byte("concurrent test data "), 100)
	const numGoroutines = 10
	const numIterations = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numIterations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				compressed, err := compressZstd(data)
				if err != nil {
					errors <- err
					return
				}

				decompressed, err := decompressZstd(compressed, len(data))
				if err != nil {
					errors <- err
					return
				}

				if !bytes.Equal(data, decompressed) {
					errors <- fmt.Errorf("data mismatch")
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("concurrent test failed: %v", err)
	}
}
