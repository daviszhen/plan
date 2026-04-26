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
	"io"
	"sync"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// ============================================================================
// V2 Column Writer
// ============================================================================

// V2ColumnWriterConfig configures the column writer.
type V2ColumnWriterConfig struct {
	// PageSize is the target page size in bytes.
	PageSize int
	// Compression is the compression algorithm to use.
	Compression V2CompressionType
	// Encoding is the encoding to use (auto-select if not specified).
	Encoding *EncodingType
	// ComputeStats whether to compute column statistics.
	ComputeStats bool
}

// DefaultV2ColumnWriterConfig returns a default configuration.
func DefaultV2ColumnWriterConfig() V2ColumnWriterConfig {
	return V2ColumnWriterConfig{
		PageSize:     V2DefaultPageSize,
		Compression:  V2CompressionNone,
		Encoding:     nil, // auto-select
		ComputeStats: true,
	}
}

// V2ColumnWriter writes column data in v2 format.
type V2ColumnWriter struct {
	config V2ColumnWriterConfig
	writer io.Writer

	// Column metadata
	columnIndex uint32
	logicalType common.LType
	nullable    bool

	// Buffer for collecting values
	valueBuffer []byte
	nullBitmap  []byte
	numValues   int
	nullCount   int

	// Page tracking
	pages    []V2PageIndexEntry
	totalPos uint64

	// Statistics
	stats *V2ColumnStats
}

// NewV2ColumnWriter creates a new column writer.
func NewV2ColumnWriter(w io.Writer, columnIndex uint32, logicalType common.LType, config V2ColumnWriterConfig) *V2ColumnWriter {
	return &V2ColumnWriter{
		config:      config,
		writer:      w,
		columnIndex: columnIndex,
		logicalType: logicalType,
		nullable:    true,
		valueBuffer: make([]byte, 0, config.PageSize),
		nullBitmap:  make([]byte, 0),
		stats: &V2ColumnStats{
			MinValue: nil,
			MaxValue: nil,
		},
	}
}

// WriteVector writes a chunk.Vector to the column.
func (w *V2ColumnWriter) WriteVector(vec *chunk.Vector, count int) error {
	if vec == nil || count == 0 {
		return nil
	}

	// Get value size based on type
	valueSize := w.getValueSize()

	for i := 0; i < count; i++ {
		val := vec.GetValue(i)

		// Handle nulls
		if val.IsNull {
			w.appendNull()
			continue
		}

		// Encode value
		data := w.encodeValue(val)
		w.valueBuffer = append(w.valueBuffer, data...)
		w.appendNotNull()
		w.numValues++

		// Check if we should flush a page
		if len(w.valueBuffer) >= w.config.PageSize {
			if err := w.flushPage(); err != nil {
				return err
			}
		}
	}

	_ = valueSize // May be used for fixed-width optimization
	return nil
}

// getValueSize returns the byte size for fixed-width types.
func (w *V2ColumnWriter) getValueSize() int {
	switch w.logicalType.Id {
	case common.LTID_BOOLEAN:
		return 1
	case common.LTID_TINYINT, common.LTID_UTINYINT:
		return 1
	case common.LTID_SMALLINT, common.LTID_USMALLINT:
		return 2
	case common.LTID_INTEGER, common.LTID_UINTEGER:
		return 4
	case common.LTID_BIGINT, common.LTID_UBIGINT:
		return 8
	case common.LTID_FLOAT:
		return 4
	case common.LTID_DOUBLE:
		return 8
	default:
		return -1 // Variable length
	}
}

// encodeValue encodes a value to bytes.
func (w *V2ColumnWriter) encodeValue(val *chunk.Value) []byte {
	buf := new(bytes.Buffer)

	switch w.logicalType.Id {
	case common.LTID_BOOLEAN:
		if val.Bool {
			binary.Write(buf, binary.LittleEndian, uint8(1))
		} else {
			binary.Write(buf, binary.LittleEndian, uint8(0))
		}
	case common.LTID_TINYINT:
		binary.Write(buf, binary.LittleEndian, int8(val.I64))
	case common.LTID_UTINYINT:
		binary.Write(buf, binary.LittleEndian, uint8(val.U64))
	case common.LTID_SMALLINT:
		binary.Write(buf, binary.LittleEndian, int16(val.I64))
	case common.LTID_USMALLINT:
		binary.Write(buf, binary.LittleEndian, uint16(val.U64))
	case common.LTID_INTEGER:
		binary.Write(buf, binary.LittleEndian, int32(val.I64))
	case common.LTID_UINTEGER:
		binary.Write(buf, binary.LittleEndian, uint32(val.U64))
	case common.LTID_BIGINT:
		binary.Write(buf, binary.LittleEndian, val.I64)
	case common.LTID_UBIGINT:
		binary.Write(buf, binary.LittleEndian, val.U64)
	case common.LTID_FLOAT:
		binary.Write(buf, binary.LittleEndian, float32(val.F64))
	case common.LTID_DOUBLE:
		binary.Write(buf, binary.LittleEndian, val.F64)
	case common.LTID_VARCHAR, common.LTID_CHAR:
		s := val.Str
		// Write length-prefixed string
		binary.Write(buf, binary.LittleEndian, uint32(len(s)))
		buf.WriteString(s)
	default:
		// For other types, use string representation
		s := val.String()
		binary.Write(buf, binary.LittleEndian, uint32(len(s)))
		buf.WriteString(s)
	}

	return buf.Bytes()
}

// appendNull adds a null value to the bitmap.
func (w *V2ColumnWriter) appendNull() {
	byteIdx := w.numValues / 8

	for len(w.nullBitmap) <= byteIdx {
		w.nullBitmap = append(w.nullBitmap, 0)
	}

	// Null = bit 0 (not set)
	w.nullCount++
	w.numValues++
}

// appendNotNull marks a value as not null.
// numValues has NOT been incremented yet by the caller, so the current
// position is w.numValues (same convention as appendNull).
func (w *V2ColumnWriter) appendNotNull() {
	byteIdx := w.numValues / 8
	bitIdx := w.numValues % 8

	for len(w.nullBitmap) <= byteIdx {
		w.nullBitmap = append(w.nullBitmap, 0)
	}

	// Not null = bit 1 (set)
	w.nullBitmap[byteIdx] |= 1 << bitIdx
}

// flushPage writes a page to the output.
func (w *V2ColumnWriter) flushPage() error {
	if len(w.valueBuffer) == 0 && w.numValues == 0 {
		return nil
	}

	// Determine encoding
	encoding := EncodingPlain
	if w.config.Encoding != nil {
		encoding = *w.config.Encoding
	}

	// Prepare page data
	pageData := w.valueBuffer

	// Apply compression
	compressedData, err := w.compress(pageData)
	if err != nil {
		return err
	}

	// Compute checksum
	checksum := ComputeChecksum(compressedData)

	// Create page header
	header := &V2PageHeader{
		PageType:         V2PageData,
		Compression:      w.config.Compression,
		Encoding:         encoding,
		ColumnIndex:      w.columnIndex,
		NumValues:        uint32(w.numValues),
		UncompressedSize: uint32(len(pageData)),
		CompressedSize:   uint32(len(compressedData)),
		Checksum:         checksum,
	}

	// Record page position
	entry := V2PageIndexEntry{
		Offset:      w.totalPos,
		Length:      V2PageHeaderSize + uint32(len(compressedData)),
		ColumnIndex: w.columnIndex,
		FirstRow:    w.totalPos, // Simplified - would need actual row tracking
		NumRows:     uint32(w.numValues),
	}
	w.pages = append(w.pages, entry)

	// Write header
	if err := header.Write(w.writer); err != nil {
		return err
	}
	w.totalPos += V2PageHeaderSize

	// Write data
	if _, err := w.writer.Write(compressedData); err != nil {
		return err
	}
	w.totalPos += uint64(len(compressedData))

	// Write validity bitmap if there are nulls
	if w.nullCount > 0 {
		validityHeader := &V2PageHeader{
			PageType:         V2PageValidity,
			Compression:      V2CompressionNone,
			Encoding:         EncodingPlain,
			ColumnIndex:      w.columnIndex,
			NumValues:        uint32(len(w.nullBitmap) * 8),
			UncompressedSize: uint32(len(w.nullBitmap)),
			CompressedSize:   uint32(len(w.nullBitmap)),
			Checksum:         ComputeChecksum(w.nullBitmap),
		}

		if err := validityHeader.Write(w.writer); err != nil {
			return err
		}
		w.totalPos += V2PageHeaderSize

		if _, err := w.writer.Write(w.nullBitmap); err != nil {
			return err
		}
		w.totalPos += uint64(len(w.nullBitmap))
	}

	// Update statistics
	w.stats.NullCount += uint64(w.nullCount)

	// Reset buffers
	w.valueBuffer = w.valueBuffer[:0]
	w.nullBitmap = w.nullBitmap[:0]
	w.numValues = 0
	w.nullCount = 0

	return nil
}

// compress applies compression to data.
func (w *V2ColumnWriter) compress(data []byte) ([]byte, error) {
	switch w.config.Compression {
	case V2CompressionNone:
		return data, nil
	case V2CompressionLZ4:
		return compressLZ4(data)
	case V2CompressionZstd:
		return compressZstd(data)
	default:
		return data, nil
	}
}

// Flush writes any remaining buffered data.
func (w *V2ColumnWriter) Flush() error {
	return w.flushPage()
}

// Pages returns the page index entries for this column.
func (w *V2ColumnWriter) Pages() []V2PageIndexEntry {
	return w.pages
}

// Stats returns the column statistics.
func (w *V2ColumnWriter) Stats() *V2ColumnStats {
	return w.stats
}

// TotalBytes returns the total bytes written.
func (w *V2ColumnWriter) TotalBytes() uint64 {
	return w.totalPos
}

// ============================================================================
// V2 Column Reader
// ============================================================================

// V2ColumnReader reads column data in v2 format.
type V2ColumnReader struct {
	reader io.ReaderAt
	pages  []V2PageIndexEntry

	// Column metadata
	columnIndex uint32
	logicalType common.LType
	numRows     uint64

	// Current position
	currentPage int
	currentRow  uint64
}

// NewV2ColumnReader creates a new column reader.
func NewV2ColumnReader(r io.ReaderAt, pages []V2PageIndexEntry, columnIndex uint32, logicalType common.LType, numRows uint64) *V2ColumnReader {
	return &V2ColumnReader{
		reader:      r,
		pages:       pages,
		columnIndex: columnIndex,
		logicalType: logicalType,
		numRows:     numRows,
	}
}

// ReadVector reads values into a chunk.Vector.
func (r *V2ColumnReader) ReadVector(vec *chunk.Vector, maxCount int) (int, error) {
	if r.currentRow >= r.numRows {
		return 0, io.EOF
	}

	count := 0
	for count < maxCount && r.currentPage < len(r.pages) {
		page := r.pages[r.currentPage]

		// Read page header
		headerBuf := make([]byte, V2PageHeaderSize)
		if _, err := r.reader.ReadAt(headerBuf, int64(page.Offset)); err != nil {
			return count, err
		}

		header := &V2PageHeader{}
		if err := header.Read(bytes.NewReader(headerBuf)); err != nil {
			return count, err
		}

		// Read page data
		dataBuf := make([]byte, header.CompressedSize)
		if _, err := r.reader.ReadAt(dataBuf, int64(page.Offset)+V2PageHeaderSize); err != nil {
			return count, err
		}

		// Verify checksum
		if !VerifyChecksum(dataBuf, header.Checksum) {
			return count, fmt.Errorf("checksum mismatch for page at offset %d", page.Offset)
		}

		// Decompress if needed
		data, err := r.decompress(dataBuf, header.Compression, int(header.UncompressedSize))
		if err != nil {
			return count, err
		}

		// Compute how many rows to skip within this page (for Seek support).
		// pageStartRow is the global row at which this page begins.
		var pageStartRow uint64
		for p := 0; p < r.currentPage; p++ {
			pageStartRow += uint64(r.pages[p].NumRows)
		}
		skipInPage := 0
		if r.currentRow > pageStartRow {
			skipInPage = int(r.currentRow - pageStartRow)
		}

		// Decode values, skipping rows within the page as needed
		numRead, err := r.decodeValues(vec, data, header, maxCount-count, count, skipInPage)
		if err != nil {
			return count, err
		}

		count += numRead
		r.currentRow += uint64(numRead)

		if skipInPage+numRead >= int(header.NumValues) {
			r.currentPage++
		}
	}

	return count, nil
}

// decompress decompresses data.
func (r *V2ColumnReader) decompress(data []byte, compression V2CompressionType, uncompressedSize int) ([]byte, error) {
	switch compression {
	case V2CompressionNone:
		return data, nil
	case V2CompressionLZ4:
		return decompressLZ4(data, uncompressedSize)
	case V2CompressionZstd:
		return decompressZstd(data, uncompressedSize)
	default:
		return data, nil
	}
}

// decodeValues decodes values from page data into a vector.
// skipCount specifies how many values to skip from the start of the page (for Seek support).
func (r *V2ColumnReader) decodeValues(vec *chunk.Vector, data []byte, header *V2PageHeader, maxCount int, startIdx int, skipCount int) (int, error) {
	reader := bytes.NewReader(data)

	// Skip values that come before the seek position within this page.
	for s := 0; s < skipCount; s++ {
		if _, err := r.decodeValue(reader); err != nil {
			if err == io.EOF {
				return 0, nil
			}
			return 0, err
		}
	}

	count := 0
	remaining := int(header.NumValues) - skipCount

	for count < maxCount && count < remaining {
		val, err := r.decodeValue(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return count, err
		}

		vec.SetValue(startIdx+count, val)
		count++
	}

	return count, nil
}

// decodeValue decodes a single value.
func (r *V2ColumnReader) decodeValue(reader *bytes.Reader) (*chunk.Value, error) {
	val := &chunk.Value{Typ: r.logicalType}

	switch r.logicalType.Id {
	case common.LTID_BOOLEAN:
		var v uint8
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.Bool = v != 0
		return val, nil

	case common.LTID_TINYINT:
		var v int8
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.I64 = int64(v)
		return val, nil

	case common.LTID_SMALLINT:
		var v int16
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.I64 = int64(v)
		return val, nil

	case common.LTID_INTEGER:
		var v int32
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.I64 = int64(v)
		return val, nil

	case common.LTID_BIGINT:
		var v int64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.I64 = v
		return val, nil

	case common.LTID_FLOAT:
		var v float32
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.F64 = float64(v)
		return val, nil

	case common.LTID_DOUBLE:
		var v float64
		if err := binary.Read(reader, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		val.F64 = v
		return val, nil

	case common.LTID_VARCHAR, common.LTID_CHAR:
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		strBuf := make([]byte, length)
		if _, err := io.ReadFull(reader, strBuf); err != nil {
			return nil, err
		}
		val.Str = string(strBuf)
		return val, nil

	default:
		return nil, fmt.Errorf("unsupported type: %v", r.logicalType)
	}
}

// Seek moves to a specific row.
func (r *V2ColumnReader) Seek(row uint64) error {
	if row >= r.numRows {
		return fmt.Errorf("row %d out of range [0, %d)", row, r.numRows)
	}

	// Find the page containing this row
	var rowOffset uint64 = 0
	for i, page := range r.pages {
		if rowOffset+uint64(page.NumRows) > row {
			r.currentPage = i
			r.currentRow = row
			return nil
		}
		rowOffset += uint64(page.NumRows)
	}

	return fmt.Errorf("row %d not found in pages", row)
}

// ============================================================================
// Compression Implementation
// ============================================================================

// compressLZ4 compresses data using LZ4 block compression.
// Always returns compressed data. Caller should compare sizes to decide
// whether to use compressed or original data.
func compressLZ4(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	// Pre-allocate buffer for compressed data
	maxSize := lz4.CompressBlockBound(len(data))
	compressed := make([]byte, maxSize)

	// Compress the data
	n, err := lz4.CompressBlock(data, compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("lz4 compress: %w", err)
	}

	// n == 0 means data is incompressible with LZ4 block mode
	if n == 0 {
		// Return a copy to avoid aliasing issues
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	}

	return compressed[:n], nil
}

// decompressLZ4 decompresses LZ4 block data.
func decompressLZ4(data []byte, uncompressedSize int) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	decompressed := make([]byte, uncompressedSize)
	n, err := lz4.UncompressBlock(data, decompressed)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}

	return decompressed[:n], nil
}

// zstd encoder/decoder pool for thread-safe reuse
var (
	zstdEncoderPool sync.Pool
	zstdDecoderPool sync.Pool
	zstdInitOnce    sync.Once
)

func initZstdPools() {
	zstdInitOnce.Do(func() {
		zstdEncoderPool = sync.Pool{
			New: func() interface{} {
				enc, err := zstd.NewWriter(nil,
					zstd.WithEncoderLevel(zstd.SpeedDefault),
					zstd.WithEncoderConcurrency(1),
				)
				if err != nil {
					panic(fmt.Sprintf("failed to create zstd encoder: %v", err))
				}
				return enc
			},
		}
		zstdDecoderPool = sync.Pool{
			New: func() interface{} {
				dec, err := zstd.NewReader(nil,
					zstd.WithDecoderConcurrency(1),
				)
				if err != nil {
					panic(fmt.Sprintf("failed to create zstd decoder: %v", err))
				}
				return dec
			},
		}
	})
}

// compressZstd compresses data using Zstandard.
// Always returns compressed data. Caller should compare sizes to decide
// whether to use compressed or original data.
func compressZstd(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	initZstdPools()

	enc := zstdEncoderPool.Get().(*zstd.Encoder)
	defer zstdEncoderPool.Put(enc)

	return enc.EncodeAll(data, nil), nil
}

// decompressZstd decompresses Zstandard data.
func decompressZstd(data []byte, uncompressedSize int) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	initZstdPools()

	dec := zstdDecoderPool.Get().(*zstd.Decoder)
	defer zstdDecoderPool.Put(dec)

	// Pre-allocate output buffer with expected size
	decompressed := make([]byte, 0, uncompressedSize)
	decompressed, err := dec.DecodeAll(data, decompressed)
	if err != nil {
		return nil, fmt.Errorf("zstd decompress: %w", err)
	}

	return decompressed, nil
}

// CompressWithType compresses data using the specified compression type.
func CompressWithType(data []byte, compressionType V2CompressionType) ([]byte, error) {
	switch compressionType {
	case V2CompressionNone:
		return data, nil
	case V2CompressionLZ4:
		return compressLZ4(data)
	case V2CompressionZstd:
		return compressZstd(data)
	default:
		return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
	}
}

// DecompressWithType decompresses data using the specified compression type.
func DecompressWithType(data []byte, uncompressedSize int, compressionType V2CompressionType) ([]byte, error) {
	switch compressionType {
	case V2CompressionNone:
		return data, nil
	case V2CompressionLZ4:
		return decompressLZ4(data, uncompressedSize)
	case V2CompressionZstd:
		return decompressZstd(data, uncompressedSize)
	default:
		return nil, fmt.Errorf("unsupported compression type: %v", compressionType)
	}
}
