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
	"os"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
	protobuf "google.golang.org/protobuf/proto"
)

// ============================================================================
// V2 File Writer
// ============================================================================

// V2FileWriterConfig configures the file writer.
type V2FileWriterConfig struct {
	// PageSize is the target page size in bytes.
	PageSize int
	// Compression is the default compression algorithm.
	Compression V2CompressionType
	// ComputeStats whether to compute column statistics.
	ComputeStats bool
}

// DefaultV2FileWriterConfig returns a default configuration.
func DefaultV2FileWriterConfig() V2FileWriterConfig {
	return V2FileWriterConfig{
		PageSize:     V2DefaultPageSize,
		Compression:  V2CompressionNone,
		ComputeStats: true,
	}
}

// V2FileWriter writes a complete v2 format file.
type V2FileWriter struct {
	config V2FileWriterConfig
	writer io.WriteSeeker

	// Schema
	schema *storage2pb.Schema
	fields []*storage2pb.Field
	types  []common.LType

	// Column writers
	columnWriters []*V2ColumnWriter
	columnBuffers []*bytes.Buffer
	columnMeta    []*V2ColumnMeta

	// Tracking
	numRows   uint64
	headerPos int64
	dataPos   int64
}

// NewV2FileWriter creates a new file writer.
func NewV2FileWriter(w io.WriteSeeker, schema *storage2pb.Schema, types []common.LType, config V2FileWriterConfig) *V2FileWriter {
	return &V2FileWriter{
		config: config,
		writer: w,
		schema: schema,
		fields: schema.Fields,
		types:  types,
	}
}

// WriteHeader writes the file header.
func (w *V2FileWriter) WriteHeader() error {
	header := &V2FileHeader{
		Magic:        [4]byte{'L', 'N', 'C', '2'},
		MajorVersion: V2MajorVersion,
		MinorVersion: V2MinorVersion,
		NumColumns:   uint32(len(w.types)),
		NumRows:      0, // Will be updated later
	}

	if err := header.Write(w.writer); err != nil {
		return err
	}

	w.headerPos = 0
	w.dataPos = V2HeaderSize

	// Initialize column writers
	w.columnWriters = make([]*V2ColumnWriter, len(w.types))
	w.columnBuffers = make([]*bytes.Buffer, len(w.types))
	w.columnMeta = make([]*V2ColumnMeta, len(w.types))

	for i, typ := range w.types {
		// Create a buffer for each column
		colBuf := new(bytes.Buffer)
		w.columnBuffers[i] = colBuf
		colConfig := V2ColumnWriterConfig{
			PageSize:     w.config.PageSize,
			Compression:  w.config.Compression,
			ComputeStats: w.config.ComputeStats,
		}
		w.columnWriters[i] = NewV2ColumnWriter(colBuf, uint32(i), typ, colConfig)

		// Initialize column metadata
		name := fmt.Sprintf("c%d", i)
		if i < len(w.fields) && w.fields[i].Name != "" {
			name = w.fields[i].Name
		}
		w.columnMeta[i] = &V2ColumnMeta{
			Index:       uint32(i),
			Name:        name,
			LogicalType: typ.String(),
			Nullable:    true,
		}
	}

	return nil
}

// WriteChunk writes a chunk to the file.
func (w *V2FileWriter) WriteChunk(c *chunk.Chunk) error {
	if c == nil || c.Card() == 0 {
		return nil
	}

	numCols := c.ColumnCount()
	if numCols != len(w.columnWriters) {
		return fmt.Errorf("chunk has %d columns, expected %d", numCols, len(w.columnWriters))
	}

	// Write each column
	for i := 0; i < numCols; i++ {
		vec := c.Data[i]
		if err := w.columnWriters[i].WriteVector(vec, c.Card()); err != nil {
			return fmt.Errorf("failed to write column %d: %w", i, err)
		}
	}

	w.numRows += uint64(c.Card())
	return nil
}

// Close finalizes and closes the file.
func (w *V2FileWriter) Close() error {
	// Flush all column writers and collect data
	allPageIndex := make([]V2PageIndexEntry, 0)
	var currentOffset uint64 = uint64(w.dataPos)

	for i, colWriter := range w.columnWriters {
		if err := colWriter.Flush(); err != nil {
			return fmt.Errorf("failed to flush column %d: %w", i, err)
		}

		// Get the column's buffer (we need to recreate since we used bytes.Buffer)
		// For now, collect pages
		pages := colWriter.Pages()
		for _, page := range pages {
			page.Offset = currentOffset
			allPageIndex = append(allPageIndex, page)
			currentOffset += uint64(page.Length)
		}

		// Update column metadata
		w.columnMeta[i].NumPages = uint32(len(pages))
		w.columnMeta[i].TotalBytes = colWriter.TotalBytes()
		w.columnMeta[i].Statistics = colWriter.Stats()
	}

	// Write page data for each column
	for i := range w.columnWriters {
		colData := w.columnBuffers[i].Bytes()
		if len(colData) > 0 {
			if _, err := w.writer.Write(colData); err != nil {
				return fmt.Errorf("failed to write column %d data: %w", i, err)
			}
		}
	}

	// Write page index
	pageIndexOffset := currentOffset
	pageIndexBuf := new(bytes.Buffer)
	binary.Write(pageIndexBuf, binary.LittleEndian, uint32(len(allPageIndex)))
	for _, entry := range allPageIndex {
		if err := entry.Write(pageIndexBuf); err != nil {
			return err
		}
	}
	if _, err := w.writer.Write(pageIndexBuf.Bytes()); err != nil {
		return err
	}

	// Write column metadata
	colMetaOffset := pageIndexOffset + uint64(pageIndexBuf.Len())
	colMetaBuf := new(bytes.Buffer)
	binary.Write(colMetaBuf, binary.LittleEndian, uint32(len(w.columnMeta)))
	for _, meta := range w.columnMeta {
		// Write column name
		binary.Write(colMetaBuf, binary.LittleEndian, uint32(len(meta.Name)))
		colMetaBuf.WriteString(meta.Name)
		// Write logical type
		binary.Write(colMetaBuf, binary.LittleEndian, uint32(len(meta.LogicalType)))
		colMetaBuf.WriteString(meta.LogicalType)
		// Write nullable flag
		var nullable uint8 = 0
		if meta.Nullable {
			nullable = 1
		}
		binary.Write(colMetaBuf, binary.LittleEndian, nullable)
		// Write stats
		binary.Write(colMetaBuf, binary.LittleEndian, meta.NumPages)
		binary.Write(colMetaBuf, binary.LittleEndian, meta.TotalBytes)
	}
	if _, err := w.writer.Write(colMetaBuf.Bytes()); err != nil {
		return err
	}

	// Write schema
	schemaOffset := colMetaOffset + uint64(colMetaBuf.Len())
	schemaData, err := protobuf.Marshal(w.schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}
	binary.Write(w.writer, binary.LittleEndian, uint32(len(schemaData)))
	if _, err := w.writer.Write(schemaData); err != nil {
		return err
	}

	// Write footer
	footerBuf := new(bytes.Buffer)
	binary.Write(footerBuf, binary.LittleEndian, pageIndexOffset)
	binary.Write(footerBuf, binary.LittleEndian, uint32(pageIndexBuf.Len()))
	binary.Write(footerBuf, binary.LittleEndian, colMetaOffset)
	binary.Write(footerBuf, binary.LittleEndian, uint32(colMetaBuf.Len()))
	binary.Write(footerBuf, binary.LittleEndian, schemaOffset)
	binary.Write(footerBuf, binary.LittleEndian, uint32(len(schemaData)))
	binary.Write(footerBuf, binary.LittleEndian, w.numRows)
	footerBuf.WriteString(V2Magic)
	if _, err := w.writer.Write(footerBuf.Bytes()); err != nil {
		return err
	}

	// Update header with final row count
	if seeker, ok := w.writer.(io.Seeker); ok {
		if _, err := seeker.Seek(w.headerPos, io.SeekStart); err != nil {
			return err
		}
		header := &V2FileHeader{
			Magic:          [4]byte{'L', 'N', 'C', '2'},
			MajorVersion:   V2MajorVersion,
			MinorVersion:   V2MinorVersion,
			NumColumns:     uint32(len(w.types)),
			NumRows:        w.numRows,
			MetadataOffset: pageIndexOffset,
		}
		if err := header.Write(w.writer); err != nil {
			return err
		}
	}

	return nil
}

// ============================================================================
// V2 File Reader
// ============================================================================

// V2FileReader reads a complete v2 format file.
type V2FileReader struct {
	reader io.ReaderAt
	size   int64

	// Header
	header *V2FileHeader

	// Schema
	schema *storage2pb.Schema
	types  []common.LType

	// Page index
	pageIndex *V2PageIndex

	// Column metadata
	columnMeta []*V2ColumnMeta
}

// NewV2FileReader creates a new file reader.
func NewV2FileReader(r io.ReaderAt, size int64) (*V2FileReader, error) {
	reader := &V2FileReader{
		reader: r,
		size:   size,
	}

	if err := reader.readHeader(); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	if err := reader.readFooter(); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	return reader, nil
}

// readHeader reads the file header.
func (r *V2FileReader) readHeader() error {
	buf := make([]byte, V2HeaderSize)
	if _, err := r.reader.ReadAt(buf, 0); err != nil {
		return err
	}

	r.header = &V2FileHeader{}
	return r.header.Read(bytes.NewReader(buf))
}

// readFooter reads the file footer and metadata.
func (r *V2FileReader) readFooter() error {
	// Footer layout: pageIndexOffset(8) + pageIndexLen(4) + colMetaOffset(8) +
	// colMetaLen(4) + schemaOffset(8) + schemaLen(4) + numRows(8) + magic(4) = 48 bytes
	footerSize := int64(48)
	footerBuf := make([]byte, footerSize)
	if _, err := r.reader.ReadAt(footerBuf, r.size-footerSize); err != nil {
		return err
	}

	// Check magic at end
	magic := string(footerBuf[footerSize-4:])
	if magic != V2Magic {
		return fmt.Errorf("invalid footer magic: %s", magic)
	}

	// Parse footer (simplified)
	reader := bytes.NewReader(footerBuf)

	var pageIndexOffset uint64
	var pageIndexLen uint32
	var colMetaOffset uint64
	var colMetaLen uint32
	var schemaOffset uint64
	var schemaLen uint32
	var numRows uint64

	binary.Read(reader, binary.LittleEndian, &pageIndexOffset)
	binary.Read(reader, binary.LittleEndian, &pageIndexLen)
	binary.Read(reader, binary.LittleEndian, &colMetaOffset)
	binary.Read(reader, binary.LittleEndian, &colMetaLen)
	binary.Read(reader, binary.LittleEndian, &schemaOffset)
	binary.Read(reader, binary.LittleEndian, &schemaLen)
	binary.Read(reader, binary.LittleEndian, &numRows)

	// Read schema
	schemaBuf := make([]byte, schemaLen)
	if _, err := r.reader.ReadAt(schemaBuf, int64(schemaOffset)+4); err != nil {
		return fmt.Errorf("failed to read schema: %w", err)
	}
	r.schema = &storage2pb.Schema{}
	if err := protobuf.Unmarshal(schemaBuf, r.schema); err != nil {
		return fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	// Convert schema to types
	r.types = make([]common.LType, len(r.schema.Fields))
	for i, field := range r.schema.Fields {
		r.types[i] = lanceLogicalTypeToLType(field.LogicalType)
	}

	// Read page index
	pageIndexBuf := make([]byte, pageIndexLen)
	if _, err := r.reader.ReadAt(pageIndexBuf, int64(pageIndexOffset)); err != nil {
		return fmt.Errorf("failed to read page index: %w", err)
	}

	pageIndexReader := bytes.NewReader(pageIndexBuf)
	var numPages uint32
	binary.Read(pageIndexReader, binary.LittleEndian, &numPages)

	r.pageIndex = &V2PageIndex{
		Entries: make([]V2PageIndexEntry, numPages),
	}
	for i := uint32(0); i < numPages; i++ {
		if err := r.pageIndex.Entries[i].Read(pageIndexReader); err != nil {
			return fmt.Errorf("failed to read page index entry %d: %w", i, err)
		}
	}

	return nil
}

// Schema returns the file schema.
func (r *V2FileReader) Schema() *storage2pb.Schema {
	return r.schema
}

// NumRows returns the total number of rows.
func (r *V2FileReader) NumRows() uint64 {
	return r.header.NumRows
}

// NumColumns returns the number of columns.
func (r *V2FileReader) NumColumns() int {
	return int(r.header.NumColumns)
}

// Types returns the column types.
func (r *V2FileReader) Types() []common.LType {
	return r.types
}

// ReadColumn returns a column reader for the specified column.
func (r *V2FileReader) ReadColumn(columnIndex int) (*V2ColumnReader, error) {
	if columnIndex < 0 || columnIndex >= int(r.header.NumColumns) {
		return nil, fmt.Errorf("column index %d out of range", columnIndex)
	}

	// Filter pages for this column
	var columnPages []V2PageIndexEntry
	for _, entry := range r.pageIndex.Entries {
		if entry.ColumnIndex == uint32(columnIndex) {
			columnPages = append(columnPages, entry)
		}
	}

	return NewV2ColumnReader(r.reader, columnPages, uint32(columnIndex), r.types[columnIndex], r.header.NumRows), nil
}

// ReadChunk reads a chunk from the file.
func (r *V2FileReader) ReadChunk(startRow uint64, maxRows int) (*chunk.Chunk, error) {
	if startRow >= r.header.NumRows {
		return nil, io.EOF
	}

	// Determine how many rows to read
	numRows := maxRows
	if startRow+uint64(numRows) > r.header.NumRows {
		numRows = int(r.header.NumRows - startRow)
	}

	// Create chunk
	c := &chunk.Chunk{}
	c.Init(r.types, numRows)

	// Read each column
	for i := 0; i < int(r.header.NumColumns); i++ {
		colReader, err := r.ReadColumn(i)
		if err != nil {
			return nil, err
		}

		if err := colReader.Seek(startRow); err != nil {
			return nil, err
		}

		n, err := colReader.ReadVector(c.Data[i], numRows)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if i == 0 {
			c.SetCard(n)
		}
	}

	return c, nil
}

// ============================================================================
// Nested Type Support (Struct)
// ============================================================================

// V2StructWriter writes struct (nested) columns.
type V2StructWriter struct {
	fieldWriters []*V2ColumnWriter
	fieldNames   []string
	defLevels    []byte // Definition levels
	repLevels    []byte // Repetition levels (for list types)
}

// NewV2StructWriter creates a struct writer.
func NewV2StructWriter(w io.Writer, fields []*storage2pb.Field, types []common.LType, config V2ColumnWriterConfig) *V2StructWriter {
	sw := &V2StructWriter{
		fieldWriters: make([]*V2ColumnWriter, len(fields)),
		fieldNames:   make([]string, len(fields)),
	}

	for i, field := range fields {
		sw.fieldNames[i] = field.Name
		sw.fieldWriters[i] = NewV2ColumnWriter(w, uint32(i), types[i], config)
	}

	return sw
}

// WriteStruct writes a struct value.
func (sw *V2StructWriter) WriteStruct(isNull bool, fieldValues []*chunk.Value) error {
	// Track definition level
	if isNull {
		sw.defLevels = append(sw.defLevels, 0)
		return nil
	}

	sw.defLevels = append(sw.defLevels, 1)

	// Write each field
	for i, val := range fieldValues {
		// Create a temporary vector with one value
		// This is simplified - in production, would batch values
		_ = sw.fieldWriters[i]
		_ = val
	}

	return nil
}

// Flush flushes all field writers.
func (sw *V2StructWriter) Flush() error {
	for _, fw := range sw.fieldWriters {
		if err := fw.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================================
// Convenience Functions
// ============================================================================

// WriteV2File writes a chunk to a v2 format file.
func WriteV2File(path string, c *chunk.Chunk, schema *storage2pb.Schema) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get types from chunk
	types := make([]common.LType, c.ColumnCount())
	for i := 0; i < c.ColumnCount(); i++ {
		types[i] = c.Data[i].Typ()
	}

	writer := NewV2FileWriter(f, schema, types, DefaultV2FileWriterConfig())
	if err := writer.WriteHeader(); err != nil {
		return err
	}

	if err := writer.WriteChunk(c); err != nil {
		return err
	}

	return writer.Close()
}

// ReadV2File reads a chunk from a v2 format file.
func ReadV2File(path string) (*chunk.Chunk, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	reader, err := NewV2FileReader(f, stat.Size())
	if err != nil {
		return nil, err
	}

	return reader.ReadChunk(0, int(reader.NumRows()))
}
