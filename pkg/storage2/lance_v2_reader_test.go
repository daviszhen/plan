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
	"os"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
	"github.com/daviszhen/plan/pkg/util"
)

// ============================================================================
// Test helpers
// ============================================================================

// makeSchema creates a Lance-compatible protobuf schema for the given types.
func makeSchema(types []common.LType) *storage2pb.Schema {
	fields := make([]*storage2pb.Field, len(types))
	for i, typ := range types {
		fields[i] = &storage2pb.Field{
			Type:        storage2pb.Field_LEAF,
			Name:        "",
			Id:          int32(i + 1),
			ParentId:    -1,
			LogicalType: ltypeToLanceLogicalType(typ),
			Nullable:    true,
		}
	}
	return &storage2pb.Schema{Fields: fields}
}

// makeIntegerChunk creates a chunk with nCols INTEGER columns and nRows rows.
// Values are deterministic: col i, row j => value = i*1000 + j.
func makeIntegerChunk(nCols, nRows int) *chunk.Chunk {
	types := make([]common.LType, nCols)
	for i := range types {
		types[i] = common.IntegerType()
	}
	c := &chunk.Chunk{}
	cap := util.DefaultVectorSize
	if nRows > cap {
		cap = nRows
	}
	c.Init(types, cap)
	c.SetCard(nRows)
	for col := 0; col < nCols; col++ {
		for row := 0; row < nRows; row++ {
			c.Data[col].SetValue(row, &chunk.Value{
				Typ: common.IntegerType(),
				I64: int64(col*1000 + row),
			})
		}
	}
	return c
}

// writeAndReadV2File is a helper that writes a chunk to a temp file and reads it back.
func writeAndReadV2File(t *testing.T, c *chunk.Chunk, schema *storage2pb.Schema) *chunk.Chunk {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lance")

	if err := WriteV2File(path, c, schema); err != nil {
		t.Fatalf("WriteV2File failed: %v", err)
	}

	got, err := ReadV2File(path)
	if err != nil {
		t.Fatalf("ReadV2File failed: %v", err)
	}
	return got
}

// writeV2FileAndOpenReader writes a chunk to a temp file and returns a V2FileReader.
func writeV2FileAndOpenReader(t *testing.T, c *chunk.Chunk, schema *storage2pb.Schema) (*V2FileReader, *os.File) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lance")

	if err := WriteV2File(path, c, schema); err != nil {
		t.Fatalf("WriteV2File failed: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open failed: %v", err)
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatalf("Stat failed: %v", err)
	}

	reader, err := NewV2FileReader(f, stat.Size())
	if err != nil {
		f.Close()
		t.Fatalf("NewV2FileReader failed: %v", err)
	}

	return reader, f
}

// writeV2WithConfig writes a chunk using a custom config and returns the file path.
func writeV2WithConfig(t *testing.T, c *chunk.Chunk, schema *storage2pb.Schema, config V2FileWriterConfig) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lance")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("os.Create failed: %v", err)
	}
	defer f.Close()

	types := make([]common.LType, c.ColumnCount())
	for i := 0; i < c.ColumnCount(); i++ {
		types[i] = c.Data[i].Typ()
	}

	writer := NewV2FileWriter(f, schema, types, config)
	if err := writer.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}
	if err := writer.WriteChunk(c); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	return path
}

// ============================================================================
// Test 1: TestV2FileWriteAndReadRoundtrip
// ============================================================================

// TestV2FileWriteAndReadRoundtrip writes a chunk with 2 INTEGER columns
// (100 rows), reads it back, and verifies all values match.
func TestV2FileWriteAndReadRoundtrip(t *testing.T) {
	const nCols = 2
	const nRows = 100

	types := []common.LType{common.IntegerType(), common.IntegerType()}
	schema := makeSchema(types)

	c := makeIntegerChunk(nCols, nRows)

	got := writeAndReadV2File(t, c, schema)

	// Verify dimensions
	if got.Card() != nRows {
		t.Fatalf("row count mismatch: got %d, want %d", got.Card(), nRows)
	}
	if got.ColumnCount() != nCols {
		t.Fatalf("column count mismatch: got %d, want %d", got.ColumnCount(), nCols)
	}

	// Verify every value
	for col := 0; col < nCols; col++ {
		for row := 0; row < nRows; row++ {
			expected := int64(col*1000 + row)
			val := got.Data[col].GetValue(row)
			if val.I64 != expected {
				t.Errorf("col %d row %d: got %d, want %d", col, row, val.I64, expected)
			}
		}
	}
}

// ============================================================================
// Test 2: TestV2FileWriteAndReadMultipleTypes
// ============================================================================

// TestV2FileWriteAndReadMultipleTypes writes a chunk with INTEGER, BIGINT,
// and FLOAT columns (50 rows) and verifies roundtrip.
func TestV2FileWriteAndReadMultipleTypes(t *testing.T) {
	const nRows = 50

	types := []common.LType{
		common.IntegerType(),
		common.BigintType(),
		common.FloatType(),
	}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(nRows)

	for row := 0; row < nRows; row++ {
		// INTEGER column
		c.Data[0].SetValue(row, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(row * 10),
		})
		// BIGINT column
		c.Data[1].SetValue(row, &chunk.Value{
			Typ: common.BigintType(),
			I64: int64(row) * 100000,
		})
		// FLOAT column
		c.Data[2].SetValue(row, &chunk.Value{
			Typ: common.FloatType(),
			F64: float64(row) * 1.5,
		})
	}

	got := writeAndReadV2File(t, c, schema)

	if got.Card() != nRows {
		t.Fatalf("row count mismatch: got %d, want %d", got.Card(), nRows)
	}
	if got.ColumnCount() != 3 {
		t.Fatalf("column count mismatch: got %d, want 3", got.ColumnCount())
	}

	for row := 0; row < nRows; row++ {
		// INTEGER
		v0 := got.Data[0].GetValue(row)
		if v0.I64 != int64(row*10) {
			t.Errorf("INTEGER col row %d: got %d, want %d", row, v0.I64, row*10)
		}

		// BIGINT
		v1 := got.Data[1].GetValue(row)
		if v1.I64 != int64(row)*100000 {
			t.Errorf("BIGINT col row %d: got %d, want %d", row, v1.I64, int64(row)*100000)
		}

		// FLOAT - compare with tolerance since float32 roundtrip loses precision
		v2 := got.Data[2].GetValue(row)
		expected := float64(float32(float64(row) * 1.5))
		if v2.F64 != expected {
			t.Errorf("FLOAT col row %d: got %f, want %f", row, v2.F64, expected)
		}
	}
}

// ============================================================================
// Test 3: TestV2FileReaderSchema
// ============================================================================

// TestV2FileReaderSchema writes a file, opens a reader, and verifies
// Schema(), NumRows(), NumColumns(), Types().
func TestV2FileReaderSchema(t *testing.T) {
	const nRows = 30

	types := []common.LType{
		common.IntegerType(),
		common.BigintType(),
	}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(nRows)
	for row := 0; row < nRows; row++ {
		c.Data[0].SetValue(row, &chunk.Value{Typ: common.IntegerType(), I64: int64(row)})
		c.Data[1].SetValue(row, &chunk.Value{Typ: common.BigintType(), I64: int64(row * 100)})
	}

	reader, f := writeV2FileAndOpenReader(t, c, schema)
	defer f.Close()

	// Verify NumRows
	if reader.NumRows() != uint64(nRows) {
		t.Errorf("NumRows: got %d, want %d", reader.NumRows(), nRows)
	}

	// Verify NumColumns
	if reader.NumColumns() != 2 {
		t.Errorf("NumColumns: got %d, want 2", reader.NumColumns())
	}

	// Verify Types
	gotTypes := reader.Types()
	if len(gotTypes) != 2 {
		t.Fatalf("Types length: got %d, want 2", len(gotTypes))
	}
	if gotTypes[0].Id != common.LTID_INTEGER {
		t.Errorf("Types[0]: got %v, want INTEGER", gotTypes[0].Id)
	}
	if gotTypes[1].Id != common.LTID_BIGINT {
		t.Errorf("Types[1]: got %v, want BIGINT", gotTypes[1].Id)
	}

	// Verify Schema is not nil and has correct field count
	gotSchema := reader.Schema()
	if gotSchema == nil {
		t.Fatal("Schema returned nil")
	}
	if len(gotSchema.Fields) != 2 {
		t.Errorf("Schema fields: got %d, want 2", len(gotSchema.Fields))
	}
}

// ============================================================================
// Test 4: TestV2FileReaderReadColumn
// ============================================================================

// TestV2FileReaderReadColumn writes a file, uses ReadColumn() to read
// individual columns, and verifies values.
func TestV2FileReaderReadColumn(t *testing.T) {
	const nRows = 40

	types := []common.LType{common.IntegerType(), common.IntegerType()}
	schema := makeSchema(types)
	c := makeIntegerChunk(2, nRows)

	reader, f := writeV2FileAndOpenReader(t, c, schema)
	defer f.Close()

	// Read column 0
	colReader0, err := reader.ReadColumn(0)
	if err != nil {
		t.Fatalf("ReadColumn(0) failed: %v", err)
	}

	vec0 := chunk.NewVector2(common.IntegerType(), nRows)
	n0, err := colReader0.ReadVector(vec0, nRows)
	if err != nil {
		t.Fatalf("ReadVector for col 0 failed: %v", err)
	}
	if n0 != nRows {
		t.Fatalf("ReadVector col 0: got %d rows, want %d", n0, nRows)
	}

	for row := 0; row < nRows; row++ {
		val := vec0.GetValue(row)
		expected := int64(0*1000 + row) // col 0
		if val.I64 != expected {
			t.Errorf("col 0 row %d: got %d, want %d", row, val.I64, expected)
		}
	}

	// Read column 1
	colReader1, err := reader.ReadColumn(1)
	if err != nil {
		t.Fatalf("ReadColumn(1) failed: %v", err)
	}

	vec1 := chunk.NewVector2(common.IntegerType(), nRows)
	n1, err := colReader1.ReadVector(vec1, nRows)
	if err != nil {
		t.Fatalf("ReadVector for col 1 failed: %v", err)
	}
	if n1 != nRows {
		t.Fatalf("ReadVector col 1: got %d rows, want %d", n1, nRows)
	}

	for row := 0; row < nRows; row++ {
		val := vec1.GetValue(row)
		expected := int64(1*1000 + row) // col 1
		if val.I64 != expected {
			t.Errorf("col 1 row %d: got %d, want %d", row, val.I64, expected)
		}
	}

	// Out-of-range column should error
	_, err = reader.ReadColumn(-1)
	if err == nil {
		t.Error("ReadColumn(-1) should return error")
	}
	_, err = reader.ReadColumn(2)
	if err == nil {
		t.Error("ReadColumn(2) should return error for 2-column file")
	}
}

// ============================================================================
// Test 5: TestV2FileReaderReadChunkPartial
// ============================================================================

// TestV2FileReaderReadChunkPartial writes 100 rows, then uses
// ReadChunk(startRow=50, maxRows=20) for a partial read.
func TestV2FileReaderReadChunkPartial(t *testing.T) {
	const totalRows = 100
	const startRow = 50
	const maxRows = 20

	types := []common.LType{common.IntegerType()}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(totalRows)
	for row := 0; row < totalRows; row++ {
		c.Data[0].SetValue(row, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(row),
		})
	}

	reader, f := writeV2FileAndOpenReader(t, c, schema)
	defer f.Close()

	got, err := reader.ReadChunk(startRow, maxRows)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}

	if got.Card() != maxRows {
		t.Fatalf("partial read row count: got %d, want %d", got.Card(), maxRows)
	}

	for row := 0; row < got.Card(); row++ {
		val := got.Data[0].GetValue(row)
		expected := int64(startRow + row)
		if val.I64 != expected {
			t.Errorf("row %d: got %d, want %d", row, val.I64, expected)
		}
	}
}

// ============================================================================
// Test 6: TestV2FileReaderReadChunkAll
// ============================================================================

// TestV2FileReaderReadChunkAll writes 100 rows, then uses
// ReadChunk(0, 100) for a full read.
func TestV2FileReaderReadChunkAll(t *testing.T) {
	const nRows = 100

	types := []common.LType{common.IntegerType()}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(nRows)
	for row := 0; row < nRows; row++ {
		c.Data[0].SetValue(row, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(row * 7),
		})
	}

	reader, f := writeV2FileAndOpenReader(t, c, schema)
	defer f.Close()

	got, err := reader.ReadChunk(0, nRows)
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}

	if got.Card() != nRows {
		t.Fatalf("full read row count: got %d, want %d", got.Card(), nRows)
	}

	for row := 0; row < nRows; row++ {
		val := got.Data[0].GetValue(row)
		expected := int64(row * 7)
		if val.I64 != expected {
			t.Errorf("row %d: got %d, want %d", row, val.I64, expected)
		}
	}

	// Reading past end should return EOF
	_, err = reader.ReadChunk(uint64(nRows), 1)
	if err == nil {
		t.Error("ReadChunk past end should return error")
	}
}

// ============================================================================
// Test 7: TestV2FileWriterWithCompression
// ============================================================================

// TestV2FileWriterWithCompression tests writing and reading with LZ4 and
// Zstd compression, verifying roundtrip correctness.
func TestV2FileWriterWithCompression(t *testing.T) {
	compressions := []struct {
		name string
		comp V2CompressionType
	}{
		{"LZ4", V2CompressionLZ4},
		{"Zstd", V2CompressionZstd},
	}

	for _, tc := range compressions {
		t.Run(tc.name, func(t *testing.T) {
			const nRows = 100

			types := []common.LType{common.IntegerType(), common.BigintType()}
			schema := makeSchema(types)

			c := &chunk.Chunk{}
			c.Init(types, util.DefaultVectorSize)
			c.SetCard(nRows)
			for row := 0; row < nRows; row++ {
				c.Data[0].SetValue(row, &chunk.Value{
					Typ: common.IntegerType(),
					I64: int64(row),
				})
				c.Data[1].SetValue(row, &chunk.Value{
					Typ: common.BigintType(),
					I64: int64(row * 999),
				})
			}

			config := DefaultV2FileWriterConfig()
			config.Compression = tc.comp

			path := writeV2WithConfig(t, c, schema, config)

			// Read back
			got, err := ReadV2File(path)
			if err != nil {
				t.Fatalf("ReadV2File failed: %v", err)
			}

			if got.Card() != nRows {
				t.Fatalf("row count: got %d, want %d", got.Card(), nRows)
			}

			for row := 0; row < nRows; row++ {
				v0 := got.Data[0].GetValue(row)
				if v0.I64 != int64(row) {
					t.Errorf("col 0 row %d: got %d, want %d", row, v0.I64, row)
				}
				v1 := got.Data[1].GetValue(row)
				if v1.I64 != int64(row*999) {
					t.Errorf("col 1 row %d: got %d, want %d", row, v1.I64, row*999)
				}
			}
		})
	}
}

// ============================================================================
// Test 8: TestV2FileWriterWithStats
// ============================================================================

// TestV2FileWriterWithStats writes with ComputeStats=true and verifies
// that Stats() returns a non-nil result.
func TestV2FileWriterWithStats(t *testing.T) {
	const nRows = 50

	types := []common.LType{common.IntegerType()}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(nRows)
	for row := 0; row < nRows; row++ {
		c.Data[0].SetValue(row, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(row),
		})
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "stats.lance")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("os.Create failed: %v", err)
	}
	defer f.Close()

	config := DefaultV2FileWriterConfig()
	config.ComputeStats = true

	writer := NewV2FileWriter(f, schema, types, config)
	if err := writer.WriteHeader(); err != nil {
		t.Fatalf("WriteHeader failed: %v", err)
	}
	if err := writer.WriteChunk(c); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify column writer stats are populated
	if len(writer.columnWriters) == 0 {
		t.Fatal("no column writers after write")
	}

	stats := writer.columnWriters[0].Stats()
	if stats == nil {
		t.Error("Stats() returned nil with ComputeStats=true")
	}
}

// ============================================================================
// Test 9: TestV2FileEmptyChunk
// ============================================================================

// TestV2FileEmptyChunk writes and reads a chunk with 0 rows.
func TestV2FileEmptyChunk(t *testing.T) {
	types := []common.LType{common.IntegerType()}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(0)

	dir := t.TempDir()
	path := filepath.Join(dir, "empty.lance")

	if err := WriteV2File(path, c, schema); err != nil {
		t.Fatalf("WriteV2File failed: %v", err)
	}

	// Open file and verify header
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("os.Open failed: %v", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	reader, err := NewV2FileReader(f, stat.Size())
	if err != nil {
		t.Fatalf("NewV2FileReader failed: %v", err)
	}

	if reader.NumRows() != 0 {
		t.Errorf("NumRows: got %d, want 0", reader.NumRows())
	}

	if reader.NumColumns() != 1 {
		t.Errorf("NumColumns: got %d, want 1", reader.NumColumns())
	}
}

// ============================================================================
// Test 10: TestV2FileMultipleChunks
// ============================================================================

// TestV2FileMultipleChunks writes multiple chunks to the same file and
// reads all data back.
func TestV2FileMultipleChunks(t *testing.T) {
	const rowsPerChunk = 50
	const numChunks = 3
	const totalRows = rowsPerChunk * numChunks

	types := []common.LType{common.IntegerType()}
	schema := makeSchema(types)

	dir := t.TempDir()
	path := filepath.Join(dir, "multi.lance")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("os.Create failed: %v", err)
	}

	writer := NewV2FileWriter(f, schema, types, DefaultV2FileWriterConfig())
	if err := writer.WriteHeader(); err != nil {
		f.Close()
		t.Fatalf("WriteHeader failed: %v", err)
	}

	// Write multiple chunks
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		c := &chunk.Chunk{}
		c.Init(types, util.DefaultVectorSize)
		c.SetCard(rowsPerChunk)
		for row := 0; row < rowsPerChunk; row++ {
			globalRow := chunkIdx*rowsPerChunk + row
			c.Data[0].SetValue(row, &chunk.Value{
				Typ: common.IntegerType(),
				I64: int64(globalRow),
			})
		}
		if err := writer.WriteChunk(c); err != nil {
			f.Close()
			t.Fatalf("WriteChunk %d failed: %v", chunkIdx, err)
		}
	}

	if err := writer.Close(); err != nil {
		f.Close()
		t.Fatalf("Close failed: %v", err)
	}
	f.Close()

	// Read back
	got, err := ReadV2File(path)
	if err != nil {
		t.Fatalf("ReadV2File failed: %v", err)
	}

	if got.Card() != totalRows {
		t.Fatalf("row count: got %d, want %d", got.Card(), totalRows)
	}

	for row := 0; row < totalRows; row++ {
		val := got.Data[0].GetValue(row)
		if val.I64 != int64(row) {
			t.Errorf("row %d: got %d, want %d", row, val.I64, row)
		}
	}
}

// ============================================================================
// Test 11: TestV2FileLargeDataset
// ============================================================================

// TestV2FileLargeDataset writes 10000 rows and verifies roundtrip.
func TestV2FileLargeDataset(t *testing.T) {
	const nRows = 10000

	types := []common.LType{common.IntegerType(), common.BigintType()}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, nRows)
	c.SetCard(nRows)
	for row := 0; row < nRows; row++ {
		c.Data[0].SetValue(row, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(row),
		})
		c.Data[1].SetValue(row, &chunk.Value{
			Typ: common.BigintType(),
			I64: int64(row) * int64(row),
		})
	}

	got := writeAndReadV2File(t, c, schema)

	if got.Card() != nRows {
		t.Fatalf("row count: got %d, want %d", got.Card(), nRows)
	}

	// Spot-check a selection of rows
	spotChecks := []int{0, 1, 100, 999, 5000, 9999}
	for _, row := range spotChecks {
		v0 := got.Data[0].GetValue(row)
		if v0.I64 != int64(row) {
			t.Errorf("col 0 row %d: got %d, want %d", row, v0.I64, row)
		}
		v1 := got.Data[1].GetValue(row)
		expected := int64(row) * int64(row)
		if v1.I64 != expected {
			t.Errorf("col 1 row %d: got %d, want %d", row, v1.I64, expected)
		}
	}
}

// ============================================================================
// Test 12: TestV2FileColumnReaderSeek
// ============================================================================

// TestV2FileColumnReaderSeek writes data, gets a column reader, seeks to
// row 50, and reads remaining values.
func TestV2FileColumnReaderSeek(t *testing.T) {
	const totalRows = 100
	const seekRow = 50

	types := []common.LType{common.IntegerType()}
	schema := makeSchema(types)

	c := &chunk.Chunk{}
	c.Init(types, util.DefaultVectorSize)
	c.SetCard(totalRows)
	for row := 0; row < totalRows; row++ {
		c.Data[0].SetValue(row, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(row),
		})
	}

	reader, f := writeV2FileAndOpenReader(t, c, schema)
	defer f.Close()

	colReader, err := reader.ReadColumn(0)
	if err != nil {
		t.Fatalf("ReadColumn(0) failed: %v", err)
	}

	// Seek to row 50
	if err := colReader.Seek(seekRow); err != nil {
		t.Fatalf("Seek(%d) failed: %v", seekRow, err)
	}

	// Read remaining
	remaining := totalRows - seekRow
	vec := chunk.NewVector2(common.IntegerType(), remaining)
	n, err := colReader.ReadVector(vec, remaining)
	if err != nil {
		t.Fatalf("ReadVector after seek failed: %v", err)
	}

	if n != remaining {
		t.Fatalf("read count after seek: got %d, want %d", n, remaining)
	}

	for i := 0; i < n; i++ {
		val := vec.GetValue(i)
		expected := int64(seekRow + i)
		if val.I64 != expected {
			t.Errorf("after seek, index %d: got %d, want %d", i, val.I64, expected)
		}
	}

	// Seeking out-of-range should error
	if err := colReader.Seek(uint64(totalRows)); err == nil {
		t.Error("Seek to totalRows should return error")
	}
}

// ============================================================================
// Test 13: TestV2FileCorruptedFile
// ============================================================================

// TestV2FileCorruptedFile tries to read from too-short data and verifies
// that an error is returned.
func TestV2FileCorruptedFile(t *testing.T) {
	// A file that is too short to contain even a header
	shortData := []byte("LNC2tooshort")
	r := bytes.NewReader(shortData)

	_, err := NewV2FileReader(r, int64(len(shortData)))
	if err == nil {
		t.Error("expected error for corrupted/short file")
	}

	// A file that is exactly header size but has no footer
	headerOnly := make([]byte, V2HeaderSize)
	copy(headerOnly[0:4], []byte("LNC2"))
	r2 := bytes.NewReader(headerOnly)

	_, err = NewV2FileReader(r2, int64(len(headerOnly)))
	if err == nil {
		t.Error("expected error for header-only file (no footer)")
	}

	// Random garbage of decent length
	garbage := make([]byte, 256)
	for i := range garbage {
		garbage[i] = byte(i % 256)
	}
	r3 := bytes.NewReader(garbage)

	_, err = NewV2FileReader(r3, int64(len(garbage)))
	if err == nil {
		t.Error("expected error for garbage data")
	}
}

// ============================================================================
// Test 14: TestV2FileWriterCustomPageSize
// ============================================================================

// TestV2FileWriterCustomPageSize tests with small (4KB) and large (1MB)
// page sizes and verifies roundtrip correctness.
func TestV2FileWriterCustomPageSize(t *testing.T) {
	pageSizes := []struct {
		name     string
		pageSize int
	}{
		{"4KB", 4 * 1024},
		{"1MB", 1024 * 1024},
	}

	for _, tc := range pageSizes {
		t.Run(tc.name, func(t *testing.T) {
			const nRows = 200

			types := []common.LType{common.IntegerType()}
			schema := makeSchema(types)

			c := &chunk.Chunk{}
			c.Init(types, util.DefaultVectorSize)
			c.SetCard(nRows)
			for row := 0; row < nRows; row++ {
				c.Data[0].SetValue(row, &chunk.Value{
					Typ: common.IntegerType(),
					I64: int64(row * 3),
				})
			}

			config := DefaultV2FileWriterConfig()
			config.PageSize = tc.pageSize

			path := writeV2WithConfig(t, c, schema, config)

			// Read back
			got, err := ReadV2File(path)
			if err != nil {
				t.Fatalf("ReadV2File failed: %v", err)
			}

			if got.Card() != nRows {
				t.Fatalf("row count: got %d, want %d", got.Card(), nRows)
			}

			for row := 0; row < nRows; row++ {
				val := got.Data[0].GetValue(row)
				expected := int64(row * 3)
				if val.I64 != expected {
					t.Errorf("row %d: got %d, want %d", row, val.I64, expected)
				}
			}
		})
	}
}

// ============================================================================
// Test 15: TestV2ColumnWriterFlush
// ============================================================================

// TestV2ColumnWriterFlush writes values to a column writer, flushes,
// and verifies that pages are created.
func TestV2ColumnWriterFlush(t *testing.T) {
	const nRows = 100

	buf := new(bytes.Buffer)
	config := DefaultV2ColumnWriterConfig()
	config.PageSize = 128 // Very small page to force multiple pages

	colWriter := NewV2ColumnWriter(buf, 0, common.IntegerType(), config)

	// Create a vector with values
	vec := chunk.NewVector2(common.IntegerType(), util.DefaultVectorSize)
	for i := 0; i < nRows; i++ {
		vec.SetValue(i, &chunk.Value{
			Typ: common.IntegerType(),
			I64: int64(i),
		})
	}

	// Write vector
	if err := colWriter.WriteVector(vec, nRows); err != nil {
		t.Fatalf("WriteVector failed: %v", err)
	}

	// Flush remaining data
	if err := colWriter.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	pages := colWriter.Pages()
	if len(pages) == 0 {
		t.Error("expected at least 1 page after write + flush")
	}

	// With a 128-byte page size and 4 bytes/int, we expect pages to be created
	t.Logf("pages created: %d (page_size=%d, rows=%d)", len(pages), config.PageSize, nRows)

	totalBytes := colWriter.TotalBytes()
	if totalBytes == 0 {
		t.Error("expected non-zero total bytes after writing")
	}

	// Verify each page entry has valid metadata
	for i, page := range pages {
		if page.NumRows == 0 {
			t.Errorf("page %d: NumRows should not be 0", i)
		}
		if page.Length == 0 {
			t.Errorf("page %d: Length should not be 0", i)
		}
		if page.ColumnIndex != 0 {
			t.Errorf("page %d: ColumnIndex should be 0, got %d", i, page.ColumnIndex)
		}
	}

	stats := colWriter.Stats()
	if stats == nil {
		t.Error("Stats() returned nil")
	}
}
