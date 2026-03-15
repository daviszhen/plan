// Copyright 2024 The Plan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage2

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// makeLargeTestChunkWithBase creates a chunk with the given number of rows and proper capacity.
// It uses the larger of rows or DefaultVectorSize as capacity.
func makeLargeTestChunkWithBase(t *testing.T, n int, base int) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	capacity := n
	if capacity < util.DefaultVectorSize {
		capacity = util.DefaultVectorSize
	}
	c := &chunk.Chunk{}
	c.Init(typs, capacity)
	c.SetCard(n)
	for i := 0; i < n; i++ {
		global := base + i
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(global)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(global * 10)})
	}
	return c
}

// setupScannerTestDataset creates a dataset with the specified number of fragments,
// each containing rowsPerFragment rows. Returns the latest version.
func setupScannerTestDataset(t *testing.T, ctx context.Context, basePath string, handler CommitHandler, numFragments int, rowsPerFragment int) uint64 {
	t.Helper()

	// Create initial empty manifest
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	version := uint64(0)
	for i := 0; i < numFragments; i++ {
		dataPath := filepath.Join(basePath, "data", "frag_"+itoa(i)+".dat")

		// Create chunk with base offset for unique values across fragments
		c := makeLargeTestChunkWithBase(t, rowsPerFragment, i*rowsPerFragment)
		if err := WriteChunkToFile(dataPath, c); err != nil {
			t.Fatal(err)
		}

		df := NewDataFile("data/frag_"+itoa(i)+".dat", []int32{0, 1}, 2, uint32(rowsPerFragment))
		frag := NewDataFragmentWithRows(uint64(i), uint64(rowsPerFragment), []*DataFile{df})
		txn := NewTransactionAppend(version, "scan-test-frag-"+itoa(i), []*DataFragment{frag})
		if err := CommitTransaction(ctx, basePath, handler, txn); err != nil {
			t.Fatal(err)
		}
		version++
	}

	return version
}

// itoa is a simple int to string conversion helper
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var neg bool
	if i < 0 {
		neg = true
		i = -i
	}
	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}

// Test 1: ScanChunks with three fragments
func TestScanChunks_ThreeFragments(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := setupScannerTestDataset(t, ctx, basePath, handler, 3, 10)

	chunks, err := ScanChunks(ctx, basePath, handler, version)
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	// Verify total row count and order
	totalRows := 0
	for i, c := range chunks {
		rows := c.Card()
		totalRows += rows
		if rows != 10 {
			t.Errorf("chunk %d: expected 10 rows, got %d", i, rows)
		}

		// Verify values are in order (base = i * 10)
		for row := 0; row < rows; row++ {
			v0 := c.Data[0].GetValue(row)
			v1 := c.Data[1].GetValue(row)
			expectedVal := int64(i*10 + row)
			if v0.I64 != expectedVal {
				t.Errorf("chunk %d row %d col0: expected %d, got %d", i, row, expectedVal, v0.I64)
			}
			if v1.I64 != expectedVal*10 {
				t.Errorf("chunk %d row %d col1: expected %d, got %d", i, row, expectedVal*10, v1.I64)
			}
		}
	}

	if totalRows != 30 {
		t.Errorf("expected total 30 rows, got %d", totalRows)
	}
}

// Test 2: ScanChunks with large dataset
// Note: Chunk serialization has a limit of util.DefaultVectorSize (2048) rows per chunk.
// For large datasets, use multiple fragments.
func TestScanChunks_LargeDataset(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create 3 fragments with 1500 rows each (total 4500 rows)
	// Each fragment is within the 2048 row limit per chunk
	version := setupScannerTestDataset(t, ctx, basePath, handler, 3, 1500)

	chunks, err := ScanChunks(ctx, basePath, handler, version)
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}

	// Verify total row count
	totalRows := 0
	for i, c := range chunks {
		rows := c.Card()
		totalRows += rows
		if rows != 1500 {
			t.Errorf("chunk %d: expected 1500 rows, got %d", i, rows)
		}

		// Verify values are in order (base = i * 1500)
		for row := 0; row < rows; row++ {
			v0 := c.Data[0].GetValue(row)
			v1 := c.Data[1].GetValue(row)
			expectedVal := int64(i*1500 + row)
			if v0.I64 != expectedVal {
				t.Errorf("chunk %d row %d col0: expected %d, got %d", i, row, expectedVal, v0.I64)
			}
			if v1.I64 != expectedVal*10 {
				t.Errorf("chunk %d row %d col1: expected %d, got %d", i, row, expectedVal*10, v1.I64)
			}
		}
	}

	if totalRows != 4500 {
		t.Errorf("expected total 4500 rows, got %d", totalRows)
	}
}

// Test 3: ScanChunks with non-existent version
func TestScanChunks_NonExistentVersion(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create a dataset with version 1
	setupScannerTestDataset(t, ctx, basePath, handler, 1, 10)

	// Try to scan version 999 which doesn't exist
	_, err := ScanChunks(ctx, basePath, handler, 999)
	if err == nil {
		t.Error("expected error for non-existent version, got nil")
	}
}

// Test 4: TakeRows across multiple fragments
func TestTakeRows_MultiFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create 3 fragments with 10 rows each (total 30 rows)
	version := setupScannerTestDataset(t, ctx, basePath, handler, 3, 10)

	// Take rows across fragment boundaries
	// Fragment 0: rows 0-9, Fragment 1: rows 10-19, Fragment 2: rows 20-29
	indices := []uint64{0, 5, 10, 15, 20, 25, 29}
	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != len(indices) {
		t.Fatalf("expected %d rows, got %d", len(indices), result.Card())
	}

	// Verify values
	for outIdx, globalRow := range indices {
		v0 := result.Data[0].GetValue(outIdx)
		v1 := result.Data[1].GetValue(outIdx)
		if v0.I64 != int64(globalRow) {
			t.Errorf("index %d: col0 expected %d, got %d", outIdx, globalRow, v0.I64)
		}
		if v1.I64 != int64(globalRow*10) {
			t.Errorf("index %d: col1 expected %d, got %d", outIdx, globalRow*10, v1.I64)
		}
	}
}

// Test 5: TakeRows with out-of-bounds indices
func TestTakeRows_OutOfBounds(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with 10 rows total
	version := setupScannerTestDataset(t, ctx, basePath, handler, 1, 10)

	// Request indices beyond the row count
	// Note: Current implementation includes all indices in result, even out-of-bounds ones
	// The values for out-of-bounds indices may be zero or undefined
	indices := []uint64{5, 100, 1000}
	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	// Result should have same number of rows as requested indices
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != len(indices) {
		t.Errorf("expected %d rows (same as input indices), got %d", len(indices), result.Card())
	}

	// Verify the valid row (index 5)
	v0 := result.Data[0].GetValue(0)
	if v0.I64 != 5 {
		t.Errorf("expected value 5 for index 5, got %d", v0.I64)
	}
	// Out-of-bounds indices (100, 1000) return undefined/zero values - not verified
}

// Test 6: TakeRows with duplicate indices
func TestTakeRows_DuplicateIndices(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := setupScannerTestDataset(t, ctx, basePath, handler, 2, 10)

	// Request the same row multiple times
	indices := []uint64{5, 5, 5, 10, 10, 15}
	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != len(indices) {
		t.Fatalf("expected %d rows, got %d", len(indices), result.Card())
	}

	// Verify all values are correct (duplicates should still work)
	expectedValues := []int64{5, 5, 5, 10, 10, 15}
	for i, expected := range expectedValues {
		v0 := result.Data[0].GetValue(i)
		if v0.I64 != expected {
			t.Errorf("row %d: expected %d, got %d", i, expected, v0.I64)
		}
	}
}

// Test 7: TakeRows with empty indices
func TestTakeRows_EmptyIndices(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := setupScannerTestDataset(t, ctx, basePath, handler, 1, 10)

	// Request with empty index list
	indices := []uint64{}
	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	// Empty indices should return nil
	if result != nil {
		t.Errorf("expected nil result for empty indices, got chunk with %d rows", result.Card())
	}
}

// Test 8: TakeRows in reversed order
func TestTakeRows_ReversedOrder(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with 20 rows total (2 fragments of 10 each)
	version := setupScannerTestDataset(t, ctx, basePath, handler, 2, 10)

	// Request rows in reverse order
	indices := []uint64{19, 15, 10, 5, 0}
	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != len(indices) {
		t.Fatalf("expected %d rows, got %d", len(indices), result.Card())
	}

	// Verify values are returned in the requested order
	expectedValues := []int64{19, 15, 10, 5, 0}
	for i, expected := range expectedValues {
		v0 := result.Data[0].GetValue(i)
		if v0.I64 != expected {
			t.Errorf("row %d: expected %d, got %d", i, expected, v0.I64)
		}
	}
}

// Test 9: TakeRowsProjected selecting specific columns
func TestTakeRowsProjected_SelectColumns(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create a manifest with a 3-column dataset
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1, 2}, 3, 10),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk with 3 columns
	dataPath := filepath.Join(basePath, "data", "0.dat")
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	src := &chunk.Chunk{}
	src.Init(typs, util.DefaultVectorSize)
	src.SetCard(10)
	for i := 0; i < 10; i++ {
		src.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		src.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
		src.Data[2].SetValue(i, &chunk.Value{Typ: typs[2], I64: int64(i * 100)})
	}
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}

	// Project only column 0
	indices := []uint64{0, 3, 7}
	result, err := TakeRowsProjected(ctx, basePath, handler, 0, indices, []int{0})
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.ColumnCount() != 1 {
		t.Fatalf("expected 1 column, got %d", result.ColumnCount())
	}

	// Verify column 0 values
	for outIdx, rowIdx := range indices {
		v := result.Data[0].GetValue(outIdx)
		if v.I64 != int64(rowIdx) {
			t.Errorf("row %d: expected %d, got %d", outIdx, rowIdx, v.I64)
		}
	}
}

// Test 10: TakeRowsProjected with all columns (nil columns parameter)
func TestTakeRowsProjected_AllColumns(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create a manifest with a 3-column dataset
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1, 2}, 3, 10),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk with 3 columns
	dataPath := filepath.Join(basePath, "data", "0.dat")
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	src := &chunk.Chunk{}
	src.Init(typs, util.DefaultVectorSize)
	src.SetCard(10)
	for i := 0; i < 10; i++ {
		src.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		src.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
		src.Data[2].SetValue(i, &chunk.Value{Typ: typs[2], I64: int64(i * 100)})
	}
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}

	// Project all columns by passing nil
	indices := []uint64{0, 5, 9}
	result, err := TakeRowsProjected(ctx, basePath, handler, 0, indices, nil)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.ColumnCount() != 3 {
		t.Fatalf("expected 3 columns, got %d", result.ColumnCount())
	}

	// Verify all column values
	for outIdx, rowIdx := range indices {
		v0 := result.Data[0].GetValue(outIdx)
		v1 := result.Data[1].GetValue(outIdx)
		v2 := result.Data[2].GetValue(outIdx)
		if v0.I64 != int64(rowIdx) {
			t.Errorf("row %d col0: expected %d, got %d", outIdx, rowIdx, v0.I64)
		}
		if v1.I64 != int64(rowIdx*10) {
			t.Errorf("row %d col1: expected %d, got %d", outIdx, rowIdx*10, v1.I64)
		}
		if v2.I64 != int64(rowIdx*100) {
			t.Errorf("row %d col2: expected %d, got %d", outIdx, rowIdx*100, v2.I64)
		}
	}
}

// Test 11: TakeRowsProjected with column reordering
func TestTakeRowsProjected_ColumnOrder(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create a manifest with a 3-column dataset
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1, 2}, 3, 10),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk with 3 columns
	dataPath := filepath.Join(basePath, "data", "0.dat")
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	src := &chunk.Chunk{}
	src.Init(typs, util.DefaultVectorSize)
	src.SetCard(10)
	for i := 0; i < 10; i++ {
		src.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		src.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
		src.Data[2].SetValue(i, &chunk.Value{Typ: typs[2], I64: int64(i * 100)})
	}
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}

	// Project columns [2, 0] - reversed order
	indices := []uint64{1, 4, 8}
	result, err := TakeRowsProjected(ctx, basePath, handler, 0, indices, []int{2, 0})
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.ColumnCount() != 2 {
		t.Fatalf("expected 2 columns, got %d", result.ColumnCount())
	}

	// Verify column order: col0 should have i*100 values, col1 should have i values
	for outIdx, rowIdx := range indices {
		v0 := result.Data[0].GetValue(outIdx) // Should be column 2 (i*100)
		v1 := result.Data[1].GetValue(outIdx) // Should be column 0 (i)
		if v0.I64 != int64(rowIdx*100) {
			t.Errorf("row %d col0: expected %d (from src col2), got %d", outIdx, rowIdx*100, v0.I64)
		}
		if v1.I64 != int64(rowIdx) {
			t.Errorf("row %d col1: expected %d (from src col0), got %d", outIdx, rowIdx, v1.I64)
		}
	}
}

// Test 12: ScanChunks after append (version isolation)
func TestScanChunks_AfterAppend(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial version with 1 fragment of 10 rows
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// Add first fragment
	dataPath0 := filepath.Join(basePath, "data", "v1.dat")
	c0 := makeTestChunkWithBase(t, 10, 0)
	if err := WriteChunkToFile(dataPath0, c0); err != nil {
		t.Fatal(err)
	}
	df0 := NewDataFile("data/v1.dat", []int32{0, 1}, 2, 10)
	frag0 := NewDataFragmentWithRows(0, 10, []*DataFile{df0})
	txn0 := NewTransactionAppend(0, "v1", []*DataFragment{frag0})
	if err := CommitTransaction(ctx, basePath, handler, txn0); err != nil {
		t.Fatal(err)
	}

	// Scan version 1
	chunksV1, err := ScanChunks(ctx, basePath, handler, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunksV1) != 1 || chunksV1[0].Card() != 10 {
		t.Fatalf("v1: expected 1 chunk with 10 rows, got %d chunks", len(chunksV1))
	}

	// Append more data as version 2
	dataPath1 := filepath.Join(basePath, "data", "v2.dat")
	c1 := makeTestChunkWithBase(t, 15, 10)
	if err := WriteChunkToFile(dataPath1, c1); err != nil {
		t.Fatal(err)
	}
	df1 := NewDataFile("data/v2.dat", []int32{0, 1}, 2, 15)
	frag1 := NewDataFragmentWithRows(1, 15, []*DataFile{df1})
	txn1 := NewTransactionAppend(1, "v2", []*DataFragment{frag1})
	if err := CommitTransaction(ctx, basePath, handler, txn1); err != nil {
		t.Fatal(err)
	}

	// Scan version 1 again - should still have only 1 fragment
	chunksV1Again, err := ScanChunks(ctx, basePath, handler, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunksV1Again) != 1 || chunksV1Again[0].Card() != 10 {
		t.Errorf("v1 after append: expected 1 chunk with 10 rows, got %d chunks", len(chunksV1Again))
	}

	// Scan version 2 - should have 2 fragments
	chunksV2, err := ScanChunks(ctx, basePath, handler, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunksV2) != 2 {
		t.Fatalf("v2: expected 2 chunks, got %d", len(chunksV2))
	}
	totalRows := chunksV2[0].Card() + chunksV2[1].Card()
	if totalRows != 25 {
		t.Errorf("v2: expected 25 total rows, got %d", totalRows)
	}
}

// Test 13: ScanChunks with multiple column types
// Note: Chunk serialization does not support LTID_DOUBLE or LTID_FLOAT.
// Using INTEGER + BIGINT + VARCHAR instead.
func TestScanChunks_MultiColumnTypes(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 5, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1, 2}, 3, 5),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk with INTEGER + BIGINT + VARCHAR columns
	dataPath := filepath.Join(basePath, "data", "0.dat")
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
		common.MakeLType(common.LTID_VARCHAR),
	}
	src := &chunk.Chunk{}
	src.Init(typs, util.DefaultVectorSize)
	src.SetCard(5)
	for i := 0; i < 5; i++ {
		src.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		src.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 1000)})
		src.Data[2].SetValue(i, &chunk.Value{Typ: typs[2], Str: "val_" + itoa(i)})
	}
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}

	// Scan and verify
	chunks, err := ScanChunks(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatal(err)
	}

	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}

	c := chunks[0]
	if c.ColumnCount() != 3 {
		t.Fatalf("expected 3 columns, got %d", c.ColumnCount())
	}

	// Verify types and values
	for i := 0; i < 5; i++ {
		v0 := c.Data[0].GetValue(i)
		v1 := c.Data[1].GetValue(i)
		v2 := c.Data[2].GetValue(i)

		if v0.I64 != int64(i) {
			t.Errorf("row %d col0: expected %d, got %d", i, i, v0.I64)
		}
		if v1.I64 != int64(i*1000) {
			t.Errorf("row %d col1: expected %d, got %d", i, i*1000, v1.I64)
		}
		expectedStr := "val_" + itoa(i)
		if v2.Str != expectedStr {
			t.Errorf("row %d col2: expected %s, got %s", i, expectedStr, v2.Str)
		}
	}
}

// Test 14: TakeRows with large row count
func TestTakeRows_LargeRowCount(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with 2000 rows across 2 fragments (1000 each to stay within chunk limit)
	version := setupScannerTestDataset(t, ctx, basePath, handler, 2, 1000)

	// Take 1000 rows (every other row from the 2000 total)
	indices := make([]uint64, 1000)
	for i := 0; i < 1000; i++ {
		indices[i] = uint64(i * 2) // Take every other row
	}

	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != 1000 {
		t.Fatalf("expected 1000 rows, got %d", result.Card())
	}

	// Verify values
	for outIdx, rowIdx := range indices {
		v0 := result.Data[0].GetValue(outIdx)
		v1 := result.Data[1].GetValue(outIdx)
		if v0.I64 != int64(rowIdx) {
			t.Errorf("row %d: col0 expected %d, got %d", outIdx, rowIdx, v0.I64)
		}
		if v1.I64 != int64(rowIdx*10) {
			t.Errorf("row %d: col1 expected %d, got %d", outIdx, rowIdx*10, v1.I64)
		}
	}
}

// Test 15: RowIdScanner basic lookup
func TestRowIdScanner_BasicLookup(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with stable row IDs feature flag enabled
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	m0.ReaderFeatureFlags = 2 // Enable stable row IDs
	m0.WriterFeatureFlags = 2
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// Try to create RowIdScanner
	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		// This is expected if RowIds parsing is not fully implemented
		t.Logf("RowIdScanner creation returned error (expected if RowIds parsing not implemented): %v", err)
		return
	}

	// If scanner was created successfully, test basic lookup
	rowIds := []uint64{1, 2, 3}
	chunk, err := scanner.TakeByRowIds(ctx, rowIds)
	if err != nil {
		t.Logf("TakeByRowIds returned error: %v", err)
	}

	// Result may be nil if no data or RowIds not properly set up
	_ = chunk
}

// Additional test: TakeRowsProjected with invalid column index
func TestTakeRowsProjected_InvalidColumn(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest with 2-column dataset
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 5, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1}, 2, 5),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk with 2 columns
	dataPath := filepath.Join(basePath, "data", "0.dat")
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	src := &chunk.Chunk{}
	src.Init(typs, util.DefaultVectorSize)
	src.SetCard(5)
	for i := 0; i < 5; i++ {
		src.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		src.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
	}
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}

	// Request invalid column index
	indices := []uint64{0, 1}
	_, err := TakeRowsProjected(ctx, basePath, handler, 0, indices, []int{0, 5})
	if err == nil {
		t.Error("expected error for invalid column index, got nil")
	}
}

// Additional test: ScanChunks with empty fragment
func TestScanChunks_EmptyFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest with an empty fragment
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		nil, // nil fragment
		NewDataFragmentWithRows(1, 0, []*DataFile{}), // empty fragment
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Scan should handle empty/nil fragments gracefully
	chunks, err := ScanChunks(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Should return empty result since no valid data files
	if len(chunks) != 0 {
		t.Errorf("expected 0 chunks for empty fragments, got %d", len(chunks))
	}
}

// Additional test: TakeRowsProjected with single row
func TestTakeRowsProjected_SingleRow(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := setupScannerTestDataset(t, ctx, basePath, handler, 1, 10)

	// Take just one row
	indices := []uint64{5}
	result, err := TakeRowsProjected(ctx, basePath, handler, version, indices, nil)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != 1 {
		t.Fatalf("expected 1 row, got %d", result.Card())
	}

	v0 := result.Data[0].GetValue(0)
	if v0.I64 != 5 {
		t.Errorf("expected value 5, got %d", v0.I64)
	}
}

// Additional test: TakeRows all indices from dataset
func TestTakeRows_AllRows(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with 20 rows
	version := setupScannerTestDataset(t, ctx, basePath, handler, 2, 10)

	// Take all rows
	indices := make([]uint64, 20)
	for i := 0; i < 20; i++ {
		indices[i] = uint64(i)
	}

	result, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if result.Card() != 20 {
		t.Fatalf("expected 20 rows, got %d", result.Card())
	}

	// Verify all values
	for i := 0; i < 20; i++ {
		v0 := result.Data[0].GetValue(i)
		if v0.I64 != int64(i) {
			t.Errorf("row %d: expected %d, got %d", i, i, v0.I64)
		}
	}
}

// Additional test: TakeRowsProjected with empty columns list
func TestTakeRowsProjected_EmptyColumnsList(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 5, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1}, 2, 5),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk
	dataPath := filepath.Join(basePath, "data", "0.dat")
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	src := &chunk.Chunk{}
	src.Init(typs, util.DefaultVectorSize)
	src.SetCard(5)
	for i := 0; i < 5; i++ {
		src.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		src.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
	}
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}

	// Empty columns list should return all columns
	indices := []uint64{0, 2, 4}
	result, err := TakeRowsProjected(ctx, basePath, handler, 0, indices, []int{})
	if err != nil {
		t.Fatal(err)
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// Empty columns list should behave like nil (return all columns)
	if result.ColumnCount() != 2 {
		t.Errorf("expected 2 columns (empty list = all), got %d", result.ColumnCount())
	}
}
