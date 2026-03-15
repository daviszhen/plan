package storage2

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func makeTestChunk(t *testing.T, n int) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(n)
	for i := 0; i < n; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 10)})
	}
	return c
}

func makeTestChunkWithBase(t *testing.T, n int, base int) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(n)
	for i := 0; i < n; i++ {
		global := base + i
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(global)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(global * 10)})
	}
	return c
}

func prepareManifestWithOneFragment(t *testing.T, ctx context.Context, basePath string, handler CommitHandler, rows int) uint64 {
	t.Helper()
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// write data file
	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := WriteChunkToFile(dataPath, makeTestChunk(t, rows)); err != nil {
		t.Fatal(err)
	}

	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, uint64(rows), []*DataFile{df})
	txn := NewTransactionAppend(0, "scan-test", []*DataFragment{frag})
	if err := CommitTransaction(ctx, basePath, handler, txn); err != nil {
		t.Fatal(err)
	}
	return 1
}

func TestScanChunksSingleFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 5)

	chunks, err := ScanChunks(ctx, basePath, handler, version)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	c := chunks[0]
	if c.Card() != 5 || c.ColumnCount() != 2 {
		t.Fatalf("chunk Card=%d Cols=%d", c.Card(), c.ColumnCount())
	}
	for i := 0; i < 5; i++ {
		v0 := c.Data[0].GetValue(i)
		v1 := c.Data[1].GetValue(i)
		if v0.I64 != int64(i) || v1.I64 != int64(i*10) {
			t.Errorf("row %d: got (%d,%d)", i, v0.I64, v1.I64)
		}
	}
}

func TestScanChunksEmptyDataset(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// commit empty manifest version 0
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	chunks, err := ScanChunks(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks, got %d", len(chunks))
	}
}

func prepareManifestWithTwoFragments(t *testing.T, ctx context.Context, basePath string, handler CommitHandler) uint64 {
	t.Helper()
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// fragment 0: 4 rows, global rows 0..3
	dataPath0 := filepath.Join(basePath, "data", "0.dat")
	if err := WriteChunkToFile(dataPath0, makeTestChunkWithBase(t, 4, 0)); err != nil {
		t.Fatal(err)
	}
	df0 := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag0 := NewDataFragmentWithRows(0, 4, []*DataFile{df0})
	if err := CommitTransaction(ctx, basePath, handler, NewTransactionAppend(0, "frag0", []*DataFragment{frag0})); err != nil {
		t.Fatal(err)
	}

	// fragment 1: 6 rows, global rows 4..9
	dataPath1 := filepath.Join(basePath, "data", "1.dat")
	if err := WriteChunkToFile(dataPath1, makeTestChunkWithBase(t, 6, 4)); err != nil {
		t.Fatal(err)
	}
	df1 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	frag1 := NewDataFragmentWithRows(1, 6, []*DataFile{df1})
	if err := CommitTransaction(ctx, basePath, handler, NewTransactionAppend(1, "frag1", []*DataFragment{frag1})); err != nil {
		t.Fatal(err)
	}
	return 2
}

func TestTakeRowsSingleFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 10)

	indices := []uint64{0, 3, 7, 9}
	chunkTaken, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}
	if chunkTaken == nil {
		t.Fatal("expected non-nil chunk")
	}
	if chunkTaken.Card() != len(indices) {
		t.Fatalf("Card=%d want %d", chunkTaken.Card(), len(indices))
	}
	for outRow, idx := range indices {
		v0 := chunkTaken.Data[0].GetValue(outRow)
		v1 := chunkTaken.Data[1].GetValue(outRow)
		if v0.I64 != int64(idx) || v1.I64 != int64(idx*10) {
			t.Errorf("outRow %d from idx %d: got (%d,%d)", outRow, idx, v0.I64, v1.I64)
		}
	}
}

func TestTakeRowsMultiFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithTwoFragments(t, ctx, basePath, handler)

	// total rows = 4 + 6 = 10; pick indices across fragments, with duplicates and unordered.
	indices := []uint64{0, 5, 3, 8, 5}
	chunkTaken, err := TakeRows(ctx, basePath, handler, version, indices)
	if err != nil {
		t.Fatal(err)
	}
	if chunkTaken == nil {
		t.Fatal("expected non-nil chunk")
	}
	if chunkTaken.Card() != len(indices) {
		t.Fatalf("Card=%d want %d", chunkTaken.Card(), len(indices))
	}
	for outRow, idx := range indices {
		v0 := chunkTaken.Data[0].GetValue(outRow)
		v1 := chunkTaken.Data[1].GetValue(outRow)
		if v0.I64 != int64(idx) || v1.I64 != int64(idx*10) {
			t.Errorf("outRow %d from idx %d: got (%d,%d)", outRow, idx, v0.I64, v1.I64)
		}
	}
}

func TestTakeRowsProjectedColumns(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Build a manifest with a single fragment whose first data file has 3 columns.
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 10, []*DataFile{
			NewDataFile("data/0.dat", []int32{0, 1, 2}, 1, 0),
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatal(err)
	}

	// Write a chunk with 3 columns: col0=i, col1=i*10, col2=i*100.
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

	indices := []uint64{0, 3, 7}
	// Project only columns 0 and 2.
	chunkTaken, err := TakeRowsProjected(ctx, basePath, handler, 0, indices, []int{0, 2})
	if err != nil {
		t.Fatal(err)
	}
	if chunkTaken == nil {
		t.Fatal("expected non-nil chunk")
	}
	if chunkTaken.Card() != len(indices) || chunkTaken.ColumnCount() != 2 {
		t.Fatalf("Card=%d Cols=%d, want Card=%d Cols=2", chunkTaken.Card(), chunkTaken.ColumnCount(), len(indices))
	}
	for outRow, idx := range indices {
		v0 := chunkTaken.Data[0].GetValue(outRow)
		v2 := chunkTaken.Data[1].GetValue(outRow)
		if v0.I64 != int64(idx) || v2.I64 != int64(idx*100) {
			t.Errorf("row %d from idx %d: got (v0=%d,v2=%d)", outRow, idx, v0.I64, v2.I64)
		}
	}
}
