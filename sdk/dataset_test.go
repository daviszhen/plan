package sdk

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/daviszhen/plan/pkg/util"
)

func TestCreateAndOpenDataset(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create empty dataset
	ds, err := CreateDataset(ctx, basePath).WithCommitHandler(NewLocalRenameCommitHandler()).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	if v := ds.Version(); v != 0 {
		t.Errorf("version want 0 got %d", v)
	}
	count, _ := ds.CountRows()
	if count != 0 {
		t.Errorf("count want 0 got %d", count)
	}

	// Write chunk to file and append fragment
	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	if v := ds.Version(); v != 1 {
		t.Errorf("version want 1 got %d", v)
	}
	count, _ = ds.CountRows()
	if count != 10 {
		t.Errorf("count want 10 got %d", count)
	}

	// Open at version 1
	ds2, err := OpenDataset(ctx, basePath).WithVersion(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	if ds2.Version() != 1 {
		t.Errorf("open version want 1 got %d", ds2.Version())
	}
}

func TestOpenDatasetLatest(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create and append so we have version 1
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	_ = storage2.WriteChunkToFile(dataPath, emptyChunk(t))
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 5, []*DataFile{df})
	_ = ds.Append(ctx, []*DataFragment{frag})
	ds.Close()

	// Open without WithVersion -> latest
	ds2, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	if ds2.Version() != 1 {
		t.Errorf("latest version want 1 got %d", ds2.Version())
	}
}

func TestDeleteAndOverwrite(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	_ = storage2.WriteChunkToFile(dataPath, emptyChunk(t))
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 3, []*DataFile{df})
	_ = ds.Append(ctx, []*DataFragment{frag})
	ds.Close()

	ds2, _ := OpenDataset(ctx, basePath).Build()
	defer ds2.Close()
	if err := ds2.Delete(ctx, "id > 0"); err != nil {
		t.Fatal(err)
	}
	if ds2.Version() != 2 {
		t.Errorf("after delete version want 2 got %d", ds2.Version())
	}

	// Overwrite with new fragment
	dataPath2 := filepath.Join(basePath, "data", "1.dat")
	_ = storage2.WriteChunkToFile(dataPath2, emptyChunk(t))
	df2 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	frag2 := NewDataFragmentWithRows(1, 2, []*DataFile{df2})
	if err := ds2.Overwrite(ctx, []*DataFragment{frag2}); err != nil {
		t.Fatal(err)
	}
	if ds2.Version() != 3 {
		t.Errorf("after overwrite version want 3 got %d", ds2.Version())
	}
	count, _ := ds2.CountRows()
	if count != 2 {
		t.Errorf("after overwrite count want 2 got %d", count)
	}
}

func TestDatasetTake(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset and append 10 rows
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	indices := []uint64{0, 3, 7, 9}
	ch, err := ds.Take(ctx, indices)
	if err != nil {
		t.Fatal(err)
	}
	if ch == nil {
		t.Fatal("expected non-nil chunk")
	}
	if ch.Card() != len(indices) {
		t.Fatalf("Card=%d want %d", ch.Card(), len(indices))
	}
	for outRow, idx := range indices {
		v0 := ch.Data[0].GetValue(outRow)
		v1 := ch.Data[1].GetValue(outRow)
		if v0.I64 != int64(idx) || v1.I64 != int64(idx*100) {
			t.Errorf("outRow %d from idx %d: got (%d,%d)", outRow, idx, v0.I64, v1.I64)
		}
	}
}

func TestDatasetTakeProjected(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// Create dataset and append 10 rows with 3 columns.
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
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
	if err := storage2.WriteChunkToFile(dataPath, src); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1, 2}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	indices := []uint64{0, 4, 9}
	ch, err := ds.TakeProjected(ctx, indices, []int{0, 2})
	if err != nil {
		t.Fatal(err)
	}
	if ch == nil {
		t.Fatal("expected non-nil chunk")
	}
	if ch.Card() != len(indices) || ch.ColumnCount() != 2 {
		t.Fatalf("Card=%d Cols=%d, want Card=%d Cols=2", ch.Card(), ch.ColumnCount(), len(indices))
	}
	for outRow, idx := range indices {
		v0 := ch.Data[0].GetValue(outRow)
		v2 := ch.Data[1].GetValue(outRow)
		if v0.I64 != int64(idx) || v2.I64 != int64(idx*100) {
			t.Errorf("outRow %d from idx %d: got (v0=%d,v2=%d)", outRow, idx, v0.I64, v2.I64)
		}
	}
}

func TestOpenInvalidPath(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	basePath := filepath.Join(base, "nonexistent")

	_, err := OpenDataset(ctx, basePath).Build()
	if err == nil {
		t.Fatal("expected error when opening invalid path, got nil")
	}
}

// TestOpenNonExist verifies opening a path where no dataset exists returns an error
// (corresponds to Lance testOpenNonExist / testOpenInvalidPath).
func TestOpenNonExist(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir() // empty, no _versions

	_, err := OpenDataset(ctx, basePath).Build()
	if err == nil {
		t.Fatal("expected error when opening non-existent dataset")
	}
}

// TestOpenExistingManifestDataset opens a dataset created only by low-level API
// (manifest committed via CommitHandler), then opens with SDK and verifies (Lance testOpenSerializedManifest).
func TestOpenExistingManifestDataset(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := storage2.NewManifest(0)
	m0.Fragments = []*storage2.DataFragment{}
	m0.NextRowId = 1
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	ds, err := OpenDataset(ctx, basePath).WithCommitHandler(handler).WithVersion(0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if v := ds.Version(); v != 0 {
		t.Errorf("Version() want 0 got %d", v)
	}
	if n, _ := ds.CountRows(); n != 0 {
		t.Errorf("CountRows() want 0 got %d", n)
	}
}

func TestDatasetDataSize(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// version 0 manifest with explicit FileSizeBytes
	m0 := storage2.NewManifest(0)
	m0.Fragments = []*storage2.DataFragment{
		storage2.NewDataFragment(0, []*storage2.DataFile{
			{
				Path:          "data/0.dat",
				FileSizeBytes: 100,
			},
		}),
		storage2.NewDataFragment(1, []*storage2.DataFile{
			{
				Path:          "data/1.dat",
				FileSizeBytes: 200,
			},
		}),
	}
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	ds, err := OpenDataset(ctx, basePath).WithCommitHandler(handler).WithVersion(0).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	size, err := ds.DataSize()
	if err != nil {
		t.Fatal(err)
	}
	if size != 300 {
		t.Fatalf("DataSize() = %d, want 300", size)
	}
}

func TestDatasetVersioning(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// version 0: empty dataset
	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	if v := ds.Version(); v != 0 {
		t.Fatalf("initial version want 0 got %d", v)
	}
	if latest, err := ds.LatestVersion(); err != nil || latest != 0 {
		t.Fatalf("initial latest want 0 got %d err=%v", latest, err)
	}

	// version 1: first append (5 rows)
	dataPath0 := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath0, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df0 := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag0 := NewDataFragmentWithRows(0, 5, []*DataFile{df0})
	if err := ds.Append(ctx, []*DataFragment{frag0}); err != nil {
		t.Fatal(err)
	}
	if v := ds.Version(); v != 1 {
		t.Fatalf("after first append version want 1 got %d", v)
	}
	if latest, err := ds.LatestVersion(); err != nil || latest != 1 {
		t.Fatalf("after first append latest want 1 got %d err=%v", latest, err)
	}
	if count, _ := ds.CountRows(); count != 5 {
		t.Fatalf("after first append rows want 5 got %d", count)
	}

	// version 2: second append (3 rows)
	dataPath1 := filepath.Join(basePath, "data", "1.dat")
	if err := storage2.WriteChunkToFile(dataPath1, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df1 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	frag1 := NewDataFragmentWithRows(1, 3, []*DataFile{df1})
	if err := ds.Append(ctx, []*DataFragment{frag1}); err != nil {
		t.Fatal(err)
	}
	if v := ds.Version(); v != 2 {
		t.Fatalf("after second append version want 2 got %d", v)
	}
	if latest, err := ds.LatestVersion(); err != nil || latest != 2 {
		t.Fatalf("after second append latest want 2 got %d err=%v", latest, err)
	}
	if count, _ := ds.CountRows(); count != 8 {
		t.Fatalf("after second append rows want 8 got %d", count)
	}

	// Open at specific version 1
	dsV1, err := OpenDataset(ctx, basePath).WithVersion(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer dsV1.Close()
	if dsV1.Version() != 1 {
		t.Errorf("open v1: Version() want 1 got %d", dsV1.Version())
	}
	if count, _ := dsV1.CountRows(); count != 5 {
		t.Errorf("open v1: CountRows want 5 got %d", count)
	}

	// Open latest (version 2)
	dsLatest, err := OpenDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer dsLatest.Close()
	if dsLatest.Version() != 2 {
		t.Errorf("open latest: Version() want 2 got %d", dsLatest.Version())
	}
	if count, _ := dsLatest.CountRows(); count != 8 {
		t.Errorf("open latest: CountRows want 8 got %d", count)
	}
}

// TestCheckoutVersion simulates checkout by opening at an old version and verifying CountRows (Lance testDatasetCheckoutVersion).
func TestCheckoutVersion(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, _ := CreateDataset(ctx, basePath).Build()
	_ = storage2.WriteChunkToFile(filepath.Join(basePath, "data", "0.dat"), emptyChunk(t))
	_ = storage2.WriteChunkToFile(filepath.Join(basePath, "data", "1.dat"), emptyChunk(t))
	_ = ds.Append(ctx, []*DataFragment{NewDataFragmentWithRows(0, 5, []*DataFile{NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)})})
	_ = ds.Append(ctx, []*DataFragment{NewDataFragmentWithRows(1, 3, []*DataFile{NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)})})
	ds.Close()

	// "Checkout" version 1: open at v1, expect 5 rows
	dsV1, err := OpenDataset(ctx, basePath).WithVersion(1).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer dsV1.Close()
	if dsV1.Version() != 1 {
		t.Errorf("checkout v1: Version() want 1 got %d", dsV1.Version())
	}
	if c, _ := dsV1.CountRows(); c != 5 {
		t.Errorf("checkout v1: CountRows want 5 got %d", c)
	}

	// Open latest: version 2, 8 rows
	dsLatest, _ := OpenDataset(ctx, basePath).Build()
	defer dsLatest.Close()
	if dsLatest.Version() != 2 || mustCount(t, dsLatest) != 8 {
		t.Errorf("latest: version=%d count=%d", dsLatest.Version(), mustCount(t, dsLatest))
	}
}

// TestOpenDatasetWithTag verifies opening a dataset by tag name (Lance testTags 部分能力).
func TestOpenDatasetWithTag(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// version 0: empty manifest
	m0 := storage2.NewManifest(0)
	m0.Fragments = []*storage2.DataFragment{}
	m0.NextRowId = 1
	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		t.Fatal(err)
	}

	// v1: append with tag "v1"
	txn1 := storage2.NewTransactionAppend(0, "txn-v1", []*storage2.DataFragment{
		storage2.NewDataFragment(0, nil),
	})
	txn1.Tag = "v1"
	if err := storage2.CommitTransaction(ctx, basePath, handler, txn1); err != nil {
		t.Fatal(err)
	}

	// v2: append with tag "v2"
	txn2 := storage2.NewTransactionAppend(1, "txn-v2", []*storage2.DataFragment{
		storage2.NewDataFragment(1, nil),
	})
	txn2.Tag = "v2"
	if err := storage2.CommitTransaction(ctx, basePath, handler, txn2); err != nil {
		t.Fatal(err)
	}

	// v3: another append also tagged "v1"
	txn3 := storage2.NewTransactionAppend(2, "txn-v1b", []*storage2.DataFragment{
		storage2.NewDataFragment(2, nil),
	})
	txn3.Tag = "v1"
	if err := storage2.CommitTransaction(ctx, basePath, handler, txn3); err != nil {
		t.Fatal(err)
	}

	// Open by tag "v1" -> latest tagged version 3
	dsV1, err := OpenDatasetWithTag(ctx, basePath, "v1")
	if err != nil {
		t.Fatal(err)
	}
	defer dsV1.Close()
	if dsV1.Version() != 3 {
		t.Errorf("OpenDatasetWithTag(v1): Version() want 3 got %d", dsV1.Version())
	}

	// Open by tag "v2" -> version 2
	dsV2, err := OpenDatasetWithTag(ctx, basePath, "v2")
	if err != nil {
		t.Fatal(err)
	}
	defer dsV2.Close()
	if dsV2.Version() != 2 {
		t.Errorf("OpenDatasetWithTag(v2): Version() want 2 got %d", dsV2.Version())
	}

	// Missing tag -> expect error
	if _, err := OpenDatasetWithTag(ctx, basePath, "missing"); err == nil {
		t.Fatal("expected error for missing tag, got nil")
	}
}

func mustCount(t *testing.T, ds Dataset) uint64 {
	t.Helper()
	c, err := ds.CountRows()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// TestCreateOnExistingDir: second CreateDataset.Build() on same path yields version 0 again (overwrites/resets).
func TestCreateOnExistingDir(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds1, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	_ = storage2.WriteChunkToFile(dataPath, emptyChunk(t))
	_ = ds1.Append(ctx, []*DataFragment{NewDataFragmentWithRows(0, 10, []*DataFile{NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)})})
	ds1.Close()

	ds2, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds2.Close()
	if ds2.Version() != 0 {
		t.Errorf("second create: Version() want 0 got %d", ds2.Version())
	}
	if c, _ := ds2.CountRows(); c != 0 {
		t.Errorf("second create: CountRows want 0 got %d", c)
	}
}

// TestDelete is an end-to-end test for Delete (Lance testDeleteRows); current impl deletes by fragment.
func TestDelete(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(basePath, "data", "0.dat")
	_ = storage2.WriteChunkToFile(dataPath, emptyChunk(t))
	_ = ds.Append(ctx, []*DataFragment{NewDataFragmentWithRows(0, 7, []*DataFile{NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)})})
	ds.Close()

	ds2, _ := OpenDataset(ctx, basePath).Build()
	defer ds2.Close()
	if err := ds2.Delete(ctx, "id > 0"); err != nil {
		t.Fatal(err)
	}
	if ds2.Version() != 2 {
		t.Errorf("after delete Version want 2 got %d", ds2.Version())
	}
	if c, _ := ds2.CountRows(); c != 0 {
		t.Errorf("after delete CountRows want 0 got %d", c)
	}
}

func emptyChunk(t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(10)
	for i := 0; i < 10; i++ {
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			I64: int64(i),
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			I64: int64(i * 100),
		})
	}
	return c
}
