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
	return c
}
