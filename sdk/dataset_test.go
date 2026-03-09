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
