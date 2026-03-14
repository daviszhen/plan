package sdk

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestCountRows_SDK_Basic tests SDK layer basic row counting.
// Corresponds to Lance count_lance_file basic scenario
func TestCountRows_SDK_Basic(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create empty dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Count rows in empty dataset
	count, err := dataset.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)

	// Create data file
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 2)})
	}

	// Write data file
	dataPath := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath, c))

	// Create fragment and append
	dataFile := storage2.NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := storage2.NewDataFragmentWithRows(0, 100, []*storage2.DataFile{dataFile})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment})
	require.NoError(t, err)

	// Count rows
	count, err = dataset.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(100), count)
}

// TestCountRows_SDK_WithFilter tests SDK layer counting with filter.
// Corresponds to Lance count_lance_file with filter scenario
func TestCountRows_SDK_WithFilter(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create data: c0 = 0..99
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 2)})
	}

	dataPath := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath, c))

	dataFile := storage2.NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := storage2.NewDataFragmentWithRows(0, 100, []*storage2.DataFile{dataFile})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment})
	require.NoError(t, err)

	// Test: c0 > 50 (should count 49 rows)
	count, err := dataset.CountRowsWithFilter(ctx, "c0 > 50")
	require.NoError(t, err)
	require.Equal(t, uint64(49), count)

	// Test: c0 >= 50 (should count 50 rows)
	count, err = dataset.CountRowsWithFilter(ctx, "c0 >= 50")
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)

	// Test: c0 < 25 (should count 25 rows)
	count, err = dataset.CountRowsWithFilter(ctx, "c0 < 25")
	require.NoError(t, err)
	require.Equal(t, uint64(25), count)

	// Test: c0 = 50 (should count 1 row)
	count, err = dataset.CountRowsWithFilter(ctx, "c0 = 50")
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
}

// TestCountRows_SDK_ComplexFilter tests SDK layer counting with complex filter.
func TestCountRows_SDK_ComplexFilter(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create data
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i % 10)})
	}

	dataPath := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath, c))

	dataFile := storage2.NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := storage2.NewDataFragmentWithRows(0, 100, []*storage2.DataFile{dataFile})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment})
	require.NoError(t, err)

	// Test: c0 > 10 AND c0 < 20 (should count 9 rows)
	count, err := dataset.CountRowsWithFilter(ctx, "c0 > 10 AND c0 < 20")
	require.NoError(t, err)
	require.Equal(t, uint64(9), count)

	// Test: c0 < 10 OR c0 > 90 (should count 19 rows)
	count, err = dataset.CountRowsWithFilter(ctx, "c0 < 10 OR c0 > 90")
	require.NoError(t, err)
	require.Equal(t, uint64(19), count)

	// Test: NOT c0 < 50 (should count 50 rows)
	count, err = dataset.CountRowsWithFilter(ctx, "NOT c0 < 50")
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)

	// Test: c1 = 5 (should count 10 rows)
	count, err = dataset.CountRowsWithFilter(ctx, "c1 = 5")
	require.NoError(t, err)
	require.Equal(t, uint64(10), count)
}

// TestCountRows_SDK_EmptyResult tests SDK layer counting with filter that matches no rows.
func TestCountRows_SDK_EmptyResult(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create data
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(50)

	for i := 0; i < 50; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
	}

	dataPath := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath, c))

	dataFile := storage2.NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment := storage2.NewDataFragmentWithRows(0, 50, []*storage2.DataFile{dataFile})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment})
	require.NoError(t, err)

	// Test: c0 > 1000 (no matches)
	count, err := dataset.CountRowsWithFilter(ctx, "c0 > 1000")
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)

	// Test: c0 < 0 (no matches)
	count, err = dataset.CountRowsWithFilter(ctx, "c0 < 0")
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)
}

// TestCountRows_SDK_AllMatches tests SDK layer counting with filter that matches all rows.
func TestCountRows_SDK_AllMatches(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create data
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(75)

	for i := 0; i < 75; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
	}

	dataPath := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath, c))

	dataFile := storage2.NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment := storage2.NewDataFragmentWithRows(0, 75, []*storage2.DataFile{dataFile})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment})
	require.NoError(t, err)

	// Test: c0 >= 0 (all matches)
	count, err := dataset.CountRowsWithFilter(ctx, "c0 >= 0")
	require.NoError(t, err)
	require.Equal(t, uint64(75), count)

	// Test: c0 < 100 (all matches)
	count, err = dataset.CountRowsWithFilter(ctx, "c0 < 100")
	require.NoError(t, err)
	require.Equal(t, uint64(75), count)
}

// TestCountRows_SDK_MultipleVersions tests counting across versions.
func TestCountRows_SDK_MultipleVersions(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Version 1: Add 50 rows
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c1 := &chunk.Chunk{}
	c1.Init(typs, util.DefaultVectorSize)
	c1.SetCard(50)

	for i := 0; i < 50; i++ {
		c1.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
	}

	dataPath1 := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath1, c1))

	dataFile1 := storage2.NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment1 := storage2.NewDataFragmentWithRows(0, 50, []*storage2.DataFile{dataFile1})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment1})
	require.NoError(t, err)

	count, err := dataset.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)
	require.Equal(t, uint64(1), dataset.Version())

	// Version 2: Add 30 more rows
	c2 := &chunk.Chunk{}
	c2.Init(typs, util.DefaultVectorSize)
	c2.SetCard(30)

	for i := 0; i < 30; i++ {
		c2.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 50)})
	}

	dataPath2 := filepath.Join(tmpDir, "data", "1.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath2, c2))

	dataFile2 := storage2.NewDataFile("data/1.dat", []int32{0}, 1, 0)
	fragment2 := storage2.NewDataFragmentWithRows(1, 30, []*storage2.DataFile{dataFile2})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment2})
	require.NoError(t, err)

	count, err = dataset.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(80), count)
	require.Equal(t, uint64(2), dataset.Version())

	// Count with filter across all rows
	count, err = dataset.CountRowsWithFilter(ctx, "c0 >= 25")
	require.NoError(t, err)
	require.Equal(t, uint64(55), count) // 25-79 = 55 rows

	// Count with filter on first fragment range
	count, err = dataset.CountRowsWithFilter(ctx, "c0 < 50")
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)
}

// TestCountRows_SDK_VersionIsolation tests that counting on old version returns old count.
func TestCountRows_SDK_VersionIsolation(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)

	// Version 1: Add 50 rows
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c1 := &chunk.Chunk{}
	c1.Init(typs, util.DefaultVectorSize)
	c1.SetCard(50)

	for i := 0; i < 50; i++ {
		c1.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
	}

	dataPath1 := filepath.Join(tmpDir, "data", "0.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath1, c1))

	dataFile1 := storage2.NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment1 := storage2.NewDataFragmentWithRows(0, 50, []*storage2.DataFile{dataFile1})
	err = dataset.Append(ctx, []*storage2.DataFragment{fragment1})
	require.NoError(t, err)

	dataset.Close()

	// Open at version 1
	dataset1, err := OpenDataset(ctx, tmpDir).WithVersion(1).Build()
	require.NoError(t, err)
	defer dataset1.Close()

	count, err := dataset1.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)

	// Open latest and add more data
	dataset2, err := OpenDataset(ctx, tmpDir).Build()
	require.NoError(t, err)

	c2 := &chunk.Chunk{}
	c2.Init(typs, util.DefaultVectorSize)
	c2.SetCard(30)

	for i := 0; i < 30; i++ {
		c2.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 50)})
	}

	dataPath2 := filepath.Join(tmpDir, "data", "1.dat")
	require.NoError(t, storage2.WriteChunkToFile(dataPath2, c2))

	dataFile2 := storage2.NewDataFile("data/1.dat", []int32{0}, 1, 0)
	fragment2 := storage2.NewDataFragmentWithRows(1, 30, []*storage2.DataFile{dataFile2})
	err = dataset2.Append(ctx, []*storage2.DataFragment{fragment2})
	require.NoError(t, err)

	count, err = dataset2.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(80), count)

	dataset2.Close()

	// Verify version 1 still has 50 rows
	dataset1Again, err := OpenDataset(ctx, tmpDir).WithVersion(1).Build()
	require.NoError(t, err)
	defer dataset1Again.Close()

	count, err = dataset1Again.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)
}
