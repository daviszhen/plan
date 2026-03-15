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

func TestScannerBasic(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	// 准备一个包含 10 行 (i, i*100) 的数据集
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
	defer ds.Close()

	scanner := ds.Scanner().Build()
	defer scanner.Close()

	var got0 []int64
	var got1 []int64
	for scanner.Next() {
		var v0, v1 int64
		if err := scanner.Scan(&v0, &v1); err != nil {
			t.Fatal(err)
		}
		got0 = append(got0, v0)
		got1 = append(got1, v1)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got0) != 10 || len(got1) != 10 {
		t.Fatalf("scanner returned %d rows (col0), want 10", len(got0))
	}
	for i := 0; i < 10; i++ {
		if got0[i] != int64(i) || got1[i] != int64(i*100) {
			t.Errorf("row %d: got (%d,%d)", i, got0[i], got1[i])
		}
	}
}

func TestScannerWithColumns(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

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
	defer ds.Close()

	// 仅选择第二列（c1）
	scanner := ds.Scanner().WithColumns("c1").Build()
	defer scanner.Close()

	var got []int64
	for scanner.Next() {
		var v int64
		if err := scanner.Scan(&v); err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("scanner returned %d rows, want 10", len(got))
	}
	for i := 0; i < 10; i++ {
		if got[i] != int64(i*100) {
			t.Errorf("row %d: got %d, want %d", i, got[i], int64(i*100))
		}
	}
}

func TestScannerWithFilter(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

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
	defer ds.Close()

	// 只保留 c0 >= 5 的行
	scanner := ds.Scanner().WithFilter("c0 >= 5").Build()
	defer scanner.Close()

	var got0 []int64
	for scanner.Next() {
		var v0 int64
		if err := scanner.Scan(&v0); err != nil {
			t.Fatal(err)
		}
		got0 = append(got0, v0)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got0) != 5 {
		t.Fatalf("scanner returned %d rows, want 5", len(got0))
	}
	for i, v := range got0 {
		if v != int64(i+5) {
			t.Errorf("row %d: got %d, want %d", i, v, int64(i+5))
		}
	}
}

// TestScannerCountLikeLance 模拟 count_lance_file：按条件统计行数。
func TestScannerCountLikeLance(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

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
	defer ds.Close()

	// 等价于：count where c0 >= 3 and c0 <= 7
	scanner := ds.Scanner().WithFilter("c0 >= 3").Build()
	defer scanner.Close()

	var cnt int
	for scanner.Next() {
		var v0 int64
		if err := scanner.Scan(&v0); err != nil {
			t.Fatal(err)
		}
		if v0 <= 7 {
			cnt++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if cnt != 5 {
		t.Fatalf("count = %d, want 5", cnt)
	}
}

func TestScannerWithRowId(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	scanner := ds.Scanner().WithRowId().Build()
	defer scanner.Close()

	var rowIDs []uint64
	for scanner.Next() {
		rec, err := scanner.GetRow()
		if err != nil {
			t.Fatal(err)
		}
		if rec.RowID == nil {
			t.Fatal("expected RowID to be set")
		}
		rowIDs = append(rowIDs, *rec.RowID)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) != 10 {
		t.Fatalf("got %d row IDs, want 10", len(rowIDs))
	}

	// Verify row IDs are in (fragmentID << 32) | rowOffset format
	// Fragment 0, rows 0-9 => rowIDs should be 0<<32|0, 0<<32|1, ..., 0<<32|9
	for i, rid := range rowIDs {
		fragID := rid >> 32
		rowOffset := rid & 0xFFFFFFFF
		if fragID != 0 {
			t.Errorf("row %d: fragment ID = %d, want 0", i, fragID)
		}
		if rowOffset != uint64(i) {
			t.Errorf("row %d: row offset = %d, want %d", i, rowOffset, i)
		}
	}
}

func TestScannerWithRowIdMultiFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Append first fragment
	dataPath0 := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath0, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df0 := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag0 := NewDataFragmentWithRows(0, 10, []*DataFile{df0})
	if err := ds.Append(ctx, []*DataFragment{frag0}); err != nil {
		t.Fatal(err)
	}

	// Append second fragment
	dataPath1 := filepath.Join(basePath, "data", "1.dat")
	if err := storage2.WriteChunkToFile(dataPath1, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df1 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	frag1 := NewDataFragmentWithRows(0, 10, []*DataFile{df1})
	if err := ds.Append(ctx, []*DataFragment{frag1}); err != nil {
		t.Fatal(err)
	}

	scanner := ds.Scanner().WithRowId().Build()
	defer scanner.Close()

	var rowIDs []uint64
	for scanner.Next() {
		rec, err := scanner.GetRow()
		if err != nil {
			t.Fatal(err)
		}
		if rec.RowID == nil {
			t.Fatal("expected RowID to be set")
		}
		rowIDs = append(rowIDs, *rec.RowID)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(rowIDs) != 20 {
		t.Fatalf("got %d row IDs, want 20", len(rowIDs))
	}

	// First 10 rows: fragment 0
	for i := 0; i < 10; i++ {
		fragID := rowIDs[i] >> 32
		offset := rowIDs[i] & 0xFFFFFFFF
		if fragID != 0 {
			t.Errorf("row %d: fragment ID = %d, want 0", i, fragID)
		}
		if offset != uint64(i) {
			t.Errorf("row %d: offset = %d, want %d", i, offset, i)
		}
	}
	// Next 10 rows: fragment 1
	for i := 10; i < 20; i++ {
		fragID := rowIDs[i] >> 32
		offset := rowIDs[i] & 0xFFFFFFFF
		if fragID != 1 {
			t.Errorf("row %d: fragment ID = %d, want 1", i, fragID)
		}
		if offset != uint64(i-10) {
			t.Errorf("row %d: offset = %d, want %d", i, offset, i-10)
		}
	}
}

func TestScannerWithoutRowId(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	// Without WithRowId, RowID should be nil
	scanner := ds.Scanner().Build()
	defer scanner.Close()

	for scanner.Next() {
		rec, err := scanner.GetRow()
		if err != nil {
			t.Fatal(err)
		}
		if rec.RowID != nil {
			t.Fatal("expected RowID to be nil when WithRowId not called")
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestScannerScanInOrder(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	// Create chunk with out-of-order data: 9,8,7,6,5,4,3,2,1,0
	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, reverseChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	// Scan with ScanInOrder - should return sorted by c0
	scanner := ds.Scanner().ScanInOrder().Build()
	defer scanner.Close()

	var values []int64
	for scanner.Next() {
		var v int64
		if err := scanner.Scan(&v); err != nil {
			t.Fatal(err)
		}
		values = append(values, v)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(values) != 10 {
		t.Fatalf("got %d values, want 10", len(values))
	}

	// Verify sorted order
	for i := 0; i < len(values)-1; i++ {
		if values[i] > values[i+1] {
			t.Errorf("values not sorted: values[%d]=%d > values[%d]=%d", i, values[i], i+1, values[i+1])
		}
	}
}

func TestScannerUseIndex(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	ds, err := CreateDataset(ctx, basePath).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := storage2.WriteChunkToFile(dataPath, emptyChunk(t)); err != nil {
		t.Fatal(err)
	}
	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 10, []*DataFile{df})
	if err := ds.Append(ctx, []*DataFragment{frag}); err != nil {
		t.Fatal(err)
	}

	// UseIndex is currently a hint that doesn't change behavior
	// but should not cause errors
	scanner := ds.Scanner().UseIndex("nonexistent_index").WithFilter("c0 > 5").Build()
	defer scanner.Close()

	var count int
	for scanner.Next() {
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	// Filter "c0 > 5" should return 4 rows (6,7,8,9)
	if count != 4 {
		t.Errorf("got %d rows, want 4", count)
	}
}

// reverseChunk creates a chunk with values in reverse order (9,8,7,...,0)
func reverseChunk(t *testing.T) *chunk.Chunk {
	t.Helper()
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(10)
	for i := 0; i < 10; i++ {
		// Reverse order: 9, 8, 7, ..., 0
		val := int64(9 - i)
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			I64: val,
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			I64: val * 100,
		})
	}
	return c
}
