package sdk

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/storage2"
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

