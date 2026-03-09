package storage2

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func TestWriteChunkToFileReadChunkFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "chunk.dat")

	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(0)

	if err := WriteChunkToFile(path, c); err != nil {
		t.Fatal(err)
	}
	got, err := ReadChunkFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Card() != 0 || got.ColumnCount() != 1 {
		t.Errorf("got Card=%d ColumnCount=%d", got.Card(), got.ColumnCount())
	}
}

func TestWriteChunkToFileReadChunkFromFileWithData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "chunk.dat")

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(10)
	for i := 0; i < 10; i++ {
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: common.MakeLType(common.LTID_INTEGER),
			I64: int64(i),
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: common.MakeLType(common.LTID_BIGINT),
			I64: int64(i * 100),
		})
	}

	if err := WriteChunkToFile(path, c); err != nil {
		t.Fatal(err)
	}
	got, err := ReadChunkFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Card() != 10 || got.ColumnCount() != 2 {
		t.Fatalf("got Card=%d ColumnCount=%d", got.Card(), got.ColumnCount())
	}
	for i := 0; i < 10; i++ {
		v0 := got.Data[0].GetValue(i)
		if v0.IsNull || v0.I64 != int64(i) {
			t.Errorf("row %d col0: got I64=%d IsNull=%v", i, v0.I64, v0.IsNull)
		}
		v1 := got.Data[1].GetValue(i)
		if v1.IsNull || v1.I64 != int64(i*100) {
			t.Errorf("row %d col1: got I64=%d IsNull=%v", i, v1.I64, v1.IsNull)
		}
	}
}

func TestReadChunkFromFileMissing(t *testing.T) {
	_, err := ReadChunkFromFile(filepath.Join(os.TempDir(), "nonexistent_chunk_12345.dat"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
