package storage2

import (
	"os"
	"path/filepath"
	"strings"
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

func TestWriteChunkToFileReadChunkFromFileVarlen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "chunk_varlen.dat")

	typs := []common.LType{
		common.MakeLType(common.LTID_VARCHAR),
		common.MakeLType(common.LTID_VARCHAR),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(5)

	values0 := []string{"", "a", "你好", "very-long-string-0123456789", "x"}
	values1 := []string{"α", "", "βγ", "δ", ""}

	for i := 0; i < 5; i++ {
		c.Data[0].SetValue(i, &chunk.Value{
			Typ: typs[0],
			Str: values0[i],
		})
		c.Data[1].SetValue(i, &chunk.Value{
			Typ: typs[1],
			Str: values1[i],
		})
	}

	if err := WriteChunkToFile(path, c); err != nil {
		t.Fatal(err)
	}
	got, err := ReadChunkFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Card() != 5 || got.ColumnCount() != 2 {
		t.Fatalf("got Card=%d ColumnCount=%d", got.Card(), got.ColumnCount())
	}
	for i := 0; i < 5; i++ {
		v0 := got.Data[0].GetValue(i)
		v1 := got.Data[1].GetValue(i)
		if v0.IsNull || v0.Str != values0[i] {
			t.Errorf("row %d col0: got Str=%q IsNull=%v want %q", i, v0.Str, v0.IsNull, values0[i])
		}
		if v1.IsNull || v1.Str != values1[i] {
			t.Errorf("row %d col1: got Str=%q IsNull=%v want %q", i, v1.Str, v1.IsNull, values1[i])
		}
	}
}

func TestWriteChunkToFileReadChunkFromFileVarlenExtended(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "chunk_varlen_ext.dat")

	typs := []common.LType{
		common.MakeLType(common.LTID_VARCHAR),
		common.MakeLType(common.LTID_VARCHAR),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(6)

	large := strings.Repeat("x", 64*1024) // 接近单个向量典型 block 的大字符串

	col0 := []struct {
		str   string
		isNil bool
	}{
		{"", false},        // 0 长
		{"short", false},   // 短字符串
		{large, false},     // 大 payload
		{"mixed-1", false}, // 混合
		{"", true},         // NULL
		{"尾部", false},       // 多字节
	}
	col1 := []struct {
		str   string
		isNil bool
	}{
		{"α", false},
		{"", false},
		{"βγ", false},
		{"δ", false},
		{"", true}, // NULL
		{"ε", false},
	}

	for i := 0; i < 6; i++ {
		v0 := &chunk.Value{Typ: typs[0]}
		if col0[i].isNil {
			v0.IsNull = true
		} else {
			v0.Str = col0[i].str
		}
		c.Data[0].SetValue(i, v0)

		v1 := &chunk.Value{Typ: typs[1]}
		if col1[i].isNil {
			v1.IsNull = true
		} else {
			v1.Str = col1[i].str
		}
		c.Data[1].SetValue(i, v1)
	}

	if err := WriteChunkToFile(path, c); err != nil {
		t.Fatal(err)
	}
	got, err := ReadChunkFromFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Card() != 6 || got.ColumnCount() != 2 {
		t.Fatalf("got Card=%d ColumnCount=%d", got.Card(), got.ColumnCount())
	}
	for i := 0; i < 6; i++ {
		v0 := got.Data[0].GetValue(i)
		v1 := got.Data[1].GetValue(i)
		if v0.IsNull != col0[i].isNil || (!v0.IsNull && v0.Str != col0[i].str) {
			t.Errorf("row %d col0: got Str=%q IsNull=%v want Str=%q IsNull=%v", i, v0.Str, v0.IsNull, col0[i].str, col0[i].isNil)
		}
		if v1.IsNull != col1[i].isNil || (!v1.IsNull && v1.Str != col1[i].str) {
			t.Errorf("row %d col1: got Str=%q IsNull=%v want Str=%q IsNull=%v", i, v1.Str, v1.IsNull, col1[i].str, col1[i].isNil)
		}
	}
}
