// Tests for legacy S2DF format (CreateDataFile, WriteColumn, OpenDataFile, ReadColumn).
// Canonical data file tests using pkg/chunk are in data_chunk_test.go.

package storage2

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestDataFileWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data", "0.dat")
	numRows := uint64(10)
	numCols := uint32(2)

	w, err := CreateDataFile(path, numRows, numCols)
	if err != nil {
		t.Fatal(err)
	}
	col0 := make([]byte, 10*4) // 10 int32s
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint32(col0[i*4:(i+1)*4], uint32(i))
	}
	col1 := make([]byte, 10*8) // 10 int64s
	for i := 0; i < 10; i++ {
		binary.LittleEndian.PutUint64(col1[i*8:(i+1)*8], uint64(i*100))
	}
	if err := w.WriteColumn(col0); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteColumn(col1); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenDataFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if r.NumRows() != numRows || r.NumColumns() != numCols {
		t.Errorf("rows=%d cols=%d", r.NumRows(), r.NumColumns())
	}
	c0, err := r.ReadColumn(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(c0) != 40 {
		t.Errorf("col0 len=%d", len(c0))
	}
	for i := 0; i < 10; i++ {
		if v := binary.LittleEndian.Uint32(c0[i*4 : (i+1)*4]); v != uint32(i) {
			t.Errorf("col0[%d]=%d", i, v)
		}
	}
	c1, err := r.ReadColumn(1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if v := binary.LittleEndian.Uint64(c1[i*8 : (i+1)*8]); v != uint64(i*100) {
			t.Errorf("col1[%d]=%d", i, v)
		}
	}
	if r.FileSize() <= 0 {
		t.Error("FileSize should be positive")
	}
}

func TestDataFileWriterIncompleteClose(t *testing.T) {
	dir := t.TempDir()
	w, err := CreateDataFile(filepath.Join(dir, "x.dat"), 5, 2)
	if err != nil {
		t.Fatal(err)
	}
	w.WriteColumn(make([]byte, 20))
	err = w.Close()
	if err != errWriterIncomplete {
		t.Errorf("expected errWriterIncomplete, got %v", err)
	}
}

func TestDataFileReaderColumnOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.dat")
	w, _ := CreateDataFile(path, 1, 1)
	w.WriteColumn([]byte{0})
	w.Close()
	r, _ := OpenDataFile(path)
	_, err := r.ReadColumn(1)
	if err == nil {
		t.Fatal("expected error for column 1")
	}
	r, _ = OpenDataFile(path)
	_, err = r.ReadColumn(0)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDataFileOpenMissing(t *testing.T) {
	_, err := OpenDataFile(filepath.Join(os.TempDir(), "nonexistent_s2df_12345.dat"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}
