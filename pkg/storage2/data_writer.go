package storage2

import (
	"errors"
	"os"
	"path/filepath"
)

// DataFileWriter writes a columnar data file (S2DF format).
// Call WriteColumn for each column in order, then Close.
type DataFileWriter struct {
	f          *os.File
	numRows    uint64
	numCols    uint32
	written    uint32
	colLengths []uint64
	headerBuf  [headerSize]byte
}

// CreateDataFile starts a new data file at path.
func CreateDataFile(path string, numRows uint64, numColumns uint32) (*DataFileWriter, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &DataFileWriter{
		f:          f,
		numRows:    numRows,
		numCols:    numColumns,
		colLengths: make([]uint64, 0, numColumns),
	}
	WriteHeader(w.headerBuf[:], numRows, numColumns)
	if _, err := f.Write(w.headerBuf[:]); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// WriteColumn appends one column raw bytes.
func (w *DataFileWriter) WriteColumn(data []byte) error {
	if w.written >= w.numCols {
		return errWriterClosed
	}
	n, err := w.f.Write(data)
	if err != nil {
		return err
	}
	w.colLengths = append(w.colLengths, uint64(n))
	w.written++
	return nil
}

// Close writes footer and closes the file.
func (w *DataFileWriter) Close() error {
	if w.f == nil {
		return nil
	}
	if w.written != w.numCols {
		w.f.Close()
		w.f = nil
		return errWriterIncomplete
	}
	footerLen := FooterSize(w.numCols)
	footer := make([]byte, footerLen)
	WriteFooter(footer, w.colLengths)
	if _, err := w.f.Write(footer); err != nil {
		w.f.Close()
		w.f = nil
		return err
	}
	err := w.f.Close()
	w.f = nil
	return err
}

var errWriterClosed = errors.New("data file writer: already closed")
var errWriterIncomplete = errors.New("data file writer: not all columns written")
