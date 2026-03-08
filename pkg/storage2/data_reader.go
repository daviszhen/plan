package storage2

import (
	"fmt"
	"os"
)

// DataFileReader reads a columnar data file (S2DF format).
type DataFileReader struct {
	path       string
	numRows    uint64
	numCols    uint32
	colLengths []uint64
	colOffsets []int64 // start offset of each column (after header)
	fileSize   int64
}

// OpenDataFile opens a data file and reads header + footer to expose metadata.
func OpenDataFile(path string) (*DataFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	fileSize := info.Size()
	header := make([]byte, headerSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		f.Close()
		return nil, err
	}
	numRows, numCols, err := ReadHeader(header)
	if err != nil {
		f.Close()
		return nil, err
	}
	footerLen := FooterSize(numCols)
	if fileSize < int64(headerSize+footerLen) {
		f.Close()
		return nil, fmt.Errorf("file too short: %d", fileSize)
	}
	footer := make([]byte, footerLen)
	if _, err := f.ReadAt(footer, fileSize-int64(footerLen)); err != nil {
		f.Close()
		return nil, err
	}
	f.Close()
	colLengths, err := ReadFooter(footer, numCols)
	if err != nil {
		return nil, err
	}
	if uint32(len(colLengths)) != numCols {
		return nil, fmt.Errorf("footer column count %d != header %d", len(colLengths), numCols)
	}
	colOffsets := make([]int64, numCols)
	off := int64(headerSize)
	for i := uint32(0); i < numCols; i++ {
		colOffsets[i] = off
		off += int64(colLengths[i])
	}
	if off != fileSize-int64(footerLen) {
		return nil, fmt.Errorf("column data size mismatch: offset %d vs expected %d", off, fileSize-int64(footerLen))
	}
	return &DataFileReader{
		path:       path,
		numRows:    numRows,
		numCols:    numCols,
		colLengths: colLengths,
		colOffsets: colOffsets,
		fileSize:   fileSize,
	}, nil
}

// NumRows returns the number of rows in the file.
func (r *DataFileReader) NumRows() uint64 { return r.numRows }

// NumColumns returns the number of columns.
func (r *DataFileReader) NumColumns() uint32 { return r.numCols }

// ReadColumn reads the full data of column index (0-based). Caller must not modify the returned slice.
func (r *DataFileReader) ReadColumn(index uint32) ([]byte, error) {
	if index >= r.numCols {
		return nil, fmt.Errorf("column index %d out of range [0,%d)", index, r.numCols)
	}
	f, err := os.Open(r.path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	buf := make([]byte, r.colLengths[index])
	if _, err := f.ReadAt(buf, r.colOffsets[index]); err != nil {
		return nil, err
	}
	return buf, nil
}

// FileSize returns the total file size in bytes (for DataFile metadata).
func (r *DataFileReader) FileSize() int64 { return r.fileSize }
