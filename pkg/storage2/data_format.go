// Package storage2 data file format: columnar, chunk-compatible layout.
// No dependency on pkg/chunk; type IDs are chosen to match plan's common.LTypeId for future integration.

package storage2

import (
	"encoding/binary"
	"fmt"
)

const (
	// DataFileMagic is the 4-byte file signature.
	DataFileMagic = "S2DF"
	// DataFileVersion is the format version.
	DataFileVersion uint16 = 1
)

// ColumnTypeID matches common.LTypeId values (pkg/common/type_id.go) for chunk compatibility.
// Only a subset is used here; more can be added when integrating with pkg/chunk.
const (
	ColTypeInvalid ColumnTypeID = 0
	ColTypeInt32   ColumnTypeID = 13 // LTID_INTEGER
	ColTypeInt64   ColumnTypeID = 14 // LTID_BIGINT
	ColTypeFloat64 ColumnTypeID = 23 // LTID_DOUBLE
	ColTypeBytes   ColumnTypeID = 26 // LTID_BLOB; variable-length
)

// ColumnTypeID is the type of a column in the data file.
type ColumnTypeID uint32

// FixedSize returns the byte size per row for fixed-width types, or 0 for variable-length.
func (c ColumnTypeID) FixedSize() int {
	switch c {
	case ColTypeInt32:
		return 4
	case ColTypeInt64:
		return 8
	case ColTypeFloat64:
		return 8
	case ColTypeBytes:
		return 0
	default:
		return 0
	}
}

// Header layout: magic(4) + version(2) + num_rows(8) + num_columns(4) = 18 bytes.
const (
	headerMagicLen   = 4
	headerVersionLen = 2
	headerRowsLen    = 8
	headerColsLen    = 4
	headerSize       = headerMagicLen + headerVersionLen + headerRowsLen + headerColsLen
)

// Footer layout: num_columns(4) + for each column: data_length(8) = 4 + 8*num_columns.
const footerNumColsLen = 4
const footerColLenSize = 8

// WriteHeader writes the data file header to the given 18-byte buffer.
func WriteHeader(buf []byte, numRows uint64, numColumns uint32) {
	if len(buf) < headerSize {
		panic("buffer too small for header")
	}
	copy(buf[0:4], DataFileMagic)
	binary.LittleEndian.PutUint16(buf[4:6], DataFileVersion)
	binary.LittleEndian.PutUint64(buf[6:14], numRows)
	binary.LittleEndian.PutUint32(buf[14:18], numColumns)
}

// ReadHeader parses the header and returns numRows, numColumns. Returns error if magic/version invalid.
func ReadHeader(buf []byte) (numRows uint64, numColumns uint32, err error) {
	if len(buf) < headerSize {
		return 0, 0, fmt.Errorf("header too short: %d", len(buf))
	}
	if string(buf[0:4]) != DataFileMagic {
		return 0, 0, fmt.Errorf("invalid magic: %q", buf[0:4])
	}
	if v := binary.LittleEndian.Uint16(buf[4:6]); v != DataFileVersion {
		return 0, 0, fmt.Errorf("unsupported data file version: %d", v)
	}
	numRows = binary.LittleEndian.Uint64(buf[6:14])
	numColumns = binary.LittleEndian.Uint32(buf[14:18])
	return numRows, numColumns, nil
}

// FooterSize returns the number of bytes the footer occupies.
func FooterSize(numColumns uint32) int {
	return footerNumColsLen + int(numColumns)*footerColLenSize
}

// WriteFooter writes the footer: num_columns(4) + column_byte_length(8) per column.
func WriteFooter(buf []byte, columnLengths []uint64) {
	if len(buf) < FooterSize(uint32(len(columnLengths))) {
		panic("buffer too small for footer")
	}
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(columnLengths)))
	for i, n := range columnLengths {
		binary.LittleEndian.PutUint64(buf[4+i*8:4+(i+1)*8], n)
	}
}

// ReadFooter parses the footer buffer (exactly FooterSize(numCols) bytes).
// Layout: num_columns(4) + column_byte_length(8) per column.
func ReadFooter(buf []byte, numCols uint32) ([]uint64, error) {
	footerLen := FooterSize(numCols)
	if len(buf) != footerLen {
		return nil, fmt.Errorf("footer length %d != %d", len(buf), footerLen)
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != numCols {
		return nil, fmt.Errorf("footer num_columns %d != %d", binary.LittleEndian.Uint32(buf[0:4]), numCols)
	}
	out := make([]uint64, numCols)
	for i := uint32(0); i < numCols; i++ {
		out[i] = binary.LittleEndian.Uint64(buf[4+i*8 : 4+(i+1)*8])
	}
	return out, nil
}
