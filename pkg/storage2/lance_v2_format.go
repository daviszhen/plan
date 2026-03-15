// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// ============================================================================
// Lance v2 File Format Constants
// ============================================================================

const (
	// V2Magic is the file magic for v2 format files.
	V2Magic = "LNC2"
	// V2MajorVersion is the major version of the v2 format.
	V2MajorVersion = 2
	// V2MinorVersion is the minor version of the v2 format.
	V2MinorVersion = 0

	// V2HeaderSize is the size of the file header in bytes.
	V2HeaderSize = 32
	// V2PageHeaderSize is the size of each page header.
	V2PageHeaderSize = 24

	// V2DefaultPageSize is the default page size (64KB).
	V2DefaultPageSize = 64 * 1024
	// V2MaxPageSize is the maximum page size (16MB).
	V2MaxPageSize = 16 * 1024 * 1024
	// V2MinPageSize is the minimum page size (4KB).
	V2MinPageSize = 4 * 1024
)

// V2CompressionType identifies the compression algorithm used.
type V2CompressionType uint8

const (
	// V2CompressionNone indicates no compression.
	V2CompressionNone V2CompressionType = 0
	// V2CompressionLZ4 indicates LZ4 compression.
	V2CompressionLZ4 V2CompressionType = 1
	// V2CompressionZstd indicates Zstandard compression.
	V2CompressionZstd V2CompressionType = 2
	// V2CompressionSnappy indicates Snappy compression.
	V2CompressionSnappy V2CompressionType = 3
)

func (c V2CompressionType) String() string {
	switch c {
	case V2CompressionNone:
		return "none"
	case V2CompressionLZ4:
		return "lz4"
	case V2CompressionZstd:
		return "zstd"
	case V2CompressionSnappy:
		return "snappy"
	default:
		return "unknown"
	}
}

// V2PageType identifies the type of data in a page.
type V2PageType uint8

const (
	// V2PageData contains column data.
	V2PageData V2PageType = 0
	// V2PageDictionary contains dictionary values.
	V2PageDictionary V2PageType = 1
	// V2PageValidity contains null bitmap.
	V2PageValidity V2PageType = 2
	// V2PageOffsets contains offsets for variable-length data.
	V2PageOffsets V2PageType = 3
	// V2PageDefinition contains definition levels for nested types.
	V2PageDefinition V2PageType = 4
	// V2PageRepetition contains repetition levels for nested types.
	V2PageRepetition V2PageType = 5
)

// ============================================================================
// V2 File Header
// ============================================================================

// V2FileHeader is the header at the start of a v2 format file.
type V2FileHeader struct {
	// Magic is the file magic ("LNC2").
	Magic [4]byte
	// MajorVersion is the major format version.
	MajorVersion uint16
	// MinorVersion is the minor format version.
	MinorVersion uint16
	// Flags contains format flags.
	Flags uint32
	// NumColumns is the number of columns in the file.
	NumColumns uint32
	// NumRows is the total number of rows.
	NumRows uint64
	// MetadataOffset is the offset of the metadata section.
	MetadataOffset uint64
}

// Write writes the header to a writer.
func (h *V2FileHeader) Write(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, h.Magic); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.MajorVersion); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.MinorVersion); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Flags); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.NumColumns); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.NumRows); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.MetadataOffset); err != nil {
		return err
	}
	return nil
}

// Read reads the header from a reader.
func (h *V2FileHeader) Read(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &h.Magic); err != nil {
		return err
	}
	if string(h.Magic[:]) != V2Magic {
		return fmt.Errorf("invalid magic: expected %s, got %s", V2Magic, string(h.Magic[:]))
	}
	if err := binary.Read(r, binary.LittleEndian, &h.MajorVersion); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.MinorVersion); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Flags); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.NumColumns); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.NumRows); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.MetadataOffset); err != nil {
		return err
	}
	return nil
}

// ============================================================================
// V2 Page Header
// ============================================================================

// V2PageHeader is the header for each data page.
type V2PageHeader struct {
	// PageType identifies what kind of data this page contains.
	PageType V2PageType
	// Compression identifies the compression algorithm used.
	Compression V2CompressionType
	// Encoding identifies the encoding algorithm used.
	Encoding EncodingType
	// Reserved for future use.
	Reserved uint8
	// ColumnIndex is the column this page belongs to.
	ColumnIndex uint32
	// NumValues is the number of values in this page.
	NumValues uint32
	// UncompressedSize is the size before compression.
	UncompressedSize uint32
	// CompressedSize is the size after compression.
	CompressedSize uint32
	// Checksum is a CRC32 checksum of the compressed data.
	Checksum uint32
}

// Write writes the page header to a writer.
func (h *V2PageHeader) Write(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, h.PageType); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Compression); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Encoding); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Reserved); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.ColumnIndex); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.NumValues); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.UncompressedSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.CompressedSize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, h.Checksum); err != nil {
		return err
	}
	return nil
}

// Read reads the page header from a reader.
func (h *V2PageHeader) Read(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &h.PageType); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Compression); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Encoding); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Reserved); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.ColumnIndex); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.NumValues); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.UncompressedSize); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.CompressedSize); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &h.Checksum); err != nil {
		return err
	}
	return nil
}

// ============================================================================
// V2 Column Metadata
// ============================================================================

// V2ColumnMeta contains metadata for a single column.
type V2ColumnMeta struct {
	// Index is the column index (0-based).
	Index uint32
	// Name is the column name.
	Name string
	// LogicalType is the logical type string (e.g., "int64", "string").
	LogicalType string
	// Nullable indicates if the column can contain nulls.
	Nullable bool
	// NumPages is the number of data pages for this column.
	NumPages uint32
	// TotalBytes is the total size of all pages for this column.
	TotalBytes uint64
	// Statistics contains column statistics.
	Statistics *V2ColumnStats
	// Encoding is the primary encoding used for this column.
	Encoding EncodingType
	// Compression is the compression algorithm used.
	Compression V2CompressionType
}

// V2ColumnStats contains statistics for a column.
type V2ColumnStats struct {
	// NullCount is the number of null values.
	NullCount uint64
	// DistinctCount is the approximate number of distinct values.
	DistinctCount uint64
	// MinValue is the minimum value (type-specific encoding).
	MinValue []byte
	// MaxValue is the maximum value (type-specific encoding).
	MaxValue []byte
}

// ============================================================================
// V2 File Footer
// ============================================================================

// V2FileFooter contains metadata at the end of the file.
type V2FileFooter struct {
	// SchemaLength is the length of the serialized schema.
	SchemaLength uint32
	// Schema is the serialized schema.
	Schema []byte
	// ColumnMetaLength is the length of column metadata.
	ColumnMetaLength uint32
	// ColumnMeta contains metadata for each column.
	ColumnMeta []*V2ColumnMeta
	// PageIndexOffset is the offset of the page index.
	PageIndexOffset uint64
	// PageIndexLength is the length of the page index.
	PageIndexLength uint32
}

// ============================================================================
// V2 Page Index
// ============================================================================

// V2PageIndex is an index of all pages in the file.
type V2PageIndex struct {
	// Entries contains the index entries.
	Entries []V2PageIndexEntry
}

// V2PageIndexEntry is a single entry in the page index.
type V2PageIndexEntry struct {
	// Offset is the byte offset of the page.
	Offset uint64
	// Length is the total length of the page (header + data).
	Length uint32
	// ColumnIndex is the column this page belongs to.
	ColumnIndex uint32
	// FirstRow is the first row index in this page.
	FirstRow uint64
	// NumRows is the number of rows in this page.
	NumRows uint32
}

// Write writes the page index entry to a writer.
func (e *V2PageIndexEntry) Write(w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, e.Offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.Length); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.ColumnIndex); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.FirstRow); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.NumRows); err != nil {
		return err
	}
	return nil
}

// Read reads the page index entry from a reader.
func (e *V2PageIndexEntry) Read(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &e.Offset); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.Length); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.ColumnIndex); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.FirstRow); err != nil {
		return err
	}
	if err := binary.Read(r, binary.LittleEndian, &e.NumRows); err != nil {
		return err
	}
	return nil
}

// ============================================================================
// CRC32 Checksum
// ============================================================================

// ComputeChecksum computes CRC32 checksum for data.
func ComputeChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// VerifyChecksum verifies the CRC32 checksum.
func VerifyChecksum(data []byte, expected uint32) bool {
	return ComputeChecksum(data) == expected
}
