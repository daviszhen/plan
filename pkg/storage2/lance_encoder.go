// Package storage2 provides Lance file format encoding/decoding for interoperability
// with Lance Python/Rust ecosystem.
//
// Lance is a columnar data format optimized for random access and vector search.
// This module provides conversion between pkg/chunk format and Lance format.
package storage2

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
	protobuf "google.golang.org/protobuf/proto"
)

// Lance file format constants
const (
	LanceMagic        = "LANC"
	LanceMajorVersion = 2
	LanceMinorVersion = 1
	LanceFooterSize   = 8 + 4 + 4 // metadata_position(8) + major(4) + minor(4) + magic(4)
)

// LanceEncoder encodes chunk.Chunk data to Lance file format.
type LanceEncoder struct {
	schema     *storage2pb.Schema
	fields     []*storage2pb.Field
	numRows    uint64
	chunks     [][]byte // encoded column chunks
	pageTable  []uint64 // position, length pairs for each page
	fieldTypes []common.LType
}

// NewLanceEncoder creates a new Lance encoder.
func NewLanceEncoder() *LanceEncoder {
	return &LanceEncoder{
		chunks: make([][]byte, 0),
	}
}

// EncodeSchema converts common.LType slice to Lance schema.
func (e *LanceEncoder) EncodeSchema(typs []common.LType) *storage2pb.Schema {
	e.fieldTypes = typs
	e.fields = make([]*storage2pb.Field, len(typs))

	for i, typ := range typs {
		e.fields[i] = &storage2pb.Field{
			Type:        storage2pb.Field_LEAF,
			Name:        fmt.Sprintf("c%d", i),
			Id:          int32(i + 1),
			ParentId:    -1,
			LogicalType: ltypeToLanceLogicalType(typ),
			Nullable:    true,
		}
	}

	e.schema = &storage2pb.Schema{
		Fields: e.fields,
	}

	return e.schema
}

// ltypeToLanceLogicalType converts common.LType to Lance logical type string.
func ltypeToLanceLogicalType(typ common.LType) string {
	switch typ.Id {
	case common.LTID_BOOLEAN:
		return "bool"
	case common.LTID_TINYINT:
		return "int8"
	case common.LTID_UTINYINT:
		return "uint8"
	case common.LTID_SMALLINT:
		return "int16"
	case common.LTID_USMALLINT:
		return "uint16"
	case common.LTID_INTEGER:
		return "int32"
	case common.LTID_UINTEGER:
		return "uint32"
	case common.LTID_BIGINT:
		return "int64"
	case common.LTID_UBIGINT:
		return "uint64"
	case common.LTID_HUGEINT:
		return "decimal:256:76:0"
	case common.LTID_FLOAT:
		return "float"
	case common.LTID_DOUBLE:
		return "double"
	case common.LTID_VARCHAR, common.LTID_CHAR:
		return "string"
	case common.LTID_BLOB:
		return "binary"
	case common.LTID_DATE:
		return "date32:day"
	case common.LTID_TIME:
		return "time:us"
	case common.LTID_TIMESTAMP:
		return "timestamp:us"
	case common.LTID_TIMESTAMP_TZ:
		return "timestamp:us"
	case common.LTID_DECIMAL:
		return fmt.Sprintf("decimal:128:%d:%d", typ.Width, typ.Scale)
	case common.LTID_LIST:
		return "list"
	case common.LTID_STRUCT:
		return "struct"
	default:
		return "null"
	}
}

// lanceLogicalTypeToLType converts Lance logical type to common.LType.
func lanceLogicalTypeToLType(logicalType string) common.LType {
	switch logicalType {
	case "bool":
		return common.MakeLType(common.LTID_BOOLEAN)
	case "int8":
		return common.MakeLType(common.LTID_TINYINT)
	case "uint8":
		return common.MakeLType(common.LTID_UTINYINT)
	case "int16":
		return common.MakeLType(common.LTID_SMALLINT)
	case "uint16":
		return common.MakeLType(common.LTID_USMALLINT)
	case "int32":
		return common.MakeLType(common.LTID_INTEGER)
	case "uint32":
		return common.MakeLType(common.LTID_UINTEGER)
	case "int64":
		return common.MakeLType(common.LTID_BIGINT)
	case "uint64":
		return common.MakeLType(common.LTID_UBIGINT)
	case "float":
		return common.MakeLType(common.LTID_FLOAT)
	case "double":
		return common.MakeLType(common.LTID_DOUBLE)
	case "string":
		return common.MakeLType(common.LTID_VARCHAR)
	case "binary":
		return common.MakeLType(common.LTID_BLOB)
	case "date32:day":
		return common.MakeLType(common.LTID_DATE)
	case "time:us":
		return common.MakeLType(common.LTID_TIME)
	case "timestamp:us":
		return common.MakeLType(common.LTID_TIMESTAMP)
	case "list":
		return common.MakeLType(common.LTID_LIST)
	case "struct":
		return common.MakeLType(common.LTID_STRUCT)
	default:
		// Handle decimal types
		if len(logicalType) > 8 && logicalType[:8] == "decimal:" {
			return common.MakeLType(common.LTID_DECIMAL)
		}
		return common.MakeLType(common.LTID_NULL)
	}
}

// EncodeChunk encodes a chunk.Chunk to Lance column chunks.
func (e *LanceEncoder) EncodeChunk(c *chunk.Chunk) error {
	if e.schema == nil {
		// Get types from vectors
		typs := make([]common.LType, len(c.Data))
		for i, vec := range c.Data {
			typs[i] = vec.Typ()
		}
		e.EncodeSchema(typs)
	}

	e.numRows = uint64(c.Count)
	e.chunks = make([][]byte, len(c.Data))
	e.pageTable = make([]uint64, len(c.Data)*2)

	for colIdx := 0; colIdx < len(c.Data); colIdx++ {
		data, err := e.encodeColumn(c, colIdx)
		if err != nil {
			return fmt.Errorf("failed to encode column %d: %w", colIdx, err)
		}
		e.chunks[colIdx] = data
	}

	return nil
}

// encodeColumn encodes a single column to Lance format.
func (e *LanceEncoder) encodeColumn(c *chunk.Chunk, colIdx int) ([]byte, error) {
	vec := c.Data[colIdx]
	typ := vec.Typ()
	numRows := c.Count

	var buf bytes.Buffer

	// Write validity bitmap (if nullable)
	// For simplicity, we use a simple format: 1 byte per row for validity
	validity := make([]byte, (numRows+7)/8)
	for i := 0; i < numRows; i++ {
		if vec.Mask.RowIsValid(uint64(i)) {
			validity[i/8] |= 1 << (i % 8)
		}
	}
	buf.Write(validity)

	// Write data based on type
	switch typ.Id {
	case common.LTID_BOOLEAN:
		for i := 0; i < numRows; i++ {
			var b byte
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				if val.I64 != 0 {
					b = 1
				}
			}
			buf.WriteByte(b)
		}

	case common.LTID_TINYINT, common.LTID_UTINYINT:
		for i := 0; i < numRows; i++ {
			if !vec.Mask.RowIsValid(uint64(i)) {
				buf.WriteByte(0)
			} else {
				val := vec.GetValue(i)
				buf.WriteByte(byte(val.I64))
			}
		}

	case common.LTID_SMALLINT, common.LTID_USMALLINT:
		for i := 0; i < numRows; i++ {
			var v int16
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = int16(val.I64)
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	case common.LTID_INTEGER, common.LTID_UINTEGER:
		for i := 0; i < numRows; i++ {
			var v int32
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = int32(val.I64)
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	case common.LTID_BIGINT, common.LTID_UBIGINT:
		for i := 0; i < numRows; i++ {
			var v int64
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = val.I64
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	case common.LTID_FLOAT:
		for i := 0; i < numRows; i++ {
			var v float32
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = float32(val.F64)
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	case common.LTID_DOUBLE:
		for i := 0; i < numRows; i++ {
			var v float64
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = val.F64
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	case common.LTID_VARCHAR, common.LTID_CHAR, common.LTID_BLOB:
		// Variable-length encoding: offsets + data
		offsets := make([]uint32, numRows+1)
		var dataBuf bytes.Buffer
		offset := uint32(0)

		for i := 0; i < numRows; i++ {
			offsets[i] = offset
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				dataBuf.WriteString(val.Str)
				offset += uint32(len(val.Str))
			}
		}
		offsets[numRows] = offset

		// Write offsets
		for _, off := range offsets {
			binary.Write(&buf, binary.LittleEndian, off)
		}
		// Write data
		buf.Write(dataBuf.Bytes())

	case common.LTID_DATE:
		for i := 0; i < numRows; i++ {
			var v int32
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = int32(val.I64)
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	case common.LTID_TIMESTAMP:
		for i := 0; i < numRows; i++ {
			var v int64
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = val.I64
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}

	default:
		// Generic: store as int64
		for i := 0; i < numRows; i++ {
			var v int64
			if vec.Mask.RowIsValid(uint64(i)) {
				val := vec.GetValue(i)
				v = val.I64
			}
			binary.Write(&buf, binary.LittleEndian, v)
		}
	}

	return buf.Bytes(), nil
}

// WriteTo writes the encoded Lance file to the writer.
func (e *LanceEncoder) WriteTo(w io.Writer) error {
	if e.schema == nil {
		return fmt.Errorf("no data encoded")
	}

	// Write column chunks and build page table
	var pageTable []uint64
	position := uint64(0)

	for _, chunk := range e.chunks {
		pageTable = append(pageTable, position, uint64(len(chunk)))
		n, err := w.Write(chunk)
		if err != nil {
			return fmt.Errorf("failed to write chunk: %w", err)
		}
		position += uint64(n)
	}

	// Write page table
	pageTablePos := position
	pageTableBytes := make([]byte, len(pageTable)*8)
	for i, v := range pageTable {
		binary.LittleEndian.PutUint64(pageTableBytes[i*8:], v)
	}
	if _, err := w.Write(pageTableBytes); err != nil {
		return fmt.Errorf("failed to write page table: %w", err)
	}
	position += uint64(len(pageTableBytes))

	// Write metadata
	metadataPos := position
	metadata := &storage2pb.Metadata{
		PageTablePosition: pageTablePos,
		BatchOffsets:      []int32{int32(e.numRows)},
	}
	metadataBytes, err := protobuf.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if _, err := w.Write(metadataBytes); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}
	position += uint64(len(metadataBytes))

	// Write file descriptor
	fileDesc := &storage2pb.FileDescriptor{
		Schema: e.schema,
		Length: e.numRows,
	}
	fileDescBytes, err := protobuf.Marshal(fileDesc)
	if err != nil {
		return fmt.Errorf("failed to marshal file descriptor: %w", err)
	}
	if _, err := w.Write(fileDescBytes); err != nil {
		return fmt.Errorf("failed to write file descriptor: %w", err)
	}
	position += uint64(len(fileDescBytes))

	// Write footer: metadata_position(8) + major(4) + minor(4) + magic(4)
	footer := make([]byte, 20)
	binary.LittleEndian.PutUint64(footer[0:8], metadataPos)
	binary.LittleEndian.PutUint32(footer[8:12], LanceMajorVersion)
	binary.LittleEndian.PutUint32(footer[12:16], LanceMinorVersion)
	copy(footer[16:20], LanceMagic)

	if _, err := w.Write(footer); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	return nil
}

// WriteToFile writes the encoded Lance file to a file.
func (e *LanceEncoder) WriteToFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	return e.WriteTo(f)
}

// LanceDecoder decodes Lance file format to chunk.Chunk.
type LanceDecoder struct {
	schema     *storage2pb.Schema
	numRows    uint64
	pageTable  []uint64
	metadata   *storage2pb.Metadata
	fileDesc   *storage2pb.FileDescriptor
	fieldTypes []common.LType
}

// NewLanceDecoder creates a new Lance decoder.
func NewLanceDecoder() *LanceDecoder {
	return &LanceDecoder{}
}

// ReadFrom reads a Lance file from the reader.
func (d *LanceDecoder) ReadFrom(r io.ReadSeeker) error {
	// Read footer
	footer := make([]byte, 20)
	if _, err := r.Seek(-20, io.SeekEnd); err != nil {
		return fmt.Errorf("failed to seek to footer: %w", err)
	}
	if _, err := io.ReadFull(r, footer); err != nil {
		return fmt.Errorf("failed to read footer: %w", err)
	}

	// Verify magic
	magic := string(footer[16:20])
	if magic != LanceMagic {
		return fmt.Errorf("invalid lance file: expected magic %q, got %q", LanceMagic, magic)
	}

	metadataPos := binary.LittleEndian.Uint64(footer[0:8])

	// Read file size
	fileSize, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to get file size: %w", err)
	}

	// Read everything from metadata position to footer
	dataSize := fileSize - int64(metadataPos) - 20
	if dataSize < 0 {
		return fmt.Errorf("invalid metadata position: %d", metadataPos)
	}

	// Seek to metadata position
	if _, err := r.Seek(int64(metadataPos), io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to metadata: %w", err)
	}

	data := make([]byte, dataSize)
	if _, err := io.ReadFull(r, data); err != nil {
		return fmt.Errorf("failed to read metadata section: %w", err)
	}

	// Try to parse as metadata followed by file descriptor
	// We need to find the boundary between them
	// First, try to parse as file descriptor directly
	d.fileDesc = &storage2pb.FileDescriptor{}
	if err := protobuf.Unmarshal(data, d.fileDesc); err == nil && d.fileDesc.Schema != nil {
		// It's a file descriptor
		d.schema = d.fileDesc.Schema
		d.numRows = d.fileDesc.Length
	} else {
		// Try to parse as metadata + file descriptor
		// Find where metadata ends and file descriptor begins
		// Protobuf messages are self-delimiting, so we can try to parse both
		d.metadata = &storage2pb.Metadata{}
		if err := protobuf.Unmarshal(data, d.metadata); err != nil {
			// Try parsing as file descriptor only
			d.fileDesc = &storage2pb.FileDescriptor{}
			if err := protobuf.Unmarshal(data, d.fileDesc); err != nil {
				return fmt.Errorf("failed to parse as metadata or file descriptor: %w", err)
			}
			d.schema = d.fileDesc.Schema
			d.numRows = d.fileDesc.Length
		} else {
			// Successfully parsed metadata, now try to find and parse file descriptor
			// The file descriptor should be after the metadata in the data buffer
			// We need to find where the metadata ends
			d.numRows = uint64(d.metadata.BatchOffsets[len(d.metadata.BatchOffsets)-1])

			// Try to find the file descriptor by attempting to parse from different offsets
			for offset := 1; offset < len(data); offset++ {
				d.fileDesc = &storage2pb.FileDescriptor{}
				if err := protobuf.Unmarshal(data[offset:], d.fileDesc); err == nil && d.fileDesc.Schema != nil {
					d.schema = d.fileDesc.Schema
					break
				}
			}
		}
	}

	// Convert schema to field types
	if d.schema != nil {
		d.fieldTypes = make([]common.LType, len(d.schema.Fields))
		for i, field := range d.schema.Fields {
			d.fieldTypes[i] = lanceLogicalTypeToLType(field.LogicalType)
		}
	}

	// Read page table
	if d.metadata != nil && d.metadata.PageTablePosition > 0 && d.schema != nil {
		if _, err := r.Seek(int64(d.metadata.PageTablePosition), io.SeekStart); err != nil {
			return fmt.Errorf("failed to seek to page table: %w", err)
		}

		numFields := len(d.schema.Fields)
		pageTableSize := numFields * 2 * 8 // position + length for each field
		d.pageTable = make([]uint64, numFields*2)
		pageTableBytes := make([]byte, pageTableSize)
		if _, err := io.ReadFull(r, pageTableBytes); err != nil {
			return fmt.Errorf("failed to read page table: %w", err)
		}
		for i := 0; i < numFields*2; i++ {
			d.pageTable[i] = binary.LittleEndian.Uint64(pageTableBytes[i*8:])
		}
	}

	return nil
}

// ReadFromFile reads a Lance file from a file path.
func (d *LanceDecoder) ReadFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	return d.ReadFrom(f)
}

// DecodeChunk decodes the Lance data to a chunk.Chunk.
func (d *LanceDecoder) DecodeChunk(r io.ReadSeeker) (*chunk.Chunk, error) {
	if d.schema == nil {
		return nil, fmt.Errorf("no schema loaded")
	}

	numRows := int(d.numRows)
	c := &chunk.Chunk{}
	c.Init(d.fieldTypes, numRows)
	c.SetCard(numRows)

	// Decode each column
	for colIdx, field := range d.schema.Fields {
		if colIdx*2+1 >= len(d.pageTable) {
			continue
		}

		pos := d.pageTable[colIdx*2]
		length := d.pageTable[colIdx*2+1]

		if _, err := r.Seek(int64(pos), io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek to column %d: %w", colIdx, err)
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", colIdx, err)
		}

		if err := d.decodeColumn(c, colIdx, data, field.LogicalType); err != nil {
			return nil, fmt.Errorf("failed to decode column %d: %w", colIdx, err)
		}
	}

	return c, nil
}

// decodeColumn decodes a single column from Lance format.
func (d *LanceDecoder) decodeColumn(c *chunk.Chunk, colIdx int, data []byte, logicalType string) error {
	numRows := int(d.numRows)
	vec := c.Data[colIdx]
	typ := vec.Typ()

	// Read validity bitmap
	validitySize := (numRows + 7) / 8
	validity := data[:validitySize]
	data = data[validitySize:]

	buf := bytes.NewReader(data)

	switch typ.Id {
	case common.LTID_BOOLEAN:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				b, _ := buf.ReadByte()
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(b)})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_TINYINT, common.LTID_UTINYINT:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				b, _ := buf.ReadByte()
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(b)})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_SMALLINT, common.LTID_USMALLINT:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v int16
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(v)})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_INTEGER, common.LTID_UINTEGER:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v int32
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: int64(v)})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_BIGINT, common.LTID_UBIGINT:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v int64
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: v})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_FLOAT:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v float32
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, F64: float64(v)})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_DOUBLE:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v float64
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, F64: v})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_VARCHAR, common.LTID_CHAR, common.LTID_BLOB:
		// Read offsets
		offsets := make([]uint32, numRows+1)
		for i := 0; i <= numRows; i++ {
			binary.Read(buf, binary.LittleEndian, &offsets[i])
		}

		// Read strings
		strData := make([]byte, offsets[numRows])
		buf.Read(strData)

		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				start := offsets[i]
				end := offsets[i+1]
				str := string(strData[start:end])
				vec.SetValue(i, &chunk.Value{Typ: typ, Str: str})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	case common.LTID_DATE, common.LTID_TIMESTAMP:
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v int64
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: v})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}

	default:
		// Generic: read as int64
		for i := 0; i < numRows; i++ {
			valid := (validity[i/8] & (1 << (i % 8))) != 0
			if valid {
				var v int64
				binary.Read(buf, binary.LittleEndian, &v)
				vec.SetValue(i, &chunk.Value{Typ: typ, I64: v})
			} else {
				chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
			}
		}
	}

	return nil
}

// Schema returns the decoded schema.
func (d *LanceDecoder) Schema() *storage2pb.Schema {
	return d.schema
}

// NumRows returns the number of rows.
func (d *LanceDecoder) NumRows() uint64 {
	return d.numRows
}

// FieldTypes returns the decoded field types.
func (d *LanceDecoder) FieldTypes() []common.LType {
	return d.fieldTypes
}

// WriteChunkToLanceFile writes a chunk.Chunk to a Lance file.
// This is a convenience function for simple use cases.
func WriteChunkToLanceFile(path string, c *chunk.Chunk) error {
	encoder := NewLanceEncoder()
	if err := encoder.EncodeChunk(c); err != nil {
		return err
	}
	return encoder.WriteToFile(path)
}

// ReadChunkFromLanceFile reads a chunk.Chunk from a Lance file.
// This is a convenience function for simple use cases.
func ReadChunkFromLanceFile(path string) (*chunk.Chunk, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	decoder := NewLanceDecoder()
	if err := decoder.ReadFrom(f); err != nil {
		return nil, err
	}

	return decoder.DecodeChunk(f)
}
