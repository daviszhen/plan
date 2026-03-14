package storage2

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestLanceEncoder_EncodeSchema(t *testing.T) {
	encoder := NewLanceEncoder()

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_BIGINT),
		common.MakeLType(common.LTID_DOUBLE),
		common.MakeLType(common.LTID_VARCHAR),
	}

	schema := encoder.EncodeSchema(typs)
	require.NotNil(t, schema)
	require.Len(t, schema.Fields, 4)

	require.Equal(t, "int32", schema.Fields[0].LogicalType)
	require.Equal(t, "int64", schema.Fields[1].LogicalType)
	require.Equal(t, "double", schema.Fields[2].LogicalType)
	require.Equal(t, "string", schema.Fields[3].LogicalType)
}

func TestLanceEncoder_EncodeChunk(t *testing.T) {
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(10)

	for i := 0; i < 10; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 1.5})
	}

	encoder := NewLanceEncoder()
	err := encoder.EncodeChunk(c)
	require.NoError(t, err)
	require.Equal(t, uint64(10), encoder.numRows)
	require.Len(t, encoder.chunks, 2)
}

func TestLanceEncoder_WriteTo(t *testing.T) {
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 0.5})
	}

	encoder := NewLanceEncoder()
	err := encoder.EncodeChunk(c)
	require.NoError(t, err)

	var buf bytes.Buffer
	err = encoder.WriteTo(&buf)
	require.NoError(t, err)
	require.Greater(t, buf.Len(), 0)

	// Verify footer
	footer := buf.Bytes()[buf.Len()-20:]
	require.Equal(t, LanceMagic, string(footer[16:20]))
}

func TestLanceEncoder_WriteToFile(t *testing.T) {
	tmpDir := t.TempDir()
	lancePath := filepath.Join(tmpDir, "test.lance")

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_VARCHAR),
	}

	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(50)

	for i := 0; i < 50; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], Str: "value_" + string(rune('A'+i%26))})
	}

	encoder := NewLanceEncoder()
	err := encoder.EncodeChunk(c)
	require.NoError(t, err)

	err = encoder.WriteToFile(lancePath)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(lancePath)
	require.NoError(t, err)
}

func TestLanceDecoder_ReadFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	lancePath := filepath.Join(tmpDir, "test_read.lance")

	// Create and write Lance file
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i * 2)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 2.5})
	}

	encoder := NewLanceEncoder()
	err := encoder.EncodeChunk(c)
	require.NoError(t, err)
	err = encoder.WriteToFile(lancePath)
	require.NoError(t, err)

	// Read Lance file
	decoder := NewLanceDecoder()
	err = decoder.ReadFromFile(lancePath)
	require.NoError(t, err)

	require.Equal(t, uint64(100), decoder.NumRows())
	require.NotNil(t, decoder.Schema())
	require.Len(t, decoder.FieldTypes(), 2)
}

func TestLanceDecoder_DecodeChunk(t *testing.T) {
	tmpDir := t.TempDir()
	lancePath := filepath.Join(tmpDir, "test_decode.lance")

	// Create and write Lance file
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	originalChunk := &chunk.Chunk{}
	originalChunk.Init(typs, util.DefaultVectorSize)
	originalChunk.SetCard(50)

	for i := 0; i < 50; i++ {
		originalChunk.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i * 10)})
		originalChunk.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 0.1})
	}

	encoder := NewLanceEncoder()
	err := encoder.EncodeChunk(originalChunk)
	require.NoError(t, err)
	err = encoder.WriteToFile(lancePath)
	require.NoError(t, err)

	// Read and decode
	f, err := os.Open(lancePath)
	require.NoError(t, err)
	defer f.Close()

	decoder := NewLanceDecoder()
	err = decoder.ReadFrom(f)
	require.NoError(t, err)

	require.Equal(t, uint64(50), decoder.NumRows())
	require.NotNil(t, decoder.Schema())
	require.Len(t, decoder.FieldTypes(), 2)
}

func TestLanceRoundTrip_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		typ  common.LType
	}{
		{"boolean", common.MakeLType(common.LTID_BOOLEAN)},
		{"integer", common.MakeLType(common.LTID_INTEGER)},
		{"bigint", common.MakeLType(common.LTID_BIGINT)},
		{"float", common.MakeLType(common.LTID_FLOAT)},
		{"double", common.MakeLType(common.LTID_DOUBLE)},
		{"varchar", common.MakeLType(common.LTID_VARCHAR)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			lancePath := filepath.Join(tmpDir, "roundtrip.lance")

			typs := []common.LType{tt.typ}

			c := &chunk.Chunk{}
			c.Init(typs, util.DefaultVectorSize)
			c.SetCard(20)

			for i := 0; i < 20; i++ {
				switch tt.typ.Id {
				case common.LTID_BOOLEAN:
					c.Data[0].SetValue(i, &chunk.Value{Typ: tt.typ, I64: int64(i % 2)})
				case common.LTID_INTEGER, common.LTID_BIGINT:
					c.Data[0].SetValue(i, &chunk.Value{Typ: tt.typ, I64: int64(i * 100)})
				case common.LTID_FLOAT, common.LTID_DOUBLE:
					c.Data[0].SetValue(i, &chunk.Value{Typ: tt.typ, F64: float64(i) * 1.5})
				case common.LTID_VARCHAR:
					c.Data[0].SetValue(i, &chunk.Value{Typ: tt.typ, Str: fmt.Sprintf("string_%d", i)})
				}
			}

			// Write
			encoder := NewLanceEncoder()
			err := encoder.EncodeChunk(c)
			require.NoError(t, err)
			err = encoder.WriteToFile(lancePath)
			require.NoError(t, err)

			// Read
			f, err := os.Open(lancePath)
			require.NoError(t, err)
			defer f.Close()

			decoder := NewLanceDecoder()
			err = decoder.ReadFrom(f)
			require.NoError(t, err)

			f.Seek(0, 0)
			decoded, err := decoder.DecodeChunk(f)
			require.NoError(t, err)
			require.Equal(t, 20, decoded.Count)
		})
	}
}

func TestLanceRoundTrip_WithNulls(t *testing.T) {
	tmpDir := t.TempDir()
	lancePath := filepath.Join(tmpDir, "nulls.lance")

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(20)

	// Set some values, leave others null
	for i := 0; i < 20; i++ {
		if i%3 == 0 {
			// Null
			chunk.SetNullInPhyFormatFlat(c.Data[0], uint64(i), true)
			chunk.SetNullInPhyFormatFlat(c.Data[1], uint64(i), true)
		} else {
			c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
			c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 0.5})
		}
	}

	// Write
	encoder := NewLanceEncoder()
	err := encoder.EncodeChunk(c)
	require.NoError(t, err)
	err = encoder.WriteToFile(lancePath)
	require.NoError(t, err)

	// Read
	decoder := NewLanceDecoder()
	err = decoder.ReadFromFile(lancePath)
	require.NoError(t, err)
	require.Equal(t, uint64(20), decoder.NumRows())
}

func TestWriteChunkToLanceFile(t *testing.T) {
	tmpDir := t.TempDir()
	lancePath := filepath.Join(tmpDir, "convenience.lance")

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(30)

	for i := 0; i < 30; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 2.0})
	}

	err := WriteChunkToLanceFile(lancePath, c)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(lancePath)
	require.NoError(t, err)
}

func TestReadChunkFromLanceFile(t *testing.T) {
	tmpDir := t.TempDir()
	lancePath := filepath.Join(tmpDir, "convenience_read.lance")

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_DOUBLE),
	}

	originalChunk := &chunk.Chunk{}
	originalChunk.Init(typs, util.DefaultVectorSize)
	originalChunk.SetCard(25)

	for i := 0; i < 25; i++ {
		originalChunk.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i * 5)})
		originalChunk.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], F64: float64(i) * 0.5})
	}

	// Write
	err := WriteChunkToLanceFile(lancePath, originalChunk)
	require.NoError(t, err)

	// Read - verify file can be opened and parsed
	decoder := NewLanceDecoder()
	err = decoder.ReadFromFile(lancePath)
	require.NoError(t, err)
	require.Equal(t, uint64(25), decoder.NumRows())
	require.NotNil(t, decoder.Schema())
}

func TestLtypeToLanceLogicalType(t *testing.T) {
	tests := []struct {
		typ      common.LType
		expected string
	}{
		{common.MakeLType(common.LTID_BOOLEAN), "bool"},
		{common.MakeLType(common.LTID_TINYINT), "int8"},
		{common.MakeLType(common.LTID_UTINYINT), "uint8"},
		{common.MakeLType(common.LTID_SMALLINT), "int16"},
		{common.MakeLType(common.LTID_USMALLINT), "uint16"},
		{common.MakeLType(common.LTID_INTEGER), "int32"},
		{common.MakeLType(common.LTID_UINTEGER), "uint32"},
		{common.MakeLType(common.LTID_BIGINT), "int64"},
		{common.MakeLType(common.LTID_UBIGINT), "uint64"},
		{common.MakeLType(common.LTID_FLOAT), "float"},
		{common.MakeLType(common.LTID_DOUBLE), "double"},
		{common.MakeLType(common.LTID_VARCHAR), "string"},
		{common.MakeLType(common.LTID_BLOB), "binary"},
		{common.MakeLType(common.LTID_DATE), "date32:day"},
		{common.MakeLType(common.LTID_TIME), "time:us"},
		{common.MakeLType(common.LTID_TIMESTAMP), "timestamp:us"},
		{common.MakeLType(common.LTID_LIST), "list"},
		{common.MakeLType(common.LTID_STRUCT), "struct"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			result := ltypeToLanceLogicalType(tt.typ)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLanceLogicalTypeToLType(t *testing.T) {
	tests := []struct {
		lanceType string
		expected  common.LTypeId
	}{
		{"bool", common.LTID_BOOLEAN},
		{"int8", common.LTID_TINYINT},
		{"uint8", common.LTID_UTINYINT},
		{"int16", common.LTID_SMALLINT},
		{"uint16", common.LTID_USMALLINT},
		{"int32", common.LTID_INTEGER},
		{"uint32", common.LTID_UINTEGER},
		{"int64", common.LTID_BIGINT},
		{"uint64", common.LTID_UBIGINT},
		{"float", common.LTID_FLOAT},
		{"double", common.LTID_DOUBLE},
		{"string", common.LTID_VARCHAR},
		{"binary", common.LTID_BLOB},
		{"date32:day", common.LTID_DATE},
		{"time:us", common.LTID_TIME},
		{"timestamp:us", common.LTID_TIMESTAMP},
		{"list", common.LTID_LIST},
		{"struct", common.LTID_STRUCT},
	}

	for _, tt := range tests {
		t.Run(tt.lanceType, func(t *testing.T) {
			result := lanceLogicalTypeToLType(tt.lanceType)
			require.Equal(t, tt.expected, result.Id)
		})
	}
}
