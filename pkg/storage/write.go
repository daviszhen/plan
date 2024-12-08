package storage

import (
	"bytes"
	"fmt"
	"strings"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

var _ util.Serialize = new(MetaBlockWriter)

type MetaBlockWriter struct {
	_blockMgr      BlockMgr
	_block         *Block
	_writtenBlocks map[BlockID]bool
	_offset        uint64
}

func NewMetaBlockWriter(blockMgr BlockMgr, initBlockId BlockID) *MetaBlockWriter {
	ret := &MetaBlockWriter{
		_blockMgr:      blockMgr,
		_writtenBlocks: make(map[BlockID]bool),
	}
	if initBlockId == -1 {
		initBlockId = ret.GetNextBlockId()
	}
	ret._block = blockMgr.CreateBlock(initBlockId, nil)
	util.Store[BlockID](-1, ret._block._buffer)
	ret._offset = BlockIDSize
	return ret
}

func (writer *MetaBlockWriter) GetBlockPointer() BlockPointer {
	return BlockPointer{
		_blockId: writer._block.id,
		_offset:  uint32(writer._offset),
	}
}

func (writer *MetaBlockWriter) WriteData(buffer []byte, sz int) error {
	for writer._offset+uint64(sz) > writer._block._size {
		//new block
		copyCnt := writer._block._size - writer._offset
		if copyCnt > 0 {
			util.PointerCopy2(
				util.PointerAdd(writer._block._buffer, int(writer._offset)),
				buffer,
				int(copyCnt),
			)
			buffer = buffer[copyCnt:]
			writer._offset += copyCnt
			sz -= int(copyCnt)
		}
		newBlockId := writer.GetNextBlockId()
		util.Store[BlockID](newBlockId, writer._block._buffer)
		err := writer.AdvanceBlock()
		if err != nil {
			return err
		}
		writer._block.id = newBlockId
		util.Store[BlockID](-1, writer._block._buffer)
	}
	util.PointerCopy2(
		util.PointerAdd(writer._block._buffer, int(writer._offset)),
		buffer,
		sz,
	)
	writer._offset += uint64(sz)
	return nil
}

func (writer *MetaBlockWriter) Close() error {
	util.AssertFunc(writer._block == nil)
	return nil
}

func (writer *MetaBlockWriter) Flush() error {
	if writer._offset < writer._block._size {
		util.Memset(
			util.PointerAdd(writer._block._buffer, int(writer._offset)),
			0,
			int(writer._block._size-writer._offset),
		)
	}
	err := writer.AdvanceBlock()
	if err != nil {
		return err
	}
	writer._block = nil
	return err
}

func (writer *MetaBlockWriter) AdvanceBlock() error {
	writer._writtenBlocks[writer._block.id] = true
	if writer._offset > BlockIDSize {
		err := writer._blockMgr.Write(writer._block)
		if err != nil {
			return err
		}
		writer._offset = BlockIDSize
	}
	return nil
}

func (writer *MetaBlockWriter) GetNextBlockId() BlockID {
	return writer._blockMgr.GetFreeBlockId()
}

func (writer *MetaBlockWriter) MarkWrittenBlocks() {
	for id := range writer._writtenBlocks {
		writer._blockMgr.MarkBlockAsModified(id)
	}
}

type FieldWriter struct {
	_serial     util.Serialize
	_buffer     *BufferedSerialize
	_fieldCount uint64
	_finalized  bool
}

func NewFieldWriter(serial util.Serialize) *FieldWriter {
	return &FieldWriter{
		_serial: serial,
		_buffer: NewBufferedSerialize(nil),
	}
}

func (writer *FieldWriter) Close() {
	util.AssertFunc(writer._finalized)
	_ = writer._buffer.Close()
}

func (writer *FieldWriter) AddField() {
	writer._fieldCount++
}

func (writer *FieldWriter) WriteData(buf []byte) error {
	return writer._buffer.WriteData(buf, len(buf))
}

func (writer *FieldWriter) Finalize() error {
	util.AssertFunc(!writer._finalized)
	writer._finalized = true
	util.AssertFunc(writer._fieldCount > 0)
	err := util.Write[uint64](writer._fieldCount, writer._serial)
	if err != nil {
		return err
	}
	err = util.Write[uint64](uint64(writer._buffer._data.Len()), writer._serial)
	if err != nil {
		return err
	}
	err = writer._serial.WriteData(
		writer._buffer._data.Bytes(),
		writer._buffer._data.Len())
	if err != nil {
		return err
	}
	err = writer._buffer.Close()
	if err != nil {
		return err
	}
	writer._buffer = nil
	return err
}

func WriteField[T any](value T, writer *FieldWriter) error {
	writer.AddField()

	cnt := int(unsafe.Sizeof(value))
	buf := util.PointerToSlice[byte](unsafe.Pointer(&value), cnt)
	return writer.WriteData(buf)
}

func WriteString(value string, writer *FieldWriter) error {
	writer.AddField()

	err := util.Write[uint32](uint32(len(value)), writer._buffer)
	if err != nil {
		return err
	}
	if len(value) > 0 {
		return writer._buffer.WriteData(util.UnsafeStringToBytes(value), len(value))
	}
	return nil
}

func WriteColDefs(colDefs []*ColumnDefinition, writer *FieldWriter) error {
	writer.AddField()
	err := util.Write[uint32](uint32(len(colDefs)), writer._buffer)
	if err != nil {
		return err
	}
	for _, colDef := range colDefs {
		err = colDef.Serialize(writer._buffer)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteConstraints(
	cons []Constraint,
	writer *FieldWriter) error {
	writer.AddField()
	err := util.Write[uint32](uint32(len(cons)), writer._buffer)
	if err != nil {
		return err
	}
	for _, con := range cons {
		err = con.Serialize(writer._buffer)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteStrings(strs []string, writer *FieldWriter) error {
	writer.AddField()
	err := util.Write[uint32](
		uint32(len(strs)),
		writer._buffer)
	if err != nil {
		return err
	}
	for _, str := range strs {
		err = util.WriteString(str, writer._buffer)
		if err != nil {
			return err
		}
	}
	return nil
}

func WriteBlob(data []byte, writer *FieldWriter) error {
	writer.AddField()
	if len(data) > 0 {
		return writer.WriteData(data)
	}
	return nil
}

var _ util.Serialize = new(BufferedSerialize)

type BufferedSerialize struct {
	_data *bytes.Buffer
}

func NewBufferedSerialize(buf []byte) *BufferedSerialize {
	return &BufferedSerialize{
		_data: bytes.NewBuffer(buf),
	}
}

func (serial *BufferedSerialize) WriteData(buffer []byte, len int) error {
	var err error
	n := 0
	cnt := 0
	for n < len {
		cnt, err = serial._data.Write(buffer[n:len])
		if err != nil {
			return err
		}
		n += cnt
	}
	return nil
}

func (serial *BufferedSerialize) Close() error {
	serial._data.Reset()
	serial._data = nil
	return nil
}

var _ util.Deserialize = new(BufferedDeserializer)

type BufferedDeserializer struct {
	_data *bytes.Buffer
}

func NewBufferedDeserializer(buf []byte) *BufferedDeserializer {
	ret := &BufferedDeserializer{
		_data: bytes.NewBuffer(buf),
	}
	return ret
}

func (deseril *BufferedDeserializer) ReadData(buffer []byte, len int) error {
	_, err := deseril._data.Read(buffer[:len])
	return err
}

func (deseril *BufferedDeserializer) Close() error {
	deseril._data.Reset()
	deseril._data = nil
	return nil
}

type RowGroupPointer struct {
	_rowStart     uint64
	_tupleCount   uint64
	_dataPointers []*BlockPointer
	_versions     *VersionNode
}

func (rgPtr *RowGroupPointer) String() string {
	bb := strings.Builder{}
	bb.WriteString(fmt.Sprintf("rg ptr : row start %d tuple count %d %v",
		rgPtr._rowStart,
		rgPtr._tupleCount,
		rgPtr._dataPointers))
	return bb.String()
}

type TableDataWriter struct {
	_table            *CatalogEntry
	_rowGroupPointers []*RowGroupPointer
	_checkpointMgr    *CheckpointWriter
	_tableDataWriter  *MetaBlockWriter
	_metaDataWriter   *MetaBlockWriter
}

func (writer *TableDataWriter) WriteTableData() error {
	return writer._table._storage.Checkpoint(writer)
}

func (writer *TableDataWriter) FinalizeTable(globalStats *TableStats, info *DataTableInfo) error {
	ptr := writer._tableDataWriter.GetBlockPointer()
	var err error

	err = globalStats.Serialize(writer._tableDataWriter)
	if err != nil {
		return err
	}

	err = util.Write[uint64](uint64(len(writer._rowGroupPointers)), writer._tableDataWriter)
	if err != nil {
		return err
	}
	tRows := uint64(0)
	for _, rgPtr := range writer._rowGroupPointers {
		rgCount := rgPtr._rowStart + rgPtr._tupleCount
		if rgCount > tRows {
			tRows = rgCount
		}
		err = RowGroupSerialize(rgPtr, writer._tableDataWriter)
		if err != nil {
			return err
		}
	}
	err = util.Write[BlockID](ptr._blockId, writer._metaDataWriter)
	if err != nil {
		return err
	}
	err = util.Write[uint64](uint64(ptr._offset), writer._metaDataWriter)
	if err != nil {
		return err
	}
	err = util.Write[uint64](tRows, writer._metaDataWriter)
	if err != nil {
		return err
	}

	//serialize indexes
	blkPtrs, err := info._indexes.SerializeIndexes(writer._tableDataWriter)
	if err != nil {
		return err
	}
	//indexes count
	err = util.Write[uint64](uint64(len(blkPtrs)), writer._metaDataWriter)
	if err != nil {
		return err
	}
	for _, blkPtr := range blkPtrs {
		err = util.Write[BlockID](blkPtr._blockId, writer._metaDataWriter)
		if err != nil {
			return err
		}
		err = util.Write[uint32](blkPtr._offset, writer._metaDataWriter)
		if err != nil {
			return err
		}
	}
	return nil
}

func (writer *TableDataWriter) GetRowGroupWriter(rg *RowGroup) *RowGroupWriter {
	return NewRowGroupWriter(
		writer._table,
		writer._checkpointMgr._partialBlockMgr,
		writer._tableDataWriter)
}

func (writer *TableDataWriter) AddRowGroup(
	pointer *RowGroupPointer,
	writer2 *RowGroupWriter) {
	writer._rowGroupPointers = append(writer._rowGroupPointers, pointer)
}

func NewTableDataWriter(
	checkpointMgr *CheckpointWriter,
	table *CatalogEntry,
	tableDataWriter *MetaBlockWriter,
	metaDataWriter *MetaBlockWriter,
) *TableDataWriter {
	return &TableDataWriter{
		_table:           table,
		_checkpointMgr:   checkpointMgr,
		_tableDataWriter: tableDataWriter,
		_metaDataWriter:  metaDataWriter,
	}
}

type RowGroupWriter struct {
	_table           *CatalogEntry
	_partialBlockMgr *PartialBlockMgr
	_tableDataWriter *MetaBlockWriter
}

func (writer *RowGroupWriter) GetPayloadWriter() *MetaBlockWriter {
	return writer._tableDataWriter
}

func (writer *RowGroupWriter) WriteColumnDataPointers(state *ColumnCheckpointState) error {
	metaWriter := writer._tableDataWriter
	dataPtrs := state._dataPointers
	err := util.Write[IdxType](IdxType(len(dataPtrs)), metaWriter)
	if err != nil {
		return err
	}
	for _, ptr := range dataPtrs {
		err = util.Write[uint64](ptr._rowStart, metaWriter)
		if err != nil {
			return err
		}
		err = util.Write[uint64](ptr._tupleCount, metaWriter)
		if err != nil {
			return err
		}
		err = util.Write[BlockID](ptr._blockPtr._blockId, metaWriter)
		if err != nil {
			return err
		}
		err = util.Write[uint32](ptr._blockPtr._offset, metaWriter)
		if err != nil {
			return err
		}
		err = ptr._stats.Serialize(metaWriter)
		if err != nil {
			return err
		}
	}
	return err
}

func NewRowGroupWriter(
	table *CatalogEntry,
	partialBlockMgr *PartialBlockMgr,
	tableDataWriter *MetaBlockWriter,
) *RowGroupWriter {
	return &RowGroupWriter{
		_table:           table,
		_partialBlockMgr: partialBlockMgr,
		_tableDataWriter: tableDataWriter,
	}

}

type FreeListBlockWriter struct {
	*MetaBlockWriter
	_freeListBlocks []BlockID
	_index          IdxType
}

func NewFreeListBlockWriter(
	blockMgr BlockMgr,
	freeListBlocks []BlockID,
) *FreeListBlockWriter {
	return &FreeListBlockWriter{
		MetaBlockWriter: NewMetaBlockWriter(blockMgr, freeListBlocks[0]),
		_freeListBlocks: freeListBlocks,
		_index:          1,
	}
}

func (writer *FreeListBlockWriter) GetNextBlockId() BlockID {
	if writer._index >= IdxType(len(writer._freeListBlocks)) {
		panic("ran out")
	}
	id := writer._freeListBlocks[writer._index]
	writer._index++
	return id
}
