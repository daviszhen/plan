package storage

import (
	"fmt"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

var _ util.Deserialize = new(MetaBlockReader)

type MetaBlockReader struct {
	_blockMgr         BlockMgr
	_block            *BlockHandle
	_handle           *BufferHandle
	_offset           uint64
	_nextBlock        BlockID
	_freeBlocksOnRead bool
}

func NewMetaBlockReader(
	blockMgr BlockMgr,
	id BlockID,
	freeBlocksOnRead bool) (*MetaBlockReader, error) {
	reader := &MetaBlockReader{
		_blockMgr:         blockMgr,
		_offset:           0,
		_nextBlock:        -1,
		_freeBlocksOnRead: freeBlocksOnRead,
	}
	err := reader.ReadNewBlock(id)
	return reader, err
}

func (reader *MetaBlockReader) ReadNewBlock(id BlockID) error {
	bufMgr := reader._blockMgr.BufferMgr()
	if reader._freeBlocksOnRead {
		reader._blockMgr.MarkBlockAsModified(id)
	}
	reader._block = reader._blockMgr.RegisterBlock(id, true)
	reader._handle = bufMgr.Pin(reader._block)
	reader._nextBlock = util.Load[BlockID](reader._handle.Ptr())
	util.AssertFunc(reader._nextBlock >= -1)
	reader._offset = uint64(unsafe.Sizeof(BlockID(0)))
	return nil
}

func (reader *MetaBlockReader) ReadData(buffer []byte, readSize int) error {
	pos := uint64(0)
	for reader._offset+uint64(readSize) >
		reader._handle.FileBuffer()._size {
		toRead := reader._handle.FileBuffer()._size -
			reader._offset
		if toRead > 0 {
			src := util.PointerToSlice[byte](
				util.PointerAdd(
					reader._handle.Ptr(),
					int(reader._offset),
				),
				int(toRead),
			)
			copy(buffer[pos:], src)
			readSize -= int(toRead)
			pos += toRead
		}
		if reader._nextBlock == -1 {
			return fmt.Errorf("read invalid block")
		}
		err := reader.ReadNewBlock(reader._nextBlock)
		if err != nil {
			return err
		}
	}
	src := util.PointerToSlice[byte](
		util.PointerAdd(
			reader._handle.Ptr(),
			int(reader._offset),
		),
		readSize,
	)
	copy(buffer[pos:], src)
	reader._offset += uint64(readSize)
	return nil
}

func (reader *MetaBlockReader) Close() error {
	return nil
}

func ReadRequired[T any](value *T, reader *FieldReader) error {
	if reader._fieldCount >= reader._maxFieldCount {
		return fmt.Errorf("field_count >= max_field_count")
	}
	reader.AddField()
	return util.Read[T](value, reader._source)
}

func ReadString(reader *FieldReader) (string, error) {
	if reader._fieldCount >= reader._maxFieldCount {
		return "", fmt.Errorf("field_count >= max_field_count")
	}
	reader.AddField()
	return util.ReadString(reader._source)
}

func ReadBlob(data []byte, reader *FieldReader) error {
	reader.AddField()
	return reader._source.ReadData(data, len(data))
}

type FieldReader struct {
	_source        util.Deserialize
	_fieldCount    uint64
	_maxFieldCount uint64
	_totalSize     uint64
	_finalized     bool
}

func NewFieldReader(source util.Deserialize) (*FieldReader, error) {
	ret := &FieldReader{
		_source: source,
	}
	err := util.Read[uint64](&ret._maxFieldCount, source)
	if err != nil {
		return nil, err
	}
	err = util.Read[uint64](&ret._totalSize, source)
	if err != nil {
		return nil, err
	}
	util.AssertFunc(ret._maxFieldCount > 0)
	return ret, err
}

func (reader *FieldReader) Finalize() {
	util.AssertFunc(!reader._finalized)
	reader._finalized = true
}

func (reader *FieldReader) AddField() {
	reader._fieldCount++
}

type FileCheckpointReader struct {
	_storage *StorageMgr
}

func NewFileCheckpointReader(
	storage *StorageMgr,
) *FileCheckpointReader {
	return &FileCheckpointReader{
		_storage: storage,
	}
}

func (reader *FileCheckpointReader) LoadFromStorage() (retErr error) {
	bmgr := reader._storage._blockMgr
	metaBlock := bmgr.GetMetaBlock()
	if metaBlock < 0 {
		return nil
	}
	txn, err := GTxnMgr.NewTxn("load checkpoint")
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			GTxnMgr.Rollback(txn)
		} else {
			err2 := GTxnMgr.Commit(txn)
			if err2 != nil {
				retErr = err2
			}
		}
	}()
	metaReader, err := NewMetaBlockReader(
		bmgr,
		metaBlock,
		true,
	)
	if err != nil {
		return err
	}
	defer metaReader.Close()
	err = reader.LoadCheckpoint(metaReader, txn)
	if err != nil {
		return err
	}
	return
}

func (reader *FileCheckpointReader) LoadCheckpoint(mReader *MetaBlockReader, txn *Txn) error {
	schCnt := uint32(0)
	err := util.Read[uint32](&schCnt, mReader)
	if err != nil {
		return err
	}
	for i := uint32(0); i < schCnt; i++ {
		err = reader.ReadSchema(mReader, txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (reader *FileCheckpointReader) ReadSchema(mReader *MetaBlockReader, txn *Txn) error {
	schEnt := &CatalogEntry{}
	err := schEnt.Deserialize(mReader)
	if err != nil {
		return err
	}
	//create schema
	err = GCatalog.CreateSchema2(txn, schEnt._name)
	if err != nil {
		return err
	}

	fReader, err := NewFieldReader(mReader)
	if err != nil {
		return err
	}

	tblCnt := uint32(0)
	err = ReadRequired[uint32](&tblCnt, fReader)
	if err != nil {
		return err
	}
	fReader.Finalize()

	for i := uint32(0); i < tblCnt; i++ {
		err = reader.ReadTable(mReader, txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func (reader *FileCheckpointReader) ReadTable(
	mReader *MetaBlockReader,
	txn *Txn) error {
	tabEnt := &CatalogEntry{}
	err := tabEnt.Deserialize(mReader)
	if err != nil {
		return err
	}

	schEnt := GCatalog.GetSchema(txn, tabEnt._schName)
	if schEnt == nil {
		return fmt.Errorf("no schema %s", tabEnt._schName)
	}

	info := NewDataTableInfo()
	info._schema = tabEnt._schName
	info._table = tabEnt._name
	info._colDefs = tabEnt._colDefs
	info._constraints = tabEnt._constraints
	err = reader.ReadTableData(mReader, info, txn)
	if err != nil {
		return err
	}

	_, err = GCatalog.CreateTable(txn, info)
	if err != nil {
		return err
	}
	return err
}

func (reader *FileCheckpointReader) ReadTableData(
	mReader *MetaBlockReader,
	info *DataTableInfo,
	txn *Txn) error {
	bid := BlockID(0)
	offset := uint64(0)
	err := util.Read[BlockID](&bid, mReader)
	if err != nil {
		return err
	}
	err = util.Read[uint64](&offset, mReader)
	if err != nil {
		return err
	}
	tableDataReader, err := NewMetaBlockReader(
		mReader._blockMgr,
		bid,
		true)
	if err != nil {
		return err
	}
	tableDataReader._offset = offset
	dataReader := NewTableDataReader(tableDataReader, info)
	err = dataReader.ReadTableData()
	if err != nil {
		return err
	}
	err = util.Read(&info._data._totalRows, mReader)
	if err != nil {
		return err
	}

	var indexesCount uint64
	err = util.Read[uint64](&indexesCount, mReader)
	if err != nil {
		return err
	}
	blkPtrs := make([]BlockPointer, indexesCount)
	for i := uint64(0); i < indexesCount; i++ {
		err = util.Read[BlockID](&blkPtrs[i]._blockId, mReader)
		if err != nil {
			return err
		}
		err = util.Read[uint32](&blkPtrs[i]._offset, mReader)
		if err != nil {
			return err
		}
	}
	info._indexesBlkPtrs = blkPtrs
	return nil
}

func ReadColDefs(fReader *FieldReader) ([]*ColumnDefinition, error) {
	fReader.AddField()
	colCnt := uint32(0)
	err := util.Read[uint32](&colCnt, fReader._source)
	if err != nil {
		return nil, err
	}
	ret := make([]*ColumnDefinition, 0)
	for i := uint32(0); i < colCnt; i++ {
		col := &ColumnDefinition{}
		err = col.Deserialize(fReader._source)
		if err != nil {
			return nil, err
		}
		ret = append(ret, col)
	}
	return ret, err
}

func ReadConstraints(fReader *FieldReader) ([]Constraint, error) {
	fReader.AddField()
	cnt := uint32(0)
	err := util.Read[uint32](&cnt, fReader._source)
	if err != nil {
		return nil, err
	}
	ret := make([]Constraint, 0)
	for i := uint32(0); i < cnt; i++ {
		col := &Constraint{}
		err = col.Deserialize(fReader._source)
		if err != nil {
			return nil, err
		}
		ret = append(ret, *col)
	}
	return ret, err
}

func ReadStrings(fReader *FieldReader) ([]string, error) {
	fReader.AddField()
	cnt := uint32(0)
	err := util.Read[uint32](&cnt, fReader._source)
	if err != nil {
		return nil, err
	}
	ret := make([]string, 0)
	for i := uint32(0); i < cnt; i++ {
		s, err := util.ReadString(fReader._source)
		if err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}
	return ret, nil
}

type TableDataReader struct {
	_reader *MetaBlockReader
	_info   *DataTableInfo
}

func NewTableDataReader(
	reader *MetaBlockReader,
	info *DataTableInfo) *TableDataReader {
	ret := &TableDataReader{
		_reader: reader,
		_info:   info,
	}
	info._data = NewPersistentTableData()
	return ret
}

func (reader *TableDataReader) ReadTableData() error {
	var err error
	err = reader._info._data._tableStats.Deserialize(reader._reader, reader._info._colDefs)
	if err != nil {
		return err
	}

	err = util.Read[IdxType](
		&reader._info._data._rowGroupCount,
		reader._reader)
	if err != nil {
		return err
	}
	reader._info._data._blockId = IdxType(reader._reader._block._blockId)
	reader._info._data._offset = IdxType(reader._reader._offset)
	return nil
}
