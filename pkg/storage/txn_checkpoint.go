package storage

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type CheckpointWriter struct {
	_metadataWriter      *MetaBlockWriter
	_tableMetadataWriter *MetaBlockWriter
	_partialBlockMgr     *PartialBlockMgr
	_storage             *StorageMgr
}

func NewCheckpointWriter(storage *StorageMgr) *CheckpointWriter {
	ret := &CheckpointWriter{
		_storage: storage,
	}
	ret._partialBlockMgr = NewPartialBlockManager(
		storage._blockMgr,
		CkpTypeFullCkp,
		uint32(DEFAULT_MAX_PARTIAL_BLOCK_SIZE),
		uint32(DEFAULT_MAX_USE_COUNT),
	)
	return ret
}

func (ckpWriter *CheckpointWriter) GetBlockManager() BlockMgr {
	return ckpWriter._storage._blockMgr
}
func (ckpWriter *CheckpointWriter) CreateCheckpoint() error {
	util.AssertFunc(ckpWriter._metadataWriter == nil)

	blockMgr := ckpWriter.GetBlockManager()
	ckpWriter._metadataWriter = NewMetaBlockWriter(blockMgr, -1)
	ckpWriter._tableMetadataWriter = NewMetaBlockWriter(blockMgr, -1)

	//id of the first meta block
	metaBlock := ckpWriter._metadataWriter.GetBlockPointer()._blockId

	//write the schema
	//collect committed schemas
	schemas := make([]*CatalogEntry, 0)
	GCatalog.ScanSchemas(func(ent *CatalogEntry) {
		schemas = append(schemas, ent)
	})
	err := util.Write[uint32](uint32(len(schemas)), ckpWriter._metadataWriter)
	if err != nil {
		return err
	}
	for _, sch := range schemas {
		err = ckpWriter.WriteSchema(sch)
		if err != nil {
			return err
		}
	}
	//
	err = ckpWriter._metadataWriter.Flush()
	if err != nil {
		return err
	}
	err = ckpWriter._tableMetadataWriter.Flush()
	if err != nil {
		return err
	}

	wal := ckpWriter._storage._wal
	err = wal.WriteCheckpoint(metaBlock)
	if err != nil {
		return err
	}
	err = wal.Flush()
	if err != nil {
		return err
	}

	//fill header
	var header DatabaseHeader
	header._metaBlock = metaBlock
	err = blockMgr.WriteHeader(&header)
	if err != nil {
		return err
	}

	err = wal.Truncate(0)
	if err != nil {
		return err
	}
	ckpWriter._metadataWriter.MarkWrittenBlocks()
	ckpWriter._tableMetadataWriter.MarkWrittenBlocks()
	return nil
}

func (ckpWriter *CheckpointWriter) GetMetaBlockWriter() *MetaBlockWriter {
	return ckpWriter._metadataWriter
}

func (ckpWriter *CheckpointWriter) WriteSchema(schEnt *CatalogEntry) error {
	err := schEnt.Serialize(ckpWriter.GetMetaBlockWriter())
	if err != nil {
		return err
	}

	tables := make([]*CatalogEntry, 0)
	schEnt.Scan(CatalogTypeTable, func(ent *CatalogEntry) {
		tables = append(tables, ent)
	})

	writer := NewFieldWriter(ckpWriter.GetMetaBlockWriter())
	err = WriteField[uint32](uint32(len(tables)), writer)
	if err != nil {
		return err
	}
	err = writer.Finalize()
	if err != nil {
		return err
	}

	//write table
	for _, table := range tables {
		err = ckpWriter.WriteTable(table)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ckpWriter *CheckpointWriter) WriteTable(table *CatalogEntry) error {
	err := table.Serialize(ckpWriter.GetMetaBlockWriter())
	if err != nil {
		return err
	}
	//write table data
	writer := ckpWriter.GetTableDataWriter(table)
	if writer != nil {
		return writer.WriteTableData()
	}
	return nil
}

func (ckpWriter *CheckpointWriter) GetTableDataWriter(
	table *CatalogEntry) *TableDataWriter {
	return NewTableDataWriter(
		ckpWriter,
		table,
		ckpWriter._tableMetadataWriter,
		ckpWriter._metadataWriter,
	)
}

type CheckpointReader struct {
	_storage *StorageMgr
}

type ColumnCheckpointState struct {
	_rowGroup        *RowGroup
	_columnData      *ColumnData
	_newTree         *ColumnSegmentTree
	_dataPointers    []*DataPointer
	_partialBlockMgr *PartialBlockMgr
	_globalStats     BaseStats
}

func (state *ColumnCheckpointState) GetStats() *BaseStats {
	return &state._globalStats
}

func (state *ColumnCheckpointState) FlushSegment(
	segment *ColumnSegment,
	sz IdxType) {
	util.AssertFunc(sz <= IdxType(BLOCK_SIZE))
	tupleCount := segment.Count()
	if tupleCount == 0 {
		return
	}

	state._globalStats.Merge(&segment._stats._stats)

	blockId := BlockID(-1)
	offsetInBlock := uint32(0)
	//TODO: replace by blockMgr
	var block *BlockHandle
	if uint64(segment.SegmentSize()) != BLOCK_SIZE {
		util.AssertFunc(
			uint64(segment.SegmentSize()) <
				BLOCK_SIZE)
		segment.Resize(IdxType(BLOCK_SIZE))
	}
	sz = segment.SegmentSize()
	//
	blkMgr := state._partialBlockMgr._blockMgr
	newBlockId := blkMgr.GetFreeBlockId()
	segment.ConvertToPersistent(blkMgr, newBlockId)
	block = segment._block
	blockId = block._blockId

	//copy data
	dataPtr := &DataPointer{}
	dataPtr._stats = segment._stats._stats
	dataPtr._blockPtr._blockId = blockId
	dataPtr._blockPtr._offset = offsetInBlock
	dataPtr._rowStart = uint64(state._rowGroup.Start())
	if len(state._dataPointers) != 0 {
		last := util.Back(state._dataPointers)
		dataPtr._rowStart = last._rowStart + last._tupleCount
	}
	dataPtr._tupleCount = tupleCount

	state._newTree.AppendSegment(nil, segment)
	state._dataPointers = append(state._dataPointers, dataPtr)
}

func (state *ColumnCheckpointState) WriteDataPointers(writer *RowGroupWriter) error {
	return writer.WriteColumnDataPointers(state)
}

func NewPartialBlock(
	data *ColumnData,
	segment *ColumnSegment,
	mgr BlockMgr,
	state *PartialBlockState) *PartialBlock {
	ret := &PartialBlock{
		_block:    segment._block,
		_blockMgr: mgr,
		_state:    state,
	}
	ret.AddSegmentToTail(data, segment, 0)
	return ret
}

func NewColumnCheckpointState(
	rg *RowGroup,
	column *ColumnData,
	mgr *PartialBlockMgr) *ColumnCheckpointState {
	return &ColumnCheckpointState{
		_columnData:      column,
		_rowGroup:        rg,
		_partialBlockMgr: mgr,
		_newTree:         NewColumnSegmentTree(),
	}
}

type DataPointer struct {
	_rowStart   uint64
	_tupleCount uint64
	_blockPtr   BlockPointer
	_stats      BaseStats
}

func (ptr DataPointer) String() string {
	return fmt.Sprintf("[row start %d tuple count %d %s]", ptr._rowStart, ptr._tupleCount, ptr._blockPtr)
}

type ColumnDataCheckpointer struct {
	_colData       *ColumnData
	_rowGroup      *RowGroup
	_state         *ColumnCheckpointState
	_isValidity    bool
	_intermediate  *chunk.Vector
	_nodes         []*SegmentNode[SegmentBase[ColumnSegment]]
	_compressFuncs []*CompressFunction
}

func (ckp *ColumnDataCheckpointer) Checkpoint(
	nodes []*SegmentNode[SegmentBase[ColumnSegment]]) error {
	ckp._nodes = nodes

	//FIXME: check changes
	return ckp.WriteToDisk()
}

func (ckp *ColumnDataCheckpointer) WriteToDisk() error {
	//FIXME: check persistent segment
	compress := ckp._compressFuncs[0]

	state := compress._initCompress(ckp)
	err := ckp.ScanSegments(func(vec *chunk.Vector, count IdxType) error {
		compress._compress(state, vec, count)
		return nil
	})
	if err != nil {
		return err
	}
	compress._compressFinalize(state)
	ckp._nodes = nil
	return nil
}

func (ckp *ColumnDataCheckpointer) ScanSegments(
	callback func(*chunk.Vector, IdxType) error,
) error {
	scanVec := chunk.NewFlatVector(
		ckp._intermediate.Typ(),
		STANDARD_VECTOR_SIZE)
	for _, node := range ckp._nodes {
		seg := node._node.(*ColumnSegment)
		scanState := &ColumnScanState{}
		scanState.Init(seg._type)
		scanState._current = seg
		seg.InitScan(scanState)

		for baseRowIndex := uint64(0); baseRowIndex < seg.Count(); baseRowIndex += STANDARD_VECTOR_SIZE {
			scanVec.Reference(ckp._intermediate)
			count := min(
				seg.Count()-baseRowIndex,
				STANDARD_VECTOR_SIZE,
			)
			scanState._rowIdx = seg.Start() + IdxType(baseRowIndex)
			ckp._colData.CheckpointScan(
				seg,
				scanState,
				ckp._rowGroup.Start(),
				count,
				scanVec,
			)

			err := callback(scanVec, IdxType(count))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func NewColumnDataCheckpointer(
	colData *ColumnData,
	rowGroup *RowGroup,
	state *ColumnCheckpointState,
) *ColumnDataCheckpointer {
	ret := &ColumnDataCheckpointer{
		_colData:    colData,
		_rowGroup:   rowGroup,
		_state:      state,
		_isValidity: colData._typ.Id == common.LTID_VALIDITY,
	}
	if ret._isValidity {
		ret._intermediate = chunk.NewFlatVector(
			common.BooleanType(), STANDARD_VECTOR_SIZE)
	} else {
		ret._intermediate = chunk.NewFlatVector(
			colData._typ, STANDARD_VECTOR_SIZE)
	}
	fun := GetUncompressedCompressFunction(colData._typ.GetInternalType())
	ret._compressFuncs = append(ret._compressFuncs, fun)
	return ret
}

type CheckpointLock struct {
	_mgr      *TxnMgr
	_isLocked bool
}

func NewCheckpointLock(mgr *TxnMgr) *CheckpointLock {
	return &CheckpointLock{
		_mgr: mgr,
	}
}

func (lock *CheckpointLock) Lock() {
	lock._mgr._threadIsCheckpointing = true
	lock._isLocked = true
}

func (lock *CheckpointLock) Unlock() {
	if !lock._isLocked {
		return
	}
	lock._mgr._threadIsCheckpointing = false
	lock._isLocked = false
}
