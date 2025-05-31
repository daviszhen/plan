package storage

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	STANDARD_VECTOR_SIZE     = util.DefaultVectorSize
	STANDARD_ROW_GROUPS_SIZE = 122880
	ROW_GROUP_SIZE           = STANDARD_ROW_GROUPS_SIZE
	ROW_GROUP_VECTOR_COUNT   = ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE
	MERGE_THRESHOLD          = ROW_GROUP_SIZE
)

type RowGroup struct {
	SegmentBase[RowGroup]
	_collect        *RowGroupCollection
	_versionInfo    *VersionNode
	_columns        []*ColumnData
	_columnPointers []*BlockPointer
	_rowGroupLock   sync.Mutex
	_statsLock      sync.Mutex
}

func (rg *RowGroup) InitEmpty(types []common.LType) {
	util.AssertFunc(len(rg._columns) == 0)
	for i, typ := range types {
		cd := NewColumnData(
			rg._collect._blockMgr,
			rg._collect._info,
			i,
			rg.Start(),
			typ,
			nil,
		)
		rg._columns = append(rg._columns, cd)
	}
}

func (rg *RowGroup) ColumnCount() int {
	return len(rg._columns)
}

func (rg *RowGroup) GetColumn(i int) *ColumnData {
	return rg._columns[i]
}

func (rg *RowGroup) InitAppend(state *RowGroupAppendState) {
	state._rowGroup = rg
	state._offsetInRowGroup = IdxType(rg.Count())
	state._states = make([]*ColumnAppendState, len(rg._columns))
	for i := 0; i < len(state._states); i++ {
		state._states[i] = &ColumnAppendState{}
		col := rg.GetColumn(i)
		col.InitAppend(state._states[i])
	}
}

func (rg *RowGroup) AppendVersionInfo(txn *Txn, count IdxType) {
	rowGroupStart := IdxType(rg.Count())
	rowGroupEnd := rowGroupStart + count
	if rowGroupEnd > ROW_GROUP_SIZE {
		rowGroupEnd = ROW_GROUP_SIZE
	}
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	if rg._versionInfo == nil {
		rg._versionInfo = &VersionNode{}
	}
	startVectorIdx := rowGroupStart / STANDARD_VECTOR_SIZE
	endVectorIdx := (rowGroupEnd - 1) / STANDARD_VECTOR_SIZE
	for idx := startVectorIdx; idx <= endVectorIdx; idx++ {
		start := IdxType(0)
		end := IdxType(0)
		if idx == startVectorIdx {
			start = rowGroupStart - startVectorIdx*STANDARD_VECTOR_SIZE
		}
		if idx == endVectorIdx {
			end = rowGroupEnd - endVectorIdx*STANDARD_VECTOR_SIZE
		} else {
			end = STANDARD_VECTOR_SIZE
		}
		if start == 0 && end == STANDARD_VECTOR_SIZE {
			//full vector
			info := NewConstantInfo(rg.Start() + idx*STANDARD_VECTOR_SIZE)
			info._insertId.Store(uint64(txn._id))
			info._deleteId.Store(uint64(NotDeletedId))
			rg._versionInfo._info[idx] = info
		} else {
			var info *ChunkInfo
			if rg._versionInfo._info[idx] == nil {
				//first time to vector
				info = NewVectorInfo(rg.Start() + idx*STANDARD_VECTOR_SIZE)
				rg._versionInfo._info[idx] = info
			} else {
				info = rg._versionInfo._info[idx]
			}
			info.Append(start, end, txn._id)
		}
	}
	rg.SetCount(uint64(rowGroupEnd))
}

func (rg *RowGroup) Append(
	state *RowGroupAppendState,
	data *chunk.Chunk,
	cnt IdxType) {
	for i := 0; i < rg.ColumnCount(); i++ {
		col := rg.GetColumn(i)
		col.Append(state._states[i], data.Data[i], cnt)
	}
	state._offsetInRowGroup += cnt
}

func (rg *RowGroup) MoveToCollection(collect *RowGroupCollection, newStart IdxType) {
	rg._collect = collect
	rg.SetStart(newStart)
	for _, column := range rg._columns {
		column.SetStart(newStart)
	}
	if rg._versionInfo != nil {
		rg._versionInfo.SetStart(newStart)
	}
}

func (rg *RowGroup) CommitAppend(
	commitId TxnType,
	rgStart IdxType,
	cnt IdxType) {
	rgEnd := rgStart + cnt
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	startIdx := rgStart / STANDARD_VECTOR_SIZE
	endIdx := (rgEnd - 1) / STANDARD_VECTOR_SIZE
	for idx := startIdx; idx <= endIdx; idx++ {
		start := IdxType(0)
		end := IdxType(0)
		if idx == startIdx {
			start = rgStart - startIdx*STANDARD_VECTOR_SIZE
		}
		if idx == endIdx {
			end = rgEnd - endIdx*STANDARD_VECTOR_SIZE
		} else {
			end = STANDARD_VECTOR_SIZE
		}
		info := rg._versionInfo._info[idx]
		info.CommitAppend(commitId, start, end)
	}
}

func (rg *RowGroup) InitScan(state *CollectionScanState) bool {
	colIds := state.GetColumnIds()
	state._rowGroup = rg
	state._vectorIdx = 0
	state._maxRowGroupRow = min(
		IdxType(rg.Count()),
		state._maxRow-rg.Start())
	if rg.Start() > state._maxRow {
		state._maxRowGroupRow = 0
	}
	if state._maxRowGroupRow == 0 {
		return false
	}
	for i, idx := range colIds {
		if idx != COLUMN_IDENTIFIER_ROW_ID {
			col := rg.GetColumn(int(idx))
			col.InitScan(state._columnScans[i])
		} else {
			state._columnScans[i]._current = nil
		}
	}
	return true
}

func (rg *RowGroup) Scan(txn *Txn, state *CollectionScanState, result *chunk.Chunk) {
	rg.TemplatedScan(txn, state, result, TableScanTypeRegular)
}

func (rg *RowGroup) TemplatedScan(txn *Txn, state *CollectionScanState, result *chunk.Chunk, scanTyp TableScanType) {
	allowUpdates := scanTyp != TableScanTypeCommittedRowsDisallowUpdates &&
		scanTyp != TableScanTypeCommittedRowsOmitPermanentlyDeleted
	colIds := state.GetColumnIds()
	for {
		if state._vectorIdx*STANDARD_VECTOR_SIZE >=
			state._maxRowGroupRow {
			return
		}
		currentRow := state._vectorIdx * STANDARD_VECTOR_SIZE
		maxCount := min(STANDARD_VECTOR_SIZE, state._maxRowGroupRow-currentRow)
		count := IdxType(0)
		validSel := chunk.NewSelectVector(STANDARD_VECTOR_SIZE)
		if scanTyp == TableScanTypeRegular {
			count = state._rowGroup.GetSelVector(
				txn,
				state._vectorIdx,
				validSel,
				maxCount,
			)
			if count == 0 {
				rg.NextVector(state)
				continue
			}
		} else if scanTyp == TableScanTypeCommittedRowsOmitPermanentlyDeleted {
			panic("usp")
		} else {
			count = maxCount
		}
		if count == maxCount {
			for i, idx := range colIds {
				if idx == COLUMN_IDENTIFIER_ROW_ID {
					//row id
					util.AssertFunc(result.Data[i].Typ().GetInternalType() == common.INT64)
					result.Data[i].Sequence(uint64(rg.Start()+currentRow), 1, uint64(count))
				} else {
					col := rg.GetColumn(int(idx))
					if scanTyp != TableScanTypeRegular {
						col.ScanCommitted(
							txn,
							state._vectorIdx,
							state._columnScans[i],
							result.Data[i],
							allowUpdates,
						)
					} else {
						col.Scan(
							txn,
							state._vectorIdx,
							state._columnScans[i],
							result.Data[i])
					}
				}
			}
		} else {
			approvedTupleCount := count
			sel := chunk.NewSelectVector(STANDARD_VECTOR_SIZE)
			if count != maxCount {
				sel.Init2(validSel)
			} else {
				sel.Init2(nil)
			}
			if approvedTupleCount == 0 {
				result.Reset()
				//for i := 0; i < len(colIds); i++ {
				//	colIdx := colIds[i]
				//	if colIdx == IdxType(-1) {
				//		continue
				//	}
				//}
				state._vectorIdx++
				continue
			}
			for i, idx := range colIds {
				if idx == COLUMN_IDENTIFIER_ROW_ID {
					util.AssertFunc(result.Data[i].Typ().GetInternalType() == common.INT64)
					result.Data[i].SetPhyFormat(chunk.PF_FLAT)
					resultSlice := chunk.GetSliceInPhyFormatFlat[int64](result.Data[i])
					for selIdx := IdxType(0); selIdx < approvedTupleCount; selIdx++ {
						resultSlice[selIdx] = int64(rg.Start() +
							currentRow +
							IdxType(sel.GetIndex(int(selIdx))))
					}
				} else {
					colData := rg.GetColumn(int(idx))
					if scanTyp == TableScanTypeRegular {
						colData.FilterScan(
							txn,
							state._vectorIdx,
							state._columnScans[i],
							result.Data[i],
							sel, approvedTupleCount)
					} else {
						colData.FilterScanCommitted(
							txn,
							state._vectorIdx,
							state._columnScans[i],
							result.Data[i],
							sel, approvedTupleCount,
							allowUpdates)
					}
				}
			}
			count = approvedTupleCount
		}
		result.SetCard(int(count))
		state._vectorIdx++
		break
	}
}

func (rg *RowGroup) GetSelVector(
	txn *Txn,
	vectorIdx IdxType,
	sel *chunk.SelectVector,
	maxCount IdxType) IdxType {
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	info := rg.GetChunkInfo(vectorIdx)
	if info == nil {
		return maxCount
	}
	return info.GetSelVector(txn, sel, maxCount)
}

func (rg *RowGroup) GetChunkInfo(idx IdxType) *ChunkInfo {
	if rg._versionInfo == nil {
		return nil
	}
	return rg._versionInfo._info[idx]
}

func (rg *RowGroup) NextVector(state *CollectionScanState) {
	state._vectorIdx++
	colIds := state.GetColumnIds()
	for i, idx := range colIds {
		if idx == COLUMN_IDENTIFIER_ROW_ID {
			continue
		}
		col := rg.GetColumn(int(idx))
		col.Skip(state._columnScans[i], STANDARD_VECTOR_SIZE)
	}
}

func (rg *RowGroup) Delete(
	txn *Txn,
	table *DataTable,
	ids []RowType,
	count IdxType) IdxType {
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	delState := NewVersionDeleteState(rg, txn, table, rg.Start())

	for i := IdxType(0); i < count; i++ {
		util.AssertFunc(ids[i] >= 0)
		util.AssertFunc(IdxType(ids[i]) >= rg.Start())
		util.AssertFunc(IdxType(ids[i]) < rg.Start()+IdxType(rg.Count()))
		delState.Delete(ids[i] - RowType(rg.Start()))
	}
	delState.Flush()
	return delState._deleteCount
}

func (rg *RowGroup) Update(
	txn *Txn,
	updates *chunk.Chunk,
	ids []RowType,
	offset IdxType,
	count IdxType,
	colIds []IdxType,
) {
	for i, idx := range colIds {
		util.AssertFunc(idx != COLUMN_IDENTIFIER_ROW_ID)
		colData := rg.GetColumn(int(idx))
		util.AssertFunc(colData._typ.Id ==
			updates.Data[i].Typ().Id)
		if offset > 0 {
			tvec := chunk.NewEmptyVector(
				updates.Data[i].Typ(),
				updates.Data[i].PhyFormat(),
				STANDARD_VECTOR_SIZE,
			)
			tvec.Slice3(updates.Data[i], uint64(offset), uint64(count))
			tvec.Flatten(int(count))
			colData.Update(
				txn,
				idx,
				tvec,
				ids[offset:],
				count)
		} else {
			colData.Update(
				txn,
				idx,
				updates.Data[i],
				ids,
				count)
		}
	}
}

func (rg *RowGroup) RevertAppend(rgStart IdxType) {
	if rg._versionInfo == nil {
		return
	}

	startRow := rgStart - rg.Start()
	startVecIdx := (startRow + (STANDARD_VECTOR_SIZE - 1)) / STANDARD_VECTOR_SIZE
	//clean chunk info
	for vecIdx := startVecIdx; vecIdx < ROW_GROUP_VECTOR_COUNT; vecIdx++ {
		rg._versionInfo._info[vecIdx] = nil
	}
	for _, column := range rg._columns {
		column.RevertAppend(rgStart)
	}
	minValue := min(uint64(startRow), rg.Count())
	rg.SetCount(minValue)
}

func (rg *RowGroup) InitScanWithOffset(
	state *CollectionScanState,
	vecOffset IdxType) bool {

	colIds := state.GetColumnIds()
	state._rowGroup = rg
	state._vectorIdx = vecOffset
	state._maxRowGroupRow = min(IdxType(rg.Count()),
		state._maxRow-rg.Start())
	if rg.Start() > state._maxRow {
		state._maxRowGroupRow = 0
	}
	for i := 0; i < len(colIds); i++ {
		colIdx := colIds[i]
		if colIdx != COLUMN_IDENTIFIER_ROW_ID {
			colData := rg.GetColumn(int(colIdx))
			colData.InitScanWithOffset(
				state._columnScans[i],
				rg.Start()+vecOffset*STANDARD_VECTOR_SIZE,
			)
		} else {
			state._columnScans[i]._current = nil
		}
	}

	return true
}

func (rg *RowGroup) ScanCommitted(
	state *CollectionScanState,
	result *chunk.Chunk,
	typ TableScanType) {
	id, start := GTxnMgr.Lowest()
	txn, err := GTxnMgr.NewTxn2("lowest", id, start)
	if err != nil {
		panic(err)
	}
	defer txn.Rollback()

	switch typ {
	case TableScanTypeCommittedRows,
		TableScanTypeCommittedRowsDisallowUpdates,
		TableScanTypeCommittedRowsOmitPermanentlyDeleted:
		rg.TemplatedScan(txn, state, result, typ)
	default:
		panic("usp")
	}
}

func (rg *RowGroup) UpdateColumn(
	txn *Txn,
	updates *chunk.Chunk,
	rowIds *chunk.Vector,
	colPath []IdxType) {
	idsSlice := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)
	primaryColIdx := colPath[0]
	util.AssertFunc(primaryColIdx != COLUMN_IDENTIFIER_ROW_ID)
	util.AssertFunc(primaryColIdx < IdxType(len(rg._columns)))

	colData := rg.GetColumn(int(primaryColIdx))
	colData.UpdateColumn(txn, colPath, updates.Data[0], idsSlice, updates.Card(), 1)
}

func (rg *RowGroup) Checkpoint(writer *RowGroupWriter, globalStats *TableStats) (*RowGroupPointer, error) {
	rgPtr := &RowGroupPointer{}
	result, err := rg.WriteToDisk(writer._partialBlockMgr)
	if err != nil {
		return nil, err
	}
	for i := 0; i < rg.ColumnCount(); i++ {
		globalStats.GetStats(i)._stats.Merge(result._stats[i])
	}
	//fill row group pointer
	//save column meta data
	rgPtr._rowStart = uint64(rg.Start())
	rgPtr._tupleCount = rg.Count()
	for _, state := range result._states {
		dataWriter := writer.GetPayloadWriter()
		ptr := dataWriter.GetBlockPointer()
		rgPtr._dataPointers = append(rgPtr._dataPointers, &ptr)
		err = state.WriteDataPointers(writer)
		if err != nil {
			return nil, err
		}
	}
	rgPtr._versions = rg._versionInfo
	return rgPtr, nil
}

func (rg *RowGroup) WriteToDisk(mgr *PartialBlockMgr) (*RowGroupWriteData, error) {
	result := &RowGroupWriteData{}

	for i := 0; i < rg.ColumnCount(); i++ {
		col := rg.GetColumn(i)
		state, err := col.Checkpoint(rg, mgr)
		if err != nil {
			return nil, err
		}
		result._states = append(result._states, state)
		result._stats = append(result._stats, state.GetStats())
	}
	return result, nil
}

//func (rg *RowGroup) Load() {
//
//}

func RowGroupDeserialize(
	src util.Deserialize,
	typs []common.LType) (*RowGroupPointer, error) {
	result := &RowGroupPointer{}
	reader, err := NewFieldReader(src)
	if err != nil {
		return nil, err
	}
	err = ReadRequired[uint64](&result._rowStart, reader)
	if err != nil {
		return nil, err
	}
	err = ReadRequired[uint64](&result._tupleCount, reader)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(typs); i++ {
		ptr := &BlockPointer{}
		err = util.Read[BlockID](&ptr._blockId, src)
		if err != nil {
			return nil, err
		}
		tval := uint64(0)
		err = util.Read[uint64](&tval, src)
		if err != nil {
			return nil, err
		}
		ptr._offset = uint32(tval)
		result._dataPointers = append(result._dataPointers, ptr)
	}
	result._versions, err = RowGroupDeserializeDeletes(src)
	if err != nil {
		return nil, err
	}
	reader.Finalize()
	return result, err
}

func RowGroupDeserializeDeletes(src util.Deserialize) (*VersionNode, error) {
	chunkCnt := IdxType(0)
	err := util.Read[IdxType](&chunkCnt, src)
	if err != nil {
		return nil, err
	}
	if chunkCnt == 0 {
		return nil, nil
	}
	info := &VersionNode{}
	for i := 0; i < int(chunkCnt); i++ {
		idx := IdxType(0)
		err = util.Read[IdxType](&idx, src)
		if err != nil {
			return nil, err
		}
		if idx >= ROW_GROUP_VECTOR_COUNT {
			return nil, errors.New("invalid chunk index")
		}
		info._info[idx] = &ChunkInfo{}
		err = info._info[idx].Deserialize(src)
		if err != nil {
			return nil, err
		}
		if info._info[idx]._type == EMPTY_INFO {
			info._info[idx] = nil
		}
	}
	return info, nil
}

func RowGroupSerialize(ptr *RowGroupPointer, serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := WriteField[uint64](ptr._rowStart, writer)
	if err != nil {
		return err
	}
	err = WriteField[uint64](ptr._tupleCount, writer)
	if err != nil {
		return err
	}
	bufSerial := writer._buffer
	for _, dPtr := range ptr._dataPointers {
		err = util.Write[BlockID](dPtr._blockId, bufSerial)
		if err != nil {
			return err
		}
		err = util.Write[uint64](uint64(dPtr._offset), bufSerial)
		if err != nil {
			return err
		}
	}
	err = RowGroupCheckpointDeletes(ptr._versions, bufSerial)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func RowGroupCheckpointDeletes(
	versions *VersionNode,
	serial util.Serialize) error {
	if versions == nil {
		return util.Write[IdxType](0, serial)
	}
	chunkInfoCount := IdxType(0)
	for vecIdx := 0; vecIdx < ROW_GROUP_VECTOR_COUNT; vecIdx++ {
		chunkInfo := versions._info[vecIdx]
		if chunkInfo == nil {
			continue
		}
		chunkInfoCount++
	}
	err := util.Write[IdxType](chunkInfoCount, serial)
	if err != nil {
		return err
	}
	for vecIdx := 0; vecIdx < ROW_GROUP_VECTOR_COUNT; vecIdx++ {
		chunkInfo := versions._info[vecIdx]
		if chunkInfo == nil {
			continue
		}
		err = util.Write[IdxType](IdxType(vecIdx), serial)
		if err != nil {
			return err
		}
		err = chunkInfo.Serialize(serial)
		if err != nil {
			return err
		}
	}
	return err
}

type RowGroupWriteData struct {
	_states []*ColumnCheckpointState
	_stats  []*BaseStats
}

type VersionDeleteState struct {
	_info         *RowGroup
	_txn          *Txn
	_table        *DataTable
	_currentInfo  *ChunkInfo
	_currentChunk IdxType
	_rows         [STANDARD_VECTOR_SIZE]RowType
	_count        IdxType
	_baseRow      IdxType
	_chunkRow     IdxType
	_deleteCount  IdxType
}

func (state *VersionDeleteState) Delete(rowIdx RowType) {
	util.AssertFunc(rowIdx >= 0)
	vectorIdx := IdxType(rowIdx / STANDARD_VECTOR_SIZE)
	idxInVector := IdxType(rowIdx) - vectorIdx*STANDARD_VECTOR_SIZE
	if state._currentChunk != vectorIdx {
		state.Flush()
		if state._info._versionInfo == nil {
			state._info._versionInfo = &VersionNode{}
		}
		if state._info._versionInfo._info[vectorIdx] == nil {
			state._info._versionInfo._info[vectorIdx] =
				NewVectorInfo(state._info.Start() +
					vectorIdx*STANDARD_VECTOR_SIZE)
		} else if state._info._versionInfo._info[vectorIdx]._type == CONSTANT_INFO {
			oldInfo := state._info._versionInfo._info[vectorIdx]
			//convert it to vector info
			newInfo := NewVectorInfo(state._info.Start() +
				vectorIdx*STANDARD_VECTOR_SIZE)
			newInfo._insertId.Store(oldInfo._insertId.Load())
			for i := IdxType(0); i < STANDARD_VECTOR_SIZE; i++ {
				newInfo._inserted[i].Store(oldInfo._insertId.Load())
			}
			state._info._versionInfo._info[vectorIdx] = newInfo
		}
		util.AssertFunc(state._info._versionInfo._info[vectorIdx]._type == VECTOR_INFO)
		state._currentInfo = state._info._versionInfo._info[vectorIdx]
		state._currentChunk = vectorIdx
		state._chunkRow = vectorIdx * STANDARD_VECTOR_SIZE
	}
	state._rows[state._count] = RowType(idxInVector)
	state._count++
}

func (state *VersionDeleteState) Flush() {
	if state._count == 0 {
		return
	}

	actualDeleteCount := state._currentInfo.Delete(
		state._txn._id,
		state._rows[:],
		state._count)
	state._deleteCount += actualDeleteCount
	if actualDeleteCount > 0 {
		state._txn.PushDelete(
			state._table,
			state._currentInfo,
			state._rows[:],
			actualDeleteCount,
			state._baseRow+state._chunkRow)
	}
	state._count = 0
}

func NewVersionDeleteState(
	info *RowGroup,
	txn *Txn,
	table *DataTable,
	baseRow IdxType,
) *VersionDeleteState {
	ret := &VersionDeleteState{
		_info:         info,
		_txn:          txn,
		_table:        table,
		_currentChunk: math.MaxUint64,
		_baseRow:      baseRow,
	}
	return ret
}

type TableScanType int

const (
	TableScanTypeRegular                             TableScanType = 0
	TableScanTypeCommittedRows                       TableScanType = 1
	TableScanTypeCommittedRowsDisallowUpdates        TableScanType = 2
	TableScanTypeCommittedRowsOmitPermanentlyDeleted TableScanType = 3
)

func NewRowGroup(collect *RowGroupCollection, start IdxType, count IdxType) *RowGroup {
	rg := &RowGroup{
		SegmentBase: &SegmentBaseImpl[RowGroup]{
			_start: start,
		},
		_collect: collect,
	}
	rg.SetCount(uint64(count))
	rg.SetValid(true)
	return rg
}

func NewRowGroup2(collect *RowGroupCollection, rgPtr *RowGroupPointer) (*RowGroup, error) {
	rg := NewRowGroup(collect,
		IdxType(rgPtr._rowStart),
		IdxType(rgPtr._tupleCount))
	if len(rgPtr._dataPointers) != len(collect._types) {
		panic("invalid row group column count")
	}
	rg._columnPointers = rgPtr._dataPointers
	rg._versionInfo = rgPtr._versions
	//load columns
	err := rg.loadColumns()
	if err != nil {
		return nil, err
	}
	return rg, err
}

func (rg *RowGroup) loadColumns() error {
	rg._rowGroupLock.Lock()
	defer rg._rowGroupLock.Unlock()
	if len(rg._columnPointers) != len(rg._collect._types) {
		panic("invalid column count")
	}

	rg._columns = make([]*ColumnData, len(rg._columnPointers))
	for i := 0; i < len(rg._columns); i++ {
		colData, err := rg.loadColumn(i, rg._columnPointers[i])
		if err != nil {
			return err
		}
		rg._columns[i] = colData
	}
	return nil
}

func (rg *RowGroup) loadColumn(
	colIdx int,
	blockPtr *BlockPointer,
) (*ColumnData, error) {
	columnDataReader, err := NewMetaBlockReader(
		rg._collect._blockMgr,
		blockPtr._blockId,
		true,
	)
	if err != nil {
		return nil, err
	}
	defer columnDataReader.Close()
	columnDataReader._offset = uint64(blockPtr._offset)
	colData, err := ColumnDataDeserialize(
		rg._collect._blockMgr,
		rg._collect._info,
		colIdx,
		rg.Start(),
		columnDataReader,
		rg._collect._types[colIdx],
		nil,
	)
	return colData, err
}

func (rg *RowGroup) MergeIntoStats(colIdx int, other *BaseStats) {
	colData := rg.GetColumn(colIdx)
	rg._statsLock.Lock()
	defer rg._statsLock.Unlock()
	colData.MergeIntoStats(other)
}

func (rg *RowGroup) GetStats(id int) *BaseStats {
	colData := rg.GetColumn(id)
	rg._statsLock.Lock()
	defer rg._statsLock.Unlock()
	ret := colData._stats._stats.Copy()
	return &ret
}

func ColumnDataDeserialize(
	blkMgr BlockMgr,
	info *DataTableInfo,
	idx int,
	start IdxType,
	src util.Deserialize,
	typ common.LType, parent *ColumnData) (*ColumnData, error) {
	colData := NewColumnData(
		blkMgr,
		info,
		idx,
		start,
		typ,
		parent,
	)
	err := colData.DeserializeColumn(src)
	return colData, err
}

type RowGroupCollection struct {
	_blockMgr  BlockMgr
	_totalRows atomic.Uint64
	_info      *DataTableInfo
	_types     []common.LType
	_rowStart  IdxType
	_rowGroups *RowGroupSegmentTree
	_stats     TableStats
}

func NewRowGroupCollection(
	info *DataTableInfo,
	blockMgr BlockMgr,
	types []common.LType,
	rowStart IdxType,
	totalRows IdxType,
) *RowGroupCollection {
	ret := &RowGroupCollection{
		_blockMgr: blockMgr,
		_info:     info,
		_types:    types,
		_rowStart: rowStart,
	}
	ret._totalRows.Store(uint64(totalRows))
	rgst := NewRowGroupSegmentTree(ret)
	ret._rowGroups = rgst
	return ret
}

func (collect *RowGroupCollection) InitAppend(
	txn *Txn,
	state *TableAppendState,
	appendCount IdxType) {
	state._rowStart = RowType(collect._totalRows.Load())
	state._currentRow = state._rowStart
	state._totalAppendCount = 0
	lock := collect._rowGroups.Lock()
	defer lock.Unlock()
	if collect._rowGroups.IsEmpty(lock) {
		collect.AppendRowGroup(collect._rowStart)
	}
	state._startRowGroup = collect._rowGroups.GetLastSegment(lock).(*RowGroup)
	state._startRowGroup.InitAppend(&state._rowGroupAppendState)
	state._remaining = appendCount
	state._txn = txn
	if state._remaining > 0 {
		state._startRowGroup.AppendVersionInfo(txn, state._remaining)
		collect._totalRows.Add(uint64(state._remaining))
	}
}

func (collect *RowGroupCollection) Append(data *chunk.Chunk, state *TableAppendState) bool {
	newRg := false
	appendCount := IdxType(data.Card())
	remaining := IdxType(data.Card())
	state._totalAppendCount += appendCount
	for {
		curRg := state._rowGroupAppendState._rowGroup
		appendCount := min(remaining,
			ROW_GROUP_SIZE-state._rowGroupAppendState._offsetInRowGroup)
		if appendCount > 0 {
			curRg.Append(&state._rowGroupAppendState, data, appendCount)
			//merge stats
			mergeStats := func() {
				collect._stats._lock.Lock()
				defer collect._stats._lock.Unlock()
				for i := 0; i < len(collect._types); i++ {
					curRg.MergeIntoStats(i,
						&collect._stats.GetStats(i)._stats)
				}
			}
			mergeStats()
		}
		remaining -= appendCount
		if state._remaining > 0 {
			state._remaining -= appendCount
		}
		if remaining > 0 {
			util.AssertFunc(IdxType(data.Card()) == remaining+appendCount)
			if remaining < IdxType(data.Card()) {
				sel := chunk.NewSelectVector(int(remaining))
				for i := IdxType(0); i < remaining; i++ {
					sel.SetIndex(int(i), int(appendCount+i))
				}
				data.SliceItself(sel, int(remaining))
			}
			newRg = true
			nextStart := curRg.Start() +
				state._rowGroupAppendState._offsetInRowGroup
			fun := func() {
				release := collect._rowGroups.Lock()
				defer release.Unlock()
				collect.AppendRowGroup(nextStart)
				lastRg := collect._rowGroups.GetLastSegment(release).(*RowGroup)
				lastRg.InitAppend(&state._rowGroupAppendState)
				if state._remaining > 0 {
					lastRg.AppendVersionInfo(state._txn, state._remaining)
				}
			}
			fun()
		} else {
			break
		}
	}
	state._currentRow += RowType(appendCount)
	//update distinct stats
	updateDistinct := func() {
		collect._stats._lock.Lock()
		defer collect._stats._lock.Unlock()
		for i := 0; i < len(collect._types); i++ {
			collect._stats.GetStats(i).UpdateDistinctStats(
				data.Data[i],
				data.Card(),
			)
		}
	}
	updateDistinct()
	return newRg
}

func (collect *RowGroupCollection) AppendRowGroup(start IdxType) {
	rg := NewRowGroup(collect, start, 0)
	rg.InitEmpty(collect._types)
	collect._rowGroups.AppendSegment(nil, rg)
}

func (collect *RowGroupCollection) FinalizeAppend(txn *Txn, state *TableAppendState) {
	remaining := state._totalAppendCount
	rowGroup := state._startRowGroup
	for remaining > 0 {
		appendCnt := min(remaining,
			IdxType(ROW_GROUP_SIZE-rowGroup.Count()))
		rowGroup.AppendVersionInfo(txn, appendCnt)
		remaining -= appendCnt
		next := collect._rowGroups.GetNextSegment(nil, rowGroup)
		if next != nil {
			rowGroup = next.(*RowGroup)
		} else {
			rowGroup = nil
		}
	}
	collect._totalRows.Add(uint64(state._totalAppendCount))
	state._totalAppendCount = 0
	state._startRowGroup = nil
}

func (collect *RowGroupCollection) MergeStorage(data *RowGroupCollection) {
	idx := collect._rowStart + IdxType(collect._totalRows.Load())
	segments := data._rowGroups.MoveSegments(nil)
	for _, entry := range segments {
		rg := entry._node.(*RowGroup)
		rg.MoveToCollection(collect, idx)
		idx += IdxType(rg.Count())
		collect._rowGroups.AppendSegment(nil, rg)
	}
	collect._stats.MergeStats(&data._stats)
	collect._totalRows.Add(data._totalRows.Load())
}

func (collect *RowGroupCollection) CommitAppend(
	commitId TxnType, rowStart IdxType, count IdxType) {
	rg := collect._rowGroups.GetSegment(nil, rowStart).(*RowGroup)
	curRow := rowStart
	remaining := count
	for {
		startInRg := curRow - rg.Start()
		appendCnt := min(IdxType(rg.Count())-startInRg, remaining)
		rg.CommitAppend(commitId, startInRg, appendCnt)
		curRow += appendCnt
		remaining -= appendCnt
		if remaining == 0 {
			break
		}
		rg = collect._rowGroups.GetNextSegment(nil, rg).(*RowGroup)
	}
}

func (collect *RowGroupCollection) InitializeEmpty() {
	collect._stats.InitEmpty(collect._types)
}

func (collect *RowGroupCollection) InitScan(
	state *CollectionScanState,
	columnIds []IdxType) {
	segRef := collect._rowGroups.GetRootSegment(nil)
	if segRef == nil {
		return
	}
	rg := segRef.(*RowGroup)
	state._rowGroups = collect._rowGroups
	state._maxRow = collect._rowStart +
		IdxType(collect._totalRows.Load())
	state.Init(collect._types)
	for rg != nil && !rg.InitScan(state) {
		rg = collect._rowGroups.GetNextSegment(nil, rg).(*RowGroup)
	}
}

func (collect *RowGroupCollection) Delete(
	txn *Txn,
	table *DataTable,
	ids []RowType,
	count IdxType) IdxType {
	deleteCount := IdxType(0)
	pos := IdxType(0)
	for {
		start := pos
		rg := collect._rowGroups.GetSegment(nil, IdxType(ids[start])).(*RowGroup)
		for pos++; pos < count; pos++ {
			util.AssertFunc(ids[pos] >= 0)
			if IdxType(ids[pos]) < rg.Start() {
				break
			}
			if IdxType(ids[pos]) >= rg.Start()+IdxType(rg.Count()) {
				break
			}
		}
		deleteCount += rg.Delete(txn, table, ids[start:], pos-start)
		if pos >= count {
			break
		}
	}
	return deleteCount
}

func (collect *RowGroupCollection) Scan(
	txn *Txn, fun func(data *chunk.Chunk) bool) bool {
	colIds := make([]IdxType, 0)
	for i := 0; i < len(collect._types); i++ {
		colIds = append(colIds, IdxType(i))
	}
	return collect.Scan2(txn, colIds, fun)
}

func (collect *RowGroupCollection) Scan2(
	txn *Txn,
	colIds []IdxType,
	fun func(data *chunk.Chunk) bool) bool {
	scanTyps := make([]common.LType, 0)
	for _, id := range colIds {
		scanTyps = append(scanTyps,
			collect._types[id])
	}

	data := chunk.Chunk{}
	data.Init(scanTyps, STANDARD_VECTOR_SIZE)

	state := NewTableScanState()
	state.Init(colIds)
	collect.InitScan(state._localState, colIds)

	for {
		data.Reset()
		state._localState.Scan(txn, &data)
		if data.Card() == 0 {
			return true
		}
		if !fun(&data) {
			return false
		}
	}
}

func (collect *RowGroupCollection) Update(
	txn *Txn,
	ids []RowType,
	colIds []IdxType,
	updates *chunk.Chunk) {
	pos := IdxType(0)
	for {
		start := pos
		rg := collect._rowGroups.GetSegment(nil, IdxType(ids[pos])).(*RowGroup)
		baseId := RowType(rg.Start()) +
			((ids[pos] - RowType(rg.Start())) /
				STANDARD_VECTOR_SIZE *
				STANDARD_VECTOR_SIZE)
		maxId := min(baseId+STANDARD_VECTOR_SIZE,
			RowType(rg.Start()+IdxType(rg.Count())),
		)
		for pos++; pos < IdxType(updates.Card()); pos++ {
			if ids[pos] < baseId {
				break
			}
			if ids[pos] >= maxId {
				break
			}
		}

		rg.Update(txn, updates, ids, start, pos-start, colIds)

		//merge stats
		mergeStats := func() {
			collect._stats._lock.Lock()
			defer collect._stats._lock.Unlock()
			for i := 0; i < len(colIds); i++ {
				colId := colIds[i]
				collect._stats.MergeStats2(
					int(colId),
					rg.GetStats(int(colId)),
				)
			}
		}
		mergeStats()

		if pos >= IdxType(updates.Card()) {
			break
		}
	}
}

func (collect *RowGroupCollection) RevertAppendInternal(row IdxType, count IdxType) {
	if collect._totalRows.Load() != uint64(row+count) {
		panic("interleaved appends")
	}
	collect._totalRows.Store(uint64(row))
	lock := collect._rowGroups.Lock()
	defer lock.Unlock()
	segIdx := collect._rowGroups.GetSegmentIdx(lock, row)
	seg := collect._rowGroups.GetSegmentByIndex(lock, segIdx).(*RowGroup)
	collect._rowGroups.EraseSegments(lock, segIdx)

	//unlink
	seg.SetNext(RowGroup{})
	seg.RevertAppend(row)

}

func (collect *RowGroupCollection) InitScanWithOffset(
	state *CollectionScanState,
	colIds []IdxType,
	startRow IdxType,
	endRow IdxType) {
	rg := collect._rowGroups.GetSegment(nil, startRow).(*RowGroup)
	state._rowGroups = collect._rowGroups
	state._maxRow = endRow
	state.Init(collect._types)
	startVec := (startRow - rg.Start()) / STANDARD_VECTOR_SIZE
	if !rg.InitScanWithOffset(state, startVec) {
		panic("init scan with offset failed")
	}
}

func (collect *RowGroupCollection) UpdateColumn(
	txn *Txn,
	rowIds *chunk.Vector,
	colPath []IdxType,
	updates *chunk.Chunk) {
	val := rowIds.GetValue(0)
	if RowType(val.I64) >= MAX_ROW_ID {
		panic("update column path on txn local data")
	}

	primaryColIdx := colPath[0]
	wg := collect._rowGroups.GetSegment(nil, IdxType(val.I64)).(*RowGroup)
	wg.UpdateColumn(txn, updates, rowIds, colPath)
	wg.MergeIntoStats(int(primaryColIdx),
		&collect._stats.GetStats(int(primaryColIdx))._stats,
	)
}

func (collect *RowGroupCollection) Checkpoint(writer *TableDataWriter, globalStats *TableStats) error {
	lock := collect._rowGroups.Lock()
	defer lock.Unlock()
	cnt := collect._rowGroups.GetSegmentCount(lock)
	for i := IdxType(0); i < cnt; i++ {
		rg := collect._rowGroups.GetSegmentByIndex(lock, i).(*RowGroup)
		rgWriter := writer.GetRowGroupWriter(rg)
		rgPointer, err := rg.Checkpoint(rgWriter, globalStats)
		if err != nil {
			return err
		}
		writer.AddRowGroup(rgPointer, rgWriter)
	}
	return nil
}

func (collect *RowGroupCollection) InitWithData(
	data *PersistentTableData) error {
	lock := collect._rowGroups.Lock()
	defer lock.Unlock()
	collect._totalRows.Store(uint64(data._totalRows))
	err := collect._rowGroups.InitWithData(lock, data)
	if err != nil {
		return err
	}
	collect._stats.Init(collect._types, data)
	return err
}

func (collect *RowGroupCollection) CopyStats(colIdx int) *BaseStats {
	return collect._stats.CopyStats(colIdx)
}

func (collect *RowGroupCollection) CopyStats2(other *TableStats) {
	collect._stats.CopyStats2(other)
}

type VersionNode struct {
	_info [ROW_GROUP_VECTOR_COUNT]*ChunkInfo
}

func (node *VersionNode) SetStart(start IdxType) {
	curStart := start
	for i := 0; i < ROW_GROUP_VECTOR_COUNT; i++ {
		if node._info[i] != nil {
			node._info[i]._start = curStart
		}
		curStart += STANDARD_VECTOR_SIZE
	}
}

type RowGroupAppendState struct {
	_parent           *TableAppendState
	_rowGroup         *RowGroup
	_states           []*ColumnAppendState
	_offsetInRowGroup IdxType
}

type RowGroupSegmentTree struct {
	*SegmentTree[RowGroup]
	_collect         *RowGroupCollection
	_currentRowGroup IdxType
	_maxRowGroup     IdxType
	_reader          *MetaBlockReader
}

func NewRowGroupSegmentTree(
	collect *RowGroupCollection) *RowGroupSegmentTree {
	tree := &RowGroupSegmentTree{
		SegmentTree: NewSegmentTree[RowGroup](),
		_collect:    collect,
	}
	return tree
}

func (tree *RowGroupSegmentTree) InitWithData(
	lock sync.Locker,
	data *PersistentTableData) error {
	if lock == nil {
		tree._lock.Lock()
		defer tree._lock.Unlock()
	} else {
		lock.Lock()
		defer lock.Unlock()
	}
	tree._currentRowGroup = 0
	tree._maxRowGroup = data._rowGroupCount
	var err error
	tree._reader, err = NewMetaBlockReader(
		tree._collect._blockMgr,
		BlockID(data._blockId),
		true,
	)
	if err != nil {
		return err
	}
	tree._reader._offset = uint64(data._offset)
	//load all segments
	err = tree.LoadAllSegments(lock)
	if err != nil {
		return err
	}

	return nil
}

func (tree *RowGroupSegmentTree) LoadAllSegments(lock sync.Locker) error {
	for {
		more, err := tree.LoadNextSegment(lock)
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

func (tree *RowGroupSegmentTree) LoadNextSegment(lock sync.Locker) (bool, error) {
	rg, err := tree.LoadSegment()
	if err != nil {
		return false, err
	}
	if rg != nil {
		tree.AppendSegment(lock, rg)
		return true, nil
	}
	return false, nil
}

func (tree *RowGroupSegmentTree) LoadSegment() (*RowGroup, error) {
	if tree._currentRowGroup >= tree._maxRowGroup {
		return nil, nil
	}
	rgPtr, err := RowGroupDeserialize(
		tree._reader,
		tree._collect._types,
	)
	if err != nil {
		return nil, err
	}
	tree._currentRowGroup++
	return NewRowGroup2(tree._collect, rgPtr)
}
