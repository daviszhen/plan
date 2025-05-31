package storage

import (
	"sort"
	"sync"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ColumnDataType int

const (
	ColumnDataTypeStandard ColumnDataType = 0
	ColumnDataTypeValidity ColumnDataType = 1
)

type ColumnData struct {
	_cdType      ColumnDataType
	_start       IdxType
	_count       IdxType
	_blockMgr    BlockMgr
	_info        *DataTableInfo
	_columnIndex IdxType
	_typ         common.LType
	_parent      *ColumnData
	_data        *ColumnSegmentTree
	_updateLock  sync.Mutex
	_updates     *UpdateSegment
	_version     IdxType
	_validity    *ColumnData
	_stats       *SegmentStats
}

func NewColumnData(
	mgr BlockMgr,
	info *DataTableInfo,
	colIdx int,
	start IdxType,
	typ common.LType,
	parent *ColumnData,
) *ColumnData {
	ret := &ColumnData{
		_cdType:      ColumnDataTypeStandard,
		_blockMgr:    mgr,
		_info:        info,
		_columnIndex: IdxType(colIdx),
		_start:       start,
		_typ:         typ,
		_version:     0,
		_parent:      parent,
		_data:        NewColumnSegmentTree(),
	}
	ret._validity = &ColumnData{
		_cdType:      ColumnDataTypeValidity,
		_blockMgr:    mgr,
		_info:        info,
		_columnIndex: 0,
		_start:       start,
		_typ:         common.MakeLType(common.LTID_VALIDITY),
		_parent:      ret,
		_data:        NewColumnSegmentTree(),
		_stats:       NewSegmentStats(typ),
	}
	if parent == nil {
		ret._stats = NewSegmentStats(typ)
	}
	return ret
}

func (column *ColumnData) InitAppend(state *ColumnAppendState) {
	release := column._data.Lock()
	defer release.Unlock()
	if column._data.IsEmpty(release) {
		column.AppendTransientSegment(release, column._start)
	}
	segment := column._data.GetLastSegment(release).(*ColumnSegment)
	if segment._segType == SegmentTypePersistent ||
		segment._function._initAppend == nil {
		//need new segment
		totalRows := segment.Start() + IdxType(segment.Count())
		column.AppendTransientSegment(release, totalRows)
		state._current = column._data.GetLastSegment(release).(*ColumnSegment)
	} else {
		state._current = segment
	}

	util.AssertFunc(state._current._segType == SegmentTypeTransient)
	state._current.InitAppend(state)
	if column._cdType == ColumnDataTypeStandard {
		childAppend := &ColumnAppendState{}
		column._validity.InitAppend(childAppend)
		state._childAppends = append(state._childAppends, childAppend)
	}
}

func (column *ColumnData) AppendTransientSegment(lock sync.Locker, start IdxType) {
	segSize := BLOCK_SIZE
	if start == IdxType(MAX_ROW_ID) {
		segSize = uint64(STANDARD_VECTOR_SIZE * column._typ.GetInternalType().Size())
	}
	seg := NewColumnTransientSegment(column._typ, start, IdxType(segSize))
	column._data.AppendSegment(lock, seg)
}

func (column *ColumnData) Append(
	state *ColumnAppendState,
	vector *chunk.Vector,
	cnt IdxType) {
	var vdata chunk.UnifiedFormat
	vector.ToUnifiedFormat(int(cnt), &vdata)
	column.AppendData(&column._stats._stats, state, &vdata, cnt)
}

func (column *ColumnData) AppendData(
	stats *BaseStats,
	state *ColumnAppendState,
	vdata *chunk.UnifiedFormat,
	cnt IdxType) {
	offset := IdxType(0)
	column._count += cnt
	for {
		copied := state._current.Append(state, vdata, offset, cnt)
		stats.Merge(&state._current._stats._stats)
		if copied == cnt {
			break
		}
		fun := func() {
			release := column._data.Lock()
			defer release.Unlock()
			column.AppendTransientSegment(release, state._current.Start()+
				IdxType(state._current.Count()))
			state._current = column._data.GetLastSegment(release).(*ColumnSegment)
			state._current.InitAppend(state)
		}
		fun()
		offset += copied
		cnt -= copied
	}
	if column._cdType == ColumnDataTypeStandard {
		column._validity.AppendData(&column._validity._stats._stats, state._childAppends[0], vdata, cnt)
	}
}

func (column *ColumnData) SetStart(newStart IdxType) {
	column._start = newStart
	offset := IdxType(0)
	lock := column._data.Lock()
	defer lock.Unlock()
	cnt := column._data.GetSegmentCount(lock)
	for i := IdxType(0); i < cnt; i++ {
		seg := column._data.GetSegmentByIndex(lock, i)
		seg.SetStart(newStart + offset)
		offset += IdxType(seg.Count())
	}
	column._data.Reinitialize(lock)
}

func (column *ColumnData) InitScan(state *ColumnScanState) {
	state._current = column._data.GetRootSegment(nil).(*ColumnSegment)
	state._segmentTree = column._data
	state._rowIdx = 0
	if state._current != nil {
		state._rowIdx = state._current.Start()
	}
	state._internalIdx = state._rowIdx
	state._initialized = false
	state._version = column._version
	state._scanState = nil
	state._lastOffset = 0
}

func (column *ColumnData) Skip(
	state *ColumnScanState,
	count IdxType) {
	state.Next(count)
}

func (column *ColumnData) Scan(
	txn *Txn,
	vecIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector) IdxType {
	return column.ScanVector(
		txn,
		vecIdx,
		state,
		result,
		false,
		true)
}

func (column *ColumnData) ScanCommitted(
	txn *Txn,
	vecIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector,
	allowUpdates bool,
) IdxType {
	return column.ScanVector(txn, vecIdx, state, result, true, allowUpdates)
}

func (column *ColumnData) ScanVector(
	txn *Txn,
	vecIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector,
	scanCommitted bool,
	allowUpdates bool,
) IdxType {
	scanCount := column.ScanVector2(state, result, STANDARD_VECTOR_SIZE)
	column._updateLock.Lock()
	defer column._updateLock.Unlock()
	if column._updates != nil {
		if !allowUpdates && column._updates.HasUncommittedUpdates(vecIdx) {
			panic("uncommitted updates")
		}
		result.Flatten(int(scanCount))
		if scanCommitted {
			column._updates.FetchCommitted(vecIdx, result)
		} else {
			column._updates.FetchUpdates(
				txn,
				vecIdx,
				result,
			)
		}
	}
	return scanCount
}

func (column *ColumnData) ScanVector2(
	state *ColumnScanState,
	result *chunk.Vector,
	remaining IdxType,
) IdxType {
	state._previousStates = nil
	if state._version != column._version {
		column.InitScanWithOffset(state, state._rowIdx)
		state._current.InitScan(state)
		state._initialized = true
	} else if !state._initialized {
		state._current.InitScan(state)
		state._internalIdx = state._current.Start()
		state._initialized = true
	}
	util.AssertFunc(column._data.HasSegment(nil, state._current))
	util.AssertFunc(state._version == column._version)
	util.AssertFunc(state._internalIdx <= state._rowIdx)
	if state._internalIdx < state._rowIdx {
		state._current.Skip(state)
	}
	util.AssertFunc(state._current._type == column._typ)
	initialRemaining := remaining
	for remaining > 0 {
		util.AssertFunc(state._rowIdx >= state._current.Start() &&
			state._rowIdx <= state._current.Start()+IdxType(state._current.Count()))
		scanCount := min(
			remaining,
			state._current.Start()+IdxType(state._current.Count())-state._rowIdx)
		resultOffset := initialRemaining - remaining
		if scanCount > 0 {
			state._current.Scan(
				state,
				scanCount,
				result,
				resultOffset,
				scanCount == initialRemaining,
			)
			state._rowIdx += scanCount
			remaining -= scanCount
		}
		if remaining > 0 {
			next := column._data.GetNextSegment(nil, state._current)
			if next == nil {
				break
			}
			state._previousStates = append(state._previousStates, state._scanState)
			state._current = next.(*ColumnSegment)
			state._current.InitScan(state)
			state._segmentChecked = false
			util.AssertFunc(state._rowIdx >= state._current.Start() &&
				state._rowIdx <= state._current.Start()+IdxType(state._current.Count()))
		}
	}
	state._internalIdx = state._rowIdx
	return initialRemaining - remaining
}

func (column *ColumnData) InitScanWithOffset(
	state *ColumnScanState,
	rowIdx IdxType) {
	state._current = column.GetSegment(rowIdx)
	state._segmentTree = column._data
	state._rowIdx = rowIdx
	state._internalIdx = state._current.Start()
	state._initialized = false
	state._version = column._version
	state._scanState = nil
	state._lastOffset = 0
}

func (column *ColumnData) GetSegment(rowNumber IdxType) *ColumnSegment {
	colSeg := column._data.GetSegment(nil, rowNumber)
	if colSeg == nil {
		return nil
	}
	return colSeg.(*ColumnSegment)
}

func (column *ColumnData) FilterScan(
	txn *Txn,
	vectorIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector,
	sel *chunk.SelectVector,
	count IdxType) {
	column.Scan(txn, vectorIdx, state, result)
	result.Slice2(sel, int(count))
}

func (column *ColumnData) Update(
	txn *Txn,
	colIdx IdxType,
	updateVec *chunk.Vector,
	rowIds []RowType,
	updateCount IdxType) {
	column._updateLock.Lock()
	defer column._updateLock.Unlock()
	if column._updates == nil {
		column._updates = NewUpdateSegment(column)
	}
	baseVec := chunk.NewFlatVector(column._typ, STANDARD_VECTOR_SIZE)
	state := &ColumnScanState{}
	fetchCount := column.Fetch(txn, state, rowIds[0], baseVec)
	baseVec.Flatten(int(fetchCount))
	column._updates.Update(
		txn,
		colIdx,
		updateVec,
		rowIds,
		updateCount,
		baseVec,
	)
}

func (column *ColumnData) Fetch(
	txn *Txn,
	state *ColumnScanState,
	rowId RowType,
	result *chunk.Vector) IdxType {
	util.AssertFunc(rowId >= 0)
	util.AssertFunc(IdxType(rowId) >= column._start)
	state._rowIdx = column._start +
		((IdxType(rowId) - column._start) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE)
	state._current = column._data.GetSegment(nil, state._rowIdx).(*ColumnSegment)
	state._internalIdx = state._current.Start()
	return column.ScanVector2(state, result, STANDARD_VECTOR_SIZE)
}

func (column *ColumnData) RevertAppend(startRow IdxType) {
	lock := column._data.Lock()
	defer lock.Unlock()
	lastSeg := column._data.GetLastSegment(lock).(*ColumnSegment)
	if startRow >= lastSeg.Start()+IdxType(lastSeg.Count()) {
		//nothing to revert
		return
	}
	segIdx := column._data.GetSegmentIdx(lock, startRow)
	seg := column._data.GetSegmentByIndex(lock, segIdx).(*ColumnSegment)
	column._data.EraseSegments(lock, segIdx)
	column._count = startRow - column._start
	seg.SetNext(ColumnSegment{})
	seg.RevertAppend(startRow)
}

func (column *ColumnData) FilterScanCommitted(
	txn *Txn,
	vecIdx IdxType,
	state *ColumnScanState,
	result *chunk.Vector,
	sel *chunk.SelectVector,
	count IdxType,
	allowUpdates bool) {
	column.ScanCommitted(
		txn,
		vecIdx,
		state,
		result,
		allowUpdates,
	)
	result.Slice2(sel, int(count))
}

func (column *ColumnData) UpdateColumn(
	txn *Txn,
	colPath []IdxType,
	updateVector *chunk.Vector,
	rowIds []RowType,
	updateCount int,
	depth int) {
	util.AssertFunc(depth >= len(colPath))
	column.Update(txn, colPath[0], updateVector, rowIds, IdxType(updateCount))
}

func (column *ColumnData) Checkpoint(
	rg *RowGroup,
	mgr *PartialBlockMgr) (*ColumnCheckpointState, error) {
	state := column.CreateCheckpointState(rg, mgr)
	state._globalStats = NewEmptyBaseStats(column._typ)

	lock := column._data.Lock()
	defer lock.Unlock()
	nodes := column._data.MoveSegments(lock)
	if len(nodes) == 0 {
		return state, nil
	}

	column._updateLock.Lock()
	defer column._updateLock.Unlock()

	checkpointer := NewColumnDataCheckpointer(
		column,
		rg,
		state,
	)
	err := checkpointer.Checkpoint(nodes)
	if err != nil {
		return nil, err
	}

	//replace old tree by new tree
	column._data.Replace(lock, state._newTree.SegmentTree)
	column._version++
	return state, nil
}

func (column *ColumnData) CreateCheckpointState(
	rg *RowGroup,
	mgr *PartialBlockMgr) *ColumnCheckpointState {
	return NewColumnCheckpointState(
		rg,
		column,
		mgr,
	)
}

func (column *ColumnData) CheckpointScan(
	seg *ColumnSegment,
	state *ColumnScanState,
	start IdxType,
	count uint64,
	vec *chunk.Vector) {
	seg.Scan(
		state,
		IdxType(count),
		vec,
		0,
		true)
	if column._updates != nil {
		vec.Flatten(int(count))
		column._updates.FetchCommittedRange(
			state._rowIdx-start,
			count,
			vec,
		)
	}
}

func (column *ColumnData) DeserializeColumn(
	src util.Deserialize) error {
	column._count = 0
	dataPtrCnt := IdxType(0)
	err := util.Read[IdxType](&dataPtrCnt, src)
	if err != nil {
		return err
	}
	for i := IdxType(0); i < dataPtrCnt; i++ {
		//data pointer
		dataPtr := &DataPointer{}
		err = util.Read[uint64](&dataPtr._rowStart, src)
		if err != nil {
			return err
		}
		err = util.Read[uint64](&dataPtr._tupleCount, src)
		if err != nil {
			return err
		}
		err = util.Read[BlockID](&dataPtr._blockPtr._blockId, src)
		if err != nil {
			return err
		}
		err = util.Read[uint32](&dataPtr._blockPtr._offset, src)
		if err != nil {
			return err
		}
		err = dataPtr._stats.Deserialize(src, column._typ)
		if err != nil {
			return err
		}
		if column._stats != nil {
			column._stats._stats.Merge(&dataPtr._stats)
		}

		column._count += IdxType(dataPtr._tupleCount)
		//persistent segment
		seg := NewColumnPersistentSegment(
			column._blockMgr,
			dataPtr._blockPtr._blockId,
			dataPtr._blockPtr._offset,
			column._typ,
			dataPtr._rowStart,
			dataPtr._tupleCount,
			dataPtr._stats,
		)
		column._data.AppendSegment(nil, seg)
	}
	return nil
}

func (column *ColumnData) MergeIntoStats(other *BaseStats) {
	other.Merge(&column._stats._stats)
}

type ColumnSegmentTree struct {
	*SegmentTree[ColumnSegment]
}

func NewColumnSegmentTree() *ColumnSegmentTree {
	ret := &ColumnSegmentTree{
		SegmentTree: NewSegmentTree[ColumnSegment](),
	}
	return ret
}

type InitUpdate func(
	baseInfo *UpdateInfo,
	baseData *chunk.Vector,
	updateInfo *UpdateInfo,
	update *chunk.Vector,
	sel *chunk.SelectVector,
)

type MergeUpdate func(
	baseInfo *UpdateInfo,
	baseData *chunk.Vector,
	updateInfo *UpdateInfo,
	update *chunk.Vector,
	ids []RowType,
	count IdxType,
	sel *chunk.SelectVector,
)

type FetchUpdate func(
	startTime TxnType,
	txnId TxnType,
	info *UpdateInfo,
	result *chunk.Vector,
)

type FetchCommitted func(
	info *UpdateInfo,
	result *chunk.Vector,
)

type FetchCommittedRange func(
	info *UpdateInfo,
	start, end, resultOffset IdxType,
	result *chunk.Vector,
)

type FetchRow func(
	startTime TxnType,
	txnId TxnType,
	info *UpdateInfo,
	rowIdx IdxType,
	result *chunk.Vector,
	resultIdx IdxType,
)

type RollbackUpdate func(
	baseInfo *UpdateInfo,
	rollbackInfo *UpdateInfo,
)

type StatsUpdate func(
	seg *UpdateSegment,
	update *chunk.Vector,
	count IdxType,
	sel *chunk.SelectVector,
) IdxType

func InitUpdateData[T any](
	baseInfo *UpdateInfo,
	baseData *chunk.Vector,
	updateInfo *UpdateInfo,
	update *chunk.Vector,
	sel *chunk.SelectVector) {
	updateSlice := chunk.GetSliceInPhyFormatFlat[T](update)
	tupleSlice := util.PointerToSlice[T](
		updateInfo._tupleData,
		STANDARD_VECTOR_SIZE)
	for i := 0; i < updateInfo._N; i++ {
		idx := sel.GetIndex(i)
		tupleSlice[i] = updateSlice[idx]
	}

	baseSlice := chunk.GetSliceInPhyFormatFlat[T](baseData)
	mask := chunk.GetMaskInPhyFormatFlat(baseData)
	baseTupleSlice := util.PointerToSlice[T](
		baseInfo._tupleData,
		STANDARD_VECTOR_SIZE)
	for i := 0; i < baseInfo._N; i++ {
		bIdx := baseInfo._tuples[i]
		if !mask.RowIsValid(uint64(bIdx)) {
			continue
		}
		baseTupleSlice[i] = baseSlice[bIdx]
	}
}

func GetInitUpdateData(ptyp common.PhyType) InitUpdate {
	switch ptyp {
	case common.INT32:
		return InitUpdateData[int32]
	default:
		panic("unsupported type")
	}
}

func MergeUpdateLoop[T any](
	baseInfo *UpdateInfo,
	baseData *chunk.Vector,
	updateInfo *UpdateInfo,
	update *chunk.Vector,
	ids []RowType,
	count IdxType,
	sel *chunk.SelectVector,
) {
	baseTableSlice := chunk.GetSliceInPhyFormatFlat[T](baseData)
	updateVectorSlice := chunk.GetSliceInPhyFormatFlat[T](update)
	MergeUpdateLoopInternal[T](
		baseInfo,
		baseTableSlice,
		updateInfo,
		updateVectorSlice,
		ids,
		count,
		sel,
	)
}

// MergeUpdateLoopInternal
// input:
//
//	baseTableSlice: base table data
//	new updates (updateVectorSlice,ids,count,sel)
//	existed updates (baseInfo)
//	updates the txn already done (updateInfo)
//
// output:
//
//	baseInfo(new values) : merged with lastest updates
//	updateInfo(old values): from (baseInfo or baseTableSlice)
func MergeUpdateLoopInternal[T any](
	baseInfo *UpdateInfo,
	baseTableSlice []T,
	updateInfo *UpdateInfo,
	updateVectorSlice []T,
	ids []RowType,
	count IdxType,
	sel *chunk.SelectVector,
) {
	baseId := baseInfo._segment._colData._start +
		baseInfo._vectorIndex*STANDARD_VECTOR_SIZE
	baseInfoData := util.PointerToSlice[T](
		baseInfo._tupleData,
		STANDARD_VECTOR_SIZE)
	updateInfoData := util.PointerToSlice[T](
		updateInfo._tupleData,
		STANDARD_VECTOR_SIZE)

	//step 1: prepare old values
	resultValues := make([]T, STANDARD_VECTOR_SIZE)
	resultIds := make([]int, STANDARD_VECTOR_SIZE)

	baseInfoOffset := 0
	updateInfoOffset := 0
	resultOffset := 0
	for i := IdxType(0); i < count; i++ {
		idx := sel.GetIndex(int(i))
		updateId := IdxType(ids[idx]) - baseId

		//id that is old values (in last update) before updateId saved to resultValues
		for updateInfoOffset < updateInfo._N &&
			updateInfo._tuples[updateInfoOffset] < int(updateId) {
			resultValues[resultOffset] = updateInfoData[updateInfoOffset]
			resultIds[resultOffset] = updateInfo._tuples[updateInfoOffset]
			resultOffset++
			updateInfoOffset++
		}

		//save updateId that is old values (txn update already) to resultValues
		if updateInfoOffset < updateInfo._N &&
			updateInfo._tuples[updateInfoOffset] == int(updateId) {
			resultValues[resultOffset] = updateInfoData[updateInfoOffset]
			resultIds[resultOffset] = updateInfo._tuples[updateInfoOffset]
			resultOffset++
			updateInfoOffset++
			continue
		}

		// find updateId in baseInfo
		for baseInfoOffset < baseInfo._N &&
			baseInfo._tuples[baseInfoOffset] < int(updateId) {
			baseInfoOffset++
		}

		if baseInfoOffset < baseInfo._N &&
			baseInfo._tuples[baseInfoOffset] == int(updateId) {
			//move old value in baseInfo (lastes version) to update info
			resultValues[resultOffset] = baseInfoData[baseInfoOffset]
		} else {
			//move old value in base table data to update info
			resultValues[resultOffset] = baseTableSlice[updateId]
		}
		resultIds[resultOffset] = int(updateId)
		resultOffset++
	}

	//move remaining old values (in update info) to resultValues
	for updateInfoOffset < updateInfo._N {
		resultValues[resultOffset] = updateInfoData[updateInfoOffset]
		resultIds[resultOffset] = updateInfo._tuples[updateInfoOffset]
		resultOffset++
		updateInfoOffset++
	}

	// move resultValues (old values) to updateInfo (new version old values)
	// and move resultIds to updateInfo._tuples
	updateInfo._N = resultOffset
	copy(updateInfoData, resultValues[:resultOffset])
	copy(updateInfo._tuples, resultIds[:resultOffset])

	//step 2: prepare new values
	resultOffset = 0

	//pick new value from new updates (txn will do this time)
	pickNew := func(id, aidx, count IdxType) {
		resultValues[resultOffset] = updateVectorSlice[aidx]
		resultIds[resultOffset] = int(id)
		resultOffset++
	}

	//pick new value from baseInfo (latest version)
	pickOld := func(id, bidx, count IdxType) {
		resultValues[resultOffset] = baseInfoData[bidx]
		resultIds[resultOffset] = int(id)
		resultOffset++
	}

	merge := func(id, aidx, bidx, count IdxType) {
		pickNew(id, aidx, count)
	}

	MergeLoop(
		ids,
		baseInfo._tuples,
		count,
		IdxType(baseInfo._N),
		baseId,
		merge,
		pickNew,
		pickOld,
		sel)

	baseInfo._N = resultOffset
	copy(baseInfoData, resultValues[:resultOffset])
	copy(baseInfo._tuples, resultIds[:resultOffset])
}

func MergeLoop(
	a []RowType,
	b []int,
	acount IdxType,
	bcount IdxType,
	aoffset IdxType,
	merge func(id IdxType, aidx IdxType, bidx IdxType, count IdxType),
	pickA func(id IdxType, aidx IdxType, count IdxType),
	pickB func(id IdxType, bidx IdxType, count IdxType),
	asel *chunk.SelectVector) IdxType {
	aidx, bidx := IdxType(0), IdxType(0)
	count := IdxType(0)
	for aidx < acount && bidx < bcount {
		aIndex := asel.GetIndex(int(aidx))
		aId := IdxType(a[aIndex]) - aoffset
		bId := b[bidx]
		if aId == IdxType(bId) {
			merge(aId, IdxType(aIndex), bidx, count)
			aidx++
			bidx++
			count++
		} else if aId < IdxType(bId) {
			pickA(aId, IdxType(aIndex), count)
			aidx++
			count++
		} else {
			pickB(IdxType(bId), bidx, count)
			bidx++
			count++
		}
	}

	for ; aidx < acount; aidx++ {
		aIndex := asel.GetIndex(int(aidx))
		pickA(IdxType(a[aIndex])-aoffset, IdxType(aIndex), count)
		count++
	}

	for ; bidx < bcount; bidx++ {
		pickB(IdxType(b[bidx]), bidx, count)
		count++
	}
	return count
}

func GetMergeUpdate(ptyp common.PhyType) MergeUpdate {
	switch ptyp {
	case common.INT32:
		return MergeUpdateLoop[int32]
	default:
		panic("unsupported type")
	}
}

func UpdateMergeFetch[T any](
	startTime TxnType,
	txnId TxnType,
	info *UpdateInfo,
	result *chunk.Vector) {
	resultSlice := chunk.GetSliceInPhyFormatFlat[T](result)
	UpdatesForTransaction[T](
		info,
		startTime,
		txnId,
		func(current *UpdateInfo) {
			MergeUpdateInfo[T](current, resultSlice)
		})
}

func GetFetchUpdate(ptyp common.PhyType) FetchUpdate {
	switch ptyp {
	case common.INT32:
		return UpdateMergeFetch[int32]
	default:
		panic("unsupported type")
	}
}

func MergeUpdateInfo[T any](
	current *UpdateInfo,
	resultData []T,
) {
	infoData := util.PointerToSlice[T](
		current._tupleData,
		STANDARD_VECTOR_SIZE)
	if current._N == STANDARD_VECTOR_SIZE {
		copy(resultData, infoData[:current._N])
	} else {
		for i := 0; i < current._N; i++ {
			resultData[current._tuples[i]] = infoData[i]
		}
	}
}

func UpdatesForTransaction[T any](
	current *UpdateInfo,
	startTime TxnType,
	txnId TxnType,
	callback func(info *UpdateInfo),
) {
	for current != nil {
		if TxnType(current._versionNumber.Load()) > startTime &&
			TxnType(current._versionNumber.Load()) != txnId {
			//can see this version
			callback(current)
		}
		current = current._next
	}
}

func TemplatedFetchCommitted[T any](
	info *UpdateInfo,
	result *chunk.Vector) {
	resultData := chunk.GetSliceInPhyFormatFlat[T](result)
	MergeUpdateInfo[T](info, resultData)
}

func GetFetchCommittedFunction(ptyp common.PhyType) FetchCommitted {
	switch ptyp {
	case common.INT32:
		return TemplatedFetchCommitted[int32]
	default:
		panic("unsupported type")
	}
}

func TemplatedFetchCommittedRange[T any](
	info *UpdateInfo,
	start IdxType,
	end IdxType,
	resultOffset IdxType,
	result *chunk.Vector) {
	resultData := chunk.GetSliceInPhyFormatFlat[T](result)
	MergeUpdateInfoRange[T](info, start, end, resultOffset, resultData)
}

func GetFetchCommittedRange(ptyp common.PhyType) FetchCommittedRange {
	switch ptyp {
	case common.INT32:
		return TemplatedFetchCommittedRange[int32]
	default:
		panic("unsupported type")
	}
}

func MergeUpdateInfoRange[T any](
	current *UpdateInfo,
	start IdxType,
	end IdxType,
	resultOffset IdxType,
	resultData []T,
) {
	infoData := util.PointerToSlice[T](
		current._tupleData,
		STANDARD_VECTOR_SIZE)
	for i := 0; i < current._N; i++ {
		tupleIdx := IdxType(current._tuples[i])
		if tupleIdx < start {
			continue
		} else if tupleIdx >= end {
			break
		}
		resultIdx := resultOffset + tupleIdx - start
		resultData[resultIdx] = infoData[i]
	}
}

func TemplatedFetchRow[T any](
	startTime TxnType,
	txnId TxnType,
	info *UpdateInfo,
	rowIdx IdxType,
	result *chunk.Vector,
	resultIdx IdxType,
) {
	resultData := chunk.GetSliceInPhyFormatFlat[T](result)
	UpdatesForTransaction[T](info, startTime, txnId, func(current *UpdateInfo) {
		infoData := util.PointerToSlice[T](
			current._tupleData,
			STANDARD_VECTOR_SIZE)
		for i := 0; i < current._N; i++ {
			if IdxType(current._tuples[i]) == rowIdx {
				resultData[resultIdx] = infoData[i]
				break
			} else if IdxType(current._tuples[i]) == rowIdx {
				break
			}
		}
	})
}

func GetFetchRow(ptyp common.PhyType) FetchRow {
	switch ptyp {
	case common.INT32:
		return TemplatedFetchRow[int32]
	default:
		panic("unsupported type")
	}
}

func RollbackUpdateFunc[T any](baseInfo *UpdateInfo, rollbackInfo *UpdateInfo) {
	baseData := util.PointerToSlice[T](
		baseInfo._tupleData,
		STANDARD_VECTOR_SIZE)
	rollbackData := util.PointerToSlice[T](
		rollbackInfo._tupleData,
		STANDARD_VECTOR_SIZE)
	baseOffset := 0
	for i := 0; i < rollbackInfo._N; i++ {
		id := rollbackInfo._tuples[i]
		for baseInfo._tuples[baseOffset] < id {
			baseOffset++
		}
		baseData[baseOffset] = rollbackData[i]
	}
}

func GetRollbackUpdate(ptyp common.PhyType) RollbackUpdate {
	switch ptyp {
	case common.INT32:
		return RollbackUpdateFunc[int32]
	default:
		panic("unsupported type")
	}
}

func TemplatedUpdateNumeric[T any](
	seg *UpdateSegment,
	update *chunk.Vector,
	count IdxType,
	sel *chunk.SelectVector) IdxType {
	//updateData := chunk.GetSliceInPhyFormatFlat[T](update)
	mask := chunk.GetMaskInPhyFormatFlat(update)

	if mask.AllValid() {
		sel.Init(0)
		return count
	} else {
		notNullCount := IdxType(0)
		sel.Init(STANDARD_VECTOR_SIZE)
		for i := IdxType(0); i < count; i++ {
			if mask.RowIsValid(uint64(i)) {
				sel.SetIndex(int(notNullCount), int(i))
				notNullCount++
			}
		}
		return notNullCount
	}
}

func GetStatsUpdate(ptyp common.PhyType) StatsUpdate {
	switch ptyp {
	case common.INT32:
		return TemplatedUpdateNumeric[int32]
	default:
		panic("unsupported type")
	}
}

type UpdateSegment struct {
	_colData             *ColumnData
	_lock                sync.Mutex
	_root                *UpdateNode
	_typeSize            IdxType
	_initUpdate          InitUpdate
	_mergeUpdate         MergeUpdate
	_fetchUpdate         FetchUpdate
	_fetchCommitted      FetchCommitted
	_fetchCommittedRange FetchCommittedRange
	_fetchRow            FetchRow
	_rollbackUpdate      RollbackUpdate
	_statsUpdate         StatsUpdate
}

func NewUpdateSegment(colData *ColumnData) *UpdateSegment {
	ret := &UpdateSegment{
		_colData: colData,
	}
	ret._typeSize = IdxType(colData._typ.GetInternalType().Size())
	ret._initUpdate = GetInitUpdateData(colData._typ.GetInternalType())
	ret._mergeUpdate = GetMergeUpdate(colData._typ.GetInternalType())
	ret._fetchUpdate = GetFetchUpdate(colData._typ.GetInternalType())
	ret._fetchCommitted = GetFetchCommittedFunction(colData._typ.GetInternalType())
	ret._fetchCommittedRange = GetFetchCommittedRange(colData._typ.GetInternalType())
	ret._fetchRow = GetFetchRow(colData._typ.GetInternalType())
	ret._rollbackUpdate = GetRollbackUpdate(colData._typ.GetInternalType())
	ret._statsUpdate = GetStatsUpdate(colData._typ.GetInternalType())
	return ret
}

type UpdateNodeData struct {
	_info      *UpdateInfo
	_tuples    []int
	_tupleData unsafe.Pointer
}

type UpdateNode struct {
	_info [ROW_GROUP_VECTOR_COUNT]*UpdateNodeData
}

func (seg *UpdateSegment) Update(
	txn *Txn,
	colIdx IdxType,
	update *chunk.Vector,
	rowIds []RowType,
	count IdxType,
	baseData *chunk.Vector) {
	seg._lock.Lock()
	defer seg._lock.Unlock()
	update.Flatten(int(count))
	if count == 0 {
		return
	}
	sel := chunk.NewSelectVector(0)
	seg._statsUpdate(seg, update, count, sel)
	count = SortSelectionVector(sel, count, rowIds)
	if seg._root == nil {
		seg._root = &UpdateNode{}
	}
	firstId := rowIds[sel.GetIndex(0)]
	vectorIdx := (IdxType(firstId) - seg._colData._start) / STANDARD_VECTOR_SIZE
	vectorOffset := seg._colData._start + vectorIdx*STANDARD_VECTOR_SIZE

	util.AssertFunc(IdxType(firstId) >= seg._colData._start)
	util.AssertFunc(vectorIdx < ROW_GROUP_VECTOR_COUNT)

	//var node *UpdateInfo
	if seg._root._info[vectorIdx] != nil {
		baseInfo := seg._root._info[vectorIdx]._info
		var cNode *UpdateInfo
		//check conflicts
		CheckForConflicts(
			baseInfo._next,
			txn,
			rowIds,
			sel,
			count,
			vectorOffset,
			&cNode,
		)
		//TODO:
		//find update this thread already done
		nodeX := baseInfo._next
		for nodeX != nil {
			if nodeX._versionNumber.Load() == uint64(txn._id) {
				break
			}
			nodeX = nodeX._next
		}
		//var updateInfoData []byte
		if nodeX == nil {
			//no updates
			nodeX = txn.CreateUpdateInfo(seg._typeSize, count)
			nodeX._segment = seg
			nodeX._vectorIndex = vectorIdx
			nodeX._N = 0
			nodeX._columnIndex = colIdx

			//base info --pointer--> nodeX
			nodeX._next = baseInfo._next
			if nodeX._next != nil {
				nodeX._next._prev = nodeX
			}
			nodeX._prev = baseInfo
			baseInfo._next = nodeX
		}
		//merge the update
		seg._mergeUpdate(baseInfo, baseData, nodeX, update, rowIds, count, sel)
	} else {
		result := &UpdateNodeData{}

		//update info (new value)
		result._info = &UpdateInfo{}
		result._tuples = make([]int, STANDARD_VECTOR_SIZE)
		result._tupleData = util.CMalloc(int(STANDARD_VECTOR_SIZE * seg._typeSize))
		result._info._tuples = result._tuples
		result._info._tupleData = result._tupleData
		result._info._versionNumber.Store(TRANSACTION_ID_START - 1)
		result._info._columnIndex = colIdx
		seg.InitUpdateInfo(
			result._info,
			rowIds,
			sel,
			count,
			vectorIdx,
			vectorOffset,
		)
		//base info (init value)
		txnNode := txn.CreateUpdateInfo(seg._typeSize, count)
		seg.InitUpdateInfo(
			txnNode,
			rowIds,
			sel,
			count,
			vectorIdx,
			vectorOffset,
		)
		//base data -> base info (txnNode)
		//update data -> update info (result._info)
		seg._initUpdate(txnNode, baseData, result._info, update, sel)
		//update info (result._info) --pointer--> base info (txnNode)
		result._info._next = txnNode
		result._info._prev = nil
		txnNode._next = nil
		txnNode._prev = result._info
		txnNode._columnIndex = colIdx
		seg._root._info[vectorIdx] = result
	}
}

func CheckForConflicts(
	info *UpdateInfo,
	txn *Txn,
	ids []RowType,
	sel *chunk.SelectVector,
	count IdxType,
	offset IdxType,
	cnode **UpdateInfo) {
	if info == nil {
		return
	}
	if info._versionNumber.Load() == uint64(txn._id) {
		//same txn
		*cnode = info
	} else if info._versionNumber.Load() > uint64(txn._startTime) {
		for i, j := IdxType(0), IdxType(0); ; {
			id := IdxType(ids[sel.GetIndex(int(i))]) - offset
			if int(id) == info._tuples[j] {
				panic("conflicts : other txn already update")
			} else if int(id) < info._tuples[j] {
				i++
				if i == count {
					break
				}
			} else {
				j++
				if j == IdxType(info._N) {
					break
				}
			}
		}
	}
	CheckForConflicts(info._next, txn, ids, sel, count, offset, cnode)
}

func (seg *UpdateSegment) InitUpdateInfo(
	info *UpdateInfo,
	ids []RowType,
	sel *chunk.SelectVector,
	count IdxType,
	vectorIdx IdxType,
	vectorOffset IdxType) {
	info._segment = seg
	info._vectorIndex = vectorIdx
	info._prev = nil
	info._next = nil
	info._N = int(count)
	for i := 0; i < int(count); i++ {
		idx := sel.GetIndex(i)
		id := ids[idx]
		util.AssertFunc(IdxType(id) >= vectorOffset)
		util.AssertFunc(IdxType(id) < vectorOffset+STANDARD_VECTOR_SIZE)
		info._tuples[i] = int(id) - int(vectorOffset)
	}
}

func (seg *UpdateSegment) FetchUpdates(
	txn *Txn,
	idx IdxType,
	result *chunk.Vector) {
	seg._lock.Lock()
	defer seg._lock.Unlock()
	if seg._root == nil {
		return
	}
	if seg._root._info[idx] == nil {
		return
	}
	seg._fetchUpdate(
		txn._startTime,
		txn._id,
		seg._root._info[idx]._info,
		result)
}

func (seg *UpdateSegment) FetchCommitted(
	idx IdxType,
	result *chunk.Vector) {
	seg._lock.Lock()
	defer seg._lock.Unlock()
	if seg._root == nil {
		return
	}
	if seg._root._info[idx] == nil {
		return
	}
	seg._fetchCommitted(
		seg._root._info[idx]._info,
		result)
}

func (seg *UpdateSegment) HasUpdates() bool {
	return seg._root != nil
}

func (seg *UpdateSegment) HasUpdates2(vecIdx IdxType) bool {
	if !seg.HasUpdates() {
		return false
	}
	return seg._root._info[vecIdx] != nil
}

func (seg *UpdateSegment) HasUncommittedUpdates(vecIdx IdxType) bool {
	if !seg.HasUpdates2(vecIdx) {
		return false
	}
	seg._lock.Lock()
	defer seg._lock.Unlock()
	info := seg._root._info[vecIdx]
	return info._info._next != nil
}

func (seg *UpdateSegment) RollbackUpdate(info *UpdateInfo) {
	seg._lock.Lock()
	defer seg._lock.Unlock()
	util.AssertFunc(seg._root._info[info._vectorIndex] != nil)
	seg._rollbackUpdate(seg._root._info[info._vectorIndex]._info, info)
	seg.CleanupUpdateInternal(&seg._lock, info)
}

func (seg *UpdateSegment) CleanupUpdateInternal(lock sync.Locker, info *UpdateInfo) {
	util.AssertFunc(info._prev != nil)
	prev := info._prev
	prev._next = info._next
	if prev._next != nil {
		prev._next._prev = prev
	}
}

func (seg *UpdateSegment) FetchCommittedRange(
	startRow IdxType,
	count uint64,
	result *chunk.Vector) {
	if seg._root == nil {
		return
	}
	endRow := startRow + IdxType(count)
	startVec := startRow / STANDARD_VECTOR_SIZE
	endVec := (endRow - 1) / STANDARD_VECTOR_SIZE
	for vecIdx := startVec; vecIdx <= endVec; vecIdx++ {
		if seg._root._info[vecIdx] == nil {
			continue
		}
		startInVec := IdxType(0)
		if vecIdx == startVec {
			startInVec = startRow - startVec*STANDARD_VECTOR_SIZE
		}
		endInVec := IdxType(STANDARD_VECTOR_SIZE)
		if vecIdx == endVec {
			endInVec = endRow - endVec*STANDARD_VECTOR_SIZE
		}
		resultOffset := (vecIdx * STANDARD_VECTOR_SIZE) + startInVec - startRow
		seg._fetchCommittedRange(
			seg._root._info[vecIdx]._info,
			startInVec,
			endInVec,
			resultOffset,
			result,
		)
	}
}

func SortSelectionVector(sel *chunk.SelectVector, count IdxType, ids []RowType) IdxType {
	util.AssertFunc(count > 0)
	isSorted := true
	for i := 1; i < int(count); i++ {
		prevIdx := sel.GetIndex(i - 1)
		idx := sel.GetIndex(i)
		if ids[idx] < ids[prevIdx] {
			isSorted = false
			break
		}
	}
	if isSorted {
		return count
	}
	sortedSel := chunk.NewSelectVector(int(count))
	for i := 0; i < int(count); i++ {
		sortedSel.SetIndex(i, sel.GetIndex(i))
	}
	sort.Slice(sortedSel.SelVec[:count], func(i, j int) bool {
		return ids[i] < ids[j]
	})
	pos := IdxType(1)
	for i := 1; i < int(count); i++ {
		prevIdx := sortedSel.GetIndex(i - 1)
		idx := sortedSel.GetIndex(i)
		util.AssertFunc(ids[idx] >= ids[prevIdx])
		if ids[idx] != ids[prevIdx] {
			sortedSel.SetIndex(int(pos), idx)
			pos++
		}
	}
	sel.Init2(sortedSel)
	util.AssertFunc(pos > 0)
	return pos
}

const (
	SegmentTypeTransient  uint8 = 0
	SegmentTypePersistent uint8 = 1
)

type ColumnSegment struct {
	SegmentBase[ColumnSegment]
	_type         common.LType
	_typeSize     IdxType
	_segType      uint8
	_function     *CompressFunction
	_block        *BlockHandle
	_blockId      BlockID
	_offset       IdxType
	_segmentSize  IdxType
	_segmentState *CompressedSegmentState
	_stats        *SegmentStats
}

func NewColumnSegment(typ common.LType, start IdxType, size IdxType) *ColumnSegment {
	fun := GetUncompressedCompressFunction(typ.GetInternalType())
	var block *BlockHandle
	if size < IdxType(BLOCK_SIZE) {
		block = GBufferMgr.RegisterSmallMemory(uint64(size))
	} else {
		GBufferMgr.Allocate(uint64(size), false, &block)
	}
	colSeg := &ColumnSegment{
		SegmentBase: &SegmentBaseImpl[ColumnSegment]{
			_start: start,
		},
		_type:        typ,
		_typeSize:    IdxType(typ.GetInternalType().Size()),
		_segType:     SegmentTypeTransient,
		_function:    fun,
		_block:       block,
		_blockId:     -1,
		_offset:      0,
		_segmentSize: size,
		_stats:       NewSegmentStats(typ),
	}
	colSeg.SetCount(uint64(0))
	colSeg.SetValid(true)
	if fun._initSegment != nil {
		colSeg._segmentState = fun._initSegment(colSeg, -1)
	}
	return colSeg
}

func NewColumnTransientSegment(
	typ common.LType,
	start IdxType,
	size IdxType) *ColumnSegment {
	fun := GetUncompressedCompressFunction(typ.GetInternalType())
	var block *BlockHandle
	if size < IdxType(BLOCK_SIZE) {
		block = GBufferMgr.RegisterSmallMemory(uint64(size))
	} else {
		GBufferMgr.Allocate(uint64(size), false, &block)
	}
	colSeg := &ColumnSegment{
		SegmentBase: &SegmentBaseImpl[ColumnSegment]{
			_start: start,
		},
		_type:        typ,
		_typeSize:    IdxType(typ.GetInternalType().Size()),
		_segType:     SegmentTypeTransient,
		_function:    fun,
		_block:       block,
		_blockId:     -1,
		_offset:      0,
		_segmentSize: size,
		_stats:       NewSegmentStats(typ),
	}
	colSeg.SetCount(uint64(0))
	colSeg.SetValid(true)
	if fun._initSegment != nil {
		colSeg._segmentState = fun._initSegment(colSeg, -1)
	}
	return colSeg
}

func NewColumnPersistentSegment(
	blkMgr BlockMgr,
	id BlockID,
	offset uint32,
	typ common.LType,
	start uint64,
	count uint64,
	stats BaseStats,
) *ColumnSegment {
	fun := GetUncompressedCompressFunction(typ.GetInternalType())
	var block *BlockHandle
	if id == -1 {

	} else {
		block = blkMgr.RegisterBlock(id, false)
	}
	segSize := BLOCK_SIZE
	colSeg := NewColumnSegment3(
		block,
		typ,
		SegmentTypePersistent,
		start,
		count,
		fun,
		id,
		offset,
		IdxType(segSize),
	)
	colSeg._stats = NewSegmentStats2(&stats)
	return colSeg
}

func NewColumnSegment3(
	block *BlockHandle,
	typ common.LType,
	segTyp uint8,
	start uint64,
	count uint64,
	fun *CompressFunction,
	id BlockID,
	offset uint32,
	segSize IdxType) *ColumnSegment {
	seg := NewColumnSegment(typ, IdxType(start), segSize)
	seg._typeSize = IdxType(typ.GetInternalType().Size())
	seg._segType = segTyp
	seg._function = fun
	seg._block = block
	seg._blockId = id
	seg._offset = IdxType(offset)
	seg._segmentSize = segSize
	seg.SetCount(count)
	if fun._initSegment != nil {
		seg._segmentState = fun._initSegment(seg, id)
	}

	return seg
}

func (segment *ColumnSegment) SegmentSize() IdxType {
	return segment._segmentSize
}

func (segment *ColumnSegment) InitAppend(state *ColumnAppendState) {
	util.AssertFunc(segment._segType == SegmentTypeTransient)
	state._appendState = segment._function._initAppend(segment)
}

func (segment *ColumnSegment) Append(
	state *ColumnAppendState,
	appendData *chunk.UnifiedFormat,
	offset IdxType,
	cnt IdxType,
) IdxType {
	return segment._function._append(
		state._appendState,
		segment,
		segment._stats,
		appendData,
		offset,
		cnt,
	)
}

func (segment *ColumnSegment) InitScan(state *ColumnScanState) {
	state._scanState = segment._function._initScan(segment)
}

func (segment *ColumnSegment) Skip(state *ColumnScanState) {
	segment._function._skip(
		segment,
		state,
		state._rowIdx-state._internalIdx)
	state._internalIdx = state._rowIdx
}

func (segment *ColumnSegment) Scan(
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
	resultOffset IdxType,
	entireVector bool) {
	if entireVector {
		util.AssertFunc(resultOffset == 0)
		segment.Scan2(state, scanCount, result)
	} else {
		util.AssertFunc(result.PhyFormat() == chunk.PF_FLAT)
		segment.ScanPartial(state, scanCount, result, resultOffset)
		util.AssertFunc(result.PhyFormat() == chunk.PF_FLAT)
	}
}

func (segment *ColumnSegment) Scan2(
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector) {
	segment._function._scanVector(
		segment,
		state,
		scanCount,
		result,
	)
}

func (segment *ColumnSegment) ScanPartial(
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
	resultOffset IdxType) {
	segment._function._scanPartial(
		segment,
		state,
		scanCount,
		result,
		resultOffset,
	)
}

func (segment *ColumnSegment) GetRelativeIndex(rowIdx IdxType) IdxType {
	util.AssertFunc(rowIdx >= segment.Start() &&
		rowIdx <= segment.Start()+IdxType(segment.Count()))
	return rowIdx - segment.Start()
}

func (segment *ColumnSegment) GetBlockOffset() IdxType {
	return segment._offset
}

func (segment *ColumnSegment) RevertAppend(startRow IdxType) {
	if segment._function._revertAppend != nil {
		segment._function._revertAppend(segment, startRow)
	}
	segment.SetCount(uint64(startRow - segment.Start()))
}

func (segment *ColumnSegment) FinalizeAppend(state *ColumnAppendState) IdxType {
	return segment._function._finalizeAppend(
		segment,
		segment._stats,
		segment._type.GetInternalType(),
	)
}

func (segment *ColumnSegment) Resize(idxType IdxType) {
	panic("usp")
}

func (segment *ColumnSegment) ConvertToPersistent(
	blkMgr BlockMgr,
	blkId BlockID) {
	util.AssertFunc(segment._segType == SegmentTypeTransient)
	segment._segType = SegmentTypePersistent
	segment._blockId = blkId
	segment._offset = 0

	if blkId == -1 {
		segment._block.Close()
	} else {
		segment._block = blkMgr.ConvertToPersistent(
			segment._blockId,
			segment._block,
		)
	}

	if segment._function._initSegment != nil {
		segment._segmentState = segment._function._initSegment(segment, segment._blockId)
	}
}

type ColumnAppendState struct {
	_current      *ColumnSegment
	_childAppends []*ColumnAppendState
	_appendState  *CompressAppendState
}
