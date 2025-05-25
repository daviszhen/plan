package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type TupleDataCollection struct {
	_layout   *TupleDataLayout
	_count    uint64
	_dedup    map[unsafe.Pointer]struct{}
	_alloc    *TupleDataAllocator
	_segments []*TupleDataSegment
}

func NewTupleDataCollection(layout *TupleDataLayout) *TupleDataCollection {
	ret := &TupleDataCollection{
		_layout: layout.copy(),
		_dedup:  make(map[unsafe.Pointer]struct{}),
		_alloc:  NewTupleDataAllocator(storage.GBufferMgr, layout),
	}
	return ret
}

func (tuple *TupleDataCollection) Count() int {
	return int(tuple._count)
}

func (tuple *TupleDataCollection) Append(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunk *chunk.Chunk,
	appendSel *chunk.SelectVector,
	appendCount int,
) {
	//to unified format
	toUnifiedFormat(chunkState, chunk)
	tuple.AppendUnified(pinState, chunkState, chunk, nil, appendSel, appendCount)
}

func (tuple *TupleDataCollection) AppendUnified(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunk *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	appendSel *chunk.SelectVector,
	cnt int) {
	if cnt == -1 {
		cnt = chunk.Card()
	}
	if cnt == 0 {
		return
	}
	//evaluate the heap size
	if !tuple._layout.allConst() {
		tuple.computeHeapSizes(chunkState, chunk, childrenOutput, appendSel, cnt)
	}

	//allocate buffer space for incoming Chunk in chunkstate
	tuple.Build(pinState, chunkState, 0, uint64(cnt))

	//fill row
	tuple.scatter(chunkState, chunk, appendSel, cnt, false)

	if childrenOutput != nil {
		//scatter rows
		tuple.scatter(chunkState, childrenOutput, appendSel, cnt, true)
	}
}

func (tuple *TupleDataCollection) checkDupAll() {
	//cnt := 0
	////all dup
	//dedup := make(map[unsafe.Pointer]int)
	//for xid, xpart := range tuple._parts {
	//	xrowLocs := getSliceInPhyFormatFlat[unsafe.Pointer](xpart.rowLocations)
	//	for _, loc := range xrowLocs {
	//		if uintptr(loc) == 0 {
	//			continue
	//		}
	//		if xcnt, has := dedup[loc]; has {

	//			panic("dup loc2")
	//		}
	//		dedup[loc] = cnt
	//		cnt++
	//	}
	//}
}

func (tuple *TupleDataCollection) scatter(
	state *TupleDataChunkState,
	data *chunk.Chunk,
	appendSel *chunk.SelectVector,
	cnt int,
	isChildrenOutput bool,
) {
	rowLocations := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](state._rowLocations)
	//set bitmap
	if !isChildrenOutput {
		bitsCnt := tuple._layout.columnCount() + tuple._layout.childrenOutputCount()
		maskBytes := util.SizeInBytes(bitsCnt)
		for i := 0; i < cnt; i++ {
			util.Memset(rowLocations[i], 0xFF, maskBytes)
		}
	}

	if !tuple._layout.allConst() {
		heapSizeOffset := tuple._layout.heapSizeOffset()
		heapSizes := chunk.GetSliceInPhyFormatFlat[uint64](state._heapSizes)
		for i := 0; i < cnt; i++ {
			util.Store[uint64](heapSizes[i], util.PointerAdd(rowLocations[i], heapSizeOffset))
		}
	}
	if isChildrenOutput {
		for i, colIdx := range state._childrenOutputIds {
			tuple.scatterVector(state, data.Data[i], colIdx, appendSel, cnt)
		}
	} else {
		for i, coldIdx := range state._columnIds {
			tuple.scatterVector(state, data.Data[i], coldIdx, appendSel, cnt)
		}
	}
}

func (tuple *TupleDataCollection) scatterVector(
	state *TupleDataChunkState,
	src *chunk.Vector,
	colIdx int,
	appendSel *chunk.SelectVector,
	cnt int) {
	TupleDataTemplatedScatterSwitch(
		src,
		&state._data[colIdx],
		appendSel,
		cnt,
		tuple._layout,
		state._rowLocations,
		state._heapLocations,
		colIdx)
}

// convert chunk into state.data unified format
func toUnifiedFormat(state *TupleDataChunkState, chunk *chunk.Chunk) {
	for i, colId := range state._columnIds {
		chunk.Data[i].ToUnifiedFormat(chunk.Card(), &state._data[colId])
	}
}

func toUnifiedFormatForChildrenOutput(state *TupleDataChunkState, chunk *chunk.Chunk) {
	for i, colIdx := range state._childrenOutputIds {
		chunk.Data[i].ToUnifiedFormat(chunk.Card(), &state._data[colIdx])
	}
}

// result refers to chunk state.data unified format
func getVectorData(state *TupleDataChunkState, result []*chunk.UnifiedFormat) {
	vectorData := state._data
	for i := 0; i < len(result); i++ {
		target := result[i]
		target.Sel = vectorData[i].Sel
		target.Data = vectorData[i].Data
		target.Mask = vectorData[i].Mask
		target.PTypSize = vectorData[i].PTypSize
	}
}

// evaluate the heap size of columns
func (tuple *TupleDataCollection) computeHeapSizes(
	state *TupleDataChunkState,
	data *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	appendSel *chunk.SelectVector,
	cnt int) {

	//heapSizes records the heap size of every row
	heapSizesSlice := chunk.GetSliceInPhyFormatFlat[uint64](state._heapSizes)
	util.Fill[uint64](heapSizesSlice, util.Size(heapSizesSlice), 0)

	for i := 0; i < data.ColumnCount(); i++ {
		tuple.evaluateHeapSizes(state._heapSizes, data.Data[i], &state._data[i], appendSel, cnt)
	}

	for i := 0; i < childrenOutput.ColumnCount(); i++ {
		colIdx := state._childrenOutputIds[i]
		tuple.evaluateHeapSizes(state._heapSizes, childrenOutput.Data[i], &state._data[colIdx], appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) evaluateHeapSizes(
	sizes *chunk.Vector,
	src *chunk.Vector,
	srcUni *chunk.UnifiedFormat,
	appendSel *chunk.SelectVector,
	cnt int) {
	//only care varchar type
	pTyp := src.Typ().GetInternalType()
	switch pTyp {
	case common.VARCHAR:
	default:
		return
	}
	heapSizeSlice := chunk.GetSliceInPhyFormatFlat[uint64](sizes)
	srcSel := srcUni.Sel
	srcMask := srcUni.Mask

	switch pTyp {
	case common.VARCHAR:
		srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](srcUni)
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			if srcMask.RowIsValid(uint64(srcIdx)) {
				heapSizeSlice[i] += uint64(srcSlice[srcIdx].Length())
			} else {
				heapSizeSlice[i] += uint64(common.String{}.NullLen())
			}
		}

	default:
		panic("usp phy type")
	}
}

func (tuple *TupleDataCollection) gather(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	scanSel *chunk.SelectVector,
	scanCnt int,
	colId int,
	result *chunk.Vector,
	targetSel *chunk.SelectVector) {
	TupleDataTemplatedGatherSwitch(
		layout,
		rowLocs,
		colId,
		scanSel,
		scanCnt,
		result,
		targetSel,
	)
}

func (tuple *TupleDataCollection) Gather(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	scanSel *chunk.SelectVector,
	scanCnt int,
	colIds []int,
	result *chunk.Chunk,
	targetSel *chunk.SelectVector,
) {
	for i := 0; i < len(colIds); i++ {
		tuple.gather(
			layout,
			rowLocs,
			scanSel,
			scanCnt,
			colIds[i],
			result.Data[i],
			targetSel)
	}
}

func TupleDataTemplatedScatterSwitch(
	src *chunk.Vector,
	srcFormat *chunk.UnifiedFormat,
	appendSel *chunk.SelectVector,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *chunk.Vector,
	heapLocations *chunk.Vector,
	colIdx int) {
	pTyp := src.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		TupleDataTemplatedScatter[int32](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Int32ScatterOp{},
		)
	case common.INT64:
		TupleDataTemplatedScatter[int64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Int64ScatterOp{},
		)
	case common.UINT64:
		TupleDataTemplatedScatter[uint64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Uint64ScatterOp{},
		)
	case common.VARCHAR:
		TupleDataTemplatedScatter[common.String](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.StringScatterOp{},
		)
	case common.INT8:
		TupleDataTemplatedScatter[int8](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Int8ScatterOp{},
		)
	case common.DECIMAL:
		TupleDataTemplatedScatter[common.Decimal](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.DecimalScatterOp{},
		)
	case common.DATE:
		TupleDataTemplatedScatter[common.Date](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.DateScatterOp{},
		)
	case common.DOUBLE:
		TupleDataTemplatedScatter[float64](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.Float64ScatterOp{},
		)
	case common.INT128:
		TupleDataTemplatedScatter[common.Hugeint](
			srcFormat,
			appendSel,
			cnt,
			layout,
			rowLocations,
			heapLocations,
			colIdx,
			chunk.HugeintScatterOp{},
		)
	default:
		panic("usp ")
	}
}

func TupleDataTemplatedScatter[T any](
	srcFormat *chunk.UnifiedFormat,
	appendSel *chunk.SelectVector,
	cnt int,
	layout *TupleDataLayout,
	rowLocations *chunk.Vector,
	heapLocations *chunk.Vector,
	colIdx int,
	nVal chunk.ScatterOp[T],
) {
	srcSel := srcFormat.Sel
	srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](srcFormat)
	srcMask := srcFormat.Mask

	targetLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rowLocations)
	targetHeapLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](heapLocations)
	offsetInRow := layout.offsets()[colIdx]
	if srcMask.AllValid() {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
		}
	} else {
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			if srcMask.RowIsValid(uint64(srcIdx)) {
				TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
			} else {
				TupleDataValueStore[T](nVal.NullValue(), targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
				bSlice := util.PointerToSlice[uint8](targetLocs[i], layout._rowWidth)
				tempMask := util.Bitmap{Bits: bSlice}
				tempMask.SetInvalidUnsafe(uint64(colIdx))
			}
		}
	}

}

func TupleDataValueStore[T any](src T, rowLoc unsafe.Pointer, offsetInRow int, heapLoc *unsafe.Pointer, nVal chunk.ScatterOp[T]) {
	nVal.Store(src, rowLoc, offsetInRow, heapLoc)
}

func TupleDataTemplatedGatherSwitch(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	colIdx int,
	scanSel *chunk.SelectVector,
	scanCnt int,
	target *chunk.Vector,
	targetSel *chunk.SelectVector,
) {
	pTyp := target.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		TupleDataTemplatedGather[int32](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.INT64:
		TupleDataTemplatedGather[int64](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.VARCHAR:
		TupleDataTemplatedGather[common.String](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.DECIMAL:
		TupleDataTemplatedGather[common.Decimal](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.DATE:
		TupleDataTemplatedGather[common.Date](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	case common.INT128:
		TupleDataTemplatedGather[common.Hugeint](
			layout,
			rowLocs,
			colIdx,
			scanSel,
			scanCnt,
			target,
			targetSel,
		)
	default:
		panic("usp phy type")
	}
}

func TupleDataTemplatedGather[T any](
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	colIdx int,
	scanSel *chunk.SelectVector,
	scanCnt int,
	target *chunk.Vector,
	targetSel *chunk.SelectVector,
) {
	srcLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rowLocs)
	targetData := chunk.GetSliceInPhyFormatFlat[T](target)
	targetBitmap := chunk.GetMaskInPhyFormatFlat(target)
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colIdx))
	offsetInRow := layout.offsets()[colIdx]
	for i := 0; i < scanCnt; i++ {
		base := srcLocs[scanSel.GetIndex(i)]
		srcRow := util.PointerToSlice[byte](base, layout._rowWidth)
		targetIdx := targetSel.GetIndex(i)
		rowMask := util.Bitmap{Bits: srcRow}
		if util.RowIsValidInEntry(
			rowMask.GetEntry(entryIdx),
			idxInEntry) {
			targetData[targetIdx] = util.Load[T](util.PointerAdd(base, offsetInRow))

		} else {
			targetBitmap.SetInvalid(uint64(targetIdx))
		}
	}
}

func (tuple *TupleDataCollection) InitScan(state *TupleDataScanState) {
	////////
	state._pinState._rowHandles = make(map[uint32]*storage.BufferHandle)
	state._pinState._heapHandles = make(map[uint32]*storage.BufferHandle)
	state._pinState._properties = PIN_PRRP_KEEP_PINNED

	state._segmentIdx = 0
	state._chunkIdx = 0
	state._chunkState = *NewTupleDataChunkState(tuple._layout.columnCount(), tuple._layout.childrenOutputCount())
	//read children output also
	state._chunkState._columnIds = state._colIds
	state._chunkState._columnIds = append(state._chunkState._columnIds, state._chunkState._childrenOutputIds...)
	////////
	state._init = true
}

func (tuple *TupleDataCollection) NextScanIndex(
	state *TupleDataScanState,
	segIdx *int,
	chunkIdx *int) bool {
	if state._segmentIdx >= util.Size(tuple._segments) {
		//no data
		return false
	}

	for state._chunkIdx >=
		util.Size(tuple._segments[state._segmentIdx]._chunks) {
		state._segmentIdx++
		state._chunkIdx = 0
		if state._segmentIdx >= util.Size(tuple._segments) {
			//no data
			return false
		}
	}

	*segIdx = state._segmentIdx
	*chunkIdx = state._chunkIdx
	state._chunkIdx++
	return true
}

func (tuple *TupleDataCollection) ScanAtIndex(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	colIdxs []int,
	segIndx, chunkIndx int,
	result *chunk.Chunk) {
	seg := tuple._segments[segIndx]
	data := seg._chunks[chunkIndx]
	seg._allocator.InitChunkState(
		seg,
		pinState,
		chunkState,
		chunkIndx,
		false,
	)
	result.Reset()
	tuple.Gather(
		tuple._layout,
		chunkState._rowLocations,
		chunk.IncrSelectVectorInPhyFormatFlat(),
		int(data._count),
		colIdxs,
		result,
		chunk.IncrSelectVectorInPhyFormatFlat(),
	)
	result.SetCard(int(data._count))
}

func (tuple *TupleDataCollection) Scan(state *TupleDataScanState, result *chunk.Chunk) bool {
	oldSegIdx := state._segmentIdx
	var segIdx, chunkIdx int
	if !tuple.NextScanIndex(state, &segIdx, &chunkIdx) {
		return false
	}

	if uint32(oldSegIdx) != INVALID_INDEX &&
		segIdx != oldSegIdx {
		tuple.FinalizePinState2(&state._pinState,
			tuple._segments[oldSegIdx])
	}

	tuple.ScanAtIndex(
		&state._pinState,
		&state._chunkState,
		state._chunkState._columnIds,
		segIdx,
		chunkIdx,
		result,
	)
	return true
}

// Build allocate space for chunk
// may create new segment or new block
func (tuple *TupleDataCollection) Build(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	appendOffset uint64,
	appendCount uint64,
) {
	seg := util.Back(tuple._segments)
	seg._allocator.Build(seg, pinState, chunkState, appendOffset, appendCount)
	tuple._count += appendCount
}

func (tuple *TupleDataCollection) InitAppend(pinState *TupleDataPinState, prop TupleDataPinProperties) {
	pinState._properties = prop
	if len(tuple._segments) == 0 {
		tuple._segments = append(tuple._segments,
			NewTupleDataSegment(tuple._alloc))
	}
}

func (tuple *TupleDataCollection) FinalizePinState(pinState *TupleDataPinState) {
	util.AssertFunc(len(tuple._segments) == 1)
	chunk := NewTupleDataChunk()
	tuple._alloc.ReleaseOrStoreHandles(pinState, util.Back(tuple._segments), chunk, true)
}

func (tuple *TupleDataCollection) FinalizePinState2(pinState *TupleDataPinState, seg *TupleDataSegment) {
	util.AssertFunc(len(tuple._segments) == 1)
	chunk := NewTupleDataChunk()
	tuple._alloc.ReleaseOrStoreHandles(pinState, seg, chunk, true)
}

func (tuple *TupleDataCollection) Unpin() {
	for _, seg := range tuple._segments {
		seg.Unpin()
	}
}

func (tuple *TupleDataCollection) ChunkCount() int {
	cnt := 0
	for _, segment := range tuple._segments {
		cnt += segment.ChunkCount()
	}
	return cnt
}
