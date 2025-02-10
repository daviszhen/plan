package storage

import (
	"math"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type CompressType int

const (
	CompressTypeAuto CompressType = iota
	CompressTypeUncompressed
)

type CompressAppendState struct {
	_handle *BufferHandle
}

type CompressInitAppend func(
	segment *ColumnSegment) *CompressAppendState

type CompressAppend func(
	state *CompressAppendState,
	segment *ColumnSegment,
	stats *SegmentStats,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
) IdxType

type CompressFinalizeAppend func(
	segment *ColumnSegment,
	stats *SegmentStats,
	pTyp common.PhyType) IdxType

type CompressRevertAppend func(
	segment *ColumnSegment,
	start IdxType)

type CompressInitSegmentScan func(
	segment *ColumnSegment,
) *SegmentScanState

type CompressSkip func(
	segment *ColumnSegment,
	state *ColumnScanState,
	count IdxType,
)

type CompressScanVector func(
	segment *ColumnSegment,
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
)

type CompressScanPartial func(
	segment *ColumnSegment,
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
	resultOffset IdxType,
)

type CompressionState struct {
	_checkpointer   *ColumnDataCheckpointer
	_currentSegment *ColumnSegment
	_appendState    *ColumnAppendState
}

func NewCompressionState(
	ckp *ColumnDataCheckpointer,
) *CompressionState {
	ret := &CompressionState{
		_checkpointer: ckp,
		_appendState:  &ColumnAppendState{},
	}
	ret.CreateEmptySegment(ckp._rowGroup.Start())
	return ret
}

func (state *CompressionState) CreateEmptySegment(rowStart IdxType) {
	seg := NewColumnSegment(
		state._checkpointer._colData._typ,
		rowStart,
		IdxType(BLOCK_SIZE),
	)
	state._currentSegment = seg
	state._currentSegment.InitAppend(state._appendState)
}

func (state *CompressionState) FlushSegment(sz IdxType) {
	ckpState := state._checkpointer._state
	ckpState.FlushSegment(state._currentSegment, sz)
}

func (state *CompressionState) Finalize(sz IdxType) {
	state.FlushSegment(sz)
	state._currentSegment = nil
}

type CompressInitCompress func(
	checkpointer *ColumnDataCheckpointer,
) *CompressionState

type CompressCompressData func(
	state *CompressionState,
	vec *chunk.Vector,
	count IdxType,
)

type CompressCompressFinalize func(
	state *CompressionState,
)

type StringBlock struct {
	_block  *BlockHandle
	_offset IdxType
	_size   IdxType
	_next   *StringBlock
}

type CompressedSegmentState struct {
	_head           *StringBlock
	_overflowBlocks map[BlockID]*StringBlock
}

type CompressInitSegment func(
	segment *ColumnSegment,
	blkId BlockID,
) *CompressedSegmentState

type CompressFunction struct {
	_typ              CompressType
	_dataType         common.PhyType
	_initAppend       CompressInitAppend
	_append           CompressAppend
	_finalizeAppend   CompressFinalizeAppend
	_revertAppend     CompressRevertAppend
	_initScan         CompressInitSegmentScan
	_skip             CompressSkip
	_scanVector       CompressScanVector
	_scanPartial      CompressScanPartial
	_initCompress     CompressInitCompress
	_compress         CompressCompressData
	_compressFinalize CompressCompressFinalize
	_initSegment      CompressInitSegment
}

func GetUncompressedCompressFunction(
	typ common.PhyType,
) *CompressFunction {
	var cfun *CompressFunction
	switch typ {
	case common.INT32, common.INT64, common.UINT64,
		common.BIT, common.DECIMAL, common.DATE:
		cfun = &CompressFunction{
			_typ:              CompressTypeUncompressed,
			_dataType:         typ,
			_initAppend:       FixedSizeInitAppend,
			_append:           getFixedSizeAppend(typ),
			_finalizeAppend:   getFixedSizeFinalizeAppend(typ),
			_initScan:         FixedSizeInitScan,
			_scanVector:       FixedSizeScan,
			_scanPartial:      FixedSizeScanPartial,
			_skip:             EmptySkip,
			_initCompress:     InitCompress,
			_compress:         Compress,
			_compressFinalize: FinalizeCompress,
		}
	case common.VARCHAR:
		cfun = &CompressFunction{
			_typ:              CompressTypeUncompressed,
			_dataType:         typ,
			_initSegment:      StringInitSegment,
			_initAppend:       StringInitAppend,
			_append:           StringAppend,
			_finalizeAppend:   StringFinalizeAppend,
			_initScan:         StringInitScan,
			_scanVector:       StringScan,
			_scanPartial:      StringScanPartial,
			_skip:             EmptySkip,
			_initCompress:     InitCompress,
			_compress:         Compress,
			_compressFinalize: FinalizeCompress,
		}
	}
	if cfun == nil {
		panic("usp")
	}

	return cfun
}

func getFixedSizeAppend(typ common.PhyType) CompressAppend {
	switch typ {
	case common.INT32, common.INT64, common.BIT, common.UINT64, common.DECIMAL, common.DATE:
		return FixedSizeAppend
	default:
		panic("usp")
	}
	return nil
}

func getFixedSizeFinalizeAppend(typ common.PhyType) CompressFinalizeAppend {
	switch typ {
	case common.INT32, common.INT64, common.BIT, common.UINT64, common.DECIMAL, common.DATE:
		return FixedSizeFinalizeAppend
	default:
		panic("usp")
	}
	return nil
}

func FixedSizeInitAppend(segment *ColumnSegment) *CompressAppendState {
	handle := GBufferMgr.Pin(segment._block)
	return &CompressAppendState{
		_handle: handle,
	}
}

type Appender func(
	stats *SegmentStats,
	target unsafe.Pointer,
	targetOffset IdxType,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
)

func getAppender(
	pTyp common.PhyType,
) Appender {
	switch pTyp {
	case common.BIT:
		temp := func(
			stats *SegmentStats,
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[bool](
				stats,
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.BoolScatterOp{},
				BitStatsOp{},
			)
		}
		return temp
	case common.INT32:
		temp := func(
			stats *SegmentStats,
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[int32](
				stats,
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.Int32ScatterOp{},
				Int32StatsOp{},
			)
		}
		return temp
	case common.INT64:
		temp := func(
			stats *SegmentStats,
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[int64](
				stats,
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.Int64ScatterOp{},
				Int64StatsOp{},
			)
		}
		return temp
	case common.UINT64:
		temp := func(
			stats *SegmentStats,
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[uint64](
				stats,
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.Uint64ScatterOp{},
				Uint64StatsOp{},
			)
		}
		return temp
	case common.DECIMAL:
		temp := func(
			stats *SegmentStats,
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[common.Decimal](
				stats,
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.DecimalScatterOp{},
				DecimalStatsOp{},
			)
		}
		return temp
	case common.DATE:
		temp := func(
			stats *SegmentStats,
			target unsafe.Pointer,
			targetOffset IdxType,
			data *chunk.UnifiedFormat,
			offset IdxType,
			count IdxType) {
			StandardFixedSizeAppend[common.Date](
				stats,
				target,
				targetOffset,
				data,
				offset,
				count,
				chunk.DateScatterOp{},
				DateStatsOp{},
			)
		}
		return temp
	default:
		panic("usp")
	}
	return nil
}

func StandardFixedSizeAppend[T any](
	stats *SegmentStats,
	target unsafe.Pointer,
	targetOffset IdxType,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
	sop chunk.ScatterOp[T],
	statsOp StatsOp[T],
) {
	srcDataSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](data)
	dstDataSlice := util.PointerToSlice[T](target, int(count))
	if !data.Mask.AllValid() {
		for i := IdxType(0); i < count; i++ {
			srcIdx := data.Sel.GetIndex(int(offset + i))
			dstIdx := targetOffset + i
			isNull := !data.Mask.RowIsValid(uint64(srcIdx))
			if !isNull {
				statsOp.Update(&stats._stats, &srcDataSlice[srcIdx])
				dstDataSlice[dstIdx] = srcDataSlice[srcIdx]
			} else {
				dstDataSlice[dstIdx] = sop.NullValue()
			}
		}
	} else {
		for i := IdxType(0); i < count; i++ {
			srcIdx := data.Sel.GetIndex(int(offset + i))
			dstIdx := targetOffset + i
			statsOp.Update(&stats._stats, &srcDataSlice[srcIdx])
			dstDataSlice[dstIdx] = srcDataSlice[srcIdx]
		}
	}
}

func FixedSizeAppend(
	state *CompressAppendState,
	segment *ColumnSegment,
	stats *SegmentStats,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType,
) IdxType {
	pTyp := segment._type.GetInternalType()
	target := state._handle.Ptr()
	target = util.PointerAdd(
		target,
		int(IdxType(segment.Count())*IdxType(pTyp.Size())))
	maxTupleCount := segment._segmentSize / IdxType(pTyp.Size())
	copyCount := min(count, maxTupleCount-IdxType(segment.Count()))
	appender := getAppender(pTyp)
	appender(
		stats,
		target,
		IdxType(0),
		data,
		offset,
		copyCount,
	)
	segment.AddCount(uint64(copyCount))
	return copyCount
}

func FixedSizeFinalizeAppend(
	segment *ColumnSegment,
	stats *SegmentStats,
	pTyp common.PhyType,
) IdxType {
	return IdxType(segment.Count()) * IdxType(pTyp.Size())
}

func FixedSizeInitScan(
	segment *ColumnSegment,
) *SegmentScanState {
	ret := &SegmentScanState{}
	ret._handle = GBufferMgr.Pin(segment._block)
	return ret
}

func FixedSizeScan(
	segment *ColumnSegment,
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
) {
	start := segment.GetRelativeIndex(state._rowIdx)
	ptr := state._scanState._handle.Ptr()
	bOffset := segment.GetBlockOffset()
	dataPtr := util.PointerAdd(ptr, int(bOffset))
	pTyp := segment._type.GetInternalType()
	srcPtr := util.PointerAdd(dataPtr,
		int(start*IdxType(pTyp.Size())))
	result.SetPhyFormat(chunk.PF_FLAT)
	srcSlice := util.PointerToSlice[byte](srcPtr,
		util.DefaultVectorSize*pTyp.Size())
	copy(result.Data, srcSlice)
}

func FixedSizeScanPartial(
	segment *ColumnSegment,
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
	resultOffset IdxType,
) {
	start := segment.GetRelativeIndex(state._rowIdx)
	ptr := state._scanState._handle.Ptr()
	bOffset := segment.GetBlockOffset()
	dataPtr := util.PointerAdd(ptr, int(bOffset))
	pTyp := segment._type.GetInternalType()
	srcPtr := util.PointerAdd(dataPtr,
		int(start*IdxType(pTyp.Size())))
	srcSlice := util.PointerToSlice[byte](srcPtr,
		util.DefaultVectorSize*pTyp.Size())
	result.SetPhyFormat(chunk.PF_FLAT)
	resSlice := chunk.GetDataInPhyFormatFlat(result)
	copy(
		resSlice[resultOffset*IdxType(pTyp.Size()):],
		srcSlice[:scanCount*IdxType(pTyp.Size())])
}

func EmptySkip(
	segment *ColumnSegment,
	state *ColumnScanState,
	count IdxType,
) {

}

func InitCompress(checkpointer *ColumnDataCheckpointer) *CompressionState {
	return NewCompressionState(checkpointer)
}

func Compress(
	state *CompressionState,
	data *chunk.Vector,
	count IdxType,
) {
	var vdata chunk.UnifiedFormat
	data.ToUnifiedFormat(int(count), &vdata)

	offset := IdxType(0)
	for count > 0 {
		cnt := state._currentSegment.Append(
			state._appendState,
			&vdata,
			offset,
			count)
		if cnt == count {
			return
		}

		nextStart := state._currentSegment.Start() +
			IdxType(state._currentSegment.Count())
		sz := state._currentSegment.FinalizeAppend(state._appendState)
		state.FlushSegment(sz)
		state.CreateEmptySegment(nextStart)
		offset += cnt
		count -= cnt
	}
}

func FinalizeCompress(state *CompressionState) {
	sz := state._currentSegment.FinalizeAppend(state._appendState)
	state.Finalize(sz)
}

func StringInitSegment(
	segment *ColumnSegment,
	blkId BlockID,
) *CompressedSegmentState {
	if blkId == -1 {
		handle := GBufferMgr.Pin(segment._block)
		dict := &StringDictionaryContainer{}
		dict._size = 0
		dict._end = uint32(segment.SegmentSize())
		SetDictionary(segment, handle, dict)
	} else {
		//handle := GBufferMgr.Pin(segment._block)
		//dict := GetDictionary(segment, handle)
		//fmt.Println("stringInitSegment", dict._size, dict._end)
	}
	return &CompressedSegmentState{
		_overflowBlocks: make(map[BlockID]*StringBlock),
	}
}

func SetDictionary(
	segment *ColumnSegment,
	handle *BufferHandle,
	container *StringDictionaryContainer,
) {
	startPtr := util.PointerAdd(
		handle.Ptr(),
		int(segment.GetBlockOffset()),
	)
	util.Store[uint32](container._size, startPtr)
	util.Store2[uint32](container._end, startPtr, 4)
}

func StringInitAppend(segment *ColumnSegment) *CompressAppendState {
	handle := GBufferMgr.Pin(segment._block)
	return &CompressAppendState{
		_handle: handle,
	}
}

func StringAppend(
	state *CompressAppendState,
	segment *ColumnSegment,
	stats *SegmentStats,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType) IdxType {
	return StringAppendBase(
		state._handle,
		segment,
		stats,
		data,
		offset,
		count,
	)
}

const (
	DICTIONARY_HEADER_SIZE      IdxType = 8
	BIG_STRING_MARKER           IdxType = math.MaxUint64
	BIG_STRING_MARKER_BASE_SIZE IdxType = 12
	BIG_STRING_MARKER_SIZE      IdxType = BIG_STRING_MARKER_BASE_SIZE
	COMPACTION_FLUSH_LIMIT      IdxType = IdxType(BLOCK_SIZE / 5 * 4)
	STRING_BLOCK_LIMIT          IdxType = 4096
)

func StringAppendBase(
	handle *BufferHandle,
	segment *ColumnSegment,
	stats *SegmentStats,
	data *chunk.UnifiedFormat,
	offset IdxType,
	count IdxType) IdxType {
	util.AssertFunc(segment.GetBlockOffset() == 0)
	handlePtr := handle.Ptr()
	baseCount := segment.Count()
	sourceData := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](data)
	resultData := util.PointerToSlice[int32](
		util.PointerAdd(
			handlePtr,
			int(DICTIONARY_HEADER_SIZE)),
		int(IdxType(baseCount)+count),
	)
	//offset 4 : dict offset
	dictSizeU32Ptr := (*uint32)(handlePtr)
	//offset 4 : dict end
	dictEndU32Ptr := (*uint32)(util.PointerAdd(
		handlePtr,
		common.Int32Size,
	))
	remainSpace := RemainingSpace(segment, handle)

	for i := IdxType(0); i < count; i++ {
		srcIdx := data.Sel.GetIndex(int(offset + i))
		targetIdx := IdxType(baseCount) + i
		if remainSpace < IdxType(common.Int32Size) {
			segment.AddCount(uint64(i))
			return i
		}
		remainSpace -= IdxType(common.Int32Size)
		if !data.Mask.RowIsValid(uint64(srcIdx)) {
			if targetIdx > 0 {
				resultData[targetIdx] = resultData[targetIdx-1]
			} else {
				resultData[targetIdx] = 0
			}
			continue
		}
		endPtr := util.PointerAdd(
			handlePtr,
			int(*dictEndU32Ptr),
		)

		//non null string
		strLen := sourceData[srcIdx].Length()
		useOverflowBlock := false
		requiredSpace := IdxType(strLen)
		if requiredSpace >= STRING_BLOCK_LIMIT {
			requiredSpace = BIG_STRING_MARKER_SIZE
			useOverflowBlock = true
		}
		//no space any more
		if requiredSpace > remainSpace {
			segment.AddCount(uint64(i))
			return i
		}

		StringStatsOp{}.Update(&stats._stats, &sourceData[srcIdx])

		if useOverflowBlock {
			//overflow block
			var block BlockID
			var woffset uint32
			WriteStringOverflowBlock(
				segment,
				&sourceData[srcIdx],
				&block,
				&woffset,
			)
			*dictSizeU32Ptr += uint32(BIG_STRING_MARKER_SIZE)
			remainSpace -= BIG_STRING_MARKER_SIZE
			dictPosPtr := util.PointerAdd(
				endPtr,
				-int(*dictSizeU32Ptr),
			)
			WriteStringMarker(dictPosPtr, block, woffset)
			resultData[targetIdx] = -int32(*dictSizeU32Ptr)
		} else {
			*dictSizeU32Ptr += uint32(requiredSpace)
			remainSpace -= requiredSpace
			dictPosPtr := util.PointerAdd(
				endPtr,
				-int(*dictSizeU32Ptr),
			)
			util.PointerCopy(
				dictPosPtr,
				sourceData[srcIdx].DataPtr(),
				strLen,
			)
			resultData[targetIdx] = int32(*dictSizeU32Ptr)
		}
	}
	segment.AddCount(uint64(count))
	return count
}

func WriteStringMarker(
	ptr unsafe.Pointer,
	block BlockID,
	woffset uint32) {
	util.Store[BlockID](block, ptr)
	util.Store2[uint32](woffset, ptr, common.Int64Size)
}

func WriteStringOverflowBlock(
	segment *ColumnSegment,
	str *common.String,
	resultBlk *BlockID,
	resultOffset *uint32) {
	WriteStringMemory(
		segment,
		str,
		resultBlk,
		resultOffset,
	)
}

func WriteStringMemory(
	segment *ColumnSegment,
	str *common.String,
	resultBlk *BlockID,
	resultOffset *uint32) {
	totalLength := IdxType(str.Length() + 4)
	var block *BlockHandle
	var handle *BufferHandle
	state := segment._segmentState
	if state._head == nil ||
		state._head._offset+totalLength >= state._head._size {
		//no more space
		//allocate again
		allocSize := max(totalLength, IdxType(BLOCK_SIZE))
		newBlock := &StringBlock{}
		newBlock._offset = 0
		newBlock._size = allocSize
		//allocate in memory buffer for it
		handle = GBufferMgr.Allocate(
			uint64(allocSize),
			false,
			&block)
		state._overflowBlocks[block._blockId] = newBlock
		newBlock._block = block
		newBlock._next = state._head
		state._head = newBlock
	} else {
		handle = GBufferMgr.Pin(state._head._block)
	}

	*resultBlk = state._head._block._blockId
	*resultOffset = uint32(state._head._offset)
	//fill string
	ptr := util.PointerAdd(
		handle.Ptr(),
		int(state._head._offset),
	)
	util.Store[uint32](uint32(str.Length()), ptr)
	util.PointerCopy(
		util.PointerAdd(ptr, 4),
		str.DataPtr(),
		str.Length(),
	)
	state._head._offset += totalLength
}

func RemainingSpace(
	segment *ColumnSegment,
	handle *BufferHandle) IdxType {
	dict := GetDictionary(segment, handle)
	util.AssertFunc(IdxType(dict._end) == segment.SegmentSize())
	usedSpace := IdxType(dict._size) +
		IdxType(segment.Count())*IdxType(common.Int32Size) +
		DICTIONARY_HEADER_SIZE
	util.AssertFunc(segment.SegmentSize() >= usedSpace)
	return segment.SegmentSize() - usedSpace
}

type StringDictionaryContainer struct {
	_size uint32
	_end  uint32
}

func GetDictionary(
	segment *ColumnSegment,
	handle *BufferHandle) StringDictionaryContainer {
	startPtr := util.PointerAdd(
		handle.Ptr(),
		int(segment.GetBlockOffset()),
	)
	container := StringDictionaryContainer{}
	container._size = util.Load[uint32](startPtr)
	container._end = util.Load2[uint32](startPtr, 4)
	return container
}

func StringFinalizeAppend(
	segment *ColumnSegment,
	stats *SegmentStats,
	pTyp common.PhyType) IdxType {
	handle := GBufferMgr.Pin(segment._block)
	dict := GetDictionary(segment, handle)
	util.AssertFunc(IdxType(dict._end) == segment.SegmentSize())
	//offsetSize := DICTIONARY_HEADER_SIZE + IdxType(segment.Count())*4
	//totalSize := offsetSize + IdxType(dict._size)
	//if totalSize >= COMPACTION_FLUSH_LIMIT {
	//	return segment.SegmentSize()
	//}
	//moveAmount := segment.SegmentSize() - totalSize
	//dataPtr := handle.Ptr()
	//util.PointerCopy(
	//	util.PointerAdd(dataPtr, int(offsetSize)),
	//	util.PointerAdd(dataPtr, int(dict._end-dict._size)),
	//	int(dict._size),
	//)
	//dict._end -= uint32(moveAmount)
	//util.AssertFunc(IdxType(dict._end) == totalSize)
	//SetDictionary(segment, handle, &dict)
	//return totalSize
	return segment.SegmentSize()
}

func StringInitScan(segment *ColumnSegment) *SegmentScanState {
	result := &SegmentScanState{}
	result._handle = GBufferMgr.Pin(segment._block)
	return result
}

func StringScan(
	segment *ColumnSegment,
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector) {
	StringScanPartial(
		segment,
		state,
		scanCount,
		result,
		0,
	)
}

func StringScanPartial(
	segment *ColumnSegment,
	state *ColumnScanState,
	scanCount IdxType,
	result *chunk.Vector,
	resultOffset IdxType) {
	scanState := state._scanState
	start := segment.GetRelativeIndex(state._rowIdx)
	basePtr := util.PointerAdd(
		scanState._handle.Ptr(),
		int(segment.GetBlockOffset()),
	)
	dict := GetDictionary(segment, scanState._handle)
	baseData := util.PointerToSlice[int32](
		util.PointerAdd(
			basePtr,
			int(DICTIONARY_HEADER_SIZE),
		),
		int(start+scanCount),
	)
	resultData := chunk.GetSliceInPhyFormatFlat[common.String](result)

	previousOffset := int32(0)
	if start > 0 {
		previousOffset = baseData[start-1]
	}
	for i := IdxType(0); i < scanCount; i++ {
		strLen := util.Abs(baseData[start+i]) -
			util.Abs(previousOffset)
		resultData[resultOffset+i] = FetchStringFromDict(
			segment,
			&dict,
			result,
			basePtr,
			baseData[start+i],
			strLen,
		)
		//fmt.Println("string can partial", resultData[resultOffset+i].String())
		previousOffset = baseData[start+i]
	}
}

func FetchStringFromDict(
	segment *ColumnSegment,
	dict *StringDictionaryContainer,
	result *chunk.Vector,
	basePtr unsafe.Pointer,
	dictOffset int32,
	strLen int32) common.String {
	util.AssertFunc(dictOffset < int32(BLOCK_SIZE))
	loc := FetchStringLocation(dict, basePtr, dictOffset)
	return FetchString(
		segment,
		dict,
		result,
		basePtr,
		&loc,
		strLen,
	)
}

func FetchString(
	segment *ColumnSegment,
	dict *StringDictionaryContainer,
	result *chunk.Vector,
	basePtr unsafe.Pointer,
	loc *StringLocation, strLen int32) common.String {
	if loc._blockId != -1 {
		return ReadOverflowString(
			segment,
			result,
			loc._blockId,
			loc._offset,
		)
	} else {
		if loc._offset == 0 {
			return common.String{}
		}
		dictEnd := util.PointerAdd(basePtr, int(dict._end))
		dictPos := util.PointerAdd(
			dictEnd,
			int(-loc._offset),
		)
		return common.String{
			Len:  int(strLen),
			Data: dictPos,
		}
	}
}

func ReadOverflowString(
	segment *ColumnSegment,
	result *chunk.Vector,
	blkId BlockID,
	offset int32) common.String {
	util.AssertFunc(blkId != -1)
	util.AssertFunc(offset < int32(BLOCK_SIZE))
	blkMgr := GStorageMgr._blockMgr
	state := segment._segmentState
	if blkId < MAX_BLOCK {
		//overflow string
		blkHandle := blkMgr.RegisterBlock(blkId, false)
		handle := GBufferMgr.Pin(blkHandle)

		//read header
		//no compress here currently
		compressedSize := util.Load2[uint32](
			handle.Ptr(),
			int(offset))
		uncompressedSize := util.Load2[uint32](
			handle.Ptr(),
			int(offset)+common.Int32Size)
		//no compression for string
		util.AssertFunc(compressedSize == uncompressedSize)
		remaining := uint64(compressedSize)
		offset += int32(2 * common.Int32Size)

		var decompPtr unsafe.Pointer

		if uint64(remaining) <= BLOCK_SIZE-
			uint64(common.Int64Size)-
			uint64(offset) {
			decompPtr = util.PointerAdd(
				handle.Ptr(),
				int(offset),
			)
		} else {
			//no compress for string here
			decompPtr = util.CMalloc(int(compressedSize))
			targetPtr := decompPtr

			for remaining > 0 {
				toWrite := min(
					remaining,
					BLOCK_SIZE-
						uint64(common.Int64Size)-
						uint64(offset),
				)
				util.PointerCopy(
					targetPtr,
					util.PointerAdd(handle.Ptr(), int(offset)),
					int(toWrite),
				)

				remaining -= toWrite
				offset += int32(toWrite)
				targetPtr = util.PointerAdd(
					targetPtr,
					int(toWrite),
				)
				if remaining > 0 {
					nextBlock := util.Load2[BlockID](
						handle.Ptr(),
						int(offset),
					)
					blkHandle = blkMgr.RegisterBlock(nextBlock, false)
					handle = GBufferMgr.Pin(blkHandle)
					offset = 0
				}
			}
		}
		return ReadString2(decompPtr, 0, uncompressedSize)
	} else {
		if strBlk, ok := state._overflowBlocks[blkId]; ok {
			handle := GBufferMgr.Pin(strBlk._block)
			return ReadStringWithLength(handle.Ptr(), int(offset))
		} else {
			panic("must be in memory")
		}
	}
}

func ReadStringWithLength(
	target unsafe.Pointer,
	offset int) common.String {
	ptr := util.PointerAdd(
		target,
		offset,
	)
	strLen := util.Load[uint32](ptr)
	strPtr := util.PointerAdd(
		ptr,
		common.Int32Size,
	)
	return common.String{
		Data: strPtr,
		Len:  int(strLen),
	}
}

func ReadString2(
	target unsafe.Pointer,
	offset int,
	strLen uint32) common.String {
	ptr := util.PointerAdd(
		target,
		offset,
	)
	return common.String{
		Data: ptr,
		Len:  int(strLen),
	}
}

func FetchStringLocation(
	dict *StringDictionaryContainer,
	basePtr unsafe.Pointer,
	dictOffset int32,
) StringLocation {
	util.AssertFunc(
		dictOffset >= -int32(BLOCK_SIZE) &&
			dictOffset <= int32(BLOCK_SIZE))
	if dictOffset < 0 {
		result := StringLocation{}
		ReadStringMarker(
			util.PointerAdd(
				basePtr,
				int(int32(dict._end)-(-1*dictOffset)),
			),
			&result._blockId,
			&result._offset,
		)
		return result
	} else {
		return StringLocation{
			_blockId: -1,
			_offset:  dictOffset,
		}
	}
}

func ReadStringMarker(
	target unsafe.Pointer,
	blkId *BlockID,
	offset *int32) {
	*blkId = util.Load[BlockID](target)
	*offset = util.Load2[int32](target, common.Int64Size)
}

type StringLocation struct {
	_blockId BlockID
	_offset  int32
}
