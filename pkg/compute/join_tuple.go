// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compute

import (
	"sync"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type TupleDataBlock struct {
	_handle *storage.BlockHandle
	_cap    uint64
	_size   uint64
}

func NewTupleDataBlock(bufferMgr *storage.BufferManager, cap uint64) *TupleDataBlock {
	ret := &TupleDataBlock{
		_cap:  cap,
		_size: 0,
	}
	bufferMgr.Allocate(cap, false, &ret._handle)
	return ret
}

func (block *TupleDataBlock) RemainingCapacity() uint64 {
	return block._cap - block._size
}

func (block *TupleDataBlock) RemainingRows(rowWidth uint64) uint64 {
	return block.RemainingCapacity() / rowWidth
}

func (block *TupleDataBlock) Close() {
	//TODO
}

type TupleDataAllocator struct {
	_bufferMgr  *storage.BufferManager
	_layout     *TupleDataLayout
	_rowBlocks  []*TupleDataBlock
	_heapBlocks []*TupleDataBlock
}

func NewTupleDataAllocator(bufferMgr *storage.BufferManager, layout *TupleDataLayout) *TupleDataAllocator {
	ret := &TupleDataAllocator{
		_bufferMgr: bufferMgr,
		_layout:    layout.copy(),
	}

	return ret
}

func (alloc *TupleDataAllocator) Build(
	segment *TupleDataSegment,
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	appendOffset uint64,
	appendCount uint64,
) {

	//prepare space
	if !util.Empty(segment._chunks) {
		alloc.ReleaseOrStoreHandles(
			pinState,
			segment,
			util.Back(segment._chunks),
			true,
		)
	}

	offset := uint64(0)
	//chunk idx, part idx
	chunkPartIndices := make([]util.Pair[uint64, uint64], 0)
	for offset != appendCount {
		//no chunks or last chunk is full
		if util.Empty(segment._chunks) ||
			util.Back(segment._chunks)._count == util.DefaultVectorSize {
			segment._chunks = append(segment._chunks, NewTupleDataChunk())
		}

		chunk := util.Back(segment._chunks)
		//count of rows can be recorded
		next := min(
			appendCount-offset,
			util.DefaultVectorSize-chunk._count,
		)

		//allocate space for these rows
		part := alloc.BuildChunkPart(
			pinState,
			chunkState,
			appendOffset+offset,
			next,
		)
		chunk.AddPart(part, alloc._layout)
		//chunk idx, part idx
		chunkPartIndices = append(chunkPartIndices,
			util.Pair[uint64, uint64]{
				First:  uint64(len(segment._chunks) - 1),
				Second: uint64(len(chunk._parts) - 1),
			})
		chunkPart := util.Back(chunk._parts)
		next = uint64(chunkPart._count)
		segment._count += next
		offset += next
	}

	//collect parts of these rows
	parts := make([]*TupleDataChunkPart, 0)
	for _, pair := range chunkPartIndices {
		parts = append(parts,
			segment._chunks[pair.First]._parts[pair.Second],
		)
	}

	//decide the base ptr of rows
	alloc.InitChunkStateInternal(
		pinState,
		chunkState,
		appendOffset,
		false,
		true,
		false,
		parts,
	)

	//skip merge

}

// InitChunkStateInternal evaluate the base ptr of rows
func (alloc *TupleDataAllocator) InitChunkStateInternal(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	offset uint64,
	recompute bool,
	initHeapPointers bool,
	initHeapSizes bool,
	parts []*TupleDataChunkPart,
) {
	rowLocSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](chunkState._rowLocations)
	heapSizesSlice := chunk.GetSliceInPhyFormatFlat[uint64](chunkState._heapSizes)
	heapLocSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](chunkState._heapLocations)

	rowWidth := alloc._layout.rowWidth()
	for _, part := range parts {
		next := part._count

		//evaluate the base pointer for every row
		baseRowPtr := alloc.GetRowPointer(pinState, part)
		for i := uint32(0); i < next; i++ {
			rowLocSlice[offset+uint64(i)] =
				util.PointerAdd(baseRowPtr, int(i)*rowWidth)
		}

		if alloc._layout.allConst() {
			offset += uint64(next)
			continue
		}

		if part._totalHeapSize == 0 {
			if initHeapSizes {
				panic("usp")
			}
			offset += uint64(next)
			continue
		}

		if recompute {
			panic("usp")
		}

		if initHeapSizes {
			panic("usp")
		}

		if initHeapPointers {
			//evaluate base ptr for every row saved in heap
			heapLocSlice[offset] = util.PointerAdd(
				part._baseHeapPtr,
				int(part._heapBlockOffset),
			)
			for i := uint32(1); i < next; i++ {
				idx := offset + uint64(i)
				heapLocSlice[idx] = util.PointerAdd(
					heapLocSlice[idx-1],
					int(heapSizesSlice[idx-1]),
				)
			}
		}

		offset += uint64(next)
	}
	util.AssertFunc(offset <= util.DefaultVectorSize)
}

// BuildChunkPart allocate space for rows
func (alloc *TupleDataAllocator) BuildChunkPart(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	appendOffset uint64,
	appendCount uint64,
) *TupleDataChunkPart {

	result := new(TupleDataChunkPart)

	//ensure enough space
	//no block or last block is full
	if util.Empty(alloc._rowBlocks) ||
		util.Back(alloc._rowBlocks).RemainingCapacity() <
			uint64(alloc._layout.rowWidth()) {
		alloc._rowBlocks = append(alloc._rowBlocks,
			NewTupleDataBlock(alloc._bufferMgr, storage.BLOCK_SIZE))
	}
	//the idx of the block that rows saved into
	result._rowBlockIdx = uint32(len(alloc._rowBlocks) - 1)
	rowBlock := util.Back(alloc._rowBlocks)
	//the offset in the block that rows saved into
	result._rowBlockOffset = uint32(rowBlock._size)
	//the count of rows that the block can save
	result._count = uint32(min(
		rowBlock.RemainingRows(uint64(alloc._layout.rowWidth())),
		appendCount))

	//handle variable length columns
	if !alloc._layout.allConst() {
		//heap size has been evaluated
		heapSizesSlice := chunk.GetSliceInPhyFormatFlat[uint64](chunkState._heapSizes)
		tHeapSize := uint64(0)
		//evaluate the total heap size for saved rows
		for i := uint32(0); i < result._count; i++ {
			tHeapSize += heapSizesSlice[appendOffset+uint64(i)]
		}

		if tHeapSize == 0 {
			//no need space for heap
			result._heapBlockIdx = INVALID_INDEX
			result._heapBlockOffset = INVALID_INDEX
			result._totalHeapSize = 0
			result._baseHeapPtr = nil
		} else {
			//ensure enough space
			//no heap block or last heap block is full
			if util.Empty(alloc._heapBlocks) ||
				util.Back(alloc._heapBlocks).RemainingCapacity() < heapSizesSlice[appendOffset] {
				sz := max(storage.BLOCK_SIZE, heapSizesSlice[appendOffset])
				alloc._heapBlocks = append(alloc._heapBlocks,
					NewTupleDataBlock(alloc._bufferMgr, sz),
				)
			}

			//heap block idx
			result._heapBlockIdx = uint32(len(alloc._heapBlocks) - 1)
			heapBlock := util.Back(alloc._heapBlocks)
			//offset in the heap block
			result._heapBlockOffset = uint32(heapBlock._size)

			//space length
			heapRest := heapBlock.RemainingCapacity()
			if tHeapSize <= heapRest {
				//enough for rows saved
				result._totalHeapSize = uint32(tHeapSize)
			} else {
				//not enough
				result._totalHeapSize = 0
				//as much as we can fill
				for i := uint32(0); i < result._count; i++ {
					sz := heapSizesSlice[appendOffset+uint64(i)]
					if uint64(result._totalHeapSize)+sz > heapRest {
						result._count = i
						break
					}
					result._totalHeapSize += uint32(sz)
				}
			}

			//total size added with occupied heap bytes
			heapBlock._size += uint64(result._totalHeapSize)
			result._baseHeapPtr = alloc.GetBaseHeapPointer(pinState, result)
		}
	}

	util.AssertFunc(result._count != 0 && result._count <= util.DefaultVectorSize)
	//total size added with occupied row bytes
	rowBlock._size += uint64(result._count * uint32(alloc._layout.rowWidth()))
	return result
}

func (alloc *TupleDataAllocator) GetRowPointer(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) unsafe.Pointer {
	ptr := alloc.PinRowBlock(pinState, part).Ptr()
	return util.PointerAdd(ptr, int(part._rowBlockOffset))
}

// PinRowBlock let the TupleDataPinState refers to the saved block saved
// in the TupleDataAllocator and Pin the row block
func (alloc *TupleDataAllocator) PinRowBlock(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) *storage.BufferHandle {
	idx := part._rowBlockIdx
	if handle, ok := pinState._rowHandles[idx]; !ok {
		util.AssertFunc(idx < uint32(len(alloc._rowBlocks)))
		rowBlock := alloc._rowBlocks[idx]
		util.AssertFunc(rowBlock._handle != nil)
		util.AssertFunc(uint64(part._rowBlockOffset) < rowBlock._size)
		util.AssertFunc(uint64(part._rowBlockOffset+part._count*uint32(alloc._layout.rowWidth())) <= rowBlock._size)
		handle = alloc._bufferMgr.Pin(rowBlock._handle)
		pinState._rowHandles[idx] = handle
		return handle
	} else {
		return handle
	}
}

func (alloc *TupleDataAllocator) GetBaseHeapPointer(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) unsafe.Pointer {
	return alloc.PinHeapBlock(pinState, part).Ptr()
}

// PinHeapBlock let the TupleDataPinState refers to the heap block saved
// in the TupleDataAllocator and Pin the heap block
func (alloc *TupleDataAllocator) PinHeapBlock(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) *storage.BufferHandle {
	idx := part._heapBlockIdx
	if handle, ok := pinState._heapHandles[idx]; !ok {
		util.AssertFunc(idx < uint32(len(alloc._heapBlocks)))
		heapBlock := alloc._heapBlocks[idx]
		util.AssertFunc(heapBlock._handle != nil)
		util.AssertFunc(uint64(part._heapBlockOffset) < heapBlock._size)
		util.AssertFunc(uint64(part._heapBlockOffset+part._totalHeapSize) <= heapBlock._size)

		handle = alloc._bufferMgr.Pin(heapBlock._handle)
		pinState._heapHandles[idx] = handle
		return handle
	} else {
		return handle
	}
}

// ReleaseOrStoreHandles recycle the block unused in pin state to segment pinned blocks
func (alloc *TupleDataAllocator) ReleaseOrStoreHandles(
	pinState *TupleDataPinState,
	segment *TupleDataSegment,
	chunk *TupleDataChunk,
	releaseHeap bool,
) {
	alloc.ReleaseOrStoreHandles2(
		segment,
		&segment.mu._pinnedRowHandles,
		pinState._rowHandles,
		chunk._rowBlockIds,
		alloc._rowBlocks,
		pinState._properties,
	)
	if !alloc._layout.allConst() && releaseHeap {
		alloc.ReleaseOrStoreHandles2(
			segment,
			&segment.mu._pinnedHeapHandles,
			pinState._heapHandles,
			chunk._heapBlockIds,
			alloc._heapBlocks,
			pinState._properties,
		)
	}
}

func (alloc *TupleDataAllocator) ReleaseOrStoreHandles2(
	segment *TupleDataSegment,
	pinnedHandles *[]*storage.BufferHandle,
	handles map[uint32]*storage.BufferHandle,
	blockIds UnorderedSet,
	blocks []*TupleDataBlock,
	prop TupleDataPinProperties,
) {
	for {
		found := false
		removed := make([]uint32, 0)
		for id, handle := range handles {
			if blockIds.find(uint64(id)) {
				//in used
				continue
			}
			switch prop {
			case PIN_PRRP_KEEP_PINNED:
				fun := func() {
					segment.mu.Lock()
					defer segment.mu.Unlock()
					blockCount := id + 1
					if blockCount > uint32(len(*pinnedHandles)) {
						//TODO: create vector
						need := blockCount - uint32(len(*pinnedHandles))
						*pinnedHandles = append(*pinnedHandles,
							make([]*storage.BufferHandle, need)...)
					}
					(*pinnedHandles)[id] = handle
				}
				fun()
			default:
				panic("usp")
			}
			removed = append(removed, id)
			found = true
			break
		}
		for _, rem := range removed {
			delete(handles, rem)
		}
		if !found {
			break
		}
	}
}

func (alloc *TupleDataAllocator) InitChunkState(
	seg *TupleDataSegment,
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	chunkIdx int,
	initHeap bool,
) {
	util.AssertFunc(chunkIdx < util.Size(seg._chunks))
	chunk := seg._chunks[chunkIdx]

	alloc.ReleaseOrStoreHandles(
		pinState,
		seg,
		chunk,
		len(chunk._heapBlockIds) != 0,
	)

	alloc.InitChunkStateInternal(
		pinState,
		chunkState,
		0,
		false,
		initHeap,
		initHeap,
		chunk._parts,
	)
}

type TupleDataSegment struct {
	_allocator *TupleDataAllocator
	_chunks    []*TupleDataChunk
	_count     uint64
	mu         struct {
		sync.Mutex
		_pinnedRowHandles  []*storage.BufferHandle
		_pinnedHeapHandles []*storage.BufferHandle
	}
}

func (segment *TupleDataSegment) Unpin() {
	segment.mu.Lock()
	defer segment.mu.Unlock()
	for _, handle := range segment.mu._pinnedRowHandles {
		handle.Close()
	}

	for _, handle := range segment.mu._pinnedHeapHandles {
		handle.Close()
	}
	segment.mu._pinnedRowHandles = nil
	segment.mu._pinnedHeapHandles = nil
}

func (segment *TupleDataSegment) ChunkCount() int {
	return util.Size(segment._chunks)
}

func NewTupleDataSegment(alloc *TupleDataAllocator) *TupleDataSegment {
	ret := &TupleDataSegment{
		_allocator: alloc,
	}
	return ret
}

type TupleDataChunk struct {
	_parts        []*TupleDataChunkPart
	_rowBlockIds  UnorderedSet
	_heapBlockIds UnorderedSet
	_count        uint64
}

func NewTupleDataChunk() *TupleDataChunk {
	ret := &TupleDataChunk{
		_rowBlockIds:  make(UnorderedSet),
		_heapBlockIds: make(UnorderedSet),
	}

	return ret
}

func (chunk *TupleDataChunk) AddPart(
	part *TupleDataChunkPart,
	layout *TupleDataLayout) {
	chunk._count += uint64(part._count)
	chunk._rowBlockIds.insert(uint64(part._rowBlockIdx))
	if !layout.allConst() && part._totalHeapSize > 0 {
		chunk._heapBlockIds.insert(uint64(part._heapBlockIdx))
	}
	chunk._parts = append(chunk._parts, part)
}

type TupleDataChunkPart struct {
	sync.Mutex
	_rowBlockIdx     uint32
	_rowBlockOffset  uint32
	_heapBlockIdx    uint32
	_heapBlockOffset uint32
	_baseHeapPtr     unsafe.Pointer
	_totalHeapSize   uint32
	_count           uint32
}

type TupleDataChunkState struct {
	_data              []chunk.UnifiedFormat
	_rowLocations      *chunk.Vector
	_heapLocations     *chunk.Vector
	_heapSizes         *chunk.Vector
	_columnIds         []int
	_childrenOutputIds []int
}

func NewTupleDataChunkState(cnt, childrenOutputCnt int) *TupleDataChunkState {
	ret := &TupleDataChunkState{
		_data:          make([]chunk.UnifiedFormat, cnt+childrenOutputCnt),
		_rowLocations:  chunk.NewVector2(common.PointerType(), util.DefaultVectorSize),
		_heapLocations: chunk.NewVector2(common.PointerType(), util.DefaultVectorSize),
		_heapSizes:     chunk.NewVector2(common.UbigintType(), util.DefaultVectorSize),
	}
	for i := 0; i < cnt; i++ {
		ret._columnIds = append(ret._columnIds, i)
	}
	for i := 0; i < childrenOutputCnt; i++ {
		ret._childrenOutputIds = append(ret._childrenOutputIds, i+cnt)
	}
	return ret
}

type TupleDataPinProperties int

const (
	PIN_PRRP_INVALID     TupleDataPinProperties = 0
	PIN_PRRP_KEEP_PINNED TupleDataPinProperties = 1
)

type TupleDataPinState struct {
	//the blocks for recording chunks
	_rowHandles  map[uint32]*storage.BufferHandle
	_heapHandles map[uint32]*storage.BufferHandle
	_properties  TupleDataPinProperties
}

func NewTupleDataPinState() *TupleDataPinState {
	ret := &TupleDataPinState{
		_rowHandles:  make(map[uint32]*storage.BufferHandle),
		_heapHandles: make(map[uint32]*storage.BufferHandle),
		_properties:  PIN_PRRP_INVALID,
	}
	return ret
}

type TupleDataChunkIterator struct {
	_collection    *TupleDataCollection
	_initHeap      bool
	_startSegIdx   int
	_startChunkIdx int
	_endSegIdx     int
	_endChunkIdx   int
	_state         TupleDataScanState
	_curSegIdx     int
	_curChunkIdx   int
}

func NewTupleDataChunkIterator(
	collection *TupleDataCollection,
	prop TupleDataPinProperties,
	from, to int,
	initHeap bool,
) *TupleDataChunkIterator {
	ret := &TupleDataChunkIterator{
		_collection: collection,
		_initHeap:   initHeap,
	}
	ret._state._pinState = *NewTupleDataPinState()
	ret._state._pinState._properties = prop
	layout := collection._layout
	ret._state._chunkState = *NewTupleDataChunkState(layout.columnCount(), layout.childrenOutputCount())

	util.AssertFunc(from < to)
	util.AssertFunc(from <= collection.ChunkCount())

	idx := 0
	for segIdx := 0; segIdx < util.Size(collection._segments); segIdx++ {
		seg := collection._segments[segIdx]
		if from >= idx && from <= idx+seg.ChunkCount() {
			//start
			ret._startSegIdx = segIdx
			ret._startChunkIdx = from - idx
		}
		if to >= idx && to <= idx+seg.ChunkCount() {
			//end
			ret._endSegIdx = segIdx
			ret._endChunkIdx = to - idx
		}
		idx += seg.ChunkCount()
	}
	ret.Reset()
	return ret
}

func NewTupleDataChunkIterator2(
	collection *TupleDataCollection,
	prop TupleDataPinProperties,
	initHeap bool,
) *TupleDataChunkIterator {
	return NewTupleDataChunkIterator(
		collection,
		prop,
		0,
		collection.ChunkCount(),
		initHeap,
	)
}

func (iter *TupleDataChunkIterator) Reset() {
	iter._state._segmentIdx = iter._startSegIdx
	iter._state._chunkIdx = iter._startChunkIdx
	iter._collection.NextScanIndex(
		&iter._state,
		&iter._curSegIdx,
		&iter._curChunkIdx)
	iter.InitCurrentChunk()
}

func (iter *TupleDataChunkIterator) InitCurrentChunk() {
	seg := iter._collection._segments[iter._curSegIdx]
	seg._allocator.InitChunkState(
		seg,
		&iter._state._pinState,
		&iter._state._chunkState,
		iter._curChunkIdx,
		iter._initHeap,
	)
}

func (iter *TupleDataChunkIterator) Done() bool {
	return iter._curSegIdx == iter._endSegIdx &&
		iter._curChunkIdx == iter._endChunkIdx
}

func (iter *TupleDataChunkIterator) Next() bool {
	util.AssertFunc(!iter.Done())

	oldSegIdx := iter._curSegIdx
	//check end of collection
	if !iter._collection.NextScanIndex(
		&iter._state,
		&iter._curSegIdx,
		&iter._curChunkIdx) || iter.Done() {
		iter._collection.FinalizePinState2(
			&iter._state._pinState,
			iter._collection._segments[oldSegIdx],
		)
		iter._curSegIdx = iter._endSegIdx
		iter._curChunkIdx = iter._endChunkIdx
		return false
	}

	if iter._curSegIdx != oldSegIdx {
		iter._collection.FinalizePinState2(
			&iter._state._pinState,
			iter._collection._segments[oldSegIdx],
		)
	}
	iter.InitCurrentChunk()
	return true
}

func (iter *TupleDataChunkIterator) GetCurrentChunkCount() int {
	return int(iter._collection._segments[iter._curSegIdx]._chunks[iter._curChunkIdx]._count)
}

func (iter *TupleDataChunkIterator) GetChunkState() *TupleDataChunkState {
	return &iter._state._chunkState
}

func (iter *TupleDataChunkIterator) GetRowLocations() []unsafe.Pointer {
	return chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](iter._state._chunkState._rowLocations)
}
