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

package plan

import (
	"sync"
	"unsafe"

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
	if len(segment._chunks) != 0 {
		alloc.ReleaseOrStoreHandles(
			pinState,
			segment,
			back(segment._chunks),
			true,
		)
	}

	offset := uint64(0)
	chunkPartIndices := make([]util.Pair[uint64, uint64], 0)
	for offset != appendCount {
		if len(segment._chunks) == 0 ||
			back(segment._chunks)._count == defaultVectorSize {
			segment._chunks = append(segment._chunks, &TupleDataChunk{})
		}

		chunk := back(segment._chunks)
		next := min(
			appendCount-offset,
			defaultVectorSize-chunk._count,
		)

		part := alloc.BuildChunkPart(
			pinState,
			chunkState,
			appendOffset+offset,
			next,
		)
		chunk.AddPart(part, alloc._layout)
		chunkPartIndices = append(chunkPartIndices,
			util.Pair[uint64, uint64]{
				First:  uint64(len(segment._chunks) - 1),
				Second: uint64(len(chunk._parts) - 1),
			})
		chunkPart := back(chunk._parts)
		next = uint64(chunkPart._count)
		segment._count += next
		offset += next
	}

	//
	parts := make([]*TupleDataChunkPart, 0)
	for _, pair := range chunkPartIndices {
		parts = append(parts,
			segment._chunks[pair.First]._parts[pair.Second],
		)
	}

	//
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

func (alloc *TupleDataAllocator) InitChunkStateInternal(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	offset uint64,
	recompute bool,
	initHeapPointers bool,
	initHeapSizes bool,
	parts []*TupleDataChunkPart,
) {
	rowLocSlice := getSliceInPhyFormatFlat[unsafe.Pointer](chunkState._rowLocations)
	heapSizesSlice := getSliceInPhyFormatFlat[uint64](chunkState._heapSizes)
	heapLocSlice := getSliceInPhyFormatFlat[unsafe.Pointer](chunkState._heapLocations)

	rowWidth := alloc._layout.rowWidth()
	for _, part := range parts {
		next := part._count

		//evaluate the row base pointer
		baseRowPtr := alloc.GetRowPointer(pinState, part)
		for i := uint32(0); i < next; i++ {
			rowLocSlice[offset+uint64(i)] =
				pointerAdd(baseRowPtr, int(i)*rowWidth)
		}

		if alloc._layout.allConst() {
			offset += uint64(next)
			continue
		}

		if part._totalHeapSize == 0 {
			if initHeapPointers {
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
			heapLocSlice[offset] = pointerAdd(
				part._baseHeapPtr,
				int(part._heapBlockOffset),
			)
			for i := uint32(1); i < next; i++ {
				idx := offset + uint64(i)
				heapLocSlice[idx] = pointerAdd(
					heapLocSlice[idx-1],
					int(heapSizesSlice[idx-1]),
				)
			}
		}

		offset += uint64(next)
	}
	assertFunc(offset <= defaultVectorSize)
}

func (alloc *TupleDataAllocator) BuildChunkPart(
	pinState *TupleDataPinState,
	chunkState *TupleDataChunkState,
	appendOffset uint64,
	appendCount uint64,
) *TupleDataChunkPart {

	result := new(TupleDataChunkPart)

	//ensure enough space
	if len(alloc._rowBlocks) == 0 ||
		back(alloc._rowBlocks).RemainingCapacity() <
			uint64(alloc._layout.rowWidth()) {
		alloc._rowBlocks = append(alloc._rowBlocks,
			NewTupleDataBlock(alloc._bufferMgr, storage.BLOCK_SIZE))
	}
	result._rowBlockIdx = uint32(len(alloc._rowBlocks) - 1)
	rowBlock := back(alloc._rowBlocks)
	result._rowBlockOffset = uint32(rowBlock._size)
	result._count = uint32(min(
		rowBlock.RemainingRows(uint64(alloc._layout.rowWidth())),
		appendCount))

	if !alloc._layout.allConst() {
		heapSizesSlice := getSliceInPhyFormatFlat[uint64](chunkState._heapSizes)
		tHeapSize := uint64(0)
		for i := uint32(0); i < result._count; i++ {
			tHeapSize += heapSizesSlice[appendOffset+uint64(i)]
		}

		if tHeapSize == 0 {
			result._heapBlockIdx = INVALID_INDEX
			result._heapBlockOffset = INVALID_INDEX
			result._totalHeapSize = 0
			result._baseHeapPtr = nil
		} else {

			//ensure enough space
			if len(alloc._heapBlocks) == 0 ||
				back(alloc._heapBlocks).RemainingCapacity() < heapSizesSlice[appendOffset] {
				sz := max(storage.BLOCK_SIZE, heapSizesSlice[appendOffset])
				alloc._heapBlocks = append(alloc._heapBlocks,
					NewTupleDataBlock(alloc._bufferMgr, sz),
				)
			}

			result._heapBlockIdx = uint32(len(alloc._heapBlocks) - 1)
			heapBlock := back(alloc._heapBlocks)
			result._heapBlockOffset = uint32(heapBlock._size)
			heapRest := heapBlock.RemainingCapacity()
			if tHeapSize <= heapRest {
				result._totalHeapSize = uint32(tHeapSize)
			} else {
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

			heapBlock._size += uint64(result._totalHeapSize)
			result._baseHeapPtr = alloc.GetBaseHeapPointer(pinState, result)
		}
	}

	assertFunc(result._count != 0 && result._count <= defaultVectorSize)
	rowBlock._size += uint64(result._count * uint32(alloc._layout.rowWidth()))
	return result
}

func (alloc *TupleDataAllocator) GetRowPointer(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) unsafe.Pointer {
	ptr := alloc.PinRowBlock(pinState, part).Ptr()
	return pointerAdd(ptr, int(part._rowBlockOffset))
}

func (alloc *TupleDataAllocator) PinRowBlock(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) *storage.BufferHandle {
	idx := part._rowBlockIdx
	if handle, ok := pinState._rowHandles[idx]; !ok {
		assertFunc(idx < uint32(len(alloc._rowBlocks)))
		rowBlock := alloc._rowBlocks[idx]
		assertFunc(rowBlock._handle != nil)
		assertFunc(uint64(part._rowBlockOffset) < rowBlock._size)
		assertFunc(uint64(part._rowBlockOffset+part._count*uint32(alloc._layout.rowWidth())) <= rowBlock._size)
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

func (alloc *TupleDataAllocator) PinHeapBlock(
	pinState *TupleDataPinState,
	part *TupleDataChunkPart) *storage.BufferHandle {
	idx := part._heapBlockIdx
	if handle, ok := pinState._heapHandles[idx]; !ok {
		assertFunc(idx < uint32(len(alloc._heapBlocks)))
		heapBlock := alloc._heapBlocks[idx]
		assertFunc(heapBlock._handle != nil)
		assertFunc(uint64(part._heapBlockOffset) < heapBlock._size)
		assertFunc(uint64(part._heapBlockOffset+part._totalHeapSize) <= heapBlock._size)

		handle = alloc._bufferMgr.Pin(heapBlock._handle)
		pinState._heapHandles[idx] = handle
		return handle
	} else {
		return handle
	}
}

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

type TupleDataChunk struct {
	_parts        []*TupleDataChunkPart
	_rowBlockIds  UnorderedSet
	_heapBlockIds UnorderedSet
	_count        uint64
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
	_data          []UnifiedFormat
	_rowLocations  *Vector
	_heapLocations *Vector
	_heapSizes     *Vector
	_columnIds     []uint64
}

type TupleDataPinProperties int

const (
	PIN_PRRP_INVALID     TupleDataPinProperties = 0
	PIN_PRRP_KEEP_PINNED TupleDataPinProperties = 1
)

type TupleDataPinState struct {
	_rowHandles  map[uint32]*storage.BufferHandle
	_heapHandles map[uint32]*storage.BufferHandle
	_properties  TupleDataPinProperties
}
