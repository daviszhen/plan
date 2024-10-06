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

package storage

import (
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/util"
)

type Block struct {
	*FileBuffer
	id BlockID
}

func NewBlock(alloc *Allocator, id BlockID) *Block {
	ret := &Block{
		FileBuffer: NewFileBuffer(alloc, BLOCK, BLOCK_SIZE),
		id:         id,
	}

	return ret
}

type BlockState int

const (
	UNLOADED BlockState = 0
	LOADED   BlockState = 1
)

type BlockHandle struct {
	sync.Mutex
	_blockMgr   *BlockManager
	_state      atomic.Value
	_readers    atomic.Int32
	_blockId    BlockID
	_buffer     *FileBuffer
	_canDestroy bool
}

func NewBlockHandle(blockMgr *BlockManager, blockId BlockID) *BlockHandle {
	ret := &BlockHandle{
		_blockMgr: blockMgr,
		_blockId:  blockId,
	}
	ret._state.Store(UNLOADED)
	return ret
}
func NewBlockHandle2(
	blockMgr *BlockManager,
	blockId BlockID,
	buffer *FileBuffer,
	canDestroy bool,
) *BlockHandle {
	ret := &BlockHandle{
		_blockMgr:   blockMgr,
		_blockId:    blockId,
		_buffer:     buffer,
		_canDestroy: canDestroy,
	}
	ret._state.Store(LOADED)
	return ret
}

func (handle *BlockHandle) Close() {
	handle._buffer.Close()
	handle._buffer = nil
	handle._blockMgr.UnregisterBlock(handle._blockId, handle._canDestroy)
}

func (handle *BlockHandle) Load(inhandle *BlockHandle, reusableBuffer *FileBuffer) *BufferHandle {
	if handle._state.Load().(BlockState) == LOADED {
		return &BufferHandle{
			_handle: inhandle,
			_node:   inhandle._buffer,
		}
	}

	if handle._blockId < MAX_BLOCK {
		block := AllocateBlock(handle._blockMgr, reusableBuffer, handle._blockId)
		handle._blockMgr.Read(block)
		handle._buffer = block.FileBuffer
	} else {
		if handle._canDestroy {
			return &BufferHandle{}
		} else {
			panic("usp")
		}
	}
	handle._state.Store(LOADED)
	return &BufferHandle{
		_handle: handle,
		_node:   handle._buffer,
	}
}

func AllocateBlock(
	blockMgr *BlockManager,
	reusableBuffer *FileBuffer,
	id BlockID) *Block {
	if reusableBuffer != nil {
		if reusableBuffer._typ == BLOCK {
			return &Block{
				FileBuffer: reusableBuffer,
				id:         id,
			}
		}
		return blockMgr.CreateBlock(id, reusableBuffer)
	} else {
		return blockMgr.CreateBlock(id, nil)
	}
}

func (handle *BlockHandle) UnloadAndTakeBlock() *FileBuffer {
	if handle._state.Load().(BlockState) == UNLOADED {
		return nil
	}
	util.AssertFunc(handle.CanUnload())
	if handle._blockId >= MAX_BLOCK && !handle._canDestroy {
		panic("usp")
	}
	handle._state.Store(UNLOADED)
	ptr := handle._buffer
	handle._buffer = nil
	return ptr
}

func (handle *BlockHandle) CanUnload() bool {
	if handle._state.Load().(BlockState) == UNLOADED {
		return false
	}
	if handle._readers.Load() > 0 {
		return false
	}
	//TODO: temporary block
	return true
}

func (handle *BlockHandle) Unload() {
	buf := handle.UnloadAndTakeBlock()
	buf.Close()
}

func (handle *BlockHandle) ResizeBuffer(sz uint64, delta int64) {
	handle._buffer.Resize(sz)
}

type BlockManager struct {
	_bufferMgr *BufferManager
	_blocks    map[BlockID]BlockHandle
}

func (mgr *BlockManager) ConvertBlock(id BlockID, srcBuffer *FileBuffer) *Block {
	return &Block{
		FileBuffer: srcBuffer,
		id:         id,
	}
}

func (mgr *BlockManager) CreateBlock(id BlockID, srcBuffer *FileBuffer) *Block {
	if srcBuffer != nil {
		return mgr.ConvertBlock(id, srcBuffer)
	} else {
		return NewBlock(mgr._bufferMgr._bufferAlloc, id)
	}
}

func (mgr *BlockManager) Read(block *Block) {
	//TODO:
}

func (mgr *BlockManager) RegisterBlock(id BlockID, isMetaBlock bool) *BlockHandle {
	return nil
}

func (mgr *BlockManager) UnregisterBlock(id BlockID, canDestroy bool) {

}
