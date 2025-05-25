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
	"fmt"
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
	util.AssertFunc(ret.AllocSize()&(SECTOR_SIZE-1) == 0)
	return ret
}

func NewBlock2(src *FileBuffer, id BlockID) *Block {
	ret := &Block{
		FileBuffer: NewFileBuffer2(src, BLOCK),
		id:         id,
	}
	util.AssertFunc(ret.AllocSize()&(SECTOR_SIZE-1) == 0)
	return ret
}

type BlockState int

const (
	UNLOADED BlockState = 0
	LOADED   BlockState = 1
)

type BlockHandle struct {
	sync.Mutex
	_blockMgr   BlockMgr
	_state      atomic.Value
	_readers    atomic.Int32
	_blockId    BlockID
	_buffer     *FileBuffer
	_canDestroy bool
}

func NewBlockHandle(blockMgr BlockMgr, blockId BlockID) *BlockHandle {
	ret := &BlockHandle{
		_blockMgr: blockMgr,
		_blockId:  blockId,
	}
	ret._state.Store(UNLOADED)
	return ret
}
func NewBlockHandle2(
	blockMgr BlockMgr,
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
		if err := handle._blockMgr.Read(block); err != nil {
			panic(err)
		}
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
	blockMgr BlockMgr,
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
	return true
}

func (handle *BlockHandle) Unload() {
	buf := handle.UnloadAndTakeBlock()
	buf.Close()
}

func (handle *BlockHandle) ResizeBuffer(sz uint64, delta int64) {
	handle._buffer.Resize(sz)
}

type DatabaseHeader struct {
	_iteration  uint64
	_metaBlock  BlockID
	_freeList   BlockID
	_blockCount uint64
}

func (header *DatabaseHeader) Serialize(serial util.Serialize) error {
	err := util.Write[uint64](header._iteration, serial)
	if err != nil {
		return err
	}
	err = util.Write[BlockID](header._metaBlock, serial)
	if err != nil {
		return err
	}
	err = util.Write[BlockID](header._freeList, serial)
	if err != nil {
		return err
	}
	err = util.Write[uint64](header._blockCount, serial)
	if err != nil {
		return err
	}
	return nil
}

func (header *DatabaseHeader) Deserialize(deserial *BufferedDeserializer) error {
	err := util.Read[uint64](&header._iteration, deserial)
	if err != nil {
		return err
	}
	err = util.Read[BlockID](&header._metaBlock, deserial)
	if err != nil {
		return err
	}
	err = util.Read[BlockID](&header._freeList, deserial)
	if err != nil {
		return err
	}
	err = util.Read[uint64](&header._blockCount, deserial)
	if err != nil {
		return err
	}
	return nil
}

const (
	MAGIC_BYTE_SIZE   uint64 = 4
	MAGIC_BYTE_OFFSET uint64 = BLOCK_HEADER_SIZE
	FLAG_COUNT        uint64 = 4
	magic                    = "plan"
	VERSION_NUMBER    uint64 = 1
)

type MainHeader struct {
	_magicByte     [MAGIC_BYTE_SIZE]byte
	_versionNumber uint64
	_flags         [FLAG_COUNT]uint64
}

func NewMainHeader() *MainHeader {
	header := &MainHeader{
		_versionNumber: VERSION_NUMBER,
	}
	copy(header._magicByte[:], magic)
	return header
}

func (header *MainHeader) Serialize(serial util.Serialize) error {
	err := serial.WriteData(header._magicByte[:], int(MAGIC_BYTE_SIZE))
	if err != nil {
		return err
	}
	err = util.Write[uint64](header._versionNumber, serial)
	if err != nil {
		return err
	}
	//writer
	writer := NewFieldWriter(serial)
	for i := uint64(0); i < FLAG_COUNT; i++ {
		err = WriteField[uint64](header._flags[i], writer)
		if err != nil {
			return err
		}
	}
	err = writer.Finalize()
	return err
}

func (header *MainHeader) Deserialize(deserial util.Deserialize) error {
	err := util.Read[uint64](&header._versionNumber, deserial)
	if err != nil {
		return err
	}
	//field reader
	reader, err := NewFieldReader(deserial)
	if err != nil {
		return err
	}
	for i := uint64(0); i < FLAG_COUNT; i++ {
		err = ReadRequired[uint64](&header._flags[i], reader)
		if err != nil {
			return err
		}
	}
	reader.Finalize()
	return nil
}

func SerializeMainHeader(header *MainHeader, buffer *FileBuffer) error {
	var err error
	dst := util.PointerToSlice[byte](
		buffer._buffer,
		int(FILE_HEADER_SIZE))
	serial := NewBufferedSerialize(dst[:0])
	defer serial.Close()
	err = header.Serialize(serial)
	if err != nil {
		return err
	}
	return err
}

func DeserializeMainHeader(buffer *FileBuffer) (MainHeader, error) {
	src := util.PointerToSlice[byte](
		buffer._buffer,
		int(FILE_HEADER_SIZE))
	deserial := NewBufferedDeserializer(src)
	ret := MainHeader{}
	err := ret.Deserialize(deserial)
	return ret, err
}

func SerializeDatabaseHeader(header *DatabaseHeader, buffer *FileBuffer) error {
	var err error
	dst := util.PointerToSlice[byte](
		buffer._buffer,
		int(FILE_HEADER_SIZE))
	serial := NewBufferedSerialize(dst[:0])
	defer serial.Close()
	err = header.Serialize(serial)
	if err != nil {
		return err
	}
	return err
}

func DeserializeDatabaseHeader(buffer *FileBuffer) (DatabaseHeader, error) {
	src := util.PointerToSlice[byte](
		buffer._buffer,
		int(FILE_HEADER_SIZE))
	deserial := NewBufferedDeserializer(src)
	ret := DatabaseHeader{}
	err := ret.Deserialize(deserial)
	return ret, err
}

type BlockPointer struct {
	_blockId BlockID
	_offset  uint32
}

func (ptr BlockPointer) String() string {
	return fmt.Sprintf("bptr:%d %d,", ptr._blockId, ptr._offset)
}
