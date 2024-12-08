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
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

//#include <stdio.h>
//#include <stdlib.h>
import "C"

type AllocateData struct {
	alloc *Allocator
	ptr   unsafe.Pointer
	sz    uint64
}

type Allocator struct {
}

func NewAllocator() *Allocator {
	return &Allocator{}
}

func (alloc *Allocator) AllocateData(sz uint64) unsafe.Pointer {
	ptr := C.malloc(C.size_t(sz))
	if ptr == nil {
		panic(fmt.Sprintf("allocate %d bytes failed.", sz))
	}
	return ptr
}

func (alloc *Allocator) FreeData(ptr unsafe.Pointer, sz uint64) {
	C.free(ptr)
}

func (alloc *Allocator) ReallocateData(ptr unsafe.Pointer, oldSz, sz uint64) unsafe.Pointer {
	ptr2 := C.realloc(ptr, C.size_t(sz))
	if ptr2 == nil {
		panic(fmt.Sprintf("realloc %d bytes failed.", sz))
	}
	return ptr2
}

func (alloc *Allocator) Allocate(sz uint64) AllocateData {
	ptr := alloc.AllocateData(sz)
	return AllocateData{
		alloc: alloc,
		ptr:   ptr,
		sz:    sz,
	}
}

type BufferManager struct {
	_tempDir      string
	_tempLock     sync.Mutex
	_tempId       atomic.Uint64
	_tempBlockMgr BlockMgr
	_bufferAlloc  *Allocator
}

var GBufferMgr *BufferManager

func init() {

}

func NewBufferManager(tmp string) *BufferManager {
	ret := &BufferManager{
		_tempDir:     tmp,
		_bufferAlloc: NewAllocator(),
	}
	ret._tempId.Store(uint64(MAX_BLOCK))
	ret._tempBlockMgr = NewMemoryBlockMgr(ret)

	return ret
}

func (mgr *BufferManager) ConstructManagedBuffer(
	sz uint64,
	source *FileBuffer,
	typ FileBufferType,
) *FileBuffer {
	if source != nil {
		util.AssertFunc(source.AllocSize() == AllocSize(sz))
		ret := &FileBuffer{
			_bufferAlloc:    source._bufferAlloc,
			_typ:            typ,
			_buffer:         source._buffer,
			_size:           source._size,
			_internalBuffer: source._internalBuffer,
			_internalSize:   source._internalSize,
		}
		source.Init()
		return ret
	} else {
		return NewFileBuffer(mgr._bufferAlloc, typ, sz)
	}
}

func (mgr *BufferManager) RegisterMemory(
	sz uint64,
	canDestroy bool,
) *BlockHandle {
	util.AssertFunc(sz >= BLOCK_SIZE)
	buffer := mgr.ConstructManagedBuffer(sz, nil, MANAGED_BUFFER)
	mgr._tempId.Add(1)
	id := mgr._tempId.Load()
	return NewBlockHandle2(mgr._tempBlockMgr, BlockID(id), buffer, canDestroy)
}

func (mgr *BufferManager) RegisterSmallMemory(
	sz uint64,
) *BlockHandle {
	util.AssertFunc(sz < BLOCK_SIZE)
	buffer := mgr.ConstructManagedBuffer(sz, nil, TINY_BUFFER)
	mgr._tempId.Add(1)
	id := mgr._tempId.Load()
	return NewBlockHandle2(mgr._tempBlockMgr, BlockID(id), buffer, false)
}

func (mgr *BufferManager) Allocate(
	sz uint64,
	canDestory bool,
	block **BlockHandle,
) *BufferHandle {
	var local *BlockHandle
	if block == nil {
		block = &local
	}
	*block = mgr.RegisterMemory(sz, canDestory)
	return mgr.Pin(*block)
}

func (mgr *BufferManager) ReAllocate(
	handle *BlockHandle,
	sz uint64,
) {
	util.AssertFunc(sz >= BLOCK_SIZE)
	handle.Lock()
	defer handle.Unlock()
	util.AssertFunc(handle._state.Load() == LOADED)
	allocSz, _ := handle._buffer.CalculateMemory(sz)
	delta := int64(allocSz - handle._buffer.AllocSize())
	if delta == 0 {
		return
	}
	handle.ResizeBuffer(sz, delta)
}

func (mgr *BufferManager) Pin(handle *BlockHandle) *BufferHandle {
	handle.Lock()
	defer handle.Unlock()
	if handle._state.Load().(BlockState) == LOADED {
		handle._readers.Add(1)
		return handle.Load(handle, nil)
	}
	util.AssertFunc(handle._readers.Load() == 0)
	handle._readers.Store(1)
	return handle.Load(handle, nil)
}

func (mgr *BufferManager) Unpin(handle *BlockHandle) {
	handle.Lock()
	defer handle.Unlock()
	if handle._buffer == nil || handle._buffer._typ == TINY_BUFFER {
		return
	}
	util.AssertFunc(handle._readers.Load() > 0)
	handle._readers.Add(-1)
	if handle._readers.Load() == 0 {
		handle.Close()
	}
}

type FileBufferType int

const (
	BLOCK          FileBufferType = 1
	MANAGED_BUFFER FileBufferType = 2
	TINY_BUFFER    FileBufferType = 3
)

type FileBuffer struct {
	_bufferAlloc    *Allocator
	_typ            FileBufferType
	_buffer         unsafe.Pointer
	_size           uint64
	_internalBuffer unsafe.Pointer
	_internalSize   uint64
}

func NewFileBuffer(
	alloc *Allocator,
	bufferType FileBufferType,
	sz uint64) *FileBuffer {
	ret := &FileBuffer{
		_bufferAlloc: alloc,
		_typ:         bufferType,
	}
	ret.Init()
	if sz > 0 {
		ret.Resize(sz)
	}
	return ret
}

func NewFileBuffer2(
	src *FileBuffer,
	bufferType FileBufferType,
) *FileBuffer {
	ret := &FileBuffer{
		_bufferAlloc:    src._bufferAlloc,
		_typ:            bufferType,
		_buffer:         src._buffer,
		_size:           src._size,
		_internalBuffer: src._internalBuffer,
		_internalSize:   src._internalSize,
	}
	src.Init()
	return ret
}

func (fbuf *FileBuffer) Init() {
	fbuf._buffer = nil
	fbuf._internalBuffer = nil
	fbuf._internalSize = 0
	fbuf._size = 0
}

func (fbuf *FileBuffer) Close() {
	if fbuf == nil {
		return
	}
	if fbuf._internalBuffer != nil {
		fbuf._bufferAlloc.FreeData(fbuf._internalBuffer, fbuf._internalSize)
	}
}

func (fbuf *FileBuffer) CalculateMemory(sz uint64) (headerSz uint64, allocSz uint64) {
	if fbuf._typ == TINY_BUFFER {
		return 0, sz
	} else {
		return BLOCK_HEADER_SIZE,
			util.AlignValue(BLOCK_HEADER_SIZE+sz, SECTOR_SIZE)
	}
}

func (fbuf *FileBuffer) Resize(nsz uint64) {
	headerSz, allocSz := fbuf.CalculateMemory(nsz)
	fbuf.ReallocBuffer(allocSz)
	if nsz > 0 {
		fbuf._buffer = util.PointerAdd(fbuf._internalBuffer, int(headerSz))
		fbuf._size = fbuf._internalSize - headerSz
	}
}
func (fbuf *FileBuffer) ReallocBuffer(nSz uint64) {
	var ptr unsafe.Pointer
	if fbuf._internalBuffer != nil {
		ptr = fbuf._bufferAlloc.ReallocateData(fbuf._internalBuffer, fbuf._internalSize, nSz)
	} else {
		ptr = fbuf._bufferAlloc.AllocateData(nSz)
	}
	fbuf._internalBuffer = ptr
	fbuf._internalSize = nSz
	fbuf._size = 0
	fbuf._buffer = nil
}

func (fbuf *FileBuffer) AllocSize() uint64 {
	return fbuf._internalSize
}

func (fbuf *FileBuffer) Clear() {
	if fbuf._internalBuffer != nil {
		util.Memset(fbuf._internalBuffer, 0, int(fbuf._internalSize))
	}
}

func (fbuf *FileBuffer) Write(handle *os.File, loc uint64) error {
	src := util.PointerToSlice[byte](
		fbuf._internalBuffer,
		int(fbuf._internalSize))
	_, err := handle.WriteAt(src, int64(loc))
	return err
}

func (fbuf *FileBuffer) Read(handle *os.File, loc uint64) error {
	dst := util.PointerToSlice[byte](
		fbuf._internalBuffer,
		int(fbuf._internalSize))
	_, err := handle.ReadAt(dst, int64(loc))
	return err
}

type BufferHandle struct {
	_handle *BlockHandle
	_node   *FileBuffer
}

func (handle *BufferHandle) FileBuffer() *FileBuffer {
	return handle._node
}

func (handle *BufferHandle) Ptr() unsafe.Pointer {
	return handle._node._buffer
}

func (handle *BufferHandle) Close() {
	if handle._handle == nil || handle._node == nil {
		return
	}
	if handle._handle._blockMgr != nil {
		handle._handle._blockMgr.Unpin(handle._handle)
	}
	handle._handle = nil
	handle._node = nil
}
