package storage

import (
	"fmt"
	"os"
	"sync"

	"github.com/daviszhen/plan/pkg/util"
)

type BlockMgr interface {
	ConvertBlock(id BlockID, srcBuffer *FileBuffer) *Block
	CreateBlock(id BlockID, srcBuffer *FileBuffer) *Block
	GetFreeBlockId() BlockID
	IsRootBlock(BlockID) bool
	MarkBlockAsFree(BlockID)
	MarkBlockAsModified(BlockID)
	IncreaseBlockReferenceCount(BlockID)
	GetMetaBlock() BlockID
	Read(block *Block) error
	Write2(*FileBuffer, BlockID) error
	Write(block *Block) error
	WriteHeader(*DatabaseHeader) error
	TotalBlocks() uint64
	FreeBlocks() uint64
	RegisterBlock(id BlockID, isMetaBlock bool) *BlockHandle
	ClearMetaBlockHandles()
	ConvertToPersistent(BlockID, *BlockHandle) *BlockHandle
	UnregisterBlock(id BlockID, canDestroy bool)
	Unpin(handle *BlockHandle)
	BufferMgr() *BufferManager
}

type MemoryBlockMgr struct {
	_bufferMgr  *BufferManager
	_blocksLock sync.Mutex
	_blocks     map[BlockID]*BlockHandle
	_metaBlocks map[BlockID]*BlockHandle
}

func NewMemoryBlockMgr(bufMgr *BufferManager) *MemoryBlockMgr {
	return &MemoryBlockMgr{
		_bufferMgr:  bufMgr,
		_blocks:     make(map[BlockID]*BlockHandle),
		_metaBlocks: make(map[BlockID]*BlockHandle),
	}
}

func (impl *MemoryBlockMgr) BufferMgr() *BufferManager {
	return impl._bufferMgr
}

func (impl *MemoryBlockMgr) Unpin(handle *BlockHandle) {
	impl._bufferMgr.Unpin(handle)
}

func (impl *MemoryBlockMgr) ConvertBlock(id BlockID, srcBuffer *FileBuffer) *Block {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) CreateBlock(id BlockID, srcBuffer *FileBuffer) *Block {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) GetFreeBlockId() BlockID {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) IsRootBlock(id BlockID) bool {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) MarkBlockAsFree(id BlockID) {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) MarkBlockAsModified(id BlockID) {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) IncreaseBlockReferenceCount(id BlockID) {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) GetMetaBlock() BlockID {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) Read(block *Block) error {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) Write2(buffer *FileBuffer, id BlockID) error {
	//TODO implement me
	panic("implement me")
	return nil
}

func (impl *MemoryBlockMgr) Write(block *Block) error {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) WriteHeader(header *DatabaseHeader) error {
	//TODO implement me
	panic("implement me")
	return nil
}

func (impl *MemoryBlockMgr) TotalBlocks() uint64 {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) FreeBlocks() uint64 {
	//TODO implement me
	panic("implement me")
}

func (impl *MemoryBlockMgr) RegisterBlock(
	id BlockID,
	isMetaBlock bool) *BlockHandle {
	impl._blocksLock.Lock()
	defer impl._blocksLock.Unlock()
	if ptr, ok := impl._blocks[id]; ok {
		return ptr
	}
	//create new block
	handle := NewBlockHandle(impl, id)
	if isMetaBlock {
		impl._metaBlocks[id] = handle
	}
	impl._blocks[id] = handle
	return handle
}

func (impl *MemoryBlockMgr) ClearMetaBlockHandles() {
	//FIXME: free handle
	impl._metaBlocks = make(map[BlockID]*BlockHandle)
}

func (impl *MemoryBlockMgr) ConvertToPersistent(
	id BlockID, oldBlock *BlockHandle) *BlockHandle {
	impl._bufferMgr.Pin(oldBlock)
	util.AssertFunc(oldBlock._state.Load().(BlockState) == LOADED)
	util.AssertFunc(oldBlock._buffer != nil)
	util.AssertFunc(oldBlock._buffer.AllocSize() <= BLOCK_ALLOC_SIZE)

	//register new block
	newBlock := impl.RegisterBlock(id, false)
	util.AssertFunc(newBlock._state.Load().(BlockState) == UNLOADED)
	util.AssertFunc(oldBlock._readers.Load() == 0)

	//move data from old block to new block
	newBlock._state.Store(LOADED)
	//newBlock._buffer =
	blk := impl.ConvertBlock(id, oldBlock._buffer)
	newBlock._buffer = blk.FileBuffer

	//clear old buffer
	oldBlock._buffer.Close()
	oldBlock._buffer = nil
	oldBlock._state.Store(UNLOADED)
	oldBlock.Close()

	//write new block into disk
	impl.Write2(newBlock._buffer, id)
	return newBlock
}

func (impl *MemoryBlockMgr) UnregisterBlock(id BlockID, canDestroy bool) {
	impl._blocksLock.Lock()
	defer impl._blocksLock.Unlock()
	delete(impl._blocks, id)
}

type FileBlockMgr struct {
	_bufferMgr  *BufferManager
	_blocksLock sync.Mutex
	_blocks     map[BlockID]*BlockHandle
	_metaBlocks map[BlockID]*BlockHandle
	//
	_activeHeader   uint8
	_path           string
	_readOnly       bool
	_handle         *os.File
	_headerBuffer   *FileBuffer
	_freeList       map[BlockID]bool
	_multiUseBlocks map[BlockID]uint32
	_modifiedBlocks map[BlockID]bool
	_metaBlock      BlockID
	_maxBlock       BlockID
	_freeListId     BlockID
	_iterationCount uint64
	_blockLock      sync.Mutex
}

func NewFileBlockMgr(
	bufferMgr *BufferManager,
	path string,
	readOnly bool) *FileBlockMgr {
	ret := &FileBlockMgr{
		_bufferMgr:      bufferMgr,
		_blocks:         make(map[BlockID]*BlockHandle),
		_metaBlocks:     make(map[BlockID]*BlockHandle),
		_path:           path,
		_freeList:       make(map[BlockID]bool),
		_multiUseBlocks: make(map[BlockID]uint32),
		_modifiedBlocks: make(map[BlockID]bool),
	}
	ret._headerBuffer = NewFileBuffer(
		bufferMgr._bufferAlloc,
		MANAGED_BUFFER,
		FILE_HEADER_SIZE-BLOCK_HEADER_SIZE)
	ret._iterationCount = 0
	ret._readOnly = readOnly
	return ret
}

func (mgr *FileBlockMgr) BufferMgr() *BufferManager {
	return mgr._bufferMgr
}

func (mgr *FileBlockMgr) Unpin(handle *BlockHandle) {
	mgr._bufferMgr.Unpin(handle)
}

func (mgr *FileBlockMgr) ConvertBlock(id BlockID, srcBuffer *FileBuffer) *Block {
	return NewBlock2(srcBuffer, id)
}

func (mgr *FileBlockMgr) CreateBlock(id BlockID, srcBuffer *FileBuffer) *Block {
	if srcBuffer != nil {
		return mgr.ConvertBlock(id, srcBuffer)
	} else {
		return NewBlock(mgr._bufferMgr._bufferAlloc, id)
	}
}

func (mgr *FileBlockMgr) Read(block *Block) error {
	return mgr.ReadAndChecksum(
		block.FileBuffer,
		BLOCK_START+uint64(block.id)*BLOCK_ALLOC_SIZE)
}

func (mgr *FileBlockMgr) RegisterBlock(
	id BlockID,
	isMetaBlock bool) *BlockHandle {
	mgr._blocksLock.Lock()
	defer mgr._blocksLock.Unlock()
	if ptr, ok := mgr._blocks[id]; ok {
		return ptr
	}
	//create new block
	handle := NewBlockHandle(mgr, id)
	if isMetaBlock {
		mgr._metaBlocks[id] = handle
	}
	mgr._blocks[id] = handle
	return handle
}

func (mgr *FileBlockMgr) ClearMetaBlockHandles() {
	//FIXME: free handle
	mgr._metaBlocks = make(map[BlockID]*BlockHandle)
}

func (mgr *FileBlockMgr) ConvertToPersistent(
	id BlockID, oldBlock *BlockHandle) *BlockHandle {
	mgr._bufferMgr.Pin(oldBlock)
	util.AssertFunc(oldBlock._state.Load().(BlockState) == LOADED)
	util.AssertFunc(oldBlock._buffer != nil)
	util.AssertFunc(oldBlock._buffer.AllocSize() <= BLOCK_ALLOC_SIZE)

	//register new block
	newBlock := mgr.RegisterBlock(id, false)
	util.AssertFunc(newBlock._state.Load().(BlockState) == UNLOADED)
	util.AssertFunc(newBlock._readers.Load() == 0)

	//move data from old block to new block
	newBlock._state.Store(LOADED)
	//newBlock._buffer =
	blk := mgr.ConvertBlock(id, oldBlock._buffer)
	newBlock._buffer = blk.FileBuffer

	//clear old buffer
	oldBlock._buffer.Close()
	oldBlock._buffer = nil
	oldBlock._state.Store(UNLOADED)
	oldBlock.Close()

	//write new block into disk
	if err := mgr.Write2(newBlock._buffer, id); err != nil {
		panic(err)
	}
	return newBlock
}

func (mgr *FileBlockMgr) UnregisterBlock(id BlockID, canDestroy bool) {
	mgr._blocksLock.Lock()
	defer mgr._blocksLock.Unlock()
	delete(mgr._blocks, id)
}

func (mgr *FileBlockMgr) GetFileFlags(createNew bool) int {
	if mgr._readOnly {
		util.AssertFunc(!createNew)
		return os.O_RDONLY
	} else {
		t := os.O_RDWR
		if createNew {
			t |= os.O_CREATE
		}
		return t
	}
}

func (mgr *FileBlockMgr) CreateNewDatabase() error {
	var err error
	flags := mgr.GetFileFlags(true)
	mgr._handle, err = os.OpenFile(mgr._path, flags, 0755)
	if err != nil {
		return err
	}
	mgr._headerBuffer.Clear()

	//1. fill the new header
	mainHeader := NewMainHeader()
	mainHeader._versionNumber = VERSION_NUMBER
	err = SerializeMainHeader(mainHeader, mgr._headerBuffer)
	if err != nil {
		return err
	}
	//1.2 write main header into file
	err = mgr.ChecksumAndWrite(mgr._headerBuffer, 0)
	if err != nil {
		return err
	}
	mgr._headerBuffer.Clear()

	//2. save database header
	var h1, h2 DatabaseHeader
	//header 1
	h1._iteration = 0
	h1._metaBlock = -1
	h1._freeList = -1
	h1._blockCount = 0
	err = SerializeDatabaseHeader(&h1, mgr._headerBuffer)
	if err != nil {
		return err
	}
	err = mgr.ChecksumAndWrite(mgr._headerBuffer, FILE_HEADER_SIZE)
	if err != nil {
		return err
	}

	//header 2
	h2._iteration = 0
	h2._metaBlock = -1
	h2._freeList = -1
	h2._blockCount = 0
	err = SerializeDatabaseHeader(&h2, mgr._headerBuffer)
	if err != nil {
		return err
	}
	err = mgr.ChecksumAndWrite(mgr._headerBuffer, FILE_HEADER_SIZE*2)
	if err != nil {
		return err
	}
	err = mgr._handle.Sync()
	if err != nil {
		return err
	}
	mgr._iterationCount = 0
	mgr._activeHeader = 1
	mgr._maxBlock = 0
	return nil
}

func (mgr *FileBlockMgr) ReadAndChecksum(
	block *FileBuffer,
	loc uint64,
) error {
	err := block.Read(mgr._handle, loc)
	if err != nil {
		return err
	}

	check := util.Load[uint64](block._internalBuffer)
	res := util.Checksum(block._buffer, block._size)
	if check != res {
		return fmt.Errorf("checksum failed")
	}
	return nil
}

func (mgr *FileBlockMgr) ChecksumAndWrite(
	block *FileBuffer,
	loc uint64,
) error {
	res := util.Checksum(block._buffer, block._size)
	util.Store[uint64](res, block._internalBuffer)
	return block.Write(mgr._handle, loc)
}

func (mgr *FileBlockMgr) LoadExistingDatabase() error {
	var err error
	flags := mgr.GetFileFlags(false)
	mgr._handle, err = os.OpenFile(mgr._path, flags, 0755)
	if err != nil {
		return err
	}
	//1. check main header
	err = mgr.ReadAndChecksum(mgr._headerBuffer, 0)
	if err != nil {
		return err
	}
	_, err = DeserializeMainHeader(mgr._headerBuffer)
	if err != nil {
		return err
	}

	//2. read database header
	var h1, h2 DatabaseHeader
	err = mgr.ReadAndChecksum(mgr._headerBuffer, FILE_HEADER_SIZE)
	if err != nil {
		return err
	}
	h1, err = DeserializeDatabaseHeader(mgr._headerBuffer)
	if err != nil {
		return err
	}
	err = mgr.ReadAndChecksum(mgr._headerBuffer, FILE_HEADER_SIZE*2)
	if err != nil {
		return err
	}
	h2, err = DeserializeDatabaseHeader(mgr._headerBuffer)
	if err != nil {
		return err
	}
	if h1._iteration > h2._iteration {
		mgr._activeHeader = 0
		mgr.Init(&h1)
	} else {
		mgr._activeHeader = 1
		mgr.Init(&h2)
	}
	err = mgr.LoadFreeList()
	if err != nil {
		return err
	}
	return nil
}

func (mgr *FileBlockMgr) Init(header *DatabaseHeader) {
	mgr._freeListId = header._freeList
	mgr._metaBlock = header._metaBlock
	mgr._iterationCount = header._iteration
	mgr._maxBlock = BlockID(header._blockCount)
}

func (mgr *FileBlockMgr) LoadFreeList() error {
	if mgr._freeListId == -1 {
		return nil
	}
	reader, err := NewMetaBlockReader(
		mgr,
		mgr._freeListId,
		true,
	)
	if err != nil {
		return err
	}
	var freeListCount uint64
	err = util.Read[uint64](&freeListCount, reader)
	if err != nil {
		return err
	}
	mgr._freeList = make(map[BlockID]bool)
	for i := uint64(0); i < freeListCount; i++ {
		var id BlockID
		err = util.Read[BlockID](&id, reader)
		if err != nil {
			return err
		}
		mgr._freeList[id] = true
	}
	var multiUseBlocksCount uint64
	err = util.Read[uint64](&multiUseBlocksCount, reader)
	if err != nil {
		return err
	}
	mgr._multiUseBlocks = make(map[BlockID]uint32)
	for i := uint64(0); i < multiUseBlocksCount; i++ {
		var id BlockID
		var count uint32
		err = util.Read[BlockID](&id, reader)
		if err != nil {
			return err
		}
		err = util.Read[uint32](&count, reader)
		if err != nil {
			return err
		}
		mgr._multiUseBlocks[id] = count
	}
	return err
}

func (mgr *FileBlockMgr) WriteHeader(header *DatabaseHeader) error {
	mgr._iterationCount++
	header._iteration = mgr._iterationCount

	freeListBlocks := mgr.GetFreeListBlocks()

	for id := range mgr._modifiedBlocks {
		mgr._freeList[id] = true
	}
	mgr._modifiedBlocks = make(map[BlockID]bool)

	if len(freeListBlocks) != 0 {
		writer := NewFreeListBlockWriter(mgr, freeListBlocks)
		ptr := writer.GetBlockPointer()
		header._freeList = ptr._blockId
		for _, blockId := range freeListBlocks {
			mgr._modifiedBlocks[blockId] = true
		}
		err := util.Write[uint64](
			uint64(len(mgr._freeList)),
			writer)
		if err != nil {
			return err
		}
		for blockId := range mgr._freeList {
			err = util.Write[BlockID](blockId, writer)
			if err != nil {
				return err
			}
		}
		err = util.Write[uint64](
			uint64(len(mgr._multiUseBlocks)),
			writer)
		if err != nil {
			return err
		}
		for blockId, use := range mgr._multiUseBlocks {
			err = util.Write[BlockID](blockId, writer)
			if err != nil {
				return err
			}
			err = util.Write[uint32](use, writer)
			if err != nil {
				return err
			}
		}
		err = writer.Flush()
		if err != nil {
			return err
		}
	} else {
		header._freeList = -1
	}
	header._blockCount = uint64(mgr._maxBlock)
	err := mgr._handle.Sync()
	if err != nil {
		return err
	}
	mgr._headerBuffer.Clear()
	err = SerializeDatabaseHeader(header, mgr._headerBuffer)
	if err != nil {
		return err
	}
	loc := uint64(0)
	if mgr._activeHeader == 1 {
		loc = FILE_HEADER_SIZE
	} else {
		loc = FILE_HEADER_SIZE * 2
	}
	err = mgr.ChecksumAndWrite(
		mgr._headerBuffer,
		loc)
	if err != nil {
		return err
	}
	mgr._activeHeader = (mgr._activeHeader + 1) % 2
	return mgr._handle.Sync()
}

func (mgr *FileBlockMgr) GetFreeListBlocks() []BlockID {
	freeListBlocks := make([]BlockID, 0)

	if len(mgr._freeList) != 0 ||
		len(mgr._multiUseBlocks) != 0 ||
		len(mgr._modifiedBlocks) != 0 {
		freeListSize := 8 + 8*(len(mgr._freeList)+len(mgr._modifiedBlocks))
		multiUseBlocksSize := 8 + (8+4)*len(mgr._multiUseBlocks)
		totalSize := freeListSize + multiUseBlocksSize
		spaceInBlock := int(BLOCK_SIZE - 4*8)
		totalBlocks := (totalSize + spaceInBlock - 1) / spaceInBlock
		for i := 0; i < totalBlocks; i++ {
			blockId := mgr.GetFreeBlockId()
			freeListBlocks = append(freeListBlocks, blockId)
		}
	}
	return freeListBlocks
}

func (mgr *FileBlockMgr) GetFreeBlockId() BlockID {
	mgr._blockLock.Lock()
	defer mgr._blockLock.Unlock()
	block := BlockID(0)
	if len(mgr._freeList) != 0 {
		for id := range mgr._freeList {
			block = id
			break
		}
		delete(mgr._freeList, block)
	} else {
		block = mgr._maxBlock
		mgr._maxBlock++
	}
	return block
}
func (mgr *FileBlockMgr) Write(block *Block) error {
	return mgr.Write2(block.FileBuffer, block.id)
}

func (mgr *FileBlockMgr) Write2(buffer *FileBuffer, id BlockID) error {
	return mgr.ChecksumAndWrite(
		buffer,
		BLOCK_START+uint64(id)*BLOCK_ALLOC_SIZE)
}

func (mgr *FileBlockMgr) MarkBlockAsModified(id BlockID) {
	mgr._blockLock.Lock()
	defer mgr._blockLock.Unlock()
	if cnt, ok := mgr._multiUseBlocks[id]; ok {
		cnt--
		if cnt <= 1 {
			delete(mgr._multiUseBlocks, id)
		} else {
			mgr._multiUseBlocks[id] = cnt
		}
		return
	}
	if _, ok := mgr._freeList[id]; ok {
		panic(fmt.Sprintf("%d should not be in free list", id))
	}
	mgr._modifiedBlocks[id] = true
}

func (mgr *FileBlockMgr) IsRootBlock(root BlockID) bool {
	return root == mgr._metaBlock
}

func (mgr *FileBlockMgr) MarkBlockAsFree(id BlockID) {
	mgr._blockLock.Lock()
	defer mgr._blockLock.Unlock()
	if _, ok := mgr._freeList[id]; ok {
		panic(fmt.Sprintf("%d already in free list", id))
	}
	delete(mgr._multiUseBlocks, id)
	mgr._freeList[id] = true
}

func (mgr *FileBlockMgr) IncreaseBlockReferenceCount(id BlockID) {
	mgr._blockLock.Lock()
	defer mgr._blockLock.Unlock()
	if cnt, ok := mgr._multiUseBlocks[id]; ok {
		mgr._multiUseBlocks[id] = cnt + 1
	} else {
		mgr._multiUseBlocks[id] = 2
	}
}

func (mgr *FileBlockMgr) GetMetaBlock() BlockID {
	return mgr._metaBlock
}

func (mgr *FileBlockMgr) TotalBlocks() uint64 {
	mgr._blockLock.Lock()
	defer mgr._blockLock.Unlock()
	return uint64(mgr._maxBlock)
}

func (mgr *FileBlockMgr) FreeBlocks() uint64 {
	mgr._blockLock.Lock()
	defer mgr._blockLock.Unlock()
	return uint64(len(mgr._freeList))
}

type PartialBlockState struct {
	_blockId       BlockID
	_blockSize     uint32
	_offsetInBlock uint32
	_blockUseCount uint32
}

type PartialColumnSegment struct {
	_data          *ColumnData
	_segment       *ColumnSegment
	_offsetInBlock uint32
}

type PartialBlock struct {
	_state    *PartialBlockState
	_blockMgr BlockMgr
	_block    *BlockHandle
	_segments []*PartialColumnSegment
}

func (pblock *PartialBlock) AddSegmentToTail(data *ColumnData, segment *ColumnSegment, block uint32) {
	pblock._segments = append(pblock._segments, &PartialColumnSegment{
		_data:          data,
		_segment:       segment,
		_offsetInBlock: block,
	})
}

type PartialBlockAllocation struct {
	_blockMgr       BlockMgr
	_allocationSize uint32
	_state          *PartialBlockState
	_partialBlock   *PartialBlock
}

const (
	CkpTypeFullCkp       = 0
	CkpTypeAppendToTable = 1
)

const (
	DEFAULT_MAX_PARTIAL_BLOCK_SIZE        = BLOCK_SIZE / 5 * 4
	DEFAULT_MAX_USE_COUNT          uint64 = 1 << 20
	MAX_BLOCK_MAP_SIZE             uint64 = 1 << 31
)

type PartialBlockMgr struct {
	_blockMgr              BlockMgr
	_ckpTyp                int
	_partiallyFilledBlocks map[uint64][]PartialBlock
	_writtenBlocks         map[BlockID]bool
	_maxPartialBlockSize   uint32
	_maxUseCount           uint32
}

func (partial *PartialBlockMgr) GetBlockAllocation(sz IdxType) *PartialBlockAllocation {
	alloc := &PartialBlockAllocation{}
	alloc._blockMgr = partial._blockMgr
	alloc._allocationSize = uint32(sz)

	if uint32(sz) <= partial._maxPartialBlockSize &&
		partial.GetPartialBlock(sz, &alloc._partialBlock) {
		alloc._partialBlock._state._blockUseCount++
		alloc._state = alloc._partialBlock._state
		if partial._ckpTyp == CkpTypeFullCkp {
			partial._blockMgr.IncreaseBlockReferenceCount(alloc._state._blockId)
		}
	} else {
		partial.AllocateBlock(alloc._state, sz)
	}
	return alloc
}

func (partial *PartialBlockMgr) GetPartialBlock(
	sz IdxType,
	partialBlock **PartialBlock,
) bool {
	if arr, ok := partial._partiallyFilledBlocks[uint64(sz)]; !ok {
		return false
	} else {
		util.AssertFunc(len(arr) > 0)
		util.AssertFunc(arr[0]._state._offsetInBlock > 0)
		*partialBlock = &arr[0]
		arr = arr[1:]
		return true
	}
}

func (partial *PartialBlockMgr) AllocateBlock(state *PartialBlockState, sz IdxType) {
	if partial._ckpTyp == CkpTypeFullCkp {
		state._blockId = partial._blockMgr.GetFreeBlockId()
	} else {
		state._blockId = -1
	}
	state._blockSize = uint32(BLOCK_SIZE)
	state._offsetInBlock = 0
	state._blockUseCount = 1
}

func (partial *PartialBlockMgr) RegisterPartialBlock(alloc *PartialBlockAllocation) {

}

func NewPartialBlockManager(
	blockMgr BlockMgr,
	ckpTyp int,
	maxPartialBlockSize uint32,
	maxUseCount uint32,
) *PartialBlockMgr {
	ret := &PartialBlockMgr{
		_blockMgr:              blockMgr,
		_ckpTyp:                ckpTyp,
		_partiallyFilledBlocks: make(map[uint64][]PartialBlock),
		_writtenBlocks:         make(map[BlockID]bool),
		_maxPartialBlockSize:   maxPartialBlockSize,
		_maxUseCount:           maxUseCount,
	}

	return ret
}
