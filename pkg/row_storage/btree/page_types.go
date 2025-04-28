package btree

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

const (
	BTREE_FLAG_LEFTMOST     uint16 = 0x1
	BTREE_FLAG_RIGHTMOST    uint16 = 0x2
	BTREE_FLAG_LEAF         uint16 = 0x4
	BTREE_FLAG_BROKEN_SPLIT uint16 = 0x8
	BTREE_FLAG_PRE_CLEANUP  uint16 = 0x10
	BTREE_FLAGS_ROOT_INIT   uint16 = (BTREE_FLAG_LEFTMOST | BTREE_FLAG_RIGHTMOST | BTREE_FLAG_LEAF)
)

const (
	InvalidRightLink uint64 = 0xFFFFFFFFFFFFFFFF
)

func RightLinkIsValid(rightLink uint64) bool {
	return rightLink != InvalidRightLink
}

var (
	pageDescSize = int(unsafe.Sizeof(PageDesc{}))
)

type PageDesc struct {
	oids       RelOids
	ionum      int
	flags_type uint32 //flags:4 , type:28
	waitC      chan struct{}
}

func (desc *PageDesc) GetFlags() uint32 {
	return uint32(desc.flags_type & 0xF)
}

func (desc *PageDesc) SetFlags(val uint32) {
	desc.flags_type = (desc.flags_type & ^uint32(0xF)) | (val & 0xF)
}

func (desc *PageDesc) GetType() uint32 {
	return uint32(desc.flags_type >> 4)
}

func (desc *PageDesc) SetType(val uint32) {
	desc.flags_type = (desc.flags_type & (^(uint32(0xFFFFFFF) << 4))) | ((val & 0xFFFFFFF) << 4)
}

const (
	BT_PAGE_CHUNK_DESC_SIZE = int(unsafe.Sizeof(BTPageChunkDesc{}))
)

type BTPageChunkDesc struct {
	fields uint32 //shortLocation:12,offset:10,hikeyShortLocation:8,hikeyFlags:2
}

func (desc *BTPageChunkDesc) GetShortLocation() uint32 {
	return desc.fields & 0xFFF
}

func (desc *BTPageChunkDesc) SetShortLocation(val uint32) {
	desc.fields = (desc.fields & ^uint32(0xFFF)) | (val & 0xFFF)
}

func (desc *BTPageChunkDesc) GetOffset() uint32 {
	return (desc.fields >> 12) & 0x3FF
}

func (desc *BTPageChunkDesc) SetOffset(val uint32) {
	desc.fields = (desc.fields & ^(uint32(0x3FF) << 12)) | ((val & 0x3FF) << 12)
}

func (desc *BTPageChunkDesc) GetHikeyShortLocation() uint32 {
	return (desc.fields >> 22) & 0xFF
}

func (desc *BTPageChunkDesc) SetHikeyShortLocation(val uint32) {
	desc.fields = (desc.fields & ^(uint32(0xFF) << 22)) | ((val & 0xFF) << 22)
}

func (desc *BTPageChunkDesc) GetHikeyFlags() uint32 {
	return (desc.fields >> 30) & 0x3
}

func (desc *BTPageChunkDesc) SetHikeyFlags(val uint32) {
	desc.fields = (desc.fields & ^(uint32(0x3) << 30)) | ((val & 0x3) << 30)
}

const (
	BT_PAGE_HEADER_SIZE              = uint32(unsafe.Sizeof(BTPageHeader{}))
	BT_PAGE_HEADER_CHUNK_DESC_OFFSET = uint32(unsafe.Offsetof(BTPageHeader{}.chunksDesc))
)

type BTPageHeader struct {
	header           PageHeader
	checkpointNum    uint32
	undoLocation     UndoLocation
	csn              CommitSeqNo
	rightLink        uint64
	flagsF1F2        uint32 //flags:6, f1:11, f2:15
	maxKeyLen        LocationIndex
	prevInsertOffset OffsetNumber
	chunksCount      OffsetNumber
	itemsCount       OffsetNumber
	hikeysEnd        OffsetNumber
	dataSize         LocationIndex
	chunksDesc       [1]BTPageChunkDesc
}

func (header *BTPageHeader) GetFlags() uint16 {
	return uint16(header.flagsF1F2 & 0x3F)
}
func (header *BTPageHeader) SetFlags(val uint16) {
	header.flagsF1F2 = (header.flagsF1F2 & ^uint32(0x3F)) | uint32(val&0x3F)
}

func (header *BTPageHeader) GetField1() uint16 {
	return uint16(header.flagsF1F2 >> 6 & 0x7FF)
}
func (header *BTPageHeader) SetField1(val uint16) {
	header.flagsF1F2 = (header.flagsF1F2 & ^(uint32(0x7FF) << 6)) | (uint32(val&0x7FF) << 6)
}
func (header *BTPageHeader) GetField2() uint16 {
	return uint16(header.flagsF1F2 >> 17 & 0x7FFF)
}

func (header *BTPageHeader) SetField2(val uint16) {
	header.flagsF1F2 = (header.flagsF1F2 & ^(uint32(0x7FFF) << 17)) | (uint32(val&0x7FFF) << 17)
}

func (h *BTPageHeader) GetChunkDesc(n int) *BTPageChunkDesc {
	return (*BTPageChunkDesc)(util.PointerAdd(
		unsafe.Pointer(&h.chunksDesc[0]), //first chunksDesc ptr
		n*BT_PAGE_CHUNK_DESC_SIZE,        //ith chunkDesc offset
	))
}

type BTPageChunk struct {
	//offset relative to the chunk position
	items [1]LocationIndex
}

func (chunk *BTPageChunk) GetItem(n int) LocationIndex {
	util.AssertFunc(n >= 0)
	return *(*LocationIndex)(util.PointerAdd(
		unsafe.Pointer(&chunk.items[0]),
		n*int(LocationIndexSize)),
	)
}

func (chunk *BTPageChunk) SetItem(n int, loc LocationIndex) {
	util.AssertFunc(n >= 0)
	*(*LocationIndex)(util.PointerAdd(
		unsafe.Pointer(&chunk.items[0]),
		n*int(LocationIndexSize),
	)) = loc
}

type BTPageItemLocator struct {
	chunkOffset     OffsetNumber
	itemOffset      OffsetNumber
	chunkItemsCount OffsetNumber
	chunkSize       LocationIndex
	chunk           *BTPageChunk
}

type BTNonLeafTuphdr struct {
	downlink uint64
}

type TupleXactInfo uint64

type BTLeafTuphdr struct {
	fields TupleXactInfo //xactInfo:61,deleted:2,chainHasLocks:1
	locs   UndoLocation  //undoLocation:62,formatFlags:2
}

func ItemGetOffset(item LocationIndex) uint16 {
	return uint16(item & 0x3FFF)
}

func ItemGetFlags(item LocationIndex) uint16 {
	return uint16(item >> 14)
}

func ItemSetFlags(item LocationIndex, flags uint16) LocationIndex {
	if flags != 0 {
		return item | (LocationIndex(1) << 14)
	} else {
		return item & ^(LocationIndex(1) << 14)
	}
}

type BTItemPageFitType uint8

const (
	BTItemPageFitAsIs BTItemPageFitType = iota
	BTItemPageFitCompactRequired
	BTItemPageFitSplitRequired
)

type BTPageItem struct {
	data    unsafe.Pointer
	size    LocationIndex
	flags   uint8
	newItem bool
}

func BTPageHikeysEnd(desc *BTDesc, page unsafe.Pointer) LocationIndex {
	if PageIs(page, BTREE_FLAG_LEAF) {
		return 256
	}
	return 512
}

func BTPageFreeSpace(page unsafe.Pointer) LocationIndex {
	header := (*BTPageHeader)(page)
	return BLOCK_SIZE - header.dataSize
}

func LocationGetShort(loc uint32) uint32 {
	util.AssertFunc((loc & 3) == 0)
	return loc >> 2
}

func ShortGetLocation(loc uint32) uint32 {
	return loc << 2
}

const (
	BT_PAGE_MAX_CHUNK_ITEMS = uint32((BLOCK_SIZE - BT_PAGE_HEADER_SIZE) /
		(8 + LocationIndexSize))
	BT_PAGE_MAX_ITEMS = uint32((BLOCK_SIZE - BT_PAGE_HEADER_SIZE) /
		(8 + LocationIndexSize))
	BT_PAGE_MAX_CHUNKS = uint32((uint32(512) - BT_PAGE_HEADER_CHUNK_DESC_OFFSET) /
		uint32(8+BT_PAGE_CHUNK_DESC_SIZE))
)
