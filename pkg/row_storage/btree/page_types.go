package btree

import "unsafe"

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

var (
	pageDescSize = int(unsafe.Sizeof(PageDesc{}))
)

type PageDesc struct {
	oids       RelOids
	ionum      int
	flags_type uint32 //flags:4 , type:28
}

func (desc *PageDesc) GetFlags() uint32 {
	return uint32(desc.flags_type & 0xF)
}

func (desc *PageDesc) SetFlags(val uint32) {
	desc.flags_type |= (val & 0xF)
}

func (desc *PageDesc) GetType() uint32 {
	return uint32(desc.flags_type >> 4)
}

func (desc *PageDesc) SetType(val uint32) {
	desc.flags_type |= (val & 0xFFFFFFF) << 4
}

type BTPageChunkDesc struct {
	fields uint32 //shortLocation:12,offset:10,hikeyShortLocation:8,hikeyFlags:2
}

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
	header.flagsF1F2 |= uint32(val & 0x3F)
}

func (header *BTPageHeader) GetField1() uint16 {
	return uint16(header.flagsF1F2 >> 6 & 0x7FF)
}
func (header *BTPageHeader) SetField1(val uint16) {
	header.flagsF1F2 |= uint32(val&0x7FF) << 6
}
func (header *BTPageHeader) GetField2() uint16 {
	return uint16(header.flagsF1F2 >> 17 & 0x7FFF)
}

func (header *BTPageHeader) SetField2(val uint16) {
	header.flagsF1F2 |= uint32(val&0x7FFF) << 17
}
