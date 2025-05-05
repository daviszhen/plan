package btree

import (
	"unsafe"
)

func init() {
}

type CommitSeqNo uint64
type LocationIndex uint16

const (
	COMMITSEQNO_FROZEN CommitSeqNo = 0x3
)

const (
	LocationIndexSize = uint32(unsafe.Sizeof(LocationIndex(0)))
	ItemIdDataSize    = uint32(unsafe.Sizeof(ItemData{}))
)

type ItemData struct {
	linePointer uint32 //off:15,flags:2,len:15
}

type OffsetNumber uint16

const (
	InvalidOffsetNumber OffsetNumber = 0
	FirstOffsetNumber   OffsetNumber = 1
	MaxOffsetNumber     OffsetNumber = OffsetNumber(BLOCK_SIZE / ItemIdDataSize)
)
