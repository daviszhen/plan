package btree

import (
	"fmt"
	"unsafe"
)

func init() {
	itemSz := unsafe.Sizeof(ItemData{})
	if itemSz != ItemIdDataSize {
		panic(fmt.Sprintf("ItemData size is not 32 bytes %d %d", itemSz, ItemIdDataSize))
	}
}

type CommitSeqNo uint64
type LocationIndex uint64

const (
	COMMITSEQNO_FROZEN CommitSeqNo = 0x3
)

const (
	ItemIdDataSize = 4
)

type ItemData struct {
	linePointer uint32 //off:15,flags:2,len:15
}

type OffsetNumber uint16

const (
	InvalidOffsetNumber OffsetNumber = 0
	FirstOffsetNumber   OffsetNumber = 1
	MaxOffsetNumber     OffsetNumber = (BLOCK_SIZE / ItemIdDataSize)
)
