package btree

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

type Blkno uint32
type UndoLocation uint64

type BTRootInfo struct {
	rootPageBlkno       Blkno
	rootPageChangeCount uint32
	metaPageBlkno       Blkno
}

type BTDesc struct {
	rootInfo BTRootInfo
	arg      any
}

type PageHeader struct {
	stateAtomic      uint32 //atomic
	usageCountAtomic uint32 //atomic
	pageChangeCount  uint32
}

const (
	BLOCK_SIZE = 8192
)

var (
	sharedBuffsers unsafe.Pointer
	pageDescs      unsafe.Pointer
)

func GetInMemPage(blkno Blkno) unsafe.Pointer {
	return util.PointerAdd(sharedBuffsers, int(blkno)*BLOCK_SIZE)
}

func GetInMemPageDesc(blkno Blkno) *PageDesc {
	return (*PageDesc)(util.PointerAdd(pageDescs,
		int(blkno)*pageDescSize))
}
