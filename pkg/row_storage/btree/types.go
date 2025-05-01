package btree

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

const (
	BLOCK_SIZE           = 8192
	InvalidUsageCount    = 0xFF
	UCM_USAGE_LEVELS     = 7
	UCM_FREE_PAGES_LEVEL = 7
	UCM_LEVELS           = 8
)

type Blkno uint32
type UndoLocation uint64

const (
	InvalidUndoLocation UndoLocation = 0x2 << 60
)

type Oid uint32

type IndexType uint32

const (
	IndexInvalid IndexType = 0
	IndexToast             = 1
	IndexBridge            = 2
	IndexPrimary           = 3
	IndexUnique            = 4
	IndexRegular           = 5
)

type BTRootInfo struct {
	rootPageBlkno       Blkno
	rootPageChangeCount uint32
	metaPageBlkno       Blkno
}

type BTDesc struct {
	rootInfo BTRootInfo
	arg      any
	oids     RelOids
	idxType  IndexType
}

type PageHeader struct {
	stateAtomic      uint32 //atomic
	usageCountAtomic uint32 //atomic
	pageChangeCount  uint32
}

type RelOids struct {
	datoid  Oid
	reloid  Oid
	relnode Oid
}

var (
	gSharedBuffsers unsafe.Pointer
	gPageDescs      unsafe.Pointer
)

func InitPages(cnt int) {
	gSharedBuffsers = util.CMalloc(BLOCK_SIZE * cnt)
	gPageDescs = util.CMalloc(pageDescSize * cnt)
}

func ClosePages() {
	util.CFree(gSharedBuffsers)
	util.CFree(gPageDescs)
}

func GetInMemPage(blkno Blkno) unsafe.Pointer {
	return util.PointerAdd(
		gSharedBuffsers,
		int(blkno)*BLOCK_SIZE)
}

func GetInMemPageDesc(blkno Blkno) *PageDesc {
	return (*PageDesc)(
		util.PointerAdd(gPageDescs,
			int(blkno)*pageDescSize))
}

func GetPageHeader(pagePtr unsafe.Pointer) *PageHeader {
	return (*PageHeader)(pagePtr)
}
