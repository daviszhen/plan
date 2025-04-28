package btree

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

const (
	MAX_DEPTH            = 32
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
	ops      BTreeOps
}

func BTLen(desc *BTDesc, tuple Tuple, lenType LengthType) int {
	return desc.ops.Len(desc, tuple, lenType)
}

func BTTupleMakeKey(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple {
	return desc.ops.TupleMakeKey(desc, tuple, data, keepVersion, allocated)
}

type LengthType int

const (
	TupleLength             LengthType = 0
	KeyLength                          = 1
	TupleKeyLength                     = 2
	TupleKeyLengthNoVersion            = 3
)

type BTreeOps interface {
	Len(desc *BTDesc, tuple Tuple, lenType LengthType) int
	TupleMakeKey(desc *BTDesc, tuple Tuple, data unsafe.Pointer, keepVersion bool, allocated *bool) Tuple
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
	gPageCount      int
	gPageDescs      []*PageDesc
)

func InitPages(cnt int) {
	gPageCount = cnt
	gSharedBuffsers = util.CMalloc(BLOCK_SIZE * cnt)
	util.CMemset(gSharedBuffsers, 0, BLOCK_SIZE*cnt)
	gPageDescs = make([]*PageDesc, cnt)
	for i := 0; i < cnt; i++ {
		gPageDescs[i] = &PageDesc{
			waitC: make(chan struct{}, 1),
		}
	}
}

func ClosePages() {
	util.CFree(gSharedBuffsers)
	for _, desc := range gPageDescs {
		close(desc.waitC)
	}
}

func GetInMemPage(blkno Blkno) unsafe.Pointer {
	if blkno >= Blkno(gPageCount) {
		panic("blkno out of range")
	}
	return util.PointerAdd(
		gSharedBuffsers,
		int(blkno)*BLOCK_SIZE)
}

func GetInMemPageDesc(blkno Blkno) *PageDesc {
	if blkno >= Blkno(gPageCount) {
		panic("blkno out of range")
	}
	return gPageDescs[blkno]
}

func GetPageHeader(pagePtr unsafe.Pointer) *PageHeader {
	return (*PageHeader)(pagePtr)
}
