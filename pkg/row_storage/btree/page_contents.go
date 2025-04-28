package btree

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

func PageIs(pagePtr unsafe.Pointer, flags uint16) bool {
	header := (*BTPageHeader)(pagePtr)
	return util.FlagIsSet(header.GetFlags(), flags)
}

func PageGetLevel(pagePtr unsafe.Pointer) uint16 {
	header := (*BTPageHeader)(pagePtr)
	if PageIs(pagePtr, BTREE_FLAG_LEAF) {
		return 0
	} else {
		return header.GetField1()
	}
}

func PageSetLevel(pagePtr unsafe.Pointer, level uint16) {
	util.AssertFunc(!PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	header.SetField1(level)
}

func PageGetNOnDisk(pagePtr unsafe.Pointer) uint16 {
	util.AssertFunc(!PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	return header.GetField2()
}

func PageSetNOnDisk(pagePtr unsafe.Pointer, n uint16) {
	util.AssertFunc(!PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	header.SetField2(n)
}

func PageIncNOnDisk(pagePtr unsafe.Pointer) {
	util.AssertFunc(!PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	header.SetField2(header.GetField2() + 1)
}

func PageDecNOnDisk(pagePtr unsafe.Pointer) {
	util.AssertFunc(!PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	header.SetField2(header.GetField2() - 1)
}

func PageGetNVacated(pagePtr unsafe.Pointer) uint16 {
	util.AssertFunc(PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	return header.GetField2()
}

func PageSetNVacated(pagePtr unsafe.Pointer, n uint16) {
	util.AssertFunc(PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	header.SetField2(n)
}

func PageAddNVacated(pagePtr unsafe.Pointer, s uint16) {
	util.AssertFunc(PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	header.SetField2(header.GetField2() + s)
}

func PageSubNVacated(pagePtr unsafe.Pointer, s uint16) {
	util.AssertFunc(PageIs(pagePtr, BTREE_FLAG_LEAF))
	header := (*BTPageHeader)(pagePtr)
	util.AssertFunc(header.GetField2() >= s)
	header.SetField2(header.GetField2() - s)
}

func initNewBtreePage(
	desc *BTDesc,
	blkno Blkno,
	flags uint16,
	level uint16,
	noLock bool,
) {
	pagePtr := GetInMemPage(blkno)
	pageDesc := GetInMemPageDesc(blkno)
	util.CMemset(pagePtr, 0, BLOCK_SIZE)
	header := (*BTPageHeader)(pagePtr)
	if !noLock {
		lockPage(blkno)
		pageBlockReads(blkno)
	}
	pageDesc.oids = desc.oids
	pageDesc.SetType(uint32(desc.idxType))
	header.SetFlags(flags)
	if util.FlagIsSet(flags, BTREE_FLAG_LEAF) {
		header.SetField1(0)
		PageSetNVacated(pagePtr, 0)
	} else {
		PageSetLevel(pagePtr, level)
		PageSetNOnDisk(pagePtr, 0)
	}
	header.rightLink = InvalidRightLink
	header.csn = COMMITSEQNO_FROZEN
	header.undoLocation = InvalidUndoLocation
	header.checkpointNum = 0
	header.itemsCount = 0
	header.prevInsertOffset = MaxOffsetNumber
	header.maxKeyLen = 0
	pageChangeUsageCount(blkno, 1%UCM_USAGE_LEVELS)
}
