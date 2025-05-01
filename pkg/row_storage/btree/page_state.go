package btree

import "sync/atomic"

func lockPage(blkno Blkno) {

}

func pageBlockReads(blkno Blkno) {
	//TODO: implement pageBlockReads
}

func pageChangeUsageCount(blkno Blkno, usageCount uint32) {
	pagePtr := GetInMemPage(blkno)
	header := GetPageHeader(pagePtr)
	atomic.SwapUint32(&header.usageCountAtomic, usageCount)
}
