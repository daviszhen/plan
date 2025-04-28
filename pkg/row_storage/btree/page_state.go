package btree

import (
	"sync/atomic"
	"time"

	"github.com/daviszhen/plan/pkg/util"
	"github.com/petermattis/goid"
	"golang.org/x/sync/syncmap"
)

const (
	MaxPagesPerProcess = 8
)

var (
	gThreadPages syncmap.Map
)

func GetThreadPages() *ThreadPages {
	gid := goid.Get()
	val, _ := gThreadPages.LoadOrStore(gid, &ThreadPages{})
	return val.(*ThreadPages)
}

type MyLockedPage struct {
	blkno Blkno
	state uint32
}

type ThreadPages struct {
	myLockedPages                  [MaxPagesPerProcess]MyLockedPage
	myInProgressSplitPages         [MAX_DEPTH * 2]Blkno
	numberOfMyLockedPages          int
	numberOfMyInProgressSplitPages int
}

func getMyLockedPageIndex(tPages *ThreadPages, blkno Blkno) int {
	for i := 0; i < tPages.numberOfMyLockedPages; i++ {
		if tPages.myLockedPages[i].blkno == blkno {
			return i
		}
	}
	return -1
}

func myLockedPageAdd(blkno Blkno, state uint32) {
	tPages := GetThreadPages()
	util.AssertFunc(getMyLockedPageIndex(tPages, blkno) < 0)
	util.AssertFunc(tPages.numberOfMyLockedPages < MaxPagesPerProcess)
	tPages.myLockedPages[tPages.numberOfMyLockedPages].blkno = blkno
	tPages.myLockedPages[tPages.numberOfMyLockedPages].state = state
	tPages.numberOfMyLockedPages++
}

func myLockedPageDel(blkno Blkno) uint32 {
	tPages := GetThreadPages()
	index := getMyLockedPageIndex(tPages, blkno)
	util.AssertFunc(index >= 0)
	state := tPages.myLockedPages[index].state
	tPages.myLockedPages[index] = tPages.myLockedPages[tPages.numberOfMyLockedPages-1]
	tPages.numberOfMyLockedPages--
	return state
}

func myLockedPageGetState(blkno Blkno) uint32 {
	tPages := GetThreadPages()
	index := getMyLockedPageIndex(tPages, blkno)
	util.AssertFunc(index >= 0)
	return tPages.myLockedPages[index].state
}

func haveLockedPages() bool {
	tPages := GetThreadPages()
	return tPages.numberOfMyLockedPages > 0
}

const (
	//low 4 bits
	PageStateLockedFlag = 4
	PageStateNoReadFlag = 8
	//high 28 bits. change count
	PageStateChangeCountOne  = 16
	PageStateChangeCountMask = 0xfffffff0
)

func PageStateIsLocked(state uint32) bool {
	return util.FlagIsSet(state, PageStateLockedFlag)
}

func PageStateLock(state uint32) uint32 {
	return state | PageStateLockedFlag
}

func PageStateReadIsBlocked(state uint32) bool {
	return util.FlagIsSet(state, PageStateNoReadFlag)
}

func PageStateBlockRead(state uint32) uint32 {
	return state | PageStateLockedFlag | PageStateNoReadFlag
}

func pageIncUsageCount(blkno Blkno, usageCount uint32) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	atomic.CompareAndSwapUint32(&header.usageCountAtomic, usageCount, usageCount+1)
}

func lockPage(blkno Blkno) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	desc := GetInMemPageDesc(blkno)
	var newState, state uint32
	util.AssertFunc(getMyLockedPageIndex(GetThreadPages(), blkno) < 0)
	pageIncUsageCount(blkno, atomic.LoadUint32(&header.usageCountAtomic))
	for {
		state = atomic.LoadUint32(&header.stateAtomic)
		if !PageStateIsLocked(state) {
			newState = PageStateLock(state)
		} else {
			select {
			case <-desc.waitC:
				continue
			case <-time.After(time.Millisecond * 100):
				continue
			}
		}
		if atomic.CompareAndSwapUint32(&header.stateAtomic, state, newState) {
			break
		}
	}
	myLockedPageAdd(blkno, state|PageStateLockedFlag)
}

func pageWaitForReadEnable(blkno Blkno) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	desc := GetInMemPageDesc(blkno)
	for {
		state := atomic.LoadUint32(&header.stateAtomic)
		if !PageStateReadIsBlocked(state) {
			break
		} else {
			select {
			case <-desc.waitC:
				continue
			case <-time.After(time.Millisecond * 100):
				continue
			}
		}
	}
}

func pageWaitForChangeCount(blkno Blkno, state uint32) uint32 {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	desc := GetInMemPageDesc(blkno)
	for {
		<-desc.waitC
		curState := atomic.LoadUint32(&header.stateAtomic)
		//if the changeCount changed, it returns
		if (curState & PageStateChangeCountMask) !=
			(state & PageStateChangeCountMask) {
			return curState
		}
	}
}

// wait for a change
// unlock first
// if the page changed, it locks the page again
func relockPage(blkno Blkno) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	state := myLockedPageGetState(blkno)
	unlockPage(blkno)
	pageIncUsageCount(blkno, atomic.LoadUint32(&header.usageCountAtomic))
	pageWaitForChangeCount(blkno, state)
	lockPage(blkno)
}

// try to lock the page.
// return: true - locked
func tryLockPage(blkno Blkno) bool {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	state := atomic.OrUint32(&header.stateAtomic, PageStateLockedFlag)
	if PageStateIsLocked(state) {
		return false
	}
	myLockedPageAdd(blkno, state|PageStateLockedFlag)
	return true
}

// declare the page as locked
func declarePageAsLocked(blkno Blkno) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	myLockedPageAdd(blkno, atomic.LoadUint32(&header.stateAtomic))
}

func pageIsLocked(blkno Blkno) bool {
	return getMyLockedPageIndex(GetThreadPages(), blkno) >= 0
}

// block reading on the page
func pageBlockReads(blkno Blkno) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	tPages := GetThreadPages()
	idx := getMyLockedPageIndex(tPages, blkno)
	state := atomic.OrUint32(&header.stateAtomic, PageStateNoReadFlag)
	util.AssertFunc(util.FlagIsSet(state, PageStateLockedFlag))
	tPages.myLockedPages[idx].state |= PageStateNoReadFlag
}

func unlockPage(blkno Blkno) {
	ptr := GetInMemPage(blkno)
	header := GetPageHeader(ptr)
	desc := GetInMemPageDesc(blkno)
	var state uint32
	state = myLockedPageDel(blkno)
	if util.FlagIsSet(state, PageStateNoReadFlag) {
		state = atomic.AddUint32(
			&header.stateAtomic,
			PageStateChangeCountOne-(state&(PageStateLockedFlag|PageStateNoReadFlag)),
		)
	} else {
		state = atomic.AndUint32(
			&header.stateAtomic,
			^uint32(PageStateLockedFlag),
		)
		state &= ^uint32(PageStateLockedFlag)
	}
	desc.waitC <- struct{}{}
}

func releaseAllPageLocks() {
	tPages := GetThreadPages()
	for tPages.numberOfMyLockedPages > 0 {
		unlockPage(tPages.myLockedPages[0].blkno)
	}
}

func btreeRegisterInProgressSplit(leftBlkno Blkno) {
	tPages := GetThreadPages()
	util.AssertFunc(tPages.numberOfMyInProgressSplitPages+1 <=
		len(tPages.myInProgressSplitPages))
	tPages.myInProgressSplitPages[tPages.numberOfMyInProgressSplitPages] = leftBlkno
	tPages.numberOfMyInProgressSplitPages++
}

func btreeUnregisterInProgressSplit(leftBlkno Blkno) {
	tPages := GetThreadPages()
	util.AssertFunc(tPages.numberOfMyInProgressSplitPages > 0)
	for i := 0; i < tPages.numberOfMyInProgressSplitPages; i++ {
		if tPages.myInProgressSplitPages[i] == leftBlkno {
			tPages.myInProgressSplitPages[i] =
				tPages.myInProgressSplitPages[tPages.numberOfMyInProgressSplitPages-1]
			tPages.numberOfMyInProgressSplitPages--
			return
		}
	}
	util.AssertFunc(false)
}

func btreeMarkIncompleteSplits() {
	tPages := GetThreadPages()
	for i := 0; i < tPages.numberOfMyInProgressSplitPages; i++ {
		btreeSplitMarkFinished(tPages.myInProgressSplitPages[i], true, false)
	}
	tPages.numberOfMyInProgressSplitPages = 0
}

func btreeSplitMarkFinished(leftBlkno Blkno, useLock bool, success bool) {

	if useLock {
		lockPage(leftBlkno)
		pageBlockReads(leftBlkno)
	}

	ptr := (GetInMemPage(leftBlkno))
	header := (*BTPageHeader)(ptr)

	util.AssertFunc(RightLinkIsValid(header.rightLink))
	util.AssertFunc(!useLock || !PageIs(ptr, BTREE_FLAG_BROKEN_SPLIT))

	if success {
		header.SetFlags(header.GetFlags() & ^BTREE_FLAG_BROKEN_SPLIT)
		header.rightLink = InvalidRightLink
	} else {
		header.SetFlags(header.GetFlags() | BTREE_FLAG_BROKEN_SPLIT)
	}

	if useLock {
		unlockPage(leftBlkno)
	}
}

func pageChangeUsageCount(blkno Blkno, usageCount uint32) {
	pagePtr := GetInMemPage(blkno)
	header := GetPageHeader(pagePtr)
	atomic.SwapUint32(&header.usageCountAtomic, usageCount)
}
