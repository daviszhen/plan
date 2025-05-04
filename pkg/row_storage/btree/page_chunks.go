package btree

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

func initPageFirstChunk(
	desc *BTDesc,
	p unsafe.Pointer,
	hikeySize LocationIndex) {
	header := (*BTPageHeader)(p)

	util.AssertFunc(hikeySize == util.AlignValue(hikeySize, 8))

	header.chunksCount = 1
	header.itemsCount = 0
	header.hikeysEnd = OffsetNumber(util.AlignValue(BT_PAGE_HEADER_SIZE, 8)) + OffsetNumber(hikeySize)

	if header.hikeysEnd > OffsetNumber(BTPageHikeysEnd(desc, p)) {
		header.dataSize = LocationIndex(header.hikeysEnd)
	} else {
		header.dataSize = BTPageHikeysEnd(desc, p)
	}
	header.chunksDesc[0].SetHikeyShortLocation(LocationGetShort(util.AlignValue(BT_PAGE_HEADER_SIZE, 8)))
	header.chunksDesc[0].SetShortLocation(LocationGetShort(uint32(header.dataSize)))
	header.chunksDesc[0].SetOffset(0)
	header.chunksDesc[0].SetHikeyFlags(0)
}

func pageChunkFillLocator(
	p unsafe.Pointer,
	chunkOffset OffsetNumber,
	locator *BTPageItemLocator,
) {
	header := (*BTPageHeader)(p)
	util.AssertFunc(chunkOffset < header.chunksCount)
	currChunkDesc := header.GetChunkDesc(int(chunkOffset))
	if chunkOffset+1 < header.chunksCount {
		nextChunkDesc := header.GetChunkDesc(int(chunkOffset + 1))
		locator.chunkItemsCount =
			OffsetNumber(
				nextChunkDesc.GetOffset() -
					currChunkDesc.GetOffset())
		locator.chunkSize = LocationIndex(
			ShortGetLocation(nextChunkDesc.GetShortLocation()) -
				ShortGetLocation(currChunkDesc.GetShortLocation()))
	} else {
		locator.chunkItemsCount = header.itemsCount -
			OffsetNumber(currChunkDesc.GetOffset())
		locator.chunkSize = header.dataSize -
			LocationIndex(ShortGetLocation(currChunkDesc.GetShortLocation()))
	}
	locator.chunkOffset = chunkOffset
	locator.itemOffset = 0
	locator.chunk = (*BTPageChunk)(util.PointerAdd(
		p,
		int(ShortGetLocation(currChunkDesc.GetShortLocation()))))
}

func pageItemFillLocator(
	p unsafe.Pointer,
	itemOffset OffsetNumber,
	locator *BTPageItemLocator,
) {
	header := (*BTPageHeader)(p)
	chunkOffset := OffsetNumber(0)
	for chunkOffset < header.chunksCount-1 &&
		uint32(itemOffset) >= header.GetChunkDesc(int(chunkOffset+1)).GetOffset() {
		chunkOffset++
	}
	pageChunkFillLocator(p, chunkOffset, locator)
	locator.itemOffset = itemOffset -
		OffsetNumber(header.GetChunkDesc(int(chunkOffset)).GetOffset())
}

func pageItemFillLocatorBackwards(
	p unsafe.Pointer,
	itemOffset OffsetNumber,
	locator *BTPageItemLocator,
) {
	header := (*BTPageHeader)(p)
	chunkOffset := header.chunksCount - 1
	for uint32(itemOffset) < header.GetChunkDesc(int(chunkOffset)).GetOffset() {
		util.AssertFunc(chunkOffset > 0)
		chunkOffset--
	}
	pageChunkFillLocator(p, chunkOffset, locator)
	locator.itemOffset = itemOffset -
		OffsetNumber(header.GetChunkDesc(int(chunkOffset)).GetOffset())
}

func pageLocatorNextChunk(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
) bool {
	header := (*BTPageHeader)(p)
	for locator.itemOffset >= locator.chunkItemsCount {
		if locator.chunkOffset+1 < header.chunksCount {
			pageChunkFillLocator(p, locator.chunkOffset+1, locator)
		} else {
			return false
		}
	}
	return true
}

func pageLocatorPrevChunk(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
) bool {
	for {
		if locator.chunkOffset > 0 {
			pageChunkFillLocator(p, locator.chunkOffset-1, locator)
			if locator.chunkItemsCount == 0 {
				continue
			} else {
				util.AssertFunc(locator.chunkItemsCount > 0)
				locator.itemOffset = locator.chunkItemsCount - 1
				return true
			}
		} else {
			util.AssertFunc(locator.chunkOffset == 0)
			locator.chunk = nil
			return false
		}
	}
}

func pageLocatorFitsNewItem(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
	itemSize LocationIndex,
) bool {
	nextSize := util.AlignValue(OffsetNumber(LocationIndexSize)*(locator.chunkItemsCount+1), 8)
	curSize := util.AlignValue(OffsetNumber(LocationIndexSize)*locator.chunkItemsCount, 8)
	sizeDiff := LocationIndex(nextSize - curSize)
	sizeDiff += util.AlignValue(itemSize, 8)
	return BTPageFreeSpace(p) >= sizeDiff
}
