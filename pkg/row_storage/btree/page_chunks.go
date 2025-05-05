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

func pageLocatorGetItemSize(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
) LocationIndex {
	var itemOffset, nextItemOffset LocationIndex
	header := (*BTPageHeader)(p)
	//offset relative to the chunk beginning
	itemOffset = LocationIndex(ItemGetOffset(
		locator.chunk.GetItem(int(locator.itemOffset))))

	if locator.itemOffset+1 < locator.chunkItemsCount {
		//next item in same chunk
		nextItemOffset = LocationIndex(ItemGetOffset(
			locator.chunk.GetItem(int(locator.itemOffset + 1))))
	} else {
		//next item in next chunk
		diff := util.PointerSub(
			unsafe.Pointer(locator.chunk),
			p)
		//plus the offset relative to the page beginning
		itemOffset += LocationIndex(diff)
		if locator.chunkOffset+1 < header.chunksCount {
			nextItemOffset = LocationIndex(ShortGetLocation(
				header.GetChunkDesc(int(locator.chunkOffset + 1)).GetShortLocation()))
		} else {
			nextItemOffset = header.dataSize
		}
	}
	return nextItemOffset - itemOffset
}

func pageLocatorResizeItem(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
	newSize LocationIndex,
) {
	var nextItemPtr, endPtr unsafe.Pointer
	header := (*BTPageHeader)(p)
	//figure out the data length that needs to be moved
	util.AssertFunc(newSize == util.AlignValue(newSize, 8))
	curSz := pageLocatorGetItemSize(p, locator)
	util.AssertFunc(newSize >= curSz)
	dataShift := newSize - curSz
	util.AssertFunc(dataShift == util.AlignValue(dataShift, 8))

	//if no shift is needed, just return
	if dataShift == 0 {
		return
	}

	util.AssertFunc(LocationIndex(locator.itemOffset) < locator.chunkSize)
	//deside the start position of the next item
	if locator.itemOffset+1 < locator.chunkItemsCount {
		nextItemPtr = util.PointerAdd(
			unsafe.Pointer(locator.chunk),
			int(ItemGetOffset(locator.chunk.GetItem(int(locator.itemOffset+1)))))
	} else {
		//if the next item is in the next chunk, use the chunk size as the start position
		nextItemPtr = util.PointerAdd(
			unsafe.Pointer(locator.chunk),
			int(locator.chunkSize))
	}
	//decide the end position of the data currently
	endPtr = util.PointerAdd(
		p,
		int(header.dataSize))

	//ensure the data will not overflow the page
	util.AssertFunc(
		util.PointerLessEqual(
			util.PointerAdd(endPtr, int(dataShift)),
			util.PointerAdd(p, BLOCK_SIZE),
		))

	//move the data after the item to allocate the space
	//holding the item with new size
	//dest position = new position of the next item
	dst := util.PointerAdd(nextItemPtr, int(dataShift))
	//length of the data to be moved = end of the page - next item
	dlen := util.PointerSub(endPtr, nextItemPtr)
	util.CMemmove(dst, nextItemPtr, int(dlen))

	//chunk position moves dataShift
	for i := locator.chunkOffset + 1; i < header.chunksCount; i++ {
		header.GetChunkDesc(int(i)).SetShortLocation(
			header.GetChunkDesc(int(i)).GetShortLocation() +
				LocationGetShort(uint32(dataShift)))
	}
	header.dataSize += dataShift

	//item position moves dataShift in current chunk
	//do not change position of item in next chunk.
	//the position of next chunk has been changed
	for i := locator.itemOffset + 1; i < locator.chunkItemsCount; i++ {
		locator.chunk.SetItem(
			int(i),
			locator.chunk.GetItem(int(i))+dataShift)
	}

	locator.chunkSize += dataShift
}

func pageLocatorInsertItem(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
	itemSize LocationIndex,
) {
	header := (*BTPageHeader)(p)
	var itemPtr unsafe.Pointer

	util.AssertFunc(itemSize == util.AlignValue(itemSize, 8))

	curItemsLen := util.AlignValue(
		OffsetNumber(LocationIndexSize)*locator.chunkItemsCount,
		8)
	nextItemsLen := util.AlignValue(
		OffsetNumber(LocationIndexSize)*(locator.chunkItemsCount+1),
		8)
	itemsShift := LocationIndex(nextItemsLen - curItemsLen)

	dataShift := itemsShift + itemSize

	/*
		layout: item0_offset,item1_offset,...,item0,item1,...

		firstItemPtr = item0 position
				     = the end of the items array also
	*/
	firstItemPtr := util.PointerAdd(
		unsafe.Pointer(locator.chunk),
		int(curItemsLen))

	//current item position item_i_offset
	if locator.itemOffset < locator.chunkItemsCount {
		itemPtr = util.PointerAdd(
			unsafe.Pointer(locator.chunk),
			int(ItemGetOffset(locator.chunk.GetItem(int(locator.itemOffset)))))
	} else {
		util.AssertFunc(locator.itemOffset == locator.chunkItemsCount)
		itemPtr = util.PointerAdd(
			unsafe.Pointer(locator.chunk),
			int(locator.chunkSize))
	}
	endPtr := util.PointerAdd(
		p,
		int(header.dataSize))

	util.AssertFunc(
		util.PointerLessEqual(
			util.PointerAdd(endPtr, int(dataShift)),
			util.PointerAdd(p, BLOCK_SIZE),
		),
	)

	util.AssertFunc(
		util.PointerLessEqual(itemPtr, endPtr))

	//move the data to get a space for new item
	dstPtr := util.PointerAdd(itemPtr, int(dataShift))
	dataLen := util.PointerSub(endPtr, itemPtr)
	util.CMemmove(
		dstPtr,
		itemPtr,
		int(dataLen))

	//chunks after the current chunk moves
	for i := locator.chunkOffset + 1; i < header.chunksCount; i++ {
		desc := header.GetChunkDesc(int(i))
		desc.SetShortLocation(
			desc.GetShortLocation() +
				LocationGetShort(uint32(dataShift)))
		desc.SetOffset(desc.GetOffset() + 1)
	}
	header.itemsCount++
	header.dataSize += dataShift

	if itemsShift != 0 {
		//extend items array
		dstPtr = util.PointerAdd(firstItemPtr, int(itemsShift))
		dataLen = util.PointerSub(itemPtr, firstItemPtr)
		//move item0, item1,...,
		//move items to get a space for new item offset
		util.CMemmove(
			dstPtr,
			firstItemPtr,
			int(dataLen))
		//update item0_offset, item1_offset,...,
		for i := 0; i < int(locator.itemOffset); i++ {
			locator.chunk.SetItem(i,
				locator.chunk.GetItem(i)+itemsShift)
		}
	}

	//update item_i_offset,item_(i+1)_offset,...
	for i := locator.chunkItemsCount; i > locator.itemOffset; i-- {
		locator.chunk.SetItem(int(i),
			locator.chunk.GetItem(int(i-1))+dataShift)
	}
	//update new_item_offset = item_i_offset + itemsShift
	itemOff := util.PointerSub(
		itemPtr,
		unsafe.Pointer(locator.chunk))
	locator.chunk.SetItem(
		int(locator.itemOffset),
		LocationIndex(itemOff)+itemsShift)

	//adjust the locator
	locator.chunkItemsCount++
	locator.chunkSize += dataShift
}

func pageMergeChunks(
	p unsafe.Pointer,
	index OffsetNumber,
) {
	var (
		tmpItems [BT_PAGE_MAX_CHUNK_ITEMS]LocationIndex
		hikeyShift,
		hikeyShift2,
		shift1,
		shift2 LocationIndex
		// i,
		count1,
		count2 OffsetNumber
		loc1, loc2 BTPageItemLocator
		chunk1DataPtr,
		chunk1EndPtr,
		chunk2DataPtr,
		endPtr,
		p1_1,
		p1_2,
		p2_1,
		p2_2 unsafe.Pointer
		len1,
		len2 int
	)
	header := (*BTPageHeader)(p)
	util.AssertFunc(index+1 < header.chunksCount)

	pageChunkFillLocator(p, index, &loc1)
	pageChunkFillLocator(p, index+1, &loc2)

	count1 = loc1.chunkItemsCount
	count2 = loc2.chunkItemsCount

	chunk1ItemsArrSizeAlign := util.AlignValue(count1*
		OffsetNumber(LocationIndexSize), 8)
	chunk2ItemsArrSizeAlign := util.AlignValue(count2*
		OffsetNumber(LocationIndexSize), 8)
	mergedItemsArrSizeAlign := util.AlignValue((count1+count2)*
		OffsetNumber(LocationIndexSize), 8)

	chunk1DataPtr = util.PointerAdd(
		unsafe.Pointer(loc1.chunk),
		int(chunk1ItemsArrSizeAlign),
	)
	chunk1EndPtr = util.PointerAdd(
		unsafe.Pointer(loc1.chunk),
		int(loc1.chunkSize),
	)
	chunk1Len := util.PointerSub(chunk1EndPtr, chunk1DataPtr)

	chunk2DataPtr = util.PointerAdd(
		unsafe.Pointer(loc2.chunk),
		int(chunk2ItemsArrSizeAlign),
	)
	endPtr = util.PointerAdd(p,
		int(header.dataSize))

	//align that chunk1 data should be moved to the right
	//only chunk1 data excluding chunk1 items array
	shift1 = LocationIndex(mergedItemsArrSizeAlign - chunk1ItemsArrSizeAlign)

	//align that chunk2 data should be moved to the left
	//only chunk2 data excluding chunk2 items array
	shift2 = LocationIndex(chunk1ItemsArrSizeAlign + chunk2ItemsArrSizeAlign -
		mergedItemsArrSizeAlign)

	tmpItems[0] = 0
	for i := 0; i < int(count2); i++ {
		offsetToChunk2DataBeginPosition := loc2.chunk.GetItem(i) -
			LocationIndex(chunk2ItemsArrSizeAlign)
		//merged item arrary size + chunk1 data length
		startPositionOfChunk2DataInMerged := LocationIndex(mergedItemsArrSizeAlign) +
			LocationIndex(chunk1Len)
		newLoc := offsetToChunk2DataBeginPosition +
			startPositionOfChunk2DataInMerged
		tmpItems[i] = newLoc
	}

	if shift1 != 0 {
		//move chunk1 data to the right
		for i := 0; i < int(count1); i++ {
			loc1.chunk.SetItem(i,
				loc1.chunk.GetItem(i)+shift1)
		}
		dstPtr := util.PointerAdd(chunk1DataPtr, int(shift1))
		dataLen := util.PointerSub(chunk1EndPtr, chunk1DataPtr)
		util.CMemmove(
			dstPtr,
			chunk1DataPtr,
			int(dataLen),
		)
	}

	if shift2 != 0 {
		//move chunk2 data to the left
		dstPtr := util.PointerAdd(chunk2DataPtr, -int(shift2))
		dataLen := util.PointerSub(endPtr, chunk2DataPtr)
		util.CMemmove(
			dstPtr,
			chunk2DataPtr,
			int(dataLen),
		)
		//shrink data size
		header.dataSize -= shift2
	}

	//copy chunk2 items array to dest position
	dst := util.PointerAdd(
		unsafe.Pointer(loc1.chunk),
		int(LocationIndexSize*uint32(count1)))

	util.CMemcpy(dst,
		unsafe.Pointer(&tmpItems[0]),
		int(LocationIndexSize*uint32(count2)),
	)

	//chunk desc array size
	part1 := util.AlignValue(
		BT_PAGE_HEADER_CHUNK_DESC_OFFSET+
			uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)

	part2 := util.AlignValue(
		BT_PAGE_HEADER_CHUNK_DESC_OFFSET+
			uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount-1), 8)

	hikeyShift = LocationIndex(part1 - part2)

	indexHikeyLoc := ShortGetLocation(header.GetChunkDesc(int(index)).GetHikeyShortLocation())
	index1HikeyLoc := ShortGetLocation(header.GetChunkDesc(int(index + 1)).GetHikeyShortLocation())

	hikeyShift2 = hikeyShift +
		LocationIndex(index1HikeyLoc) -
		LocationIndex(indexHikeyLoc)

	desc0HikeyLoc := ShortGetLocation(header.GetChunkDesc(0).GetHikeyShortLocation())

	p1_1 = util.PointerAdd(
		p,
		int(desc0HikeyLoc-uint32(hikeyShift)),
	)
	p1_2 = util.PointerAdd(
		p,
		int(desc0HikeyLoc),
	)

	len1 = int(indexHikeyLoc - desc0HikeyLoc)

	p2_1 = util.PointerAdd(
		p,
		int(indexHikeyLoc-uint32(hikeyShift)),
	)
	p2_2 = util.PointerAdd(
		p,
		int(index1HikeyLoc),
	)
	len2 = int(uint32(header.hikeysEnd) - index1HikeyLoc)

	indexDesc := header.GetChunkDesc(int(index))
	index1Desc := header.GetChunkDesc(int(index + 1))
	indexDesc.SetHikeyFlags(index1Desc.GetHikeyFlags())

	for i := index + 2; i < header.chunksCount; i++ {
		iDesc := header.GetChunkDesc(int(i))
		i_1Desc := header.GetChunkDesc(int(i - 1))
		i_1Desc.SetOffset(iDesc.GetOffset())
		i_1Desc.SetHikeyFlags(iDesc.GetHikeyFlags())
		i_1Desc.SetHikeyShortLocation(iDesc.GetHikeyShortLocation() - LocationGetShort(uint32(hikeyShift2)))
		i_1Desc.SetShortLocation(iDesc.GetShortLocation() - LocationGetShort(uint32(shift2)))
	}

	if hikeyShift > 0 {
		for i := 0; i <= int(index); i++ {
			iDesc := header.GetChunkDesc(int(i))
			iDesc.SetHikeyShortLocation(iDesc.GetHikeyShortLocation() - LocationGetShort(uint32(hikeyShift)))
		}
		util.CMemmove(p1_1, p1_2, len1)
	}
	util.CMemmove(p2_1, p2_2, len2)

	header.hikeysEnd -= OffsetNumber(hikeyShift2)
	header.chunksCount--
}
