package btree

import (
	"fmt"
	"sync/atomic"
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
	util.AssertFunc(chunkOffset <= header.chunksCount)
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

	fmt.Println(
		"chunk1ItemsArrSizeAlign",
		chunk1ItemsArrSizeAlign,
		"chunk2ItemsArrSizeAlign",
		chunk2ItemsArrSizeAlign,
		"mergedItemsArrSizeAlign",
		mergedItemsArrSizeAlign,
	)

	chunk1DataPtr = util.PointerAdd(
		unsafe.Pointer(loc1.chunk),
		int(chunk1ItemsArrSizeAlign),
	)
	chunk1EndPtr = util.PointerAdd(
		unsafe.Pointer(loc1.chunk),
		int(loc1.chunkSize),
	)
	//only data length
	//excluding chunk1 items array
	chunk1DataLen := util.PointerSub(chunk1EndPtr, chunk1DataPtr)
	if chunk1DataLen == int64(loc1.chunkSize) {
		util.AssertFunc(chunk1DataLen == 0)
	}
	chunk2DataPtr = util.PointerAdd(
		unsafe.Pointer(loc2.chunk),
		int(chunk2ItemsArrSizeAlign),
	)
	//including chunk2 items array
	endPtr = util.PointerAdd(p,
		int(header.dataSize))

	//Before merge:
	//|--chunk1 items array--|--chunk1 data--|--chunk2 items array--|--chunk2 data--|
	//
	//After merge:
	//|--chunk1 items array--|--chunk2 items array--|--chunk1 data--|--chunk2 data--|

	//distance that chunk1 data should be moved to the right
	//only chunk1 data excluding chunk1 items array
	shift1 = LocationIndex(mergedItemsArrSizeAlign - chunk1ItemsArrSizeAlign)

	//distance that chunk2 data should be moved to the left
	//only chunk2 data excluding chunk2 items array
	shift2 = LocationIndex(chunk1ItemsArrSizeAlign + chunk2ItemsArrSizeAlign -
		mergedItemsArrSizeAlign)

	//evaluate the item offset of the data in chunk2
	tmpItems[0] = 0
	for i := 0; i < int(count2); i++ {
		offsetToChunk2DataBeginPosition := loc2.chunk.GetItem(i) -
			LocationIndex(chunk2ItemsArrSizeAlign)
		//merged item arrary size + chunk1 data length
		startPositionOfChunk2DataInMerged := LocationIndex(mergedItemsArrSizeAlign) +
			LocationIndex(chunk1DataLen)
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
		util.CMemmove(
			dstPtr,
			chunk1DataPtr,
			int(chunk1DataLen),
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

	//Before merge:
	//|--chunk desc array--|--chunk1 hikey--|--chunk2 hikey--|
	//
	//After merge:
	//|--chunk desc array(shrink)--|--chunk2 hikey--|

	//shrink chunk desc array
	//chunk desc array size before merge
	part1 := util.AlignValue(
		BT_PAGE_HEADER_CHUNK_DESC_OFFSET+
			uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount), 8)

	//chunk desc array size after merge
	part2 := util.AlignValue(
		BT_PAGE_HEADER_CHUNK_DESC_OFFSET+
			uint32(BT_PAGE_CHUNK_DESC_SIZE)*uint32(header.chunksCount-1), 8)

	//distance that originated from shrinking chunk desc array
	hikeyShift = LocationIndex(part1 - part2)

	indexHikeyLoc := ShortGetLocation(header.GetChunkDesc(int(index)).GetHikeyShortLocation())
	index1HikeyLoc := ShortGetLocation(header.GetChunkDesc(int(index + 1)).GetHikeyShortLocation())

	chunk1HikeyLen := LocationIndex(index1HikeyLoc) - LocationIndex(indexHikeyLoc)

	//distance that hikey area after chunk1 that should be moved to overwrite chunk1 hikey
	hikeyShift2 = hikeyShift + chunk1HikeyLen

	desc0HikeyLoc := ShortGetLocation(header.GetChunkDesc(0).GetHikeyShortLocation())

	//dest position of the hikey area before chunk1's hikey
	p1_1 = util.PointerAdd(
		p,
		int(desc0HikeyLoc-uint32(hikeyShift)),
	)
	//src position of the hikey
	p1_2 = util.PointerAdd(
		p,
		int(desc0HikeyLoc),
	)

	//length of the hikey area before the chunk1's hikey
	len1 = int(indexHikeyLoc - desc0HikeyLoc)

	//dest position of the hikey area after chunk1's hikey
	p2_1 = util.PointerAdd(
		p,
		int(indexHikeyLoc-uint32(hikeyShift)),
	)

	//src position of the hikey area after chunk1's hikey
	p2_2 = util.PointerAdd(
		p,
		int(index1HikeyLoc),
	)
	//length of the hikey area after chunk1's hikey
	len2 = int(uint32(header.hikeysEnd) - index1HikeyLoc)

	//owrite the chunk1 hikey flags by the chunk2's
	indexDesc := header.GetChunkDesc(int(index))
	index1Desc := header.GetChunkDesc(int(index + 1))
	indexDesc.SetHikeyFlags(index1Desc.GetHikeyFlags())

	//shrink & update chunk desc array
	for i := index + 2; i < header.chunksCount; i++ {
		iDesc := header.GetChunkDesc(int(i))
		i_1Desc := header.GetChunkDesc(int(i - 1))
		i_1Desc.SetOffset(iDesc.GetOffset())
		i_1Desc.SetHikeyFlags(iDesc.GetHikeyFlags())
		i_1Desc.SetHikeyShortLocation(iDesc.GetHikeyShortLocation() -
			LocationGetShort(uint32(hikeyShift2)))
		i_1Desc.SetShortLocation(iDesc.GetShortLocation() -
			LocationGetShort(uint32(shift2)))
	}

	if hikeyShift > 0 {
		//update the new hikey location before chunk1 and chunk1
		for i := 0; i <= int(index); i++ {
			iDesc := header.GetChunkDesc(int(i))
			iDesc.SetHikeyShortLocation(iDesc.GetHikeyShortLocation() -
				LocationGetShort(uint32(hikeyShift)))
		}
		//move the hikey area before chunk1's hikey
		util.CMemmove(p1_1, p1_2, len1)
	}
	//move the hikey area after chunk1's hikey
	util.CMemmove(p2_1, p2_2, len2)

	//shrink the hikey area
	header.hikeysEnd -= OffsetNumber(hikeyShift2)
	//shrink the chunk desc array
	header.chunksCount--
}

func pageLocatorDeleteItem(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
) {
	var (
		itemsShift,
		dataShift,
		itemSize int
		firstItemPtr,
		itemPtr,
		endPtr unsafe.Pointer
	)
	header := (*BTPageHeader)(p)
	itemSize = int(pageLocatorGetItemSize(p, locator))
	util.AssertFunc(itemSize == util.AlignValue(itemSize, 8))

	//Before delete item1:
	//|--item0_offset,item1_offset,item2_offset,...--|--item0,item1,item2,...--|
	//
	//After delete item1:
	//|--item0_offset,item2_offset,...--|--item0,item2,...--|

	itemArraySize := util.AlignValue(
		OffsetNumber(LocationIndexSize)*locator.chunkItemsCount,
		8)
	shrinkedItemArraySize := util.AlignValue(
		OffsetNumber(LocationIndexSize)*(locator.chunkItemsCount-1),
		8)

	//distance that elements after the deleting one in the item array should be moved to the left
	itemsShift = int(itemArraySize - shrinkedItemArraySize)

	//distance that the data after the deleting item should be moved to the left
	dataShift = itemsShift + itemSize

	//the start position of the item data
	firstItemPtr = util.PointerAdd(
		unsafe.Pointer(locator.chunk),
		int(itemArraySize),
	)
	util.AssertFunc(locator.itemOffset < OffsetNumber(locator.chunkSize))

	//the start position of the deleting item
	itemPtr = util.PointerAdd(
		unsafe.Pointer(locator.chunk),
		int(ItemGetOffset(locator.chunk.GetItem(int(locator.itemOffset)))),
	)
	endPtr = util.PointerAdd(
		p,
		int(header.dataSize),
	)

	ptr1 := util.PointerAdd(endPtr, -dataShift)
	ptr2 := util.PointerAdd(itemPtr, -itemsShift)
	util.AssertFunc(util.PointerLessEqual(ptr2, ptr1))

	//update elements in the item array after the deleting one (included)
	for i := locator.itemOffset; i < locator.chunkItemsCount-1; i++ {
		i_1_item := locator.chunk.GetItem(int(i + 1))
		locator.chunk.SetItem(int(i), i_1_item-LocationIndex(dataShift))
	}

	if itemsShift != 0 {
		//move the data before deleting one when shrinking the item arrary
		dst := util.PointerAdd(firstItemPtr, -int(itemsShift))
		dataLen := util.PointerSub(itemPtr, firstItemPtr)
		util.CMemmove(
			dst,
			firstItemPtr,
			int(dataLen),
		)
		//update item offset of the items before deleting one
		for i := 0; i < int(locator.itemOffset); i++ {
			locator.chunk.SetItem(int(i),
				locator.chunk.GetItem(int(i))-LocationIndex(itemsShift))
		}
	}

	//move data after deleting one
	dst := util.PointerAdd(itemPtr, -itemsShift)
	src := util.PointerAdd(itemPtr, itemSize)
	dataLen := util.PointerSub(endPtr, itemPtr) - int64(itemSize)
	util.CMemmove(
		dst,
		src,
		int(dataLen),
	)

	//update chunk desc arrary
	for i := locator.chunkOffset + 1; i < header.chunksCount; i++ {
		i_desc := header.GetChunkDesc(int(i))
		i_desc.SetShortLocation(
			i_desc.GetShortLocation() -
				LocationGetShort(uint32(dataShift)),
		)
		i_desc.SetOffset(i_desc.GetOffset() - 1)
	}
	header.itemsCount--
	header.dataSize -= LocationIndex(dataShift)

	//update the locator
	locator.chunkItemsCount--
	locator.chunkSize -= LocationIndex(dataShift)

	if locator.chunkItemsCount == 0 {
		if locator.chunkOffset > 0 {
			//merge chunk-1,chunk
			pageMergeChunks(p, locator.chunkOffset-1)
			pageChunkFillLocator(p, locator.chunkOffset-1, locator)
			locator.itemOffset = locator.chunkItemsCount
		} else if locator.chunkOffset+1 < header.chunksCount {
			//merge chunk,chunk+1
			pageMergeChunks(p, locator.chunkOffset)
			pageChunkFillLocator(p, locator.chunkOffset, locator)
		}
	}
}

// pageSplitChunk 用于将一个 chunk 拆分为左右两个 chunk，并在页面中插入新的 chunk 描述符和 hikey。
//
// 特殊的地方：chunkoffset及其左边的chunk数据不动。尽管chunk desc数组和hikey向右边扩展了。
// 调用者保证：hikey右边的空间足够大。不需要移动chunk0~chunkoffset的数据。
func pageSplitChunk(
	p unsafe.Pointer,
	locator *BTPageItemLocator,
	hikeysEnd, hikeySize LocationIndex,
) {
	var (
		tmpItems [BT_PAGE_MAX_CHUNK_ITEMS]LocationIndex
		leftItemsShift,
		rightItemsShift,
		dataShift,
		chunkDescShift,
		hikeyShift LocationIndex
		firstItemPtr,
		itemPtr,
		endPtr,
		rightChunkPtr,
		firstHikeyPtr,
		hikeyPtr,
		hikeyEndPtr unsafe.Pointer
		leftItemsCount,
		rightItemsCount OffsetNumber
	)
	header := (*BTPageHeader)(p)
	util.AssertFunc(hikeySize == util.AlignValue(hikeySize, 8))

	leftItemsCount = locator.itemOffset
	rightItemsCount = locator.chunkItemsCount - locator.itemOffset

	itemArraySizeAlign := util.AlignValue(
		OffsetNumber(LocationIndexSize)*locator.chunkItemsCount,
		8)

	leftItemArraySizeAlign := util.AlignValue(
		OffsetNumber(LocationIndexSize)*leftItemsCount,
		8)

	rightItemArraySizeAlign := util.AlignValue(
		OffsetNumber(LocationIndexSize)*rightItemsCount,
		8)

	//data position of first item
	firstItemPtr = util.PointerAdd(
		unsafe.Pointer(locator.chunk),
		int(itemArraySizeAlign),
	)
	//data position of the item to be split
	itemOffset := ItemGetOffset(locator.chunk.GetItem(int(locator.itemOffset)))
	itemPtr = util.PointerAdd(
		unsafe.Pointer(locator.chunk),
		int(itemOffset),
	)
	endPtr = util.PointerAdd(
		p,
		int(header.dataSize),
	)

	//p <= firstItemPtr <= itemPtr <= endPtr
	util.AssertFunc(
		util.PointerLessEqual(p, firstItemPtr) &&
			util.PointerLessEqual(firstItemPtr, itemPtr) &&
			util.PointerLessEqual(itemPtr, endPtr))

	util.AssertFunc(
		util.PointerLessEqual(
			endPtr,
			util.PointerAdd(p, BLOCK_SIZE),
		))

	leftItemsShift = LocationIndex(itemArraySizeAlign - leftItemArraySizeAlign)
	rightItemsShift = LocationIndex(OffsetNumber(itemOffset) - rightItemArraySizeAlign)

	//update positions of the items in the right
	for i := locator.itemOffset; i < locator.chunkItemsCount; i++ {
		tmpItems[i-locator.itemOffset] = locator.chunk.GetItem(int(i)) -
			rightItemsShift
	}

	//update positions of the items in the left
	for i := 0; i < int(locator.itemOffset); i++ {
		locator.chunk.SetItem(int(i),
			locator.chunk.GetItem(int(i))-leftItemsShift)
	}

	//move the data of left chunk
	dst := util.PointerAdd(firstItemPtr, -int(leftItemsShift))
	dataLen := util.PointerSub(itemPtr, firstItemPtr)
	util.CMemmove(
		dst,
		firstItemPtr,
		int(dataLen),
	)

	//move the data of right chunk
	dataShift = LocationIndex(rightItemArraySizeAlign) +
		LocationIndex(leftItemArraySizeAlign) -
		LocationIndex(itemArraySizeAlign)

	util.AssertFunc(
		util.PointerLessEqual(
			util.PointerAdd(itemPtr,
				int(dataShift+LocationIndex(util.PointerSub(endPtr, itemPtr)))),
			util.PointerAdd(p, BLOCK_SIZE),
		))

	dst = util.PointerAdd(itemPtr, int(dataShift))
	util.CMemmove(
		dst,
		itemPtr,
		int(util.PointerSub(endPtr, itemPtr)),
	)

	//move the item array of right chunk
	rightChunkPtr = util.PointerAdd(
		util.PointerAdd(itemPtr, -int(itemArraySizeAlign)),
		int(leftItemArraySizeAlign),
	)

	rightItemArraySize := LocationIndexSize * uint32(rightItemsCount)
	util.CMemmove(
		rightChunkPtr,
		unsafe.Pointer(&tmpItems[0]),
		int(rightItemArraySize),
	)

	//add new hikey and new chunk desc
	//
	newChunkDescArraySizeAlign := util.AlignValue(
		OffsetNumber(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)+
			OffsetNumber(BT_PAGE_CHUNK_DESC_SIZE)*(header.chunksCount+1),
		8,
	)
	oldChunkDescArraySizeAlign := util.AlignValue(
		OffsetNumber(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)+
			OffsetNumber(BT_PAGE_CHUNK_DESC_SIZE)*header.chunksCount,
		8,
	)
	//distance of hikey area before the new hikey
	chunkDescShift = LocationIndex(newChunkDescArraySizeAlign - oldChunkDescArraySizeAlign)
	//distance of hikey area after the new hikey
	hikeyShift = chunkDescShift + hikeySize

	firstHikeyPtr = util.PointerAdd(
		p,
		int(ShortGetLocation(header.GetChunkDesc(0).GetHikeyShortLocation())),
	)
	hikeyPtr = util.PointerAdd(
		p,
		int(ShortGetLocation(header.GetChunkDesc(int(locator.chunkOffset)).GetHikeyShortLocation())),
	)
	hikeyEndPtr = util.PointerAdd(
		p,
		int(header.hikeysEnd),
	)
	//p <= firstHikeyPtr <= hikeyPtr <= hikeyEndPtr
	util.AssertFunc(
		util.PointerLessEqual(p, firstHikeyPtr) &&
			util.PointerLessEqual(firstHikeyPtr, hikeyPtr) &&
			util.PointerLessEqual(hikeyPtr, hikeyEndPtr),
	)

	//split hikey area to add new hikey
	util.AssertFunc(
		util.PointerLessEqual(
			util.PointerAdd(hikeyEndPtr, int(hikeyShift)),
			util.PointerAdd(p, int(hikeysEnd)),
		),
	)

	//move hikey area after the new hikey
	dst = util.PointerAdd(hikeyPtr, int(hikeyShift))
	dataLen = util.PointerSub(hikeyEndPtr, hikeyPtr)
	util.CMemmove(
		dst,
		hikeyPtr,
		int(dataLen))

	//move hikey area before the new hikey
	dst = util.PointerAdd(firstHikeyPtr, int(chunkDescShift))
	dataLen = util.PointerSub(hikeyPtr, firstHikeyPtr)
	util.CMemmove(
		dst,
		firstHikeyPtr,
		int(dataLen),
	)

	//update chunk descs before the new chunk
	for i := 0; i <= int(locator.chunkOffset); i++ {
		iDesc := header.GetChunkDesc(int(i))
		iDesc.SetHikeyShortLocation(
			iDesc.GetHikeyShortLocation() +
				LocationGetShort(uint32(chunkDescShift)))
	}

	//extend chunk descs array
	for i := header.chunksCount; i > locator.chunkOffset; i-- {
		iDesc := header.GetChunkDesc(int(i))
		i_1Desc := header.GetChunkDesc(int(i - 1))
		iDesc.SetHikeyShortLocation(i_1Desc.GetHikeyShortLocation() +
			LocationGetShort(uint32(hikeyShift)))
		iDesc.SetHikeyFlags(i_1Desc.GetHikeyFlags())
		iDesc.SetOffset(i_1Desc.GetOffset())
		iDesc.SetShortLocation(i_1Desc.GetShortLocation() +
			LocationGetShort(uint32(dataShift)))
	}

	//init new chunk desc
	i := locator.chunkOffset + 1
	iDesc := header.GetChunkDesc(int(i))
	i_1Desc := header.GetChunkDesc(int(i - 1))
	iDesc.SetHikeyShortLocation(i_1Desc.GetHikeyShortLocation() +
		LocationGetShort(uint32(hikeySize)))
	iDesc.SetOffset(i_1Desc.GetOffset() + uint32(leftItemsCount))
	iDesc.SetShortLocation(LocationGetShort(uint32(util.PointerSub(rightChunkPtr, p))))
	iDesc.SetHikeyFlags(i_1Desc.GetHikeyFlags())
	header.chunksCount++
	header.hikeysEnd += OffsetNumber(hikeyShift)
	header.dataSize += dataShift

	pageChunkFillLocator(p, i, locator)
}

// 尝试将一个 chunk 拆分为左右两个 chunk。
// 先判断是否要分裂，如果需要，则尝试找到最佳的拆分点。
// 如果找到最佳拆分点，则调用 pageSplitChunk 进行拆分。
// 注意：分裂后，将chunkOffset及其之后的hikey往后移。
// 会用新chunk(chunkOffset+1)的第一个item数据，作为
// chunkOffset的hikey。
func pageSplitChunkIfNeeded(
	desc *BTDesc,
	p unsafe.Pointer,
	locator *BTPageItemLocator,
) {
	var (
		chunkOffset                           OffsetNumber
		hikeysFreeSpace, dataFreeSpace        LocationIndex
		bestHikeySize, bestHikeySizeUnaligned LocationIndex
	)
	header := (*BTPageHeader)(p)
	bestOffset := -1
	bestScore := float32(0.0)
	hikeysEnd := BTPageHikeysEnd(desc, p)
	newHikey := FixedKey{}

	if header.hikeysEnd >= OffsetNumber(hikeysEnd) {
		return
	}

	chunkOffset = locator.chunkOffset

	condLeft := float32(locator.chunkSize) /
		float32(BLOCK_SIZE-hikeysEnd)
	condRight := float32(util.AlignValue(header.maxKeyLen, 8)) * 2.0 /
		float32(hikeysEnd-LocationIndex(BT_PAGE_HEADER_CHUNK_DESC_OFFSET))

	if condLeft < condRight {
		return
	}

	//theoretically, the free hikey area is hikeysEnd - header.hikeysEnd
	hikeysFreeSpace = hikeysEnd - LocationIndex(header.hikeysEnd)
	newChunkDescArraySizeAlign := util.AlignValue(
		OffsetNumber(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)+
			OffsetNumber(BT_PAGE_CHUNK_DESC_SIZE)*(header.chunksCount+1),
		8,
	)
	oldChunkDescArraySizeAlign := util.AlignValue(
		OffsetNumber(BT_PAGE_HEADER_CHUNK_DESC_OFFSET)+
			OffsetNumber(BT_PAGE_CHUNK_DESC_SIZE)*header.chunksCount,
		8,
	)
	//subtract the size of one new chunk desc
	hikeysFreeSpace -= LocationIndex(newChunkDescArraySizeAlign -
		oldChunkDescArraySizeAlign)

	dataFreeSpace = LocationIndex(BLOCK_SIZE) - header.dataSize

	for i := OffsetNumber(1); i < locator.chunkItemsCount; i++ {
		var (
			hikeySize,
			hikeySizeUnaligned,
			dataSize,
			leftDataSize,
			rightDataSize LocationIndex
		)
		score := float32(0.0)

		//use the item of locator as the new hikey
		locator.itemOffset = i
		if PageIs(p, BTREE_FLAG_LEAF) {
			tuple := Tuple{}
			tuple.data = util.PointerAdd(
				BTPageLocatorGetItem(p, locator),
				int(BT_LEAF_TUPHDR_SIZE))
			tuple.formatFlags = uint8(BTPageGetItemFlags(p, locator))
			hikeySizeUnaligned = LocationIndex(BTLen(desc, tuple, TupleKeyLengthNoVersion))
			hikeySize = util.AlignValue(hikeySizeUnaligned, 8)
		} else {
			hikeySize = BTPageGetItemSize(p, locator) -
				LocationIndex(BT_NON_LEAF_TUPHDR_SIZE)
			hikeySizeUnaligned = hikeySize
		}

		util.AssertFunc(hikeySize > 0)

		if hikeySize > hikeysFreeSpace {
			continue
		}

		leftItemArraySizeAlign := util.AlignValue(
			OffsetNumber(LocationIndexSize)*i,
			8)

		rightItemArraySizeAlign := util.AlignValue(
			OffsetNumber(LocationIndexSize)*(locator.chunkItemsCount-i),
			8)

		itemArraySizeAlign := util.AlignValue(
			OffsetNumber(LocationIndexSize)*locator.chunkItemsCount,
			8)

		//about item array extension
		dataSize = LocationIndex(leftItemArraySizeAlign +
			rightItemArraySizeAlign -
			itemArraySizeAlign)
		if dataSize > dataFreeSpace {
			continue
		}

		leftDataSize = LocationIndex(ItemGetOffset(locator.chunk.GetItem(int(locator.itemOffset))))
		rightDataSize = locator.chunkSize - leftDataSize
		//substractthe item array size
		leftDataSize -= LocationIndex(itemArraySizeAlign)

		//more average or balance, more better
		score = float32(min(leftDataSize, rightDataSize)) / float32(hikeySize)

		if score > bestScore {
			bestOffset = int(i)
			bestHikeySize = hikeySize
			bestHikeySizeUnaligned = hikeySizeUnaligned
			bestScore = score
		}
	}

	if bestOffset < 0 {
		return
	}

	locator.itemOffset = OffsetNumber(bestOffset)
	if PageIs(p, BTREE_FLAG_LEAF) {
		tuple := Tuple{}
		allocated := false
		tuple.data = util.PointerAdd(
			BTPageLocatorGetItem(p, locator),
			int(BT_LEAF_TUPHDR_SIZE))
		tuple.formatFlags = uint8(BTPageGetItemFlags(p, locator))
		//make newhikey from tuple
		newHikey.tuple = BTTupleMakeKey(
			desc,
			tuple,
			unsafe.Pointer(&newHikey.fixedData[0]),
			false,
			&allocated)

		if bestHikeySize != bestHikeySizeUnaligned {
			util.CMemset(
				unsafe.Pointer(&newHikey.fixedData[bestHikeySizeUnaligned]),
				0,
				int(bestHikeySize-bestHikeySizeUnaligned))
		}
		util.AssertFunc(allocated == false)
	} else {
		key := Tuple{}
		key.data = util.PointerAdd(
			BTPageLocatorGetItem(p, locator),
			int(BT_NON_LEAF_TUPHDR_SIZE),
		)
		key.formatFlags = uint8(BTPageGetItemFlags(p, locator))
		copyFixedKey(desc, &newHikey, key)
	}

	pageSplitChunk(p, locator, hikeysEnd, bestHikeySize)

	dst := util.PointerAdd(
		p,
		int(ShortGetLocation(
			header.GetChunkDesc(int(chunkOffset)).GetHikeyShortLocation())),
	)
	util.PointerCopy(
		dst,
		unsafe.Pointer(&(newHikey.fixedData[0])),
		int(bestHikeySize),
	)
	header.GetChunkDesc(int(chunkOffset)).
		SetHikeyFlags(
			uint32(newHikey.tuple.formatFlags))
}

func itemGetKeySize(
	desc *BTDesc,
	leaf bool,
	item *BTPageItem,
) LocationIndex {
	var tuple Tuple
	if leaf {
		var pos int
		if item.newItem {
			pos = 0
		} else {
			pos = int(BT_LEAF_TUPHDR_SIZE)
		}
		tuple.data = util.PointerAdd(
			item.data,
			pos)
		tuple.formatFlags = item.flags
		return LocationIndex(util.AlignValue(BTLen(desc, tuple, TupleKeyLengthNoVersion), 8))
	} else {
		util.AssertFunc(!item.newItem)
		tuple.data = util.PointerAdd(
			item.data,
			int(BT_NON_LEAF_TUPHDR_SIZE),
		)
		tuple.formatFlags = item.flags
		return LocationIndex(util.AlignValue(BTLen(desc, tuple, KeyLength), 8))
	}
}

// BTPageReorg splits one chunk into multiple chunks.
func BTPageReorg(
	desc *BTDesc,
	p unsafe.Pointer,
	items [BT_PAGE_MAX_CHUNK_ITEMS]BTPageItem,
	count OffsetNumber,
	hikeySize LocationIndex,
	hikey Tuple,
	newLoc *BTPageItemLocator,
) {
	chunksCount := 0
	totalDataSize := LocationIndex(0)
	itemHeaderSize := LocationIndex(0)
	if PageIs(p, BTREE_FLAG_LEAF) {
		itemHeaderSize = LocationIndex(BT_LEAF_TUPHDR_SIZE)
	} else {
		itemHeaderSize = LocationIndex(BT_NON_LEAF_TUPHDR_SIZE)
	}

	var chunk *BTPageChunk
	header := (*BTPageHeader)(p)
	var ptr, hikeysPtr unsafe.Pointer
	var chunkOffsets [BT_PAGE_MAX_CHUNKS + 1]OffsetNumber //item index
	var itemsArray [BT_PAGE_MAX_CHUNK_ITEMS]LocationIndex //item location
	var i, j int
	var hikeysFreeSpace, hikeysFreeSpaceLeft,
		dataFreeSpace, dataFreeSpaceLeft,
		hikeysEnd LocationIndex
	isRightmost := PageIs(p, BTREE_FLAG_RIGHTMOST)
	var chunkDataSize, //sum of continuous item size
		maxKeyLen LocationIndex

	//evaluate the theoretical hikeys end
	pageHikeysEnd := BTPageHikeysEnd(desc, p)
	hikeysEnd = max(pageHikeysEnd,
		LocationIndex(util.AlignValue8(BT_PAGE_HEADER_SIZE))+
			util.AlignValue8(hikeySize),
	)

	//total data size = sum of all items size
	totalDataSize = 0
	for i := 0; i < int(count); i++ {
		totalDataSize += items[i].size
	}

	//free hikeys space = theoretical hikeysEnd - (page header size + hikey size)
	hikeysFreeSpace = hikeysEnd -
		(LocationIndex(util.AlignValue8(BT_PAGE_HEADER_SIZE)) + util.AlignValue8(hikeySize))
	hikeysFreeSpaceLeft = hikeysFreeSpace
	//free data space = (block size - hikeysEnd) - total data size - item array size
	dataFreeSpace = (BLOCK_SIZE - hikeysEnd) -
		totalDataSize -
		util.AlignValue8(LocationIndex(count)*LocationIndex(LocationIndexSize))
	dataFreeSpaceLeft = dataFreeSpace

	maxKeyLen = util.AlignValue8(hikeySize)

	chunkOffsets[0] = 0
	j = 1
	chunkDataSize = 0
	if count >= 1 {
		chunkDataSize += items[0].size
	}

	if PageIs(p, BTREE_FLAG_LEAF) && count > 0 {
		maxKeyLen = max(maxKeyLen,
			itemGetKeySize(desc,
				PageIs(p, BTREE_FLAG_LEAF),
				&items[0]))
	}

	for i := OffsetNumber(1); i < count; i++ {
		var (
			nextKeySize,
			hikeySizeDiff,
			dataSpaceDiff LocationIndex
			dataSizeRatio float32
		)
		//item size actually
		nextKeySize = itemGetKeySize(desc, PageIs(p, BTREE_FLAG_LEAF), &items[i])
		maxKeyLen = max(maxKeyLen, nextKeySize)
		j_plus_1_len := util.AlignValue8(BT_PAGE_HEADER_CHUNK_DESC_OFFSET +
			uint32((j+1)*BT_PAGE_CHUNK_DESC_SIZE))
		j_len := util.AlignValue8(BT_PAGE_HEADER_CHUNK_DESC_OFFSET +
			uint32(j*BT_PAGE_CHUNK_DESC_SIZE))
		//hikey extend size = item size + chunk desc array extend size
		hikeySizeDiff = nextKeySize + LocationIndex(j_plus_1_len-j_len)
		//data space extend size = splited item array extend size
		dataSpaceDiff = LocationIndex(util.AlignValueWaste8(
			LocationIndexSize * uint32((i - chunkOffsets[j-1])),
		))

		//free hikey space or data space is not enough,
		//pass them
		if hikeySizeDiff > hikeysFreeSpaceLeft ||
			dataSpaceDiff > dataFreeSpaceLeft {
			chunkDataSize += items[i].size
			continue
		}

		//in a word: data ratio, meta ratio
		//evaluate current chunk data size ratio
		dataSizeRatio = float32(chunkDataSize) / float32(totalDataSize)
		//evaluate extend hikey space ratio
		ratio1 := float32(nextKeySize+LocationIndex(BT_PAGE_CHUNK_DESC_SIZE)) / float32(hikeysFreeSpace)
		//evaluate splited item array extend ratio
		ratio2 := float32(dataSpaceDiff) / float32(dataFreeSpace)

		//data ratio >= meta ratio,choose it
		if dataSizeRatio >= ratio1 &&
			dataSizeRatio >= ratio2 {
			hikeysFreeSpaceLeft -= hikeySizeDiff
			dataFreeSpaceLeft -= dataSpaceDiff
			chunkOffsets[j] = i
			chunkDataSize = 0
			j++
		}

		chunkDataSize += items[i].size
	}

	util.AssertFunc(uint32(j) <= BT_PAGE_MAX_CHUNKS)
	//last
	chunkOffsets[j] = count
	chunksCount = j

	//evaluate items location for every chunk
	ptr = util.PointerAdd(p, int(hikeysEnd))
	for j := 0; j < chunksCount; j++ {
		chunkItemsCount := chunkOffsets[j+1] - chunkOffsets[j]
		//item shift = item array size
		itemShift := LocationIndex(util.AlignValue8(LocationIndexSize * uint32(chunkItemsCount)))

		//update item location
		for i := chunkOffsets[j]; i < chunkOffsets[j+1]; i++ {
			itemsArray[i] = ItemSetFlags(itemShift, uint16(items[i].flags))
			itemShift += items[i].size
		}
		//to next chunk
		ptr = util.PointerAdd(ptr, int(itemShift))
	}

	header.maxKeyLen = maxKeyLen
	header.dataSize = LocationIndex(util.PointerSub(ptr, p))
	header.chunksCount = OffsetNumber(chunksCount)

	//rearrange data from last chunk to first chunk
	for j := chunksCount - 1; j >= 0; j-- {
		chunkItemsCount := chunkOffsets[j+1] - chunkOffsets[j]

		for i := int(chunkOffsets[j+1]) - 1; i >= int(chunkOffsets[j]); i-- {
			ptr = util.PointerAdd(ptr, -int(items[i].size))

			if util.PointerLessEqual(p, items[i].data) &&
				util.PointerLess(items[i].data, util.PointerAdd(p, BLOCK_SIZE)) &&
				util.PointerLess(items[i].data, ptr) &&
				!items[i].newItem {
				util.CMemmove(ptr, items[i].data, int(items[i].size))
			}
		}
		//to previous chunk
		ptr = util.PointerAdd(ptr,
			-int(util.AlignValue8(LocationIndexSize*uint32(chunkItemsCount))))
	}
	util.AssertFunc(ptr == util.PointerAdd(p, int(hikeysEnd)))
	//location of first hikeys
	hikeysPtr = util.PointerAdd(p,
		int(util.AlignValue8(BT_PAGE_HEADER_CHUNK_DESC_OFFSET+
			uint32(chunksCount*BT_PAGE_CHUNK_DESC_SIZE))))
	//rearrange item array and chunk descs
	for j := 0; j < chunksCount; j++ {
		chunkItemsCount := chunkOffsets[j+1] - chunkOffsets[j]
		fillNewLoc := false
		i = int(chunkOffsets[j])

		//move item array
		util.CMemmove(ptr,
			unsafe.Pointer(&itemsArray[i]),
			int(LocationIndexSize*uint32(chunkItemsCount)))

		chunk = (*BTPageChunk)(ptr)
		//update chunk desc
		chkdesc := header.GetChunkDesc(int(j))
		chkdesc.SetShortLocation(LocationGetShort(uint32(util.PointerSub(ptr, p))))
		chkdesc.SetOffset(uint32(chunkOffsets[j]))
		//to chun data
		ptr = util.PointerAdd(
			ptr,
			int(util.AlignValue8(LocationIndexSize*uint32(chunkItemsCount))),
		)

		//move item data
		for i := chunkOffsets[j]; i < chunkOffsets[j+1]; i++ {
			if !items[i].newItem {
				if !(util.PointerLessEqual(p, items[i].data) &&
					util.PointerLess(items[i].data, util.PointerAdd(p, BLOCK_SIZE))) ||
					util.PointerLess(ptr, items[i].data) {
					util.CMemmove(ptr, items[i].data, int(items[i].size))
				}
			} else {
				newLoc.chunk = chunk
				newLoc.chunkOffset = OffsetNumber(j)
				newLoc.itemOffset = i - chunkOffsets[j]
				newLoc.chunkItemsCount = chunkItemsCount
				fillNewLoc = true
			}
			//to next item
			ptr = util.PointerAdd(ptr, int(items[i].size))
		}

		if fillNewLoc {
			newLoc.chunkSize = LocationIndex(util.PointerSub(ptr, unsafe.Pointer(chunk)))
		}

		//fill hikey data
		//hikey of chunk j-1 is the first item of chunk j
		if j > 0 {
			chunkHikeyTuple := Tuple{}
			chunkHikeySize := LocationIndex(0)
			//hikey data
			if !items[chunkOffsets[j]].newItem {
				item := chunk.GetItem(0)
				chunkHikeyTuple.formatFlags = uint8(ItemGetFlags(item))
				chunkHikeyTuple.data = util.PointerAdd(
					unsafe.Pointer(chunk),
					int(ItemGetOffset(item))+int(itemHeaderSize))
			} else {
				item := items[chunkOffsets[j]]
				chunkHikeyTuple.formatFlags = item.flags
				chunkHikeyTuple.data = item.data
			}
			if PageIs(p, BTREE_FLAG_LEAF) {
				shouldFree := false
				chunkHikeyTuple = BTTupleMakeKey(
					desc,
					chunkHikeyTuple,
					hikeysPtr,
					false,
					&shouldFree)
				util.AssertFunc(chunkHikeyTuple.data == hikeysPtr)
				util.AssertFunc(!shouldFree)
			}
			chunkHikeySize = LocationIndex(util.AlignValue8(
				BTLen(desc, chunkHikeyTuple, KeyLength)))
			if chunkHikeyTuple.data != hikeysPtr {
				util.CMemcpy(hikeysPtr, chunkHikeyTuple.data, int(chunkHikeySize))
			}
			//update chunk desc j - 1
			j_1Desc := header.GetChunkDesc(int(j - 1))
			j_1Desc.SetHikeyFlags(uint32(chunkHikeyTuple.formatFlags))
			j_1Desc.SetHikeyShortLocation(LocationGetShort(uint32(util.PointerSub(hikeysPtr, p))))
			hikeysPtr = util.PointerAdd(hikeysPtr, int(chunkHikeySize))
			util.AssertFunc(util.PointerSub(hikeysPtr, p) <= int64(hikeysEnd))
		}
	}

	//fill hikey data for the last chunk
	if !isRightmost {
		util.CMemcpy(hikeysPtr, hikey.data, int(hikeySize))
		if hikeySize != util.AlignValue8(hikeySize) {
			util.CMemset(
				util.PointerAdd(hikeysPtr, int(hikeySize)),
				0,
				int(util.AlignValue8(hikeySize)-hikeySize))
		}
		j_1Desc := header.GetChunkDesc(j - 1)
		j_1Desc.SetHikeyFlags(uint32(hikey.formatFlags))
		j_1Desc.SetHikeyShortLocation(LocationGetShort(uint32(util.PointerSub(hikeysPtr, p))))
		hikeysPtr = util.PointerAdd(hikeysPtr, int(util.AlignValue8(hikeySize)))
		util.AssertFunc(util.PointerSub(hikeysPtr, p) <= int64(hikeysEnd))
	} else {
		j_1Desc := header.GetChunkDesc(j - 1)
		j_1Desc.SetHikeyFlags(0)
		j_1Desc.SetHikeyShortLocation(LocationGetShort(uint32(util.PointerSub(hikeysPtr, p))))
	}
	header.hikeysEnd = OffsetNumber(util.PointerSub(hikeysPtr, p))
	header.itemsCount = count
}

func splitPageByChunks(
	desc *BTDesc,
	p unsafe.Pointer,
) {
	var (
		loc       BTPageItemLocator
		items     [BT_PAGE_MAX_CHUNK_ITEMS]BTPageItem
		hikey     FixedKey
		hikeySize LocationIndex
	)
	i := 0

	//collect items
	for BTPageLocatorFirst(p, &loc); BTPageLocatorIsValid(p, &loc); BTPageLocatorNext(p, &loc) {
		items[i].data = BTPageLocatorGetItem(p, &loc)
		items[i].flags = uint8(BTPageGetItemFlags(p, &loc))
		items[i].size = BTPageGetItemSize(p, &loc)
		items[i].newItem = false
		i++
	}

	if PageIs(p, BTREE_FLAG_RIGHTMOST) {
		TupleSetNULL(&hikey.tuple)
		hikeySize = 0
	} else {
		copyFixedHikey(desc, &hikey, p)
		hikeySize = BTPageGetHikeySize(p)
	}

	BTPageReorg(desc, p, items, OffsetNumber(i), hikeySize, hikey.tuple, nil)
}

// load chunk data from partial to img
func partialLoadChunk(
	partial *PartialPageState,
	img unsafe.Pointer,
	chunkOffset OffsetNumber,
) bool {
	var (
		imgState, srcState   uint32
		src                  unsafe.Pointer
		chunkBegin, chunkEnd LocationIndex
	)
	src = partial.src
	header := (*BTPageHeader)(img)

	if !partial.isPartial || partial.chunkIsLoaded[chunkOffset] {
		return true
	}

	chkdesc := header.GetChunkDesc(int(chunkOffset))
	chunkBegin = LocationIndex(ShortGetLocation(chkdesc.GetShortLocation()))
	if chunkOffset+1 < header.chunksCount {
		chunkEnd = LocationIndex(ShortGetLocation(header.GetChunkDesc(int(chunkOffset + 1)).GetShortLocation()))
	} else {
		chunkEnd = header.dataSize
	}

	util.AssertFunc(chunkBegin >= 0 && chunkBegin <= BLOCK_SIZE)
	util.AssertFunc(chunkEnd >= 0 && chunkEnd <= BLOCK_SIZE)

	util.CMemcpy(
		util.PointerAdd(img, int(chunkBegin)),
		util.PointerAdd(src, int(chunkBegin)),
		int(chunkEnd-chunkBegin))

	imgHeader := (*BTPageHeader)(img)
	srcHeader := (*BTPageHeader)(src)

	imgState = atomic.LoadUint32(&(imgHeader.header.stateAtomic))
	srcState = atomic.LoadUint32(&(srcHeader.header.stateAtomic))

	if (imgState&PageStateChangeCountMask) != (srcState&PageStateChangeCountMask) ||
		PageStateReadIsBlocked(srcState) {
		return false
	}

	if PageGetChangeCount(img) != PageGetChangeCount(src) {
		return false
	}

	partial.chunkIsLoaded[chunkOffset] = true
	return true
}

func pageLocatorFitsItem(
	desc *BTDesc,
	p unsafe.Pointer,
	locator *BTPageItemLocator,
	size LocationIndex,
	replace bool,
	csn CommitSeqNo,
) BTItemPageFitType {
	freeSpace := int(BTPageFreeSpace(p))
	spaceNeeded := int(size)
	oldItemSize := LocationIndex(0)

	util.AssertFunc(spaceNeeded == util.AlignValue8(spaceNeeded))

	if !replace {
		//add item array extend size
		aLen := util.AlignValue8(LocationIndexSize * uint32(locator.chunkItemsCount+1))
		bLen := util.AlignValue8(LocationIndexSize * uint32(locator.chunkItemsCount))
		spaceNeeded += int(aLen - bLen)
	} else {
		oldItemSize = LocationIndex(BTPageGetItemSize(p, locator))
		//only leaf page can replace item
		util.AssertFunc(PageIs(p, BTREE_FLAG_LEAF))
		//substract already occupied space in replace mode
		spaceNeeded -= int(oldItemSize)
		util.AssertFunc(spaceNeeded == util.AlignValue8(spaceNeeded))
	}

	if freeSpace >= spaceNeeded {
		//enough free space
		return BTItemPageFitAsIs
	} else if PageIs(p, BTREE_FLAG_LEAF) && desc.idxType != IndexBridge {
		//try to compact first on leaf page
		//optimistic estimated free space
		compactedFreeSpace := freeSpace + int(PageGetNVacated(p))
		if replace {
			tupHdr := (*BTLeafTuphdr)(BTPageLocatorGetItem(p, locator))
			if tupHdr.IsDeleted() &&
				XactInfoFinishedForEverybody(tupHdr.GetXactInfo()) {
				// util.AssertFunc(COMMITSEQNO_IS_INPROGRESS(csn) ||
				// 	XACT_INFO_MAP_CSN(tupHdr.GetXactInfo()) < csn)
				//substract already occupied space in replace mode
				compactedFreeSpace -= int(oldItemSize)
			}
		}

		//no enough free space, need to split
		if compactedFreeSpace < spaceNeeded {
			return BTItemPageFitSplitRequired
		}
		//real estimated free space
		compactedFreeSpace -= int(PageGetNVacated(p)) -
			int(pageGetVacatedSpace(desc, p, csn))
		if compactedFreeSpace >= spaceNeeded {
			return BTItemPageFitCompactRequired
		} else {
			return BTItemPageFitSplitRequired
		}
	} else {
		return BTItemPageFitSplitRequired
	}
}
