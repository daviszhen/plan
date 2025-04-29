package btree

func initNewBtreePage(
	desc *BTDesc,
	blkno Blkno,
	flags uint16,
	level uint16,
	noLock bool,
) {
	pagePtr := GetInMemPage(blkno)
	pageDesc := GetInMemPageDesc(blkno)
	header := (*BTPageHeader)(pagePtr)
	if !noLock {
		lockPage(blkno)
		pageBlockReads(blkno)
	}
}
