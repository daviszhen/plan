package btree

import "unsafe"

var (
	pageDescSize = int(unsafe.Sizeof(PageDesc{}))
)

type PageDesc struct {
	ionum      int
	flags_type uint32 //flags:4 , type:28
}

type BTPageChunkDesc struct {
	fields uint32 //shortLocation:12,offset:10,hikeyShortLocation:8,hikeyFlags:2
}

type BTPageHeader struct {
	header           PageHeader
	checkpointNum    uint32
	undoLocation     UndoLocation
	csn              CommitSeqNo
	rightLink        uint64
	flagsF1F2        uint32 //flags:6, f1:11, f2:15
	maxKeyLen        LocationIndex
	prevInsertOffset OffsetNumber
	chunksCount      OffsetNumber
	itemsCount       OffsetNumber
	hikeysEnd        OffsetNumber
	dataSize         LocationIndex
	chunksDesc       [1]BTPageChunkDesc
}
