package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/util"
)

type SortedData struct {
	_type       SortedDataType
	_layout     *RowLayout
	_dataBlocks []*RowDataBlock
	_heapBlocks []*RowDataBlock
}

func (d SortedData) Count() int {
	cnt := 0
	for _, blk := range d._dataBlocks {
		cnt += blk._count
	}
	return cnt
}

func NewSortedData(typ SortedDataType, layout *RowLayout) *SortedData {
	ret := &SortedData{
		_type:   typ,
		_layout: layout,
	}

	return ret
}

type SortedBlock struct {
	_radixSortingData []*RowDataBlock
	_blobSortingData  *SortedData
	_payloadData      *SortedData
	_sortLayout       *SortLayout
	_payloadLayout    *RowLayout
}

func NewSortedBlock(sortLayout *SortLayout, payloadLayout *RowLayout) *SortedBlock {
	ret := &SortedBlock{
		_sortLayout:    sortLayout,
		_payloadLayout: payloadLayout,
	}

	ret._blobSortingData = NewSortedData(SDT_BLOB, sortLayout._blobLayout)
	ret._payloadData = NewSortedData(SDT_PAYLOAD, payloadLayout)
	return ret
}

type BlockAppendEntry struct {
	_basePtr unsafe.Pointer
	_count   int
}

type RowDataCollection struct {
	_count         int
	_blockCapacity int
	_entrySize     int
	_blocks        []*RowDataBlock
}

func NewRowDataCollection(bcap int, entSize int) *RowDataCollection {
	ret := &RowDataCollection{
		_blockCapacity: bcap,
		_entrySize:     entSize,
	}

	return ret
}

func (cdc *RowDataCollection) Build(
	addedCnt int,
	keyLocs []unsafe.Pointer,
	entrySizes []int,
	sel *chunk.SelectVector) {
	appendEntries := make([]BlockAppendEntry, 0)
	remaining := addedCnt
	{
		//to last block
		cdc._count += remaining
		if len(cdc._blocks) != 0 {
			lastBlock := util.Back(cdc._blocks)
			if lastBlock._count < lastBlock._capacity {
				appendCnt := cdc.AppendToBlock(lastBlock, &appendEntries, remaining, entrySizes)
				remaining -= appendCnt
			}
		}
		for remaining > 0 {
			newBlock := cdc.CreateBlock()
			var offsetEntrySizes []int = nil
			if entrySizes != nil {
				offsetEntrySizes = entrySizes[addedCnt-remaining:]
			}
			appendCnt := cdc.AppendToBlock(newBlock, &appendEntries, remaining, offsetEntrySizes)
			util.AssertFunc(newBlock._count > 0)
			remaining -= appendCnt

		}
	}
	//fill keyLocs
	aidx := 0
	for _, entry := range appendEntries {
		next := aidx + entry._count
		if entrySizes != nil {
			for ; aidx < next; aidx++ {
				keyLocs[aidx] = entry._basePtr
				entry._basePtr = util.PointerAdd(entry._basePtr, entrySizes[aidx])
			}
		} else {
			for ; aidx < next; aidx++ {
				idx := sel.GetIndex(aidx)
				keyLocs[idx] = entry._basePtr
				entry._basePtr = util.PointerAdd(entry._basePtr, cdc._entrySize)
			}
		}
	}
}

func (cdc *RowDataCollection) AppendToBlock(
	block *RowDataBlock,
	appendEntries *[]BlockAppendEntry,
	remaining int,
	entrySizes []int) int {
	appendCnt := 0
	var dataPtr unsafe.Pointer
	if entrySizes != nil {
		util.AssertFunc(cdc._entrySize == 1)
		dataPtr = util.PointerAdd(block._ptr, block._byteOffset)
		for i := 0; i < remaining; i++ {
			if block._byteOffset+entrySizes[i] > block._capacity {
				if block._count == 0 &&
					appendCnt == 0 &&
					entrySizes[i] > block._capacity {
					block._capacity = entrySizes[i]
					block._ptr = util.CRealloc(block._ptr, block._capacity)
					dataPtr = block._ptr
					appendCnt++
					block._byteOffset += entrySizes[i]
				}
				break
			}
			appendCnt++
			block._byteOffset += entrySizes[i]
		}
	} else {
		appendCnt = min(remaining, block._capacity-block._count)
		dataPtr = util.PointerAdd(block._ptr, block._count*block._entrySize)
	}
	*appendEntries = append(*appendEntries, BlockAppendEntry{
		_basePtr: dataPtr,
		_count:   appendCnt,
	})
	block._count += appendCnt
	return appendCnt
}

func (cdc *RowDataCollection) CreateBlock() *RowDataBlock {
	nb := NewRowDataBlock(cdc._blockCapacity, cdc._entrySize)
	cdc._blocks = append(cdc._blocks, nb)
	return nb
}

func (cdc *RowDataCollection) Close() {
	for _, block := range cdc._blocks {
		block.Close()
	}
	cdc._blocks = nil
	cdc._count = 0
}
