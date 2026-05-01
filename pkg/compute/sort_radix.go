package compute

import (
	"sort"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func RadixSort(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	sortingSize int,
	sortLayout *SortLayout,
	containsString bool,
) {
	if containsString {
		begin := NewPDQIterator(dataPtr, sortLayout._entrySize)
		end := begin.plusCopy(count)
		constants := NewPDQConstants(sortLayout._entrySize, colOffset, sortingSize, end.ptr())
		pdqsortBranchless(begin, &end, constants)
	} else if count <= INSERTION_SORT_THRESHOLD {
		InsertionSort(
			dataPtr,
			nil,
			count,
			0,
			sortLayout._entrySize,
			sortLayout._comparisonSize,
			0,
			false,
		)
	} else if sortingSize <= MSD_RADIX_SORT_SIZE_THRESHOLD {
		RadixSortLSD(
			dataPtr,
			count,
			colOffset,
			sortLayout._entrySize,
			sortingSize,
		)
	} else {
		tempPtr := util.CMalloc(max(count*sortLayout._entrySize, BLOCK_SIZE))
		defer util.CFree(tempPtr)
		preAllocPtr := util.CMalloc(sortingSize * MSD_RADIX_LOCATIONS * int(unsafe.Sizeof(uint64(0))))
		defer util.CFree(preAllocPtr)
		RadixSortMSD(
			dataPtr,
			tempPtr,
			count,
			colOffset,
			sortLayout._entrySize,
			sortingSize,
			0,
			util.PointerToSlice[uint64](preAllocPtr, sortingSize*MSD_RADIX_LOCATIONS),
			false,
		)
	}
}

func SubSortTiedTuples(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	sortingSize int,
	ties []bool,
	layout *SortLayout,
	containsString bool) {
	util.AssertFunc(!ties[count-1])
	for i := 0; i < count; i++ {
		if !ties[i] {
			continue
		}

		var j int
		for j = i + 1; j < count; j++ {
			if !ties[j] {
				break
			}
		}
		RadixSort(
			util.PointerAdd(dataPtr, i*layout._entrySize),
			j-i+1,
			colOffset,
			sortingSize,
			layout,
			containsString,
		)
		i = j
	}
}

func ComputeTies(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	tieSize int,
	ties []bool,
	layout *SortLayout) {
	util.AssertFunc(!ties[count-1])
	util.AssertFunc(colOffset+tieSize <= layout._comparisonSize)
	dataPtr = util.PointerAdd(dataPtr, colOffset)
	for i := 0; i < count-1; i++ {
		ties[i] = ties[i] &&
			util.PointerMemcmp(
				dataPtr,
				util.PointerAdd(dataPtr, layout._entrySize),
				tieSize,
			) == 0
		dataPtr = util.PointerAdd(dataPtr, layout._entrySize)
	}
}

func SortTiedBlobs(
	sb *SortedBlock,
	ties []bool,
	dataPtr unsafe.Pointer,
	count int,
	tieCol int,
	layout *SortLayout) {
	util.AssertFunc(!ties[count-1])
	block := util.Back(sb._blobSortingData._dataBlocks)
	blobPtr := block._ptr
	for i := 0; i < count; i++ {
		if !ties[i] {
			continue
		}
		var j int
		for j = i; j < count; j++ {
			if !ties[j] {
				break
			}
		}
		SortTiedBlobs2(
			dataPtr,
			i,
			j+1,
			tieCol,
			ties,
			blobPtr,
			layout,
		)
		i = j
	}
}

func SortTiedBlobs2(
	dataPtr unsafe.Pointer,
	start int,
	end int,
	tieCol int,
	ties []bool,
	blobPtr unsafe.Pointer,
	layout *SortLayout,
) {
	rowWidth := layout._blobLayout.rowWidth()
	rowPtr := util.PointerAdd(dataPtr, start*layout._entrySize)
	x := int(util.Load[uint32](util.PointerAdd(rowPtr, layout._comparisonSize)))
	blobRowPtr := util.PointerAdd(
		blobPtr,
		x*rowWidth,
	)
	if !TieIsBreakable(
		tieCol,
		blobRowPtr,
		layout,
	) {
		return
	}

	entryPtrsBase := util.CMalloc((end - start) * common.PointerSize)
	defer util.CFree(entryPtrsBase)

	//prepare pointer
	entryPtrs := util.PointerToSlice[unsafe.Pointer](entryPtrsBase, end-start)
	for i := start; i < end; i++ {
		entryPtrs[i-start] = rowPtr
		rowPtr = util.PointerAdd(rowPtr, layout._entrySize)
	}

	//sort string
	order := 1
	if layout._orderTypes[tieCol] == OT_DESC {
		order = -1
	}
	colIdx := layout._sortingToBlobCol[tieCol]
	tieColOffset := layout._blobLayout.GetOffsets()[colIdx]
	logicalType := layout._blobLayout.GetTypes()[colIdx]
	sort.Slice(entryPtrs, func(i, j int) bool {
		lPtr := entryPtrs[i]
		rPtr := entryPtrs[j]
		lIdx := util.Load[uint32](util.PointerAdd(lPtr, layout._comparisonSize))
		rIdx := util.Load[uint32](util.PointerAdd(rPtr, layout._comparisonSize))
		leftPtr := util.PointerAdd(blobPtr, int(lIdx)*rowWidth+tieColOffset)
		rightPtr := util.PointerAdd(blobPtr, int(rIdx)*rowWidth+tieColOffset)
		return order*CompareVal(leftPtr, rightPtr, logicalType) < 0
	})

	//reorder
	tempBasePtr := util.CMalloc((end - start) * layout._entrySize)
	defer util.CFree(tempBasePtr)
	tempPtr := tempBasePtr

	for i := 0; i < end-start; i++ {
		util.PointerCopy(tempPtr, entryPtrs[i], layout._entrySize)
		tempPtr = util.PointerAdd(tempPtr, layout._entrySize)
	}

	util.PointerCopy(
		util.PointerAdd(dataPtr, start*layout._entrySize),
		tempBasePtr,
		(end-start)*layout._entrySize,
	)
	//check ties
	if tieCol < layout._columnCount-1 {
		idxPtr := util.PointerAdd(dataPtr,
			start*layout._entrySize+layout._comparisonSize)
		idxVal := util.Load[uint32](idxPtr)
		currentPtr := util.PointerAdd(blobPtr, int(idxVal)*rowWidth+tieColOffset)
		for i := 0; i < (end - start - 1); i++ {
			idxPtr = util.PointerAdd(idxPtr, layout._entrySize)
			idxVal2 := util.Load[uint32](idxPtr)
			nextPtr := util.PointerAdd(blobPtr, int(idxVal2)*rowWidth+tieColOffset)
			ret := CompareVal(currentPtr, nextPtr, logicalType) == 0
			ties[start+i] = ret
			currentPtr = nextPtr
		}
	}
}

func AnyTies(ties []bool, count int) bool {
	util.AssertFunc(!ties[count-1])
	anyTies := false
	for i := 0; i < count-1; i++ {
		anyTies = anyTies || ties[i]
	}
	return anyTies
}

func RadixScatter(
	v *chunk.Vector,
	vcount int,
	sel *chunk.SelectVector,
	serCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	prefixLen int,
	width int,
	offset int,
) {
	var vdata chunk.UnifiedFormat
	v.ToUnifiedFormat(vcount, &vdata)
	switch v.Typ().GetInternalType() {
	case common.BOOL:
	case common.INT32:
		TemplatedRadixScatter[int32](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			int32Encoder{},
		)
	case common.VARCHAR:
		RadixScatterStringVector(
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			prefixLen,
			offset,
		)
	case common.DECIMAL:
		TemplatedRadixScatter[common.Decimal](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			decimalEncoder{},
		)
	case common.DATE:
		TemplatedRadixScatter[common.Date](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			dateEncoder{},
		)
	case common.INT128:
		TemplatedRadixScatter[common.Hugeint](
			&vdata,
			sel,
			serCount,
			keyLocs,
			desc,
			hasNull,
			nullsFirst,
			offset,
			hugeEncoder{},
		)
	default:
		panic("usp")
	}
}

func TemplatedRadixScatter[T any](
	vdata *chunk.UnifiedFormat,
	sel *chunk.SelectVector,
	addCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	offset int,
	enc Encoder[T],
) {
	srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](vdata)
	if hasNull {
		mask := vdata.Mask
		valid := byte(0)
		if nullsFirst {
			valid = 1
		}
		invalid := 1 - valid
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			if mask.RowIsValid(uint64(srcIdx)) {
				//first byte
				util.Store[byte](valid, keyLocs[i])
				enc.EncodeData(util.PointerAdd(keyLocs[i], 1), &srcSlice[srcIdx])
				//desc , invert bits
				if desc {
					for s := 1; s < enc.TypeSize()+1; s++ {
						util.InvertBits(keyLocs[i], s)
					}
				}
			} else {
				util.Store[byte](invalid, keyLocs[i])
				util.Memset(util.PointerAdd(keyLocs[i], 1), 0, enc.TypeSize())
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], 1+enc.TypeSize())
		}
	} else {
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			enc.EncodeData(keyLocs[i], &srcSlice[srcIdx])
			if desc {
				for s := 0; s < enc.TypeSize(); s++ {
					util.InvertBits(keyLocs[i], s)
				}
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], enc.TypeSize())
		}
	}
}

func RadixSortLSD(
	dataPtr unsafe.Pointer,
	count int,
	colOffset int,
	rowWidth int,
	sortingSize int,
) {
	temp := util.CMalloc(count * rowWidth)
	defer util.CFree(temp)
	swap := false

	var counts [VALUES_PER_RADIX]uint64
	for r := 1; r <= sortingSize; r++ {
		util.Fill(counts[:], VALUES_PER_RADIX, 0)
		sourcePtr, targetPtr := dataPtr, temp
		if swap {
			sourcePtr, targetPtr = temp, dataPtr
		}
		offset := colOffset + sortingSize - r
		offsetPtr := util.PointerAdd(sourcePtr, offset)
		for i := 0; i < count; i++ {
			val := util.Load[byte](offsetPtr)
			counts[val]++
			offsetPtr = util.PointerAdd(offsetPtr, rowWidth)
		}

		maxCount := counts[0]
		for val := 1; val < VALUES_PER_RADIX; val++ {
			maxCount = max(maxCount, counts[val])
			counts[val] = counts[val] + counts[val-1]
		}
		if maxCount == uint64(count) {
			continue
		}

		rowPtr := util.PointerAdd(sourcePtr, (count-1)*rowWidth)
		for i := 0; i < count; i++ {
			val := util.Load[byte](util.PointerAdd(rowPtr, offset))
			counts[val]--
			radixOffset := counts[val]
			util.PointerCopy(
				util.PointerAdd(targetPtr, int(radixOffset)*rowWidth),
				rowPtr,
				rowWidth,
			)
			rowPtr = util.PointerAdd(rowPtr, -rowWidth)
		}
		swap = !swap
	}
	if swap {
		util.PointerCopy(
			dataPtr,
			temp,
			count*rowWidth,
		)
	}
}

func RadixSortMSD(
	origPtr unsafe.Pointer,
	tempPtr unsafe.Pointer,
	count int,
	colOffset int,
	rowWidth int,
	compWidth int,
	offset int,
	locations []uint64,
	swap bool,
) {
	sourcePtr, targetPtr := origPtr, tempPtr
	if swap {
		sourcePtr, targetPtr = tempPtr, origPtr
	}

	util.Fill[uint64](locations,
		MSD_RADIX_LOCATIONS,
		0,
	)
	counts := locations[1:]
	totalOffset := colOffset + offset
	offsetPtr := util.PointerAdd(sourcePtr, totalOffset)
	for i := 0; i < count; i++ {
		val := util.Load[byte](offsetPtr)
		counts[val]++
		offsetPtr = util.PointerAdd(offsetPtr, rowWidth)
	}

	maxCount := uint64(0)
	for radix := 0; radix < VALUES_PER_RADIX; radix++ {
		maxCount = max(maxCount, counts[radix])
		counts[radix] += locations[radix]
	}

	if maxCount != uint64(count) {
		rowPtr := sourcePtr
		for i := 0; i < count; i++ {
			val := util.Load[byte](util.PointerAdd(rowPtr, totalOffset))
			radixOffset := locations[val]
			locations[val]++
			util.PointerCopy(
				util.PointerAdd(targetPtr, int(radixOffset)*rowWidth),
				rowPtr,
				rowWidth,
			)
			rowPtr = util.PointerAdd(rowPtr, rowWidth)
		}
		swap = !swap
	}

	if offset == compWidth-1 {
		if swap {
			util.PointerCopy(
				origPtr,
				tempPtr,
				count*rowWidth,
			)
		}
		return
	}

	if maxCount == uint64(count) {
		RadixSortMSD(
			origPtr,
			tempPtr,
			count,
			colOffset,
			rowWidth,
			compWidth,
			offset+1,
			locations[MSD_RADIX_LOCATIONS:],
			swap,
		)
		return
	}

	radixCount := locations[0]
	for radix := 0; radix < VALUES_PER_RADIX; radix++ {
		loc := int(locations[radix]-radixCount) * rowWidth
		if radixCount > INSERTION_SORT_THRESHOLD {
			RadixSortMSD(
				util.PointerAdd(origPtr, loc),
				util.PointerAdd(tempPtr, loc),
				int(radixCount),
				colOffset,
				rowWidth,
				compWidth,
				offset+1,
				locations[MSD_RADIX_LOCATIONS:],
				swap,
			)
		} else if radixCount != 0 {
			InsertionSort(
				util.PointerAdd(origPtr, loc),
				util.PointerAdd(tempPtr, loc),
				int(radixCount),
				colOffset,
				rowWidth,
				compWidth,
				offset+1,
				swap,
			)
		}
		radixCount = locations[radix+1] - locations[radix]
	}
}

func Scatter(
	columns *chunk.Chunk,
	colData []*chunk.UnifiedFormat,
	layout *RowLayout,
	rows *chunk.Vector,
	stringHeap *RowDataCollection,
	sel *chunk.SelectVector,
	count int,
) {
	if count == 0 {
		return
	}

	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)
	for i := 0; i < count; i++ {
		ridx := sel.GetIndex(i)
		rowPtr := ptrs[ridx]
		bSlice := util.PointerToSlice[uint8](rowPtr, layout.CoumnCount())
		tempMask := util.Bitmap{Bits: bSlice}
		tempMask.SetAllValid(layout.CoumnCount())
	}

	//vcount := columns.card()
	offsets := layout.GetOffsets()
	types := layout.GetTypes()

	//compute the entry size of the variable size columns
	dataLocs := make([]unsafe.Pointer, util.DefaultVectorSize)
	if !layout.AllConstant() {
		entrySizes := make([]int, util.DefaultVectorSize)
		util.Fill(entrySizes, count, common.Int32Size)
		for colNo := 0; colNo < len(types); colNo++ {
			if types[colNo].GetInternalType().IsConstant() {
				continue
			}
			//vec := columns.Data[colNo]
			col := colData[colNo]
			switch types[colNo].GetInternalType() {
			case common.VARCHAR:
				ComputeStringEntrySizes(col, entrySizes, sel, count, 0)
			default:
				panic("usp internal type")
			}
		}
		stringHeap.Build(count, dataLocs, entrySizes, chunk.IncrSelectVectorInPhyFormatFlat())

		heapPointerOffset := layout.GetHeapOffset()
		for i := 0; i < count; i++ {
			rowIdx := sel.GetIndex(i)
			rowPtr := ptrs[rowIdx]
			util.Store[unsafe.Pointer](dataLocs[i], util.PointerAdd(rowPtr, heapPointerOffset))
			util.Store[uint32](uint32(entrySizes[i]), dataLocs[i])
			dataLocs[i] = util.PointerAdd(dataLocs[i], common.Int32Size)
		}
	}

	for colNo := 0; colNo < len(types); colNo++ {
		col := colData[colNo]
		colOffset := offsets[colNo]
		switch types[colNo].GetInternalType() {
		case common.INT32:
			TemplatedScatter[int32](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.Int32ScatterOp{},
			)
		case common.INT64:
			TemplatedScatter[int64](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.Int64ScatterOp{},
			)
		case common.VARCHAR:
			ScatterStringVector(
				col,
				rows,
				dataLocs,
				sel,
				count,
				colOffset,
				colNo,
				layout,
			)
		case common.DATE:
			TemplatedScatter[common.Date](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.DateScatterOp{},
			)
		case common.DECIMAL:
			TemplatedScatter[common.Decimal](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.DecimalScatterOp{},
			)
		case common.DOUBLE:
			TemplatedScatter[float64](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.Float64ScatterOp{},
			)
		case common.INT128:
			TemplatedScatter[common.Hugeint](
				col,
				rows,
				sel,
				count,
				colOffset,
				colNo,
				layout,
				chunk.HugeintScatterOp{},
			)
		default:
			panic("usp")
		}
	}
}

func ScatterStringVector(
	col *chunk.UnifiedFormat,
	rows *chunk.Vector,
	strLocs []unsafe.Pointer,
	sel *chunk.SelectVector,
	count int,
	colOffset int,
	colNo int,
	layout *RowLayout,
) {
	strSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](col)
	ptrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)

	nullStr := chunk.StringScatterOp{}.NullValue()
	for i := 0; i < count; i++ {
		idx := sel.GetIndex(i)
		colIdx := col.Sel.GetIndex(idx)
		rowPtr := ptrSlice[idx]
		if !col.Mask.RowIsValid(uint64(colIdx)) {
			colMask := util.Bitmap{
				Bits: util.PointerToSlice[byte](rowPtr, layout._flagWidth),
			}
			colMask.SetInvalidUnsafe(uint64(colNo))
			util.Store[common.String](nullStr, util.PointerAdd(rowPtr, colOffset))
		} else {
			str := strSlice[colIdx]
			newStr := common.String{
				Len:  str.Length(),
				Data: strLocs[i],
			}
			//copy varchar data from input chunk to
			//the location on the string heap
			util.PointerCopy(newStr.Data, str.DataPtr(), str.Length())
			//move strLocs[i] to the next position
			strLocs[i] = util.PointerAdd(strLocs[i], str.Length())

			//store new String obj to the row in the blob sort block
			util.Store[common.String](newStr, util.PointerAdd(rowPtr, colOffset))
		}
	}
}

func RadixScatterStringVector(
	vdata *chunk.UnifiedFormat,
	sel *chunk.SelectVector,
	addCount int,
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	prefixLen int,
	offset int,
) {
	sourceSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](vdata)
	if hasNull {
		mask := vdata.Mask
		valid := byte(0)
		if nullsFirst {
			valid = 1
		}
		invalid := 1 - valid

		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			if mask.RowIsValid(uint64(srcIdx)) {
				util.Store[byte](valid, keyLocs[i])
				EncodeStringDataPrefix(
					util.PointerAdd(keyLocs[i], 1),
					&sourceSlice[srcIdx],
					prefixLen,
				)
				//invert bits
				if desc {
					for s := 1; s < prefixLen+1; s++ {
						util.InvertBits(keyLocs[i], s)
					}
				}
			} else {
				util.Store[byte](invalid, keyLocs[i])
				util.Memset(
					util.PointerAdd(keyLocs[i], 1),
					0,
					prefixLen,
				)
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], prefixLen+1)
		}
	} else {
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			EncodeStringDataPrefix(
				keyLocs[i],
				&sourceSlice[srcIdx],
				prefixLen,
			)
			//invert bits
			if desc {
				for s := 0; s < prefixLen; s++ {
					util.InvertBits(keyLocs[i], s)
				}
			}
			keyLocs[i] = util.PointerAdd(keyLocs[i], prefixLen)
		}
	}
}

func EncodeStringDataPrefix(
	dataPtr unsafe.Pointer,
	value *common.String,
	prefixLen int) {
	l := value.Length()
	util.PointerCopy(dataPtr, value.DataPtr(), min(l, prefixLen))

	if l < prefixLen {
		util.Memset(util.PointerAdd(dataPtr, l), 0, prefixLen-l)
	}
}

func ComputeStringEntrySizes(
	col *chunk.UnifiedFormat,
	entrySizes []int,
	sel *chunk.SelectVector,
	count int,
	offset int,
) {
	data := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](col)
	for i := 0; i < count; i++ {
		idx := sel.GetIndex(i)
		colIdx := col.Sel.GetIndex(idx) + offset
		str := data[colIdx]
		if col.Mask.RowIsValid(uint64(colIdx)) {
			entrySizes[i] += str.Length()
		}
	}
}

func TemplatedScatter[T any](
	col *chunk.UnifiedFormat,
	rows *chunk.Vector,
	sel *chunk.SelectVector,
	count int,
	colOffset int,
	colNo int,
	layout *RowLayout,
	sop chunk.ScatterOp[T],
) {
	data := chunk.GetSliceInPhyFormatUnifiedFormat[T](col)
	ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)

	if !col.Mask.AllValid() {
		for i := 0; i < count; i++ {
			idx := sel.GetIndex(i)
			colIdx := col.Sel.GetIndex(idx)
			rowPtr := ptrs[idx]

			isNull := !col.Mask.RowIsValid(uint64(colIdx))
			var val T
			if isNull {
				val = sop.NullValue()
			} else {
				val = data[colIdx]
			}

			util.Store[T](val, util.PointerAdd(rowPtr, colOffset))
			if isNull {
				mask := util.Bitmap{
					Bits: util.PointerToSlice[uint8](ptrs[idx], layout.rowWidth()),
				}
				mask.SetInvalidUnsafe(uint64(colNo))
			}
		}
	} else {
		for i := 0; i < count; i++ {
			idx := sel.GetIndex(i)
			colIdx := col.Sel.GetIndex(idx)
			rowPtr := ptrs[idx]
			util.Store[T](data[colIdx], util.PointerAdd(rowPtr, colOffset))
		}
	}
}

func TieIsBreakable(
	tieCol int,
	rowPtr unsafe.Pointer,
	layout *SortLayout,
) bool {
	colIdx := layout._sortingToBlobCol[tieCol]
	rowMask := util.Bitmap{
		Bits: util.PointerToSlice[byte](rowPtr, layout._blobLayout._flagWidth),
	}
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colIdx))
	if !util.RowIsValidInEntry(
		rowMask.GetEntry(entryIdx),
		idxInEntry,
	) {
		//can not create a NULL tie
		return false
	}

	rowLayout := layout._blobLayout
	if !rowLayout.GetTypes()[colIdx].GetInternalType().IsVarchar() {
		//nested type
		return true
	}

	tieColOffset := rowLayout.GetOffsets()[colIdx]
	tieString := util.Load[common.String](util.PointerAdd(rowPtr, tieColOffset))
	return tieString.Length() >= layout._prefixLengths[tieCol]
}

func CompareVal(
	lPtr, rPtr unsafe.Pointer,
	typ common.LType,
) int {
	switch typ.GetInternalType() {
	case common.VARCHAR:
		return TemplatedCompareVal[common.String](
			lPtr,
			rPtr,
			binStringEqualOp,
			binStringLessOp,
		)
	default:
		panic("usp")
	}
}

func TemplatedCompareVal[T any](
	lPtr, rPtr unsafe.Pointer,
	equalOp BinaryOp[T, T, bool],
	lessOp BinaryOp[T, T, bool],
) int {
	lVal := util.Load[T](lPtr)
	rVal := util.Load[T](rPtr)
	eRet := false
	equalOp(&lVal, &rVal, &eRet)
	if eRet {
		return 0
	}
	lRet := false
	lessOp(&lVal, &rVal, &lRet)
	if lRet {
		return -1
	}
	return 1
}
