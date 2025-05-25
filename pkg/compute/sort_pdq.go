package compute

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/util"
)

type PDQConstants struct {
	_tmpBuf         unsafe.Pointer
	_swapOffsetsBuf unsafe.Pointer
	_iterSwapBuf    unsafe.Pointer
	_end            unsafe.Pointer
	_compOffset     int
	_compSize       int
	_entrySize      int
}

func NewPDQConstants(
	entrySize int,
	compOffset int,
	compSize int,
	end unsafe.Pointer,
) *PDQConstants {
	ret := &PDQConstants{
		_entrySize:      entrySize,
		_compOffset:     compOffset,
		_compSize:       compSize,
		_tmpBuf:         util.CMalloc(entrySize),
		_iterSwapBuf:    util.CMalloc(entrySize),
		_swapOffsetsBuf: util.CMalloc(entrySize),
		_end:            end,
	}

	return ret
}

func (pconst *PDQConstants) Close() {
	util.CFree(pconst._tmpBuf)
	util.CFree(pconst._iterSwapBuf)
	util.CFree(pconst._swapOffsetsBuf)
}

type PDQIterator struct {
	_ptr       unsafe.Pointer
	_entrySize int
}

func NewPDQIterator(ptr unsafe.Pointer, entrySize int) *PDQIterator {
	return &PDQIterator{
		_ptr:       ptr,
		_entrySize: entrySize,
	}
}

func (iter *PDQIterator) ptr() unsafe.Pointer {
	return iter._ptr
}

func (iter *PDQIterator) plus(n int) {
	iter._ptr = util.PointerAdd(iter._ptr, n*iter._entrySize)
}

func (iter PDQIterator) plusCopy(n int) PDQIterator {
	return PDQIterator{
		_ptr:       util.PointerAdd(iter._ptr, n*iter._entrySize),
		_entrySize: iter._entrySize,
	}
}

func pdqIterLess(lhs, rhs *PDQIterator) bool {
	return util.PointerLess(lhs.ptr(), rhs.ptr())
}

func pdqIterDiff(lhs, rhs *PDQIterator) int {
	tlen := util.PointerSub(lhs.ptr(), rhs.ptr())
	util.AssertFunc(tlen%int64(lhs._entrySize) == 0)
	util.AssertFunc(tlen >= 0)
	return int(tlen / int64(lhs._entrySize))
}

func pdqIterEqaul(lhs, rhs *PDQIterator) bool {
	return lhs.ptr() == rhs.ptr()
}

func pdqIterNotEqaul(lhs, rhs *PDQIterator) bool {
	return !pdqIterEqaul(lhs, rhs)
}

func pdqsortBranchless(
	begin, end *PDQIterator,
	constants *PDQConstants) {
	if begin == end {
		return
	}
	pdqsortLoop(begin, end, constants, log2(pdqIterDiff(end, begin)) > 0, true, true)
}

//func pdqsort(
//	begin, end *PDQIterator,
//	constants *PDQConstants) {
//	if begin == end {
//		return
//	}
//	pdqsortLoop(begin, end, constants, log2(pdqIterDiff(end, begin)) > 0, true, false)
//}

func pdqsortLoop(
	begin, end *PDQIterator,
	constants *PDQConstants,
	badAllowed bool,
	leftMost bool,
	branchLess bool,
) {
	for {
		size := pdqIterDiff(end, begin)
		//insert sort
		if size < insertion_sort_threshold {
			if leftMost {
				insertSort(begin, end, constants)
			} else {
				unguardedInsertSort(begin, end, constants)
			}
			return
		}

		//pivot : median of 3
		//pseudomedian of 9
		s2 := size / 2
		if size > ninther_threshold {
			b0 := begin.plusCopy(s2)
			c0 := end.plusCopy(-1)
			sort3(begin, &b0, &c0, constants)

			a1 := begin.plusCopy(1)
			b1 := begin.plusCopy(s2 - 1)
			c1 := end.plusCopy(-2)
			sort3(&a1, &b1, &c1, constants)

			a2 := begin.plusCopy(2)
			b2 := begin.plusCopy(s2 + 1)
			c2 := end.plusCopy(-3)
			sort3(&a2, &b2, &c2, constants)

			a3 := begin.plusCopy(s2 - 1)
			b3 := begin.plusCopy(s2)
			c3 := begin.plusCopy(s2 + 1)
			sort3(&a3, &b3, &c3, constants)
		} else {
			a0 := begin.plusCopy(s2)
			c0 := end.plusCopy(-1)
			sort3(&a0, begin, &c0, constants)
		}

		if !leftMost {
			a0 := begin.plusCopy(-1)
			if !comp(a0.ptr(), begin.ptr(), constants) {
				b0 := partitionLeft(begin, end, constants)
				b0.plus(1)
				begin = &b0
				continue
			}
		}

		var pivotPos PDQIterator
		var alreadyPartitioned bool
		if branchLess {
			pivotPos, alreadyPartitioned = partitionRightBranchless(begin, end, constants)
		} else {
			pivotPos, alreadyPartitioned = partitionRight(begin, end, constants)
		}

		lSize := pdqIterDiff(&pivotPos, begin)
		x := pivotPos.plusCopy(1)
		rSize := pdqIterDiff(end, &x)
		highlyUnbalanced := lSize < size/8 || rSize < size/8
		if highlyUnbalanced {
			if lSize > insertion_sort_threshold {
				b0 := begin.plusCopy(lSize / 4)
				iterSwap(begin, &b0, constants)

				a1 := pivotPos.plusCopy(-1)
				b1 := pivotPos.plusCopy(-lSize / 4)
				iterSwap(&a1, &b1, constants)

				if lSize > ninther_threshold {
					a2 := begin.plusCopy(1)
					b2 := begin.plusCopy(lSize/4 + 1)
					iterSwap(&a2, &b2, constants)

					a3 := begin.plusCopy(2)
					b3 := begin.plusCopy(lSize/4 + 2)
					iterSwap(&a3, &b3, constants)

					a4 := pivotPos.plusCopy(-2)
					b4 := pivotPos.plusCopy(-(lSize/4 + 1))
					iterSwap(&a4, &b4, constants)

					a5 := pivotPos.plusCopy(-3)
					b5 := pivotPos.plusCopy(-(lSize/4 + 2))
					iterSwap(&a5, &b5, constants)
				}
			}

			if rSize > insertion_sort_threshold {
				a0 := pivotPos.plusCopy(1)
				b0 := pivotPos.plusCopy(rSize/4 + 1)
				iterSwap(&a0, &b0, constants)

				a1 := end.plusCopy(-1)
				b1 := end.plusCopy(-(rSize / 4))
				iterSwap(&a1, &b1, constants)

				if rSize > ninther_threshold {
					a2 := pivotPos.plusCopy(2)
					b2 := pivotPos.plusCopy(rSize/4 + 2)
					iterSwap(&a2, &b2, constants)

					a3 := pivotPos.plusCopy(3)
					b3 := pivotPos.plusCopy(rSize/4 + 3)
					iterSwap(&a3, &b3, constants)

					a4 := end.plusCopy(-2)
					b4 := end.plusCopy(-(1 + rSize/4))
					iterSwap(&a4, &b4, constants)

					a5 := end.plusCopy(-3)
					b5 := end.plusCopy(-(2 + rSize/4))
					iterSwap(&a5, &b5, constants)
				}
			}
		} else {
			if alreadyPartitioned {
				if partialInsertionSort(begin, &pivotPos, constants) {
					x = pivotPos.plusCopy(1)
					if partialInsertionSort(&x, end, constants) {
						return
					}
				}
			}
		}

		//sort left part
		pdqsortLoop(begin, &pivotPos, constants, badAllowed, leftMost, branchLess)
		x = pivotPos.plusCopy(1)
		begin = &x
		leftMost = false
	}
}

func partialInsertionSort(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) bool {
	if pdqIterEqaul(begin, end) {
		return true
	}
	limit := uint64(0)
	for cur := begin.plusCopy(1); pdqIterNotEqaul(&cur, end); cur.plus(1) {
		sift := cur.plusCopy(0)
		sift_1 := cur.plusCopy(-1)
		if comp(sift.ptr(), sift_1.ptr(), constants) {
			tmp := GetTmp(sift.ptr(), constants)
			for {
				Move(sift.ptr(), sift_1._ptr, constants)
				sift.plus(-1)
				if pdqIterNotEqaul(&sift, begin) {
					sift_1.plus(-1)
					if comp(tmp, sift_1.ptr(), constants) {
						continue
					} else {
						break
					}
				}
			}

			Move(sift.ptr(), tmp, constants)
			limit += uint64(pdqIterDiff(&cur, &sift))
		}

		if limit > partial_insertion_sort_limit {
			return false
		}
	}
	return true
}

func partitionRight(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) (PDQIterator, bool) {
	pivot := GetTmp(begin.ptr(), constants)

	first := begin.plusCopy(0)
	last := end.plusCopy(0)

	//find the first one *first >= *pivot in [begin+1,...)
	for {
		first.plus(1)
		if comp(first.ptr(), pivot, constants) {
			continue
		} else {
			break
		}
	}

	//*(begin+1) >= *begin
	if pdqIterDiff(&first, begin) == 1 {
		for pdqIterLess(&first, &last) {
			last.plus(-1)
			//find the first one stricter *last < *pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			last.plus(-1)
			//find the first one stricter *last < *pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	}

	//first >= last, no pair need to be swapped
	alreadyPartitioned := !pdqIterLess(&first, &last)

	//keep swap pairs in the wrong place
	for pdqIterLess(&first, &last) {
		iterSwap(&first, &last, constants)
		for {
			first.plus(1)
			if comp(first.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
		for {
			last.plus(-1)
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	}

	pivotPos := first.plusCopy(-1)
	Move(begin.ptr(), pivotPos.ptr(), constants)
	Move(pivotPos.ptr(), pivot, constants)
	return pivotPos, alreadyPartitioned
}

// partitionRightBranchless split the [begin,end).
// the ones equal to the pivot are put in the right part.
// return
//
//	the position of the pivot.
//	already split rightly
func partitionRightBranchless(
	begin *PDQIterator,
	end *PDQIterator,
	constants *PDQConstants) (PDQIterator, bool) {
	pivot := GetTmp(begin.ptr(), constants)
	first := begin.plusCopy(0)
	last := end.plusCopy(0)

	//find the one *first >= *pivot
	for {
		first.plus(1)
		//pass A[first] < A[pivot]
		if comp(first.ptr(), pivot, constants) {
			continue
		} else {
			break
		}
	}

	//begin + 1 == first. A[first] >= pivot
	//find the *last strictly < *pivot
	if pdqIterDiff(&first, begin) == 1 {
		for pdqIterLess(&first, &last) {
			last.plus(-1)
			//pass A[last] >= pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			last.plus(-1)
			//pass A[last] >= pivot
			if !comp(last.ptr(), pivot, constants) {
				continue
			} else {
				break
			}
		}
	}

	//first >= last, no pair need to be swapped
	alreadyPartitioned := !pdqIterLess(&first, &last)
	{
		//swap data in wrong positions
		if !alreadyPartitioned {
			iterSwap(&first, &last, constants)
			first.plus(1)

			var offsetsLArr [block_size + cacheline_size]byte
			var offsetsRArr [block_size + cacheline_size]byte
			offsetsL := offsetsLArr[:]
			offsetsR := offsetsRArr[:]
			offsetsLBase := first.plusCopy(0)
			offsetsRBase := last.plusCopy(0)
			var numL, numR, startL, startR uint64
			numL, numR, startL, startR = 0, 0, 0, 0
			//block partitioning
			for pdqIterLess(&first, &last) {
				//decide the count of two offsets
				numUnknown := uint64(pdqIterDiff(&last, &first))
				leftSplit, rightSplit := uint64(0), uint64(0)
				if numL == 0 {
					if numR == 0 {
						leftSplit = numUnknown / 2
					} else {
						leftSplit = numUnknown
					}
				} else {
					leftSplit = 0
				}
				if numR == 0 {
					rightSplit = numUnknown - leftSplit
				} else {
					rightSplit = 0
				}

				//fill left offsets
				if leftSplit >= block_size {
					for i := 0; i < block_size; {
						for j := 0; j < 8; j++ {
							offsetsL[numL] = byte(i)
							i++
							if !comp(first.ptr(), pivot, constants) {
								numL += 1
							}
							first.plus(1)
						}
					}
				} else {
					for i := uint64(0); i < leftSplit; {
						offsetsL[numL] = byte(i)
						i++
						if !comp(first.ptr(), pivot, constants) {
							numL += 1
						}
						first.plus(1)
					}
				}

				if rightSplit >= block_size {
					for i := 0; i < block_size; {
						for j := 0; j < 8; j++ {
							i++
							offsetsR[numR] = byte(i)
							last.plus(-1)
							if comp(last.ptr(), pivot, constants) {
								numR += 1
							}
						}
					}
				} else {
					for i := uint64(0); i < rightSplit; {
						i++
						offsetsR[numR] = byte(i)
						last.plus(-1)
						if comp(last.ptr(), pivot, constants) {
							numR += 1
						}
					}
				}

				//swap data denotes by offsets
				num := min(numL, numR)
				swapOffsets(
					&offsetsLBase,
					&offsetsRBase,
					offsetsL[startL:],
					offsetsR[startR:],
					num,
					numL == numR,
					constants,
				)
				numL -= num
				numR -= num
				startL += num
				startR += num

				if numL == 0 {
					startL = 0
					offsetsLBase = first.plusCopy(0)
				}

				if numR == 0 {
					startR = 0
					offsetsRBase = last.plusCopy(0)
				}
			}

			//fil the rest
			if numL != 0 {
				offsetsL = offsetsL[startL:]
				for numL > 0 {
					numL--
					lhs := offsetsLBase.plusCopy(int(offsetsL[numL]))
					last.plus(-1)
					iterSwap(&lhs, &last, constants)
				}
				first = last.plusCopy(0)
			}
			if numR != 0 {
				offsetsR = offsetsR[startR:]
				for numR > 0 {
					numR--
					lhs := offsetsRBase.plusCopy(-int(offsetsR[numR]))
					iterSwap(&lhs, &first, constants)
					first.plus(1)
				}
				last = first.plusCopy(0)
			}
		}

	}

	pivotPos := first.plusCopy(-1)
	Move(begin.ptr(), pivotPos.ptr(), constants)
	Move(pivotPos.ptr(), pivot, constants)
	return pivotPos, alreadyPartitioned
}

func swapOffsets(
	first *PDQIterator,
	last *PDQIterator,
	offsetsL []byte,
	offsetsR []byte,
	num uint64,
	useSwaps bool,
	constants *PDQConstants) {
	if useSwaps {
		for i := uint64(0); i < num; i++ {
			lhs := first.plusCopy(int(offsetsL[i]))
			rhs := last.plusCopy(-int(offsetsR[i]))
			iterSwap(&lhs, &rhs, constants)
		}
	} else if num > 0 {
		lhs := first.plusCopy(int(offsetsL[0]))
		rhs := last.plusCopy(-int(offsetsR[0]))
		tmp := SwapOffsetsGetTmp(lhs.ptr(), constants)
		Move(lhs.ptr(), rhs.ptr(), constants)
		for i := uint64(1); i < num; i++ {
			lhs = first.plusCopy(int(offsetsL[i]))
			Move(rhs.ptr(), lhs.ptr(), constants)
			rhs = last.plusCopy(-int(offsetsR[i]))
			Move(lhs.ptr(), rhs.ptr(), constants)
		}
		Move(rhs.ptr(), tmp, constants)
	}
}

func partitionLeft(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) PDQIterator {
	pivot := GetTmp(begin.ptr(), constants)
	first := begin.plusCopy(0)
	last := end.plusCopy(0)
	for {
		last.plus(-1)
		//pass A[pivot] < A[last]
		if comp(pivot, last.ptr(), constants) {
			continue
		} else {
			break
		}
	}
	//last + 1 == end. A[pivot] >= A[end-1]
	if pdqIterDiff(&last, end) == 1 {
		for pdqIterLess(&first, &last) {
			first.plus(1)
			//pass A[pivot] >= A[first]
			if !comp(pivot, first.ptr(), constants) {
				continue
			} else {
				break
			}
		}
	} else {
		for {
			first.plus(1)
			//pass A[pivot] >= A[first]
			if !comp(pivot, first.ptr(), constants) {
				continue
			} else {
				break
			}
		}
	}

	for pdqIterLess(&first, &last) {
		iterSwap(&first, &last, constants)
		for {
			last.plus(-1)
			//pass A[pivot] < A[last]
			if comp(pivot, last.ptr(), constants) {
				continue
			} else {
				break
			}
		}
		for {
			first.plus(1)
			//pass A[pivot] >= A[first]
			if !comp(pivot, first.ptr(), constants) {
				continue
			} else {
				break
			}
		}
	}

	//move pivot
	Move(begin.ptr(), last.ptr(), constants)
	Move(last.ptr(), pivot, constants)

	return last.plusCopy(0)
}

func comp(l, r unsafe.Pointer, constants *PDQConstants) bool {
	util.AssertFunc(
		l == constants._tmpBuf ||
			l == constants._swapOffsetsBuf ||
			util.PointerLess(l, constants._end))

	util.AssertFunc(
		r == constants._tmpBuf ||
			r == constants._swapOffsetsBuf ||
			util.PointerLess(r, constants._end))

	lAddr := util.PointerAdd(l, constants._compOffset)
	rAddr := util.PointerAdd(r, constants._compOffset)
	return util.PointerMemcmp(lAddr, rAddr, constants._compSize) < 0
}

func GetTmp(src unsafe.Pointer, constants *PDQConstants) unsafe.Pointer {
	util.AssertFunc(src != constants._tmpBuf &&
		src != constants._swapOffsetsBuf &&
		util.PointerLess(src, constants._end))
	util.PointerCopy(constants._tmpBuf, src, constants._entrySize)
	return constants._tmpBuf
}

func SwapOffsetsGetTmp(src unsafe.Pointer, constants *PDQConstants) unsafe.Pointer {
	util.AssertFunc(src != constants._tmpBuf &&
		src != constants._swapOffsetsBuf &&
		util.PointerLess(src, constants._end))
	util.PointerCopy(constants._swapOffsetsBuf, src, constants._entrySize)
	return constants._swapOffsetsBuf
}

func Move(dst, src unsafe.Pointer, constants *PDQConstants) {
	util.AssertFunc(
		dst == constants._tmpBuf ||
			dst == constants._swapOffsetsBuf ||
			util.PointerLess(dst, constants._end))
	util.AssertFunc(src == constants._tmpBuf ||
		src == constants._swapOffsetsBuf ||
		util.PointerLess(src, constants._end))
	util.PointerCopy(dst, src, constants._entrySize)
}

// sort A[a],A[b],A[c]
func sort3(a, b, c *PDQIterator, constants *PDQConstants) {
	sort2(a, b, constants)
	sort2(b, c, constants)
	sort2(a, b, constants)
}

func sort2(a *PDQIterator, b *PDQIterator, constants *PDQConstants) {
	if comp(b.ptr(), a.ptr(), constants) {
		iterSwap(a, b, constants)
	}
}

func iterSwap(lhs *PDQIterator, rhs *PDQIterator, constants *PDQConstants) {
	util.AssertFunc(util.PointerLess(lhs.ptr(), constants._end))
	util.AssertFunc(util.PointerLess(rhs.ptr(), constants._end))
	util.PointerCopy(constants._iterSwapBuf, lhs.ptr(), constants._entrySize)
	util.PointerCopy(lhs.ptr(), rhs.ptr(), constants._entrySize)
	util.PointerCopy(rhs.ptr(), constants._iterSwapBuf, constants._entrySize)
}

// insert sort [begin,end)
func insertSort(
	begin *PDQIterator,
	end *PDQIterator,
	constants *PDQConstants) {
	if pdqIterEqaul(begin, end) {
		return
	}

	for cur := begin.plusCopy(1); pdqIterNotEqaul(&cur, end); cur.plus(1) {
		sift := cur
		sift_1 := cur.plusCopy(-1)
		if comp(sift.ptr(), sift_1.ptr(), constants) {
			//A[sift - 1] > A[sift]
			tmp := GetTmp(sift.ptr(), constants)
			for {
				Move(sift.ptr(), sift_1.ptr(), constants)
				sift.plus(-1)

				if pdqIterNotEqaul(&sift, begin) {
					sift_1.plus(-1)
					if comp(tmp, sift_1.ptr(), constants) {
						continue
					}
				}
				break
			}
			Move(sift.ptr(), tmp, constants)
		}
	}
}

// insert sort [begin,end)
// A[begin - 1] <= anyone in [begin,end)
func unguardedInsertSort(begin *PDQIterator, end *PDQIterator, constants *PDQConstants) {
	if pdqIterEqaul(begin, end) {
		return
	}

	//plusCopy := begin.plusCopy(-1)
	//assertFunc(comp(plusCopy.ptr(), begin.ptr(), constants))

	for cur := begin.plusCopy(1); pdqIterNotEqaul(&cur, end); cur.plus(1) {
		sift := cur
		sift_1 := cur.plusCopy(-1)
		if comp(sift.ptr(), sift_1.ptr(), constants) {
			//A[sift - 1] > A[sift]
			tmp := GetTmp(sift.ptr(), constants)
			for {
				Move(sift.ptr(), sift_1.ptr(), constants)
				sift.plus(-1)

				sift_1.plus(-1)
				//FIXME:here remove the if
				//if !pdqIterLess(&sift_1, begin) {
				if comp(tmp, sift_1.ptr(), constants) {
					continue
				}
				//}
				break
			}
			Move(sift.ptr(), tmp, constants)
		}
	}
}

// InsertionSort adapted in less count of values
func InsertionSort(
	origPtr unsafe.Pointer,
	tempPtr unsafe.Pointer,
	count int,
	colOffset int,
	rowWidth int,
	totalCompWidth int,
	offset int,
	swap bool,
) {
	sourcePtr, targetPtr := origPtr, tempPtr
	if swap {
		sourcePtr, targetPtr = tempPtr, origPtr
	}

	if count > 1 {
		totalOffset := colOffset + offset
		val := util.CMalloc(rowWidth)
		defer util.CFree(val)
		compWidth := totalCompWidth - offset
		for i := 1; i < count; i++ {
			//val <= sourcePtr[i][...]
			util.PointerCopy(
				val,
				util.PointerAdd(sourcePtr, i*rowWidth),
				rowWidth)
			j := i
			//memcmp (sourcePtr[j-1][totalOffset],val[totalOffset],compWidth)
			for j > 0 &&
				util.PointerMemcmp(
					util.PointerAdd(sourcePtr, (j-1)*rowWidth+totalOffset),
					util.PointerAdd(val, totalOffset),
					compWidth,
				) > 0 {
				//memcopy (sourcePtr[j][...],sourcePtr[j-1][...],rowWidth)
				util.PointerCopy(
					util.PointerAdd(sourcePtr, j*rowWidth),
					util.PointerAdd(sourcePtr, (j-1)*rowWidth),
					rowWidth,
				)
				j--
			}
			//memcpy (sourcePtr[j][...],val,rowWidth)
			util.PointerCopy(
				util.PointerAdd(sourcePtr, j*rowWidth),
				val,
				rowWidth,
			)
		}
	}

	if swap {
		util.PointerCopy(
			targetPtr,
			sourcePtr,
			count*rowWidth,
		)
	}
}
