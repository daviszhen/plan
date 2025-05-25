package chunk

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func GetSequenceInPhyFormatSequence(vec *Vector, start, incr, seqCount *int64) {
	util.AssertFunc(vec.PhyFormat().IsSequence())
	dSlice := GetSliceInPhyFormatSequence(vec)
	*start = dSlice[0]
	*incr = dSlice[1]
	*seqCount = dSlice[2]
}

func NewVector(lTyp common.LType, initData bool, cap int) *Vector {
	vec := &Vector{
		_PhyFormat: PF_FLAT,
		_Typ:       lTyp,
		Mask:       &util.Bitmap{},
	}
	if initData {
		vec.Init(cap)
	}
	return vec
}

func NewVector2(lTyp common.LType, cap int) *Vector {
	return NewVector(lTyp, true, cap)
}

func NewFlatVector(lTyp common.LType, cap int) *Vector {
	return NewVector2(lTyp, cap)
}

func NewConstVector(lTyp common.LType) *Vector {
	vec := NewVector2(lTyp, util.DefaultVectorSize)
	vec.SetPhyFormat(PF_CONST)
	return vec
}

func NewEmptyVector(typ common.LType, pf PhyFormat, cap int) *Vector {
	var vec *Vector
	switch pf {
	case PF_FLAT:
		vec = NewFlatVector(typ, cap)
	case PF_CONST:
		vec = NewConstVector(typ)
	default:
		panic("usp")
	}
	return vec
}

func Copy(
	srcP *Vector,
	dstP *Vector,
	selP *SelectVector,
	srcCount int,
	srcOffset int,
	dstOffset int,
) {
	util.AssertFunc(srcOffset <= srcCount)
	util.AssertFunc(srcP.Typ().Id == dstP.Typ().Id)
	copyCount := srcCount - srcOffset
	finished := false

	ownedSel := &SelectVector{}
	sel := selP
	src := srcP

	for !finished {
		switch src.PhyFormat() {
		case PF_DICT:
			//dict vector
			child := GetChildInPhyFormatDict(src)
			dictSel := GetSelVectorInPhyFormatDict(src)
			//
			newBuff := dictSel.Slice(sel, srcCount)
			ownedSel.Init3(newBuff)
			sel = ownedSel
			src = child
		case PF_CONST:
			sel = ZeroSelectVectorInPhyFormatConst(copyCount, ownedSel)
			finished = true
		case PF_FLAT:
			finished = true
		default:
			panic("usp")
		}
	}

	if copyCount == 0 {
		return
	}

	dstVecType := dstP.PhyFormat()
	if copyCount == 1 && dstVecType == PF_DICT {
		dstOffset = 0
		dstP.SetPhyFormat(PF_FLAT)
	}

	util.AssertFunc(dstP.PhyFormat().IsFlat())

	//copy bitmap
	dstBitmap := GetMaskInPhyFormatFlat(dstP)
	if src.PhyFormat().IsConst() {
		valid := !IsNullInPhyFormatConst(src)
		for i := 0; i < copyCount; i++ {
			dstBitmap.Set(uint64(dstOffset+i), valid)
		}
	} else {
		srcBitmap := CopyBitmap(src)
		if srcBitmap.IsMaskSet() {
			for i := 0; i < copyCount; i++ {
				idx := sel.GetIndex(srcOffset + i)

				if srcBitmap.RowIsValid(uint64(idx)) {
					if !dstBitmap.AllValid() {
						dstBitmap.SetValidUnsafe(uint64(dstOffset + i))
					}
				} else {
					if dstBitmap.AllValid() {
						initSize := max(util.DefaultVectorSize,
							dstOffset+copyCount)
						dstBitmap.Init(initSize)
					}
					dstBitmap.SetInvalidUnsafe(uint64(dstOffset + i))
				}
			}
		}
	}

	util.AssertFunc(sel != nil)

	//copy data
	switch src.Typ().GetInternalType() {
	case common.INT32:
		TemplatedCopy[int32](
			src,
			sel,
			dstP,
			srcOffset,
			dstOffset,
			copyCount,
		)
	case common.VARCHAR:
		srcSlice := GetSliceInPhyFormatFlat[common.String](src)
		dstSlice := GetSliceInPhyFormatFlat[common.String](dstP)

		for i := 0; i < copyCount; i++ {
			srcIdx := sel.GetIndex(srcOffset + i)
			dstIdx := dstOffset + i
			if dstBitmap.RowIsValid(uint64(dstIdx)) {
				srcStr := srcSlice[srcIdx]
				ptr := util.CMalloc(srcStr.Length())
				util.PointerCopy(ptr, srcStr.DataPtr(), srcStr.Length())
				dstSlice[dstIdx] = common.String{Data: ptr, Len: srcStr.Length()}
			}
		}
	default:
		panic("usp")
	}
}

func TemplatedCopy[T any](
	src *Vector,
	sel *SelectVector,
	dst *Vector,
	srcOffset int,
	dstOffset int,
	copyCount int,
) {
	srcSlice := GetSliceInPhyFormatFlat[T](src)
	dstSlice := GetSliceInPhyFormatFlat[T](dst)

	for i := 0; i < copyCount; i++ {
		srcIdx := sel.GetIndex(srcOffset + i)
		dstSlice[dstOffset+i] = srcSlice[srcIdx]
	}
}

func CopyBitmap(v *Vector) *util.Bitmap {
	switch v.PhyFormat() {
	case PF_FLAT:
		return GetMaskInPhyFormatFlat(v)
	default:
		panic("usp")
	}
}

func WriteToStorage(
	src *Vector,
	count int,
	ptr unsafe.Pointer,
) {
	if count == 0 {
		return
	}

	var vdata UnifiedFormat
	src.ToUnifiedFormat(count, &vdata)

	switch src.Typ().GetInternalType() {
	case common.INT32:
		SaveLoop[int32](&vdata, count, ptr, Int32ScatterOp{})
	case common.DECIMAL:
		SaveLoop[common.Decimal](&vdata, count, ptr, DecimalScatterOp{})
	case common.DATE:
		SaveLoop[common.Date](&vdata, count, ptr, DateScatterOp{})
	case common.INT64:
		SaveLoop[int64](&vdata, count, ptr, Int64ScatterOp{})
	case common.UINT64:
		SaveLoop[uint64](&vdata, count, ptr, Uint64ScatterOp{})
	default:
		panic("usp")
	}
}

func SaveLoop[T any](
	vdata *UnifiedFormat,
	count int,
	ptr unsafe.Pointer,
	nVal ScatterOp[T],
) {
	inSlice := GetSliceInPhyFormatUnifiedFormat[T](vdata)
	resSlice := util.PointerToSlice[T](ptr, count)
	for i := 0; i < count; i++ {
		idx := vdata.Sel.GetIndex(i)
		if !vdata.Mask.RowIsValid(uint64(idx)) {
			resSlice[i] = nVal.NullValue()
		} else {
			resSlice[i] = inSlice[idx]
		}
	}
}

func ReadFromStorage(
	ptr unsafe.Pointer,
	count int,
	res *Vector,
) {
	res.SetPhyFormat(PF_FLAT)
	switch res.Typ().GetInternalType() {
	case common.INT32:
		ReadLoop[int32](ptr, count, res)
	case common.DECIMAL:
		ReadLoop[common.Decimal](ptr, count, res)
	case common.DATE:
		ReadLoop[common.Date](ptr, count, res)
	case common.INT64:
		ReadLoop[int64](ptr, count, res)
	case common.UINT64:
		ReadLoop[uint64](ptr, count, res)
	default:
		panic("usp")
	}
}

func ReadLoop[T any](
	src unsafe.Pointer,
	count int,
	res *Vector,
) {
	srcSlice := util.PointerToSlice[T](src, count)
	resSlice := GetSliceInPhyFormatFlat[T](res)

	for i := 0; i < count; i++ {
		resSlice[i] = srcSlice[i]
	}
}
