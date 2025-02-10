// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"math/rand"
	"unsafe"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

//func isNullInPhyFormatFlat(vec *Vector, idx uint64) bool {
//	assertFunc(vec.phyFormat().isFlat())
//	return !vec.Mask.rowIsValid(idx)
//}

func booleanNullMask(left, right, result *chunk.Vector, count int, boolOp BooleanOp) {
	util.AssertFunc(left.Typ().Id == common.LTID_BOOLEAN &&
		right.Typ().Id == common.LTID_BOOLEAN &&
		result.Typ().Id == common.LTID_BOOLEAN)
	if left.PhyFormat().IsConst() && right.PhyFormat().IsConst() {
		result.SetPhyFormat(chunk.PF_CONST)
		ldata := chunk.GetSliceInPhyFormatConst[uint8](left)
		rdata := chunk.GetSliceInPhyFormatConst[uint8](right)
		target := chunk.GetSliceInPhyFormatConst[bool](result)
		null, res := boolOp.opWithNull(ldata[0] > 0, rdata[0] > 0, chunk.IsNullInPhyFormatConst(left), chunk.IsNullInPhyFormatConst(right))
		target[0] = res
		chunk.SetNullInPhyFormatConst(result, null)
	} else {
		var ldata, rdata chunk.UnifiedFormat
		left.ToUnifiedFormat(count, &ldata)
		right.ToUnifiedFormat(count, &rdata)

		result.SetPhyFormat(chunk.PF_FLAT)
		lSlice := chunk.GetSliceInPhyFormatUnifiedFormat[uint8](&ldata)
		rSlice := chunk.GetSliceInPhyFormatUnifiedFormat[uint8](&rdata)
		target := chunk.GetSliceInPhyFormatFlat[bool](result)
		targetMask := chunk.GetMaskInPhyFormatFlat(result)
		if !ldata.Mask.AllValid() || !rdata.Mask.AllValid() {
			for i := 0; i < count; i++ {
				lidx := ldata.Sel.GetIndex(i)
				ridx := rdata.Sel.GetIndex(i)
				null, res := boolOp.opWithNull(lSlice[lidx] > 0,
					rSlice[ridx] > 0,
					!ldata.Mask.RowIsValid(uint64(lidx)),
					!rdata.Mask.RowIsValid(uint64(ridx)),
				)
				target[i] = res
				targetMask.Set(uint64(i), !null)
			}
		} else {
			for i := 0; i < count; i++ {
				lidx := ldata.Sel.GetIndex(i)
				ridx := rdata.Sel.GetIndex(i)
				res := boolOp.opWithoutNull(lSlice[lidx] > 0, rSlice[ridx] > 0)
				target[i] = res
			}
		}
	}
}

// AddInPlace left += delta
func AddInPlace(input *chunk.Vector, right int64, cnt int) {
	util.AssertFunc(input.Typ().Id == common.LTID_POINTER)
	if right == 0 {
		return
	}
	switch input.PhyFormat() {
	case chunk.PF_CONST:
		util.AssertFunc(!chunk.IsNullInPhyFormatConst(input))
		data := chunk.GetSliceInPhyFormatConst[unsafe.Pointer](input)
		data[0] = util.PointerAdd(data[0], int(right))
	default:
		util.AssertFunc(input.PhyFormat().IsFlat())
		data := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](input)
		for i := 0; i < cnt; i++ {
			data[i] = util.PointerAdd(data[i], int(right))
		}
	}
}

func And(left, right, result *chunk.Vector, count int) {

}

func randomVector(
	typ common.LType,
	pf chunk.PhyFormat,
	nullRatio float32,
) *chunk.Vector {
	vec := chunk.NewEmptyVector(typ, pf, util.DefaultVectorSize)
	switch pf {
	case chunk.PF_FLAT:
		fillFlatVector(vec, util.DefaultVectorSize, nullRatio)
	case chunk.PF_CONST:
		fillConstVector(vec, nullRatio)
	default:
		panic("usp")
	}
	return vec
}

func fillConstVector(
	vec *chunk.Vector,
	nullRatio float32,
) {
	fillFlatVector(vec, 1, nullRatio)
}

func fillFlatVector(
	vec *chunk.Vector,
	count int,
	nullRatio float32,
) {
	util.AssertFunc(vec.PhyFormat().IsFlat() || vec.PhyFormat().IsConst())
	switch vec.Typ().GetInternalType() {
	case common.INT32:
		dSlice := chunk.GetSliceInPhyFormatFlat[int32](vec)
		fillLoop[int32](dSlice, vec.Mask, count, nullRatio, chunk.Int32ScatterOp{})
	case common.DECIMAL:
		dSlice := chunk.GetSliceInPhyFormatFlat[common.Decimal](vec)
		fillLoop[common.Decimal](dSlice, vec.Mask, count, nullRatio, chunk.DecimalScatterOp{})
	case common.VARCHAR:
		dSlice := chunk.GetSliceInPhyFormatFlat[common.String](vec)
		fillLoop[common.String](dSlice, vec.Mask, count, nullRatio, chunk.StringScatterOp{})
	default:
		panic("usp")
	}
}

func fillLoop[T any](
	data []T,
	mask *util.Bitmap,
	count int,
	nullRatio float32,
	nVal chunk.ScatterOp[T],
) {
	for i := 0; i < count; i++ {
		if rand.Float32() <= nullRatio {
			mask.SetInvalid(uint64(i))
		} else {
			data[i] = nVal.RandValue()
		}
	}
}

func isSameVector(
	vecA, vecB *chunk.Vector,
	count int,
) bool {
	if vecA.Typ().Id != vecB.Typ().Id {
		return false
	}
	if vecA.PhyFormat().IsConst() && vecB.PhyFormat().IsConst() {
		return checkConstVector(vecA, vecB)
	} else if vecA.PhyFormat().IsFlat() && vecB.PhyFormat().IsFlat() {
		return checkFlatVector(vecA, vecB, count)
	} else {

		var vDataA, vDataB chunk.UnifiedFormat
		vecA.ToUnifiedFormat(count, &vDataA)
		vecB.ToUnifiedFormat(count, &vDataB)
		switch vecA.Typ().GetInternalType() {
		case common.INT32:
			return checkLoop2[int32](&vDataA, &vDataB, count, equalOp[int32]{})
		case common.DECIMAL:
			return checkLoop2[common.Decimal](&vDataA, &vDataB, count, equalDecimalOp{})
		case common.VARCHAR:
			return checkLoop2[common.String](&vDataA, &vDataB, count, equalStrOp{})
		default:
			panic("usp")
		}
	}
}
func checkConstVector(
	vecA, vecB *chunk.Vector,
) bool {
	return checkFlatVector(vecA, vecB, 1)
}

func checkFlatVector(
	vecA, vecB *chunk.Vector,
	count int,
) bool {
	util.AssertFunc(
		vecA.PhyFormat().IsConst() && vecB.PhyFormat().IsConst() ||
			vecA.PhyFormat().IsFlat() && vecB.PhyFormat().IsFlat())
	util.AssertFunc(vecA.Typ().Id == vecB.Typ().Id)
	ret := false
	var maskA, maskB *util.Bitmap
	switch vecA.PhyFormat() {
	case chunk.PF_FLAT:
		maskA = chunk.GetMaskInPhyFormatFlat(vecA)
		maskB = chunk.GetMaskInPhyFormatFlat(vecB)
	case chunk.PF_CONST:
		maskA = chunk.GetMaskInPhyFormatConst(vecA)
		maskB = chunk.GetMaskInPhyFormatConst(vecB)
	case chunk.PF_DICT:
		panic("usp")
	}

	switch vecA.Typ().GetInternalType() {
	case common.INT32:
		dataA := chunk.GetSliceInPhyFormatFlat[int32](vecA)
		dataB := chunk.GetSliceInPhyFormatFlat[int32](vecB)
		ret = checkLoop[int32](
			dataA,
			maskA,
			dataB,
			maskB,
			count,
			equalOp[int32]{},
		)
	case common.DECIMAL:
		dataA := chunk.GetSliceInPhyFormatFlat[common.Decimal](vecA)
		dataB := chunk.GetSliceInPhyFormatFlat[common.Decimal](vecB)
		ret = checkLoop[common.Decimal](
			dataA,
			maskA,
			dataB,
			maskB,
			count,
			equalDecimalOp{},
		)
	case common.VARCHAR:
		dataA := chunk.GetSliceInPhyFormatFlat[common.String](vecA)
		dataB := chunk.GetSliceInPhyFormatFlat[common.String](vecB)
		ret = checkLoop[common.String](
			dataA,
			maskA,
			dataB,
			maskB,
			count,
			equalStrOp{},
		)
	default:
		panic("usp")
	}
	return ret
}

func checkLoop[T any](
	dataA []T,
	maskA *util.Bitmap,
	dataB []T,
	maskB *util.Bitmap,
	count int,
	eOp CompareOp[T],
) bool {
	for i := 0; i < count; i++ {
		mA := maskA.RowIsValid(uint64(i))
		mB := maskB.RowIsValid(uint64(i))
		if mA && mB {
			ret := eOp.operation(&dataA[i], &dataB[i])
			if !ret {
				return false
			}
		} else if !mA && !mB {
			//NULL && NULL
		} else {
			return false
		}
	}
	return true
}

func checkLoop2[T any](
	vdataA, vdataB *chunk.UnifiedFormat,
	count int,
	eOp CompareOp[T],
) bool {
	dataA := chunk.GetSliceInPhyFormatUnifiedFormat[T](vdataA)
	dataB := chunk.GetSliceInPhyFormatUnifiedFormat[T](vdataB)
	for i := 0; i < count; i++ {
		idxA := vdataA.Sel.GetIndex(i)
		idxB := vdataB.Sel.GetIndex(i)
		mA := vdataA.Mask.RowIsValid(uint64(idxA))
		mB := vdataB.Mask.RowIsValid(uint64(idxB))

		if mA && mB {
			ret := eOp.operation(&dataA[idxA], &dataB[idxB])
			if !ret {
				return false
			}
		} else if !mA && !mB {
			//NULL && NULL
		} else {
			return false
		}
	}
	return true
}

func isSameChunk(
	chA, chB *chunk.Chunk,
	count int,
) bool {
	if chA.Card() != chB.Card() {
		return false
	}
	if chA.ColumnCount() != chB.ColumnCount() {
		return false
	}
	for i := 0; i < chA.ColumnCount(); i++ {
		if !isSameVector(chA.Data[i], chB.Data[i], count) {
			return false
		}
	}
	return true
}
