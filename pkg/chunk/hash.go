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

package chunk

import (
	"unsafe"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	NULL_HASH = 0xbf58476d1ce4e5b9
)

func murmurhash64(x uint64) uint64 {
	x ^= x >> 32
	x *= 0xd6e8feb86659fd93
	x ^= x >> 32
	x *= 0xd6e8feb86659fd93
	x ^= x >> 32
	return x
}

func murmurhash32(x uint32) uint64 {
	return murmurhash64(uint64(x))
}

func CombineHashScalar(a, b uint64) uint64 {
	return (a * 0xbf58476d1ce4e5b9) ^ b
}

type HashFunc[T any] interface {
	fun(value T) uint64
}

type HashOp[T any] interface {
	operation(input T, isNull bool) uint64
}

type HashFuncInt32 struct {
}

func (hfun HashFuncInt32) fun(value int32) uint64 {
	return murmurhash32(uint32(value))
}

type HashFuncInt8 struct {
}

func (hfun HashFuncInt8) fun(value int8) uint64 {
	return murmurhash32(uint32(value))
}

type HashOpInt32 struct {
}

func (op HashOpInt32) operation(input int32, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncInt32{}.fun(input)
	}
}

type HashOpInt8 struct {
}

func (op HashOpInt8) operation(input int8, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncInt8{}.fun(input)
	}
}

const (
	M    uint64 = 0xc6a4a7935bd1e995
	SEED uint64 = 0xe17a1465
	R    uint64 = 47
)

func HashBytes(ptr unsafe.Pointer, len uint64) uint64 {
	data64 := ptr
	h := SEED ^ (len * M)

	n_blocks := len / 8
	for i := uint64(0); i < n_blocks; i++ {
		k := util.Load[uint64](util.PointerAdd(data64, int(i*uint64(common.Int64Size))))
		k *= M
		k ^= k >> R
		k *= M

		h ^= k
		h *= M
	}
	data8 := util.PointerAdd(data64, int(n_blocks*uint64(common.Int64Size)))
	switch len & 7 {
	case 7:
		val := util.Load[byte](util.PointerAdd(data8, 6))
		h ^= uint64(val) << 48
		fallthrough
	case 6:
		val := util.Load[byte](util.PointerAdd(data8, 5))
		h ^= uint64(val) << 40
		fallthrough
	case 5:
		val := util.Load[byte](util.PointerAdd(data8, 4))
		h ^= uint64(val) << 32
		fallthrough
	case 4:
		val := util.Load[byte](util.PointerAdd(data8, 3))
		h ^= uint64(val) << 24
		fallthrough
	case 3:
		val := util.Load[byte](util.PointerAdd(data8, 2))
		h ^= uint64(val) << 16
		fallthrough
	case 2:
		val := util.Load[byte](util.PointerAdd(data8, 1))
		h ^= uint64(val) << 8
		fallthrough
	case 1:
		val := util.Load[byte](data8)
		h ^= uint64(val)
		h *= M
		fallthrough
	default:
		break
	}
	h ^= h >> R
	h *= M
	h ^= h >> R
	return h
}

type HashFuncString struct {
}

func (hfun HashFuncString) fun(value common.String) uint64 {
	return HashBytes(value.DataPtr(), uint64(value.Length()))
}

type HashOpString struct {
}

func (op HashOpString) operation(input common.String, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncString{}.fun(input)
	}
}

type HashFuncDate struct {
}

func (hfun HashFuncDate) fun(value common.Date) uint64 {
	return murmurhash64(uint64(value.Year)) ^ murmurhash64(uint64(value.Month)) ^ murmurhash64(uint64(value.Day))
}

type HashOpDate struct {
}

func (op HashOpDate) operation(input common.Date, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncDate{}.fun(input)
	}
}

type HashFuncDecimal struct {
}

func (hfun HashFuncDecimal) fun(value common.Decimal) uint64 {
	neg := 0
	if value.Decimal.IsNeg() {
		neg = 1
	}
	coef := value.Coef()
	scale := value.Scale()
	return murmurhash64(uint64(neg)) ^ murmurhash64(uint64(coef)) ^ murmurhash64(uint64(scale))
}

type HashOpDecimal struct {
}

func (op HashOpDecimal) operation(input common.Decimal, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncDecimal{}.fun(input)
	}
}

type HashFuncHugeint struct {
}

func (HashFuncHugeint) fun(value common.Hugeint) uint64 {
	return murmurhash64(uint64(value.Upper)) ^ murmurhash64(uint64(value.Lower))
}

type HashOpHugeint struct{}

func (op HashOpHugeint) operation(input common.Hugeint, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncHugeint{}.fun(input)
	}
}

func HashTypeSwitch(
	input, result *Vector,
	rsel *SelectVector,
	count int,
	hasRsel bool,
) {
	util.AssertFunc(result.Typ().Id == common.LTID_UBIGINT)
	switch input.Typ().GetInternalType() {
	case common.INT32:
		TemplatedLoopHash[int32](input, result, rsel, count, hasRsel, HashOpInt32{}, HashFuncInt32{})
	case common.INT8:
		TemplatedLoopHash[int8](input, result, rsel, count, hasRsel, HashOpInt8{}, HashFuncInt8{})
	case common.VARCHAR:
		TemplatedLoopHash[common.String](input, result, rsel, count, hasRsel, HashOpString{}, HashFuncString{})
	case common.DECIMAL:
		TemplatedLoopHash[common.Decimal](input, result, rsel, count, hasRsel, HashOpDecimal{}, HashFuncDecimal{})
	case common.INT128:
		TemplatedLoopHash[common.Hugeint](input, result, rsel, count, hasRsel, HashOpHugeint{}, HashFuncHugeint{})
	default:
		panic("Unknown input type")
	}
}

func TemplatedLoopHash[T any](
	input, result *Vector,
	rsel *SelectVector,
	count int,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if input.PhyFormat().IsConst() {
		result.SetPhyFormat(PF_CONST)

		data := GetSliceInPhyFormatConst[T](input)
		resData := GetSliceInPhyFormatConst[uint64](result)
		resData[0] = hashOp.operation(data[0], IsNullInPhyFormatConst(input))
	} else {
		result.SetPhyFormat(PF_FLAT)
		var data UnifiedFormat
		input.ToUnifiedFormat(count, &data)
		TightLoopHash[T](
			GetSliceInPhyFormatUnifiedFormat[T](&data),
			GetSliceInPhyFormatFlat[uint64](result),
			rsel,
			count,
			data.Sel,
			data.Mask,
			hasRsel,
			hashOp,
			hashFun,
		)
	}
}

func TightLoopHash[T any](
	ldata []T,
	resultData []uint64,
	rsel *SelectVector,
	count int,
	selVec *SelectVector,
	mask *util.Bitmap,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.GetIndex(i)
			}
			idx := selVec.GetIndex(ridx)
			resultData[ridx] = hashOp.operation(ldata[idx], !mask.RowIsValid(uint64(idx)))
		}
	} else {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.GetIndex(i)
			}
			idx := selVec.GetIndex(ridx)
			resultData[ridx] = hashFun.fun(ldata[idx])
		}
	}
}

func CombineHashTypeSwitch(
	hashes *Vector,
	input *Vector,
	rsel *SelectVector,
	count int,
	hasRsel bool,
) {
	util.AssertFunc(hashes.Typ().Id == common.LTID_UBIGINT)
	switch input.Typ().GetInternalType() {
	case common.INT32:
		TemplatedLoopCombineHash[int32](input, hashes, rsel, count, hasRsel, HashOpInt32{}, HashFuncInt32{})
	case common.DATE:
		TemplatedLoopCombineHash[common.Date](input, hashes, rsel, count, hasRsel, HashOpDate{}, HashFuncDate{})
	case common.DECIMAL:
		TemplatedLoopCombineHash[common.Decimal](input, hashes, rsel, count, hasRsel, HashOpDecimal{}, HashFuncDecimal{})
	case common.VARCHAR:
		TemplatedLoopCombineHash[common.String](input, hashes, rsel, count, hasRsel, HashOpString{}, HashFuncString{})
	default:
		panic("Unknown input type")
	}
}

func TemplatedLoopCombineHash[T any](
	input *Vector,
	hashes *Vector,
	rsel *SelectVector,
	count int,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if input.PhyFormat().IsConst() && hashes.PhyFormat().IsConst() {
		ldata := GetSliceInPhyFormatConst[T](input)
		hashData := GetSliceInPhyFormatConst[uint64](hashes)
		otherHash := hashOp.operation(ldata[0], IsNullInPhyFormatConst(input))
		hashData[0] = CombineHashScalar(hashData[0], otherHash)
	} else {
		var data UnifiedFormat
		input.ToUnifiedFormat(count, &data)
		if hashes.PhyFormat().IsConst() {
			hashData := GetSliceInPhyFormatConst[uint64](hashes)
			hashes.SetPhyFormat(PF_FLAT)
			TightLoopCombineHashConstant[T](
				GetSliceInPhyFormatUnifiedFormat[T](&data),
				hashData[0],
				GetSliceInPhyFormatFlat[uint64](hashes),
				rsel,
				count,
				data.Sel,
				data.Mask,
				hasRsel,
				hashOp,
				hashFun,
			)
		} else {
			util.AssertFunc(hashes.PhyFormat().IsFlat())
			TightLoopCombineHash[T](
				GetSliceInPhyFormatUnifiedFormat[T](&data),
				GetSliceInPhyFormatFlat[uint64](hashes),
				rsel,
				count,
				data.Sel,
				data.Mask,
				hasRsel,
				hashOp,
				hashFun,
			)
		}
	}
}

func TightLoopCombineHashConstant[T any](
	ldata []T,
	constHash uint64,
	hashData []uint64,
	rsel *SelectVector,
	count int,
	selVec *SelectVector,
	mask *util.Bitmap,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.GetIndex(i)
			}
			idx := selVec.GetIndex(ridx)
			otherHash := hashOp.operation(ldata[idx], !mask.RowIsValid(uint64(idx)))
			hashData[ridx] = CombineHashScalar(constHash, otherHash)
		}
	} else {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.GetIndex(i)
			}
			idx := selVec.GetIndex(ridx)
			otherHash := hashFun.fun(ldata[idx])
			hashData[ridx] = CombineHashScalar(constHash, otherHash)
		}
	}
}

func TightLoopCombineHash[T any](
	ldata []T,
	hashData []uint64,
	rsel *SelectVector,
	count int,
	selVec *SelectVector,
	mask *util.Bitmap,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.GetIndex(i)
			}
			idx := selVec.GetIndex(ridx)
			otherHash := hashOp.operation(ldata[idx], !mask.RowIsValid(uint64(idx)))
			hashData[ridx] = CombineHashScalar(hashData[ridx], otherHash)
		}
	} else {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.GetIndex(i)
			}
			idx := selVec.GetIndex(ridx)
			otherHash := hashFun.fun(ldata[idx])
			hashData[ridx] = CombineHashScalar(hashData[ridx], otherHash)
		}
	}
}
