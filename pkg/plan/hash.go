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
	"unsafe"
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
		k := load[uint64](pointerAdd(data64, int(i*uint64(int64Size))))
		k *= M
		k ^= k >> R
		k *= M

		h ^= k
		h *= M
	}
	data8 := pointerAdd(data64, int(n_blocks*uint64(int64Size)))
	switch len & 7 {
	case 7:
		val := load[byte](pointerAdd(data8, 6))
		h ^= uint64(val) << 48
		fallthrough
	case 6:
		val := load[byte](pointerAdd(data8, 5))
		h ^= uint64(val) << 40
		fallthrough
	case 5:
		val := load[byte](pointerAdd(data8, 4))
		h ^= uint64(val) << 32
		fallthrough
	case 4:
		val := load[byte](pointerAdd(data8, 3))
		h ^= uint64(val) << 24
		fallthrough
	case 3:
		val := load[byte](pointerAdd(data8, 2))
		h ^= uint64(val) << 16
		fallthrough
	case 2:
		val := load[byte](pointerAdd(data8, 1))
		h ^= uint64(val) << 8
		fallthrough
	case 1:
		val := load[byte](data8)
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

func (hfun HashFuncString) fun(value String) uint64 {
	return HashBytes(value.data(), uint64(value.len()))
}

type HashOpString struct {
}

func (op HashOpString) operation(input String, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncString{}.fun(input)
	}
}

type HashFuncDate struct {
}

func (hfun HashFuncDate) fun(value Date) uint64 {
	return murmurhash64(uint64(value._year)) ^ murmurhash64(uint64(value._month)) ^ murmurhash64(uint64(value._day))
}

type HashOpDate struct {
}

func (op HashOpDate) operation(input Date, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncDate{}.fun(input)
	}
}

type HashFuncDecimal struct {
}

func (hfun HashFuncDecimal) fun(value Decimal) uint64 {
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

func (op HashOpDecimal) operation(input Decimal, isNull bool) uint64 {
	if isNull {
		return NULL_HASH
	} else {
		return HashFuncDecimal{}.fun(input)
	}
}

type HashFuncHugeint struct {
}

func (HashFuncHugeint) fun(value Hugeint) uint64 {
	return murmurhash64(uint64(value._upper)) ^ murmurhash64(uint64(value._lower))
}

type HashOpHugeint struct{}

func (op HashOpHugeint) operation(input Hugeint, isNull bool) uint64 {
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
	assertFunc(result.typ().id == LTID_UBIGINT)
	switch input.typ().getInternalType() {
	case INT32:
		TemplatedLoopHash[int32](input, result, rsel, count, hasRsel, HashOpInt32{}, HashFuncInt32{})
	case INT8:
		TemplatedLoopHash[int8](input, result, rsel, count, hasRsel, HashOpInt8{}, HashFuncInt8{})
	case VARCHAR:
		TemplatedLoopHash[String](input, result, rsel, count, hasRsel, HashOpString{}, HashFuncString{})
	case DECIMAL:
		TemplatedLoopHash[Decimal](input, result, rsel, count, hasRsel, HashOpDecimal{}, HashFuncDecimal{})
	case INT128:
		TemplatedLoopHash[Hugeint](input, result, rsel, count, hasRsel, HashOpHugeint{}, HashFuncHugeint{})
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
	if input.phyFormat().isConst() {
		result.setPhyFormat(PF_CONST)

		data := getSliceInPhyFormatConst[T](input)
		resData := getSliceInPhyFormatConst[uint64](result)
		resData[0] = hashOp.operation(data[0], isNullInPhyFormatConst(input))
	} else {
		result.setPhyFormat(PF_FLAT)
		var data UnifiedFormat
		input.toUnifiedFormat(count, &data)
		TightLoopHash[T](
			getSliceInPhyFormatUnifiedFormat[T](&data),
			getSliceInPhyFormatFlat[uint64](result),
			rsel,
			count,
			data._sel,
			data._mask,
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
	mask *Bitmap,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.getIndex(i)
			}
			idx := selVec.getIndex(ridx)
			resultData[ridx] = hashOp.operation(ldata[idx], !mask.rowIsValid(uint64(idx)))
		}
	} else {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.getIndex(i)
			}
			idx := selVec.getIndex(ridx)
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
	assertFunc(hashes.typ().id == LTID_UBIGINT)
	switch input.typ().getInternalType() {
	case INT32:
		TemplatedLoopCombineHash[int32](input, hashes, rsel, count, hasRsel, HashOpInt32{}, HashFuncInt32{})
	case DATE:
		TemplatedLoopCombineHash[Date](input, hashes, rsel, count, hasRsel, HashOpDate{}, HashFuncDate{})
	case DECIMAL:
		TemplatedLoopCombineHash[Decimal](input, hashes, rsel, count, hasRsel, HashOpDecimal{}, HashFuncDecimal{})
	case VARCHAR:
		TemplatedLoopCombineHash[String](input, hashes, rsel, count, hasRsel, HashOpString{}, HashFuncString{})
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
	if input.phyFormat().isConst() && hashes.phyFormat().isConst() {
		ldata := getSliceInPhyFormatConst[T](input)
		hashData := getSliceInPhyFormatConst[uint64](hashes)
		otherHash := hashOp.operation(ldata[0], isNullInPhyFormatConst(input))
		hashData[0] = CombineHashScalar(hashData[0], otherHash)
	} else {
		var data UnifiedFormat
		input.toUnifiedFormat(count, &data)
		if hashes.phyFormat().isConst() {
			hashData := getSliceInPhyFormatConst[uint64](hashes)
			hashes.setPhyFormat(PF_FLAT)
			TightLoopCombineHashConstant[T](
				getSliceInPhyFormatUnifiedFormat[T](&data),
				hashData[0],
				getSliceInPhyFormatFlat[uint64](hashes),
				rsel,
				count,
				data._sel,
				data._mask,
				hasRsel,
				hashOp,
				hashFun,
			)
		} else {
			assertFunc(hashes.phyFormat().isFlat())
			TightLoopCombineHash[T](
				getSliceInPhyFormatUnifiedFormat[T](&data),
				getSliceInPhyFormatFlat[uint64](hashes),
				rsel,
				count,
				data._sel,
				data._mask,
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
	mask *Bitmap,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.getIndex(i)
			}
			idx := selVec.getIndex(ridx)
			otherHash := hashOp.operation(ldata[idx], !mask.rowIsValid(uint64(idx)))
			hashData[ridx] = CombineHashScalar(constHash, otherHash)
		}
	} else {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.getIndex(i)
			}
			idx := selVec.getIndex(ridx)
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
	mask *Bitmap,
	hasRsel bool,
	hashOp HashOp[T],
	hashFun HashFunc[T],
) {
	if !mask.AllValid() {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.getIndex(i)
			}
			idx := selVec.getIndex(ridx)
			otherHash := hashOp.operation(ldata[idx], !mask.rowIsValid(uint64(idx)))
			hashData[ridx] = CombineHashScalar(hashData[ridx], otherHash)
		}
	} else {
		for i := 0; i < count; i++ {
			ridx := i
			if hasRsel {
				ridx = rsel.getIndex(i)
			}
			idx := selVec.getIndex(ridx)
			otherHash := hashFun.fun(ldata[idx])
			hashData[ridx] = CombineHashScalar(hashData[ridx], otherHash)
		}
	}
}
