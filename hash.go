package main

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
