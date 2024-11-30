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

// BoundCastData generated during bind of cast,
// used during execution of cast
type BoundCastData struct {
}

type CastParams struct {
	_errorMsg string
	_strict   bool
}

type CastFuncType func(src, res *Vector, count int, params *CastParams) bool

type BoundCastInfo struct {
	_fun      CastFuncType
	_castData *BoundCastData
}

type BindCastInfo struct{}

type BindCastInput struct {
	_funcSet *CastFunctionSet
	_info    *BindCastInfo
}

func DefaultCastFunc(
	input *BindCastInput,
	src, dst LType) *BoundCastInfo {
	assertFunc(!src.equal(dst))

	switch src.id {
	case LTID_BOOLEAN, LTID_TINYINT, LTID_SMALLINT, LTID_INTEGER, LTID_BIGINT,
		LTID_UTINYINT, LTID_USMALLINT, LTID_UINTEGER, LTID_UBIGINT, LTID_HUGEINT,
		LTID_FLOAT, LTID_DOUBLE:
		return NumericCastSwitch(input, src, dst)
	case LTID_POINTER:
		panic("usp")
	case LTID_UUID:
		panic("usp")
	case LTID_DECIMAL:
		return DecimalCastToSwitch(input, src, dst)
	case LTID_DATE:
		panic("usp")
	case LTID_TIME,
		LTID_TIME_TZ,
		LTID_TIMESTAMP,
		LTID_TIMESTAMP_TZ,
		LTID_TIMESTAMP_NS,
		LTID_TIMESTAMP_MS,
		LTID_TIMESTAMP_SEC:
		panic("usp")
	case LTID_INTERVAL:
		panic("usp")
	case LTID_VARCHAR:
		return StringCastToSwitch(input, src, dst)
	case LTID_BLOB:
		panic("usp")
	case LTID_BIT:
		panic("usp")
	case LTID_NULL:
		panic("usp")
	case LTID_MAP:
		panic("usp")
	case LTID_STRUCT:
		panic("usp")
	case LTID_LIST:
		panic("usp")
	case LTID_UNION:
		panic("usp")
	case LTID_ENUM:
		panic("usp")
	case LTID_AGGREGATE_STATE:
		panic("usp")
	default:
		panic("usp")
	}
}

func NumericCastSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch src.id {
	case LTID_BOOLEAN:
		ret = BoolCastToSwitch(input, src, dst)
	case LTID_TINYINT:
	case LTID_SMALLINT:
	case LTID_INTEGER:
		ret = IntegerCastToSwitch(input, src, dst)
	case LTID_BIGINT:
	case LTID_UTINYINT:
	case LTID_USMALLINT:
	case LTID_UINTEGER:
	case LTID_UBIGINT:
	case LTID_HUGEINT:
		ret = HugeintCastToSwitch(input, src, dst)
	case LTID_FLOAT:
		ret = FloatCastToSwitch(input, src, dst)
	case LTID_DOUBLE:
	default:
		panic("usp")
	}
	if ret == nil || ret._fun == nil {
		panic("usp")
	}
	return ret
}

func BoolCastToSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.id {
	case LTID_BOOLEAN:
	case LTID_TINYINT:
	case LTID_SMALLINT:
	case LTID_INTEGER:
	case LTID_BIGINT:
	case LTID_UTINYINT:
	case LTID_USMALLINT:
	case LTID_UINTEGER:
	case LTID_UBIGINT:
	case LTID_HUGEINT:
	case LTID_FLOAT:
	case LTID_DOUBLE:
	default:
		panic("usp")
	}
	return ret
}

func IntegerCastToSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.id {
	case LTID_BOOLEAN:
	case LTID_TINYINT:
	case LTID_SMALLINT:
	case LTID_INTEGER:
		ret._fun = MakeCastFunc[int32, int32](tryCastInt32ToInt32)
	case LTID_BIGINT:
	case LTID_UTINYINT:
	case LTID_USMALLINT:
	case LTID_UINTEGER:
	case LTID_UBIGINT:
	case LTID_HUGEINT:
	case LTID_FLOAT:
		ret._fun = MakeCastFunc[int32, float32](tryCastInt32ToFloat32)
	case LTID_DOUBLE:
		ret._fun = MakeCastFunc[int32, float64](tryCastInt32ToFloat64)
	case LTID_DECIMAL:
		decCast := func(input *int32, result *Decimal, _ bool) bool {
			return tryCastInt32ToDecimal(input, result, dst.scale, true)
		}
		ret._fun = MakeCastFunc[int32, Decimal](decCast)
	default:
		panic("usp")
	}
	return ret
}

func FloatCastToSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.id {
	case LTID_BOOLEAN:
	case LTID_TINYINT:
	case LTID_SMALLINT:
	case LTID_INTEGER:
		ret._fun = MakeCastFunc[float32, int32](tryCastFloat32ToInt32)
	case LTID_BIGINT:
	case LTID_UTINYINT:
	case LTID_USMALLINT:
	case LTID_UINTEGER:
	case LTID_UBIGINT:
	case LTID_HUGEINT:
	case LTID_FLOAT:
	case LTID_DOUBLE:
		ret._fun = MakeCastFunc[float32, float64](tryCastFloat32ToFloat64)
	default:
		panic("usp")
	}
	return ret
}

func HugeintCastToSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.id {
	case LTID_BOOLEAN:
	case LTID_TINYINT:
	case LTID_SMALLINT:
	case LTID_INTEGER:
		ret._fun = MakeCastFunc[Hugeint, int32](tryCastBigintToInt32)
	case LTID_BIGINT:
	case LTID_UTINYINT:
	case LTID_USMALLINT:
	case LTID_UINTEGER:
	case LTID_UBIGINT:
	case LTID_HUGEINT:
	case LTID_FLOAT:
	case LTID_DOUBLE:
	case LTID_DECIMAL:
		ret._fun = MakeCastFunc[Hugeint, Decimal](tryCastBigintToDecimal)
	default:
		panic("usp")
	}
	return ret
}

func DecimalCastToSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.id {
	case LTID_BOOLEAN:
	case LTID_TINYINT:
	case LTID_SMALLINT:
	case LTID_INTEGER:
	case LTID_BIGINT:
	case LTID_UTINYINT:
	case LTID_USMALLINT:
	case LTID_UINTEGER:
	case LTID_UBIGINT:
	case LTID_HUGEINT:
	case LTID_FLOAT:
		ret._fun = MakeCastFunc[Decimal, float32](tryCastDecimalToFloat32)
	case LTID_DOUBLE:
	case LTID_DECIMAL:
		decCast := func(input *Decimal, result *Decimal, _ bool) bool {
			return tryCastDecimalToDecimal(input, result, src.scale, dst.scale, true)
		}
		ret._fun = MakeCastFunc[Decimal, Decimal](decCast)
	default:
		panic("usp")
	}
	return ret
}

func StringCastToSwitch(
	input *BindCastInput,
	src, dst LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.id {
	case LTID_DATE:
		ret._fun = MakeCastFunc[String, Date](tryCastVarcharToDate)
	case LTID_INTERVAL:
		ret._fun = MakeCastFunc[String, Interval](tryCastVarcharToInterval)
	default:
		panic("usp")
	}
	return ret
}

func MakeCastFunc[T any, R any](op CastOp[T, R]) CastFuncType {
	temp := func(src *Vector, res *Vector, count int, params *CastParams) bool {
		return TryCastLoop[T, R](
			src,
			res,
			count,
			params,
			op,
		)
	}
	return temp
}
