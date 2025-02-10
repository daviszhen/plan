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
	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// BoundCastData generated during bind of cast,
// used during execution of cast
type BoundCastData struct {
}

type CastParams struct {
	_errorMsg string
	_strict   bool
}

type CastFuncType func(src, res *chunk.Vector, count int, params *CastParams) bool

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
	src, dst common.LType) *BoundCastInfo {
	util.AssertFunc(!src.Equal(dst))

	switch src.Id {
	case common.LTID_BOOLEAN, common.LTID_TINYINT, common.LTID_SMALLINT, common.LTID_INTEGER, common.LTID_BIGINT,
		common.LTID_UTINYINT, common.LTID_USMALLINT, common.LTID_UINTEGER, common.LTID_UBIGINT, common.LTID_HUGEINT,
		common.LTID_FLOAT, common.LTID_DOUBLE:
		return NumericCastSwitch(input, src, dst)
	case common.LTID_POINTER:
		panic("usp")
	case common.LTID_UUID:
		panic("usp")
	case common.LTID_DECIMAL:
		return DecimalCastToSwitch(input, src, dst)
	case common.LTID_DATE:
		panic("usp")
	case common.LTID_TIME,
		common.LTID_TIME_TZ,
		common.LTID_TIMESTAMP,
		common.LTID_TIMESTAMP_TZ,
		common.LTID_TIMESTAMP_NS,
		common.LTID_TIMESTAMP_MS,
		common.LTID_TIMESTAMP_SEC:
		panic("usp")
	case common.LTID_INTERVAL:
		panic("usp")
	case common.LTID_VARCHAR:
		return StringCastToSwitch(input, src, dst)
	case common.LTID_BLOB:
		panic("usp")
	case common.LTID_BIT:
		panic("usp")
	case common.LTID_NULL:
		panic("usp")
	case common.LTID_MAP:
		panic("usp")
	case common.LTID_STRUCT:
		panic("usp")
	case common.LTID_LIST:
		panic("usp")
	case common.LTID_UNION:
		panic("usp")
	case common.LTID_ENUM:
		panic("usp")
	case common.LTID_AGGREGATE_STATE:
		panic("usp")
	default:
		panic("usp")
	}
}

func NumericCastSwitch(
	input *BindCastInput,
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch src.Id {
	case common.LTID_BOOLEAN:
		ret = BoolCastToSwitch(input, src, dst)
	case common.LTID_TINYINT:
	case common.LTID_SMALLINT:
	case common.LTID_INTEGER:
		ret = IntegerCastToSwitch(input, src, dst)
	case common.LTID_BIGINT:
	case common.LTID_UTINYINT:
	case common.LTID_USMALLINT:
	case common.LTID_UINTEGER:
	case common.LTID_UBIGINT:
	case common.LTID_HUGEINT:
		ret = HugeintCastToSwitch(input, src, dst)
	case common.LTID_FLOAT:
		ret = FloatCastToSwitch(input, src, dst)
	case common.LTID_DOUBLE:
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
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.Id {
	case common.LTID_BOOLEAN:
	case common.LTID_TINYINT:
	case common.LTID_SMALLINT:
	case common.LTID_INTEGER:
	case common.LTID_BIGINT:
	case common.LTID_UTINYINT:
	case common.LTID_USMALLINT:
	case common.LTID_UINTEGER:
	case common.LTID_UBIGINT:
	case common.LTID_HUGEINT:
	case common.LTID_FLOAT:
	case common.LTID_DOUBLE:
	default:
		panic("usp")
	}
	return ret
}

func IntegerCastToSwitch(
	input *BindCastInput,
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.Id {
	case common.LTID_BOOLEAN:
	case common.LTID_TINYINT:
	case common.LTID_SMALLINT:
	case common.LTID_INTEGER:
		ret._fun = MakeCastFunc[int32, int32](tryCastInt32ToInt32)
	case common.LTID_BIGINT:
		ret._fun = MakeCastFunc[int32, int64](tryCastInt32ToInt64)
	case common.LTID_UTINYINT:
	case common.LTID_USMALLINT:
	case common.LTID_UINTEGER:
	case common.LTID_UBIGINT:
	case common.LTID_HUGEINT:
		ret._fun = MakeCastFunc[int32, common.Hugeint](tryCastInt32ToHugeint)
	case common.LTID_FLOAT:
		ret._fun = MakeCastFunc[int32, float32](tryCastInt32ToFloat32)
	case common.LTID_DOUBLE:
		ret._fun = MakeCastFunc[int32, float64](tryCastInt32ToFloat64)
	case common.LTID_DECIMAL:
		decCast := func(input *int32, result *common.Decimal, _ bool) bool {
			return tryCastInt32ToDecimal(input, result, dst.Scale, true)
		}
		ret._fun = MakeCastFunc[int32, common.Decimal](decCast)
	default:
		panic("usp")
	}
	return ret
}

func FloatCastToSwitch(
	input *BindCastInput,
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.Id {
	case common.LTID_BOOLEAN:
	case common.LTID_TINYINT:
	case common.LTID_SMALLINT:
	case common.LTID_INTEGER:
		ret._fun = MakeCastFunc[float32, int32](tryCastFloat32ToInt32)
	case common.LTID_BIGINT:
	case common.LTID_UTINYINT:
	case common.LTID_USMALLINT:
	case common.LTID_UINTEGER:
	case common.LTID_UBIGINT:
	case common.LTID_HUGEINT:
	case common.LTID_FLOAT:
	case common.LTID_DOUBLE:
		ret._fun = MakeCastFunc[float32, float64](tryCastFloat32ToFloat64)
	case common.LTID_DECIMAL:
		ret._fun = MakeCastFunc[float32, common.Decimal](tryCastFloat32ToDecimal)
	default:
		panic("usp")
	}
	return ret
}

func HugeintCastToSwitch(
	input *BindCastInput,
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.Id {
	case common.LTID_BOOLEAN:
	case common.LTID_TINYINT:
	case common.LTID_SMALLINT:
	case common.LTID_INTEGER:
		ret._fun = MakeCastFunc[common.Hugeint, int32](tryCastBigintToInt32)
	case common.LTID_BIGINT:
	case common.LTID_UTINYINT:
	case common.LTID_USMALLINT:
	case common.LTID_UINTEGER:
	case common.LTID_UBIGINT:
	case common.LTID_HUGEINT:
	case common.LTID_FLOAT:
		ret._fun = MakeCastFunc[common.Hugeint, float32](tryCastBigintToFloat32)
	case common.LTID_DOUBLE:
	case common.LTID_DECIMAL:
		ret._fun = MakeCastFunc[common.Hugeint, common.Decimal](tryCastBigintToDecimal)
	default:
		panic("usp")
	}
	return ret
}

func DecimalCastToSwitch(
	input *BindCastInput,
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.Id {
	case common.LTID_BOOLEAN:
	case common.LTID_TINYINT:
	case common.LTID_SMALLINT:
	case common.LTID_INTEGER:
	case common.LTID_BIGINT:
	case common.LTID_UTINYINT:
	case common.LTID_USMALLINT:
	case common.LTID_UINTEGER:
	case common.LTID_UBIGINT:
	case common.LTID_HUGEINT:
	case common.LTID_FLOAT:
		ret._fun = MakeCastFunc[common.Decimal, float32](tryCastDecimalToFloat32)
	case common.LTID_DOUBLE:
	case common.LTID_DECIMAL:
		decCast := func(input *common.Decimal, result *common.Decimal, _ bool) bool {
			return tryCastDecimalToDecimal(input, result, src.Scale, dst.Scale, true)
		}
		ret._fun = MakeCastFunc[common.Decimal, common.Decimal](decCast)
	default:
		panic("usp")
	}
	return ret
}

func StringCastToSwitch(
	input *BindCastInput,
	src, dst common.LType,
) *BoundCastInfo {
	ret := &BoundCastInfo{}
	switch dst.Id {
	case common.LTID_DATE:
		ret._fun = MakeCastFunc[common.String, common.Date](tryCastVarcharToDate)
	case common.LTID_INTERVAL:
		ret._fun = MakeCastFunc[common.String, common.Interval](tryCastVarcharToInterval)
	default:
		panic("usp")
	}
	return ret
}

func MakeCastFunc[T any, R any](op CastOp[T, R]) CastFuncType {
	temp := func(src *chunk.Vector, res *chunk.Vector, count int, params *CastParams) bool {
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
