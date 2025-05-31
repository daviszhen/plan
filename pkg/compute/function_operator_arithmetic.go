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

package compute

import (
	"math"

	"github.com/daviszhen/plan/pkg/common"
)

func addInt8(left, right, result *int8) {
	*result = *left + *right
}

func addInt16(left, right, result *int16) {
	*result = *left + *right
}

func addInt32(left, right, result *int32) {
	*result = *left + *right
}

func addInt64(left, right, result *int64) {
	*result = *left + *right
}

func addUint8(left, right, result *uint8) {
	*result = *left + *right
}

func addUint16(left, right, result *uint16) {
	*result = *left + *right
}

func addUint32(left, right, result *uint32) {
	*result = *left + *right
}

func addUint64(left, right, result *uint64) {
	*result = *left + *right
}

func GetScalarIntegerAddFunction(ptyp common.PhyType, checkOverflow bool) ScalarFunc {
	if checkOverflow {
		return GetScalarIntegerAddFunctionWithOverflowCheck(ptyp)
	} else {
		return GetScalarIntegerAddFunctionWithoutOverflowCheck(ptyp)
	}
}

func GetScalarIntegerAddFunctionWithoutOverflowCheck(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT8:
		return BinaryFunction[int8, int8, int8](addInt8)
	case common.INT16:
		return BinaryFunction[int16, int16, int16](addInt16)
	case common.INT32:
		return BinaryFunction[int32, int32, int32](addInt32)
	case common.INT64:
		return BinaryFunction[int64, int64, int64](addInt64)
	case common.UINT8:
		return BinaryFunction[uint8, uint8, uint8](addUint8)
	case common.UINT16:
		return BinaryFunction[uint16, uint16, uint16](addUint16)
	case common.UINT32:
		return BinaryFunction[uint32, uint32, uint32](addUint32)
	case common.UINT64:
		return BinaryFunction[uint64, uint64, uint64](addUint64)
	case common.DECIMAL:
		return BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDecimalAddOp)
	default:
		panic("usp")
	}
}

func addInt8CheckOf(left, right, result *int8) {
	ul := int16(*left)
	ur := int16(*right)
	ures := int16(0)
	addInt16(&ul, &ur, &ures)
	if ures > math.MaxInt8 || ures < math.MinInt8 {
		panic("int8 + int8 overflow")
	}
	*result = int8(ures)
}

func addInt16CheckOf(left, right, result *int16) {
	ul := int32(*left)
	ur := int32(*right)
	ures := int32(0)
	addInt32(&ul, &ur, &ures)
	if ures > math.MaxInt16 || ures < math.MinInt16 {
		panic("int16 + int16 overflow")
	}
	*result = int16(ures)
}

func addInt32CheckOf(left, right, result *int32) {
	ul := int64(*left)
	ur := int64(*right)
	ures := int64(0)
	addInt64(&ul, &ur, &ures)
	if ures > math.MaxInt32 || ures < math.MinInt32 {
		panic("int32 + int32 overflow")
	}
	*result = int32(ures)
}

func addInt64CheckOf(left, right, result *int64) {
	ul := uint64(*left)
	ur := uint64(*right)
	ures := int64(ul - ur)
	if (*left < 0 && *right < 0 && ures >= 0) ||
		(*left >= 0 && *right >= 0 && ures < 0) {
		*result = ures
	} else {
		panic("int64 + int64 overflow")
	}
}

func addUint8CheckOf(left, right, result *uint8) {
	ul := uint16(*left)
	ur := uint16(*right)
	ures := uint16(*result)
	addUint16(&ul, &ur, &ures)
	if ures > math.MaxUint8 {
		panic("uint8 + uint8 overflow")
	}
	*result = uint8(ures)
}

func addUint16CheckOf(left, right, result *uint16) {
	ul := uint32(*left)
	ur := uint32(*right)
	ures := uint32(0)
	addUint32(&ul, &ur, &ures)
	if ures > math.MaxUint16 {
		panic("uint16 + uint16 overflow")
	}
	*result = uint16(ures)
}

func addUint32CheckOf(left, right, result *uint32) {
	ul := uint64(*left)
	ur := uint64(*right)
	ures := uint64(0)
	addUint64(&ul, &ur, &ures)
	if ures > math.MaxUint32 {
		panic("uint16 + uint16 overflow")
	}
	*result = uint32(ures)
}

func addUint64CheckOf(left, right, result *uint64) {
	if math.MaxUint64-*left < *right {
		panic("uint64 + uint64 overflow")
	}
	*result = *left + *right
}

func GetScalarIntegerAddFunctionWithOverflowCheck(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT8:
		return BinaryFunction[int8, int8, int8](addInt8CheckOf)
	case common.INT16:
		return BinaryFunction[int16, int16, int16](addInt16CheckOf)
	case common.INT32:
		return BinaryFunction[int32, int32, int32](addInt32CheckOf)
	case common.INT64:
		return BinaryFunction[int64, int64, int64](addInt64CheckOf)
	case common.UINT8:
		return BinaryFunction[uint8, uint8, uint8](addUint8CheckOf)
	case common.UINT16:
		return BinaryFunction[uint16, uint16, uint16](addUint16CheckOf)
	case common.UINT32:
		return BinaryFunction[uint32, uint32, uint32](addUint32CheckOf)
	case common.UINT64:
		return BinaryFunction[uint64, uint64, uint64](addUint64CheckOf)
	case common.DECIMAL:
		return BinaryFunction[common.Decimal, common.Decimal, common.Decimal](binDecimalDecimalAddOp)
	default:
		panic("not implement")
	}
}

func addHugeint(left, right, result *common.Hugeint) {
	panic("usp")
}

func subHugeint(left *common.Hugeint, right *common.Hugeint, result *common.Hugeint) {
	panic("usp")
}

func addFloat32(left, right, result *float32) {
	*result = *left + *right
}

func addFloat64(left, right, result *float64) {
	*result = *left + *right
}

func GetScalarBinaryAddFunction(ptyp common.PhyType, checkOverflow bool) ScalarFunc {
	if checkOverflow {
		return GetScalarBinaryAddFunctionWithOverflowCheck(ptyp)
	} else {
		return GetScalarBinaryAddFunctionWithoutOverflowCheck(ptyp)
	}
}

func GetScalarBinaryAddFunctionWithoutOverflowCheck(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT128:
		return BinaryFunction[common.Hugeint, common.Hugeint, common.Hugeint](addHugeint)
	case common.FLOAT:
		return BinaryFunction[float32, float32, float32](addFloat32)
	case common.DOUBLE:
		return BinaryFunction[float64, float64, float64](addFloat64)
	default:
		return GetScalarIntegerAddFunction(ptyp, false)
	}
}

func GetScalarBinaryAddFunctionWithOverflowCheck(ptyp common.PhyType) ScalarFunc {
	switch ptyp {
	case common.INT128:
		return BinaryFunction[common.Hugeint, common.Hugeint, common.Hugeint](addHugeint)
	case common.FLOAT:
		return BinaryFunction[float32, float32, float32](addFloat32)
	case common.DOUBLE:
		return BinaryFunction[float64, float64, float64](addFloat64)
	default:
		return GetScalarIntegerAddFunction(ptyp, true)
	}
}
