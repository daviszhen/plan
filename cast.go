package main

import (
	"fmt"

	dec "github.com/govalues/decimal"
)

var (
	// int32 => ...
	gTryCastInt32ToInt32            tryCastInt32ToInt32
	gTryCastInt32ToInt32OpWrapper   tryCastOpWrapper[int32, int32]
	gTryCastInt32ToFloat32          tryCastInt32ToFloat32
	gTryCastInt32ToFloat64          tryCastInt32ToFloat64
	gTryCastInt32ToFloat32OpWrapper tryCastOpWrapper[int32, float32]
	gTryCastInt32ToFloat64OpWrapper tryCastOpWrapper[int32, float64]
	gTryCastInt32ToDecimal          tryCastInt32ToDecimal
	gTryCastInt32ToDecimalWrapper   tryCastOpWrapper[int32, Decimal]

	// bigint =>
	gTryCastBigintToInt32            tryCastBigintToInt32
	gTryCastBigintToInt32OpWrapper   tryCastOpWrapper[Hugeint, int32]
	gTryCastBigintToDecimal          tryCastBigintToDecimal
	gTryCastBigintToDecimalOpWrapper tryCastOpWrapper[Hugeint, Decimal]

	// float32 => ...
	gTryCastFloat32ToInt32            tryCastFloat32ToInt32
	gTryCastFloat32ToFloat64          tryCastFloat32ToFloat64
	gTryCastFloat32ToInt32OpWrapper   tryCastOpWrapper[float32, int32]
	gTryCastFloat32ToFloat64OpWrapper tryCastOpWrapper[float32, float64]

	// decimal => ...
	gTryCastDecimalToFloat32          tryCastDecimalToFloat32
	gTryCastDecimalToFloat32OpWrapper tryCastOpWrapper[Decimal, float32]
)

type tryCastOpWrapper[T any, R any] struct {
}

func (tryCast tryCastOpWrapper[T, R]) operation(
	input *T,
	result *R,
	mask *Bitmap,
	idx int,
	op unaryOp[T, R],
	fun unaryFunc[T, R]) {
	op.operation(input, result)
}

type tryCastInt32ToInt32 struct {
}

func (numCast tryCastInt32ToInt32) operation(input *int32, result *int32) {
	*result = int32(*input)
}

type tryCastInt32ToFloat32 struct {
}

func (numCast tryCastInt32ToFloat32) operation(input *int32, result *float32) {
	*result = float32(*input)
}

type tryCastInt32ToFloat64 struct {
}

func (numCast tryCastInt32ToFloat64) operation(input *int32, result *float64) {
	*result = float64(*input)
}

type tryCastInt32ToDecimal struct {
}

func (numCast tryCastInt32ToDecimal) operation(input *int32, result *Decimal) {
	*result = Decimal{
		dec.MustNew(int64(*input), result.Scale()),
	}
}

type tryCastDecimalToFloat32 struct {
}

func (numCast tryCastDecimalToFloat32) operation(input *Decimal, result *float32) {
	v, ok := input.Float64()
	assertFunc(ok)
	*result = float32(v)
}

type tryCastBigintToInt32 struct{}

func (numCast tryCastBigintToInt32) operation(input *Hugeint, result *int32) {
	val := int32(input._lower)
	if uint64(val) != input._lower {
		fmt.Println(input)
	}
	*result = val
}

type tryCastBigintToDecimal struct{}

func (numCast tryCastBigintToDecimal) operation(input *Hugeint, result *Decimal) {
	panic("usp")
}

type tryCastFloat32ToInt32 struct {
}

func (numCast tryCastFloat32ToInt32) operation(input *float32, result *int32) {
	*result = int32(*input)
}

type tryCastFloat32ToFloat64 struct {
}

func (numCast tryCastFloat32ToFloat64) operation(input *float32, result *float64) {
	*result = float64(*input)
}

func castExec(
	source, result *Vector,
	count int,
) {
	switch source.typ().id {
	case LTID_INTEGER:
		switch result.typ().id {
		case LTID_INTEGER:
			unaryGenericExec[int32, int32](
				source,
				result,
				count,
				false,
				gTryCastInt32ToInt32,
				nil,
				gTryCastInt32ToInt32OpWrapper,
			)
		case LTID_FLOAT:
			unaryGenericExec[int32, float32](
				source,
				result,
				count,
				false,
				gTryCastInt32ToFloat32,
				nil,
				gTryCastInt32ToFloat32OpWrapper,
			)
		case LTID_DOUBLE:
			unaryGenericExec[int32, float64](
				source,
				result,
				count,
				false,
				gTryCastInt32ToFloat64,
				nil,
				gTryCastInt32ToFloat64OpWrapper,
			)
		case LTID_DECIMAL:
			unaryGenericExec[int32, Decimal](
				source,
				result,
				count,
				false,
				gTryCastInt32ToDecimal,
				nil,
				gTryCastInt32ToDecimalWrapper,
			)
		default:
			panic("usp")
		}
	case LTID_FLOAT:
		switch result.typ().id {
		case LTID_INTEGER:
			unaryGenericExec[float32, int32](
				source,
				result,
				count,
				false,
				gTryCastFloat32ToInt32,
				nil,
				gTryCastFloat32ToInt32OpWrapper,
			)
		case LTID_DOUBLE:
			unaryGenericExec[float32, float64](
				source,
				result,
				count,
				false,
				gTryCastFloat32ToFloat64,
				nil,
				gTryCastFloat32ToFloat64OpWrapper,
			)
		default:
			panic("usp")
		}
	case LTID_HUGEINT:
		switch result.typ().id {
		case LTID_INTEGER:
			unaryGenericExec[Hugeint, int32](
				source,
				result,
				count,
				false,
				gTryCastBigintToInt32,
				nil,
				gTryCastBigintToInt32OpWrapper,
			)
		case LTID_DECIMAL:
			unaryGenericExec[Hugeint, Decimal](
				source,
				result,
				count,
				false,
				gTryCastBigintToDecimal,
				nil,
				gTryCastBigintToDecimalOpWrapper,
			)
		default:
			panic("usp")
		}
	case LTID_DECIMAL:
		switch result.typ().id {
		case LTID_FLOAT:
			unaryGenericExec[Decimal, float32](
				source,
				result,
				count,
				false,
				gTryCastDecimalToFloat32,
				nil,
				gTryCastDecimalToFloat32OpWrapper,
			)
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}
