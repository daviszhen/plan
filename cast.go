package main

var (
	// int32 => ...
	gTryCastInt32ToInt32            tryCastInt32ToInt32
	gTryCastInt32ToInt32OpWrapper   tryCastOpWrapper[int32, int32]
	gTryCastInt32ToFloat32          tryCastInt32ToFloat32
	gTryCastInt32ToFloat32OpWrapper tryCastOpWrapper[int32, float32]

	// float32 => ...
	gTryCastFloat32ToInt32          tryCastFloat32ToInt32
	gTryCastFloat32ToInt32OpWrapper tryCastOpWrapper[float32, int32]
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

type tryCastFloat32ToInt32 struct {
}

func (numCast tryCastFloat32ToInt32) operation(input *float32, result *int32) {
	*result = int32(*input)
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
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
}
