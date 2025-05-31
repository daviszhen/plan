package chunk

import (
	"github.com/daviszhen/plan/pkg/util"
)

type UnifiedFormat struct {
	Sel      *SelectVector
	Data     []byte
	Mask     *util.Bitmap
	InterSel SelectVector
	PTypSize int
}

func GetSliceInPhyFormatUnifiedFormat[T any](uni *UnifiedFormat) []T {
	return util.ToSlice[T](uni.Data, uni.PTypSize)
}
