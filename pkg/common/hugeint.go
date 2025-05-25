package common

import (
	"fmt"
	"math"
)

type Hugeint struct {
	Lower uint64
	Upper int64
}

func (h Hugeint) String() string {
	return fmt.Sprintf("[%d %d]", h.Upper, h.Lower)
}

func (h *Hugeint) Equal(o *Hugeint) bool {
	return h.Lower == o.Lower && h.Upper == o.Upper
}

func NegateHugeint(input *Hugeint, result *Hugeint) {
	if input.Upper == math.MinInt64 && input.Lower == 0 {
		panic("-hugeint overflow")
	}
	result.Lower = math.MaxUint64 - input.Lower + 1
	if input.Lower == 0 {
		result.Upper = -1 - input.Upper + 1
	} else {
		result.Upper = -1 - input.Upper
	}
}

// addInplace
// return
//
//	false : overflow
func AddInplace(lhs, rhs *Hugeint) bool {
	ladd := lhs.Lower + rhs.Lower
	overflow := int64(0)
	if ladd < lhs.Lower {
		overflow = 1
	}
	if rhs.Upper >= 0 {
		//rhs is positive
		if lhs.Upper > (math.MaxInt64 - rhs.Upper - overflow) {
			return false
		}
		lhs.Upper = lhs.Upper + overflow + rhs.Upper
	} else {
		//rhs is negative
		if lhs.Upper < (math.MinInt64 - rhs.Upper - overflow) {
			return false
		}
		lhs.Upper = lhs.Upper + (overflow + rhs.Upper)
	}
	lhs.Lower += rhs.Lower
	if lhs.Upper == math.MinInt64 && lhs.Lower == 0 {
		return false
	}
	return true
}

func (h *Hugeint) Add(lhs, rhs *Hugeint) {
	if !AddInplace(lhs, rhs) {
		panic("hugint and overflow")
	}
}

func (h *Hugeint) Mul(lhs, rhs *Hugeint) {}

func (h *Hugeint) Less(lhs, rhs *Hugeint) bool {
	panic("usp")
}

func (h *Hugeint) Greater(lhs, rhs *Hugeint) bool {
	panic("usp")
}
