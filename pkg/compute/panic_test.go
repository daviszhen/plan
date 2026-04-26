package compute

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/assert"
)

func TestPanic_LOT_String_Unknown(t *testing.T) {
	unknown := LOT(999)
	assert.Contains(t, unknown.String(), "unknown")
}

func TestPanic_POT_String_Unknown(t *testing.T) {
	unknown := POT(999)
	assert.Contains(t, unknown.String(), "unknown")
}

func TestPanic_DecideSide_UnknownExprType(t *testing.T) {
	e := &Expr{Typ: ET(999), DataTyp: common.IntegerType()}
	assert.NotPanics(t, func() {
		decideSide(e, map[uint64]bool{}, map[uint64]bool{})
	})
}

func TestPanic_CollectColRefs_UnknownExprType(t *testing.T) {
	e := &Expr{Typ: ET(999), DataTyp: common.IntegerType()}
	set := make(ColumnBindSet)
	assert.NotPanics(t, func() {
		collectColRefs(e, set)
	})
}

func TestPanic_ReferTo_UnknownExprType(t *testing.T) {
	e := &Expr{Typ: ET(999), DataTyp: common.IntegerType()}
	assert.NotPanics(t, func() {
		referTo(e, 1)
	})
}

func TestPanic_ReplaceColRef_UnknownExprType(t *testing.T) {
	e := &Expr{Typ: ET(999), DataTyp: common.IntegerType()}
	assert.NotPanics(t, func() {
		replaceColRef(e, ColumnBind{0, 0}, ColumnBind{1, 1})
	})
}
