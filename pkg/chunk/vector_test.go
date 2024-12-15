package chunk

import (
	"github.com/daviszhen/plan/pkg/common"
)

//
//func TestBitmap(t *testing.T) {
//	mask := &util.Bitmap{
//		Bits: []uint8{0, 0},
//	}
//	mask.Set(0, true)
//	assert.True(t, len(mask.Bits) == 2)
//	assert.True(t, mask.RowIsValid(0))
//	mask.Set(0, false)
//	assert.True(t, !mask.RowIsValid(0))
//	assert.True(t, mask.Bits != nil)
//	mask.Set(util.DefaultVectorSize, false)
//	assert.True(t, !mask.RowIsValid(util.DefaultVectorSize))
//}
//

//
//func newBoolConstVector(b bool, null bool) *Vector {
//	vec := NewConstVector(common.BooleanType())
//	data := GetSliceInPhyFormatConst[bool](vec)
//	data[0] = b
//	SetNullInPhyFormatConst(vec, null)
//	return vec
//}
//
//func newBoolFlatVectorImpl(b bool, null bool, cnt int, fun func(i int) bool) *Vector {
//	vec := NewFlatVector(common.BooleanType(), cnt)
//	data := GetSliceInPhyFormatFlat[bool](vec)
//	for i := 0; i < cnt; i++ {
//		data[i] = b
//		if null && fun(i) {
//			SetNullInPhyFormatFlat(vec, uint64(i), null)
//		}
//	}
//	return vec
//}
//
//func newBoolFlatVectorEven(b bool, null bool, cnt int) *Vector {
//	return newBoolFlatVectorImpl(b, null, cnt, func(i int) bool {
//		return i%2 == 0
//	})
//}
//
//func newBoolFlatVectorOdd(b bool, null bool, cnt int) *Vector {
//	return newBoolFlatVectorImpl(b, null, cnt, func(i int) bool {
//		return i%2 != 0
//	})
//}
//
//func newInt32FlatVectorImpl(null bool, cnt int, fill func(i int) int32, fun func(i int) bool) *Vector {
//	vec := NewFlatVector(common.IntegerType(), cnt)
//	data := GetSliceInPhyFormatFlat[int32](vec)
//	for i := 0; i < cnt; i++ {
//		data[i] = fill(i)
//		if null && fun(i) {
//			SetNullInPhyFormatFlat(vec, uint64(i), null)
//		}
//	}
//	return vec
//}
//
//func newInt32FlatVectorEven(null bool, cnt int) *Vector {
//	return newInt32FlatVectorImpl(
//		null,
//		cnt,
//		func(i int) int32 {
//			return int32(i)
//		},
//		func(i int) bool {
//			return i%2 == 0
//		})
//}
//
//func newInt32FlatVectorOdd(null bool, cnt int) *Vector {
//	return newInt32FlatVectorImpl(
//		null,
//		cnt,
//		func(i int) int32 {
//			return int32(i)
//		},
//		func(i int) bool {
//			return i%2 != 0
//		})
//}
//
//func Test_booleanNullMask(t *testing.T) {
//	type args struct {
//		left   *Vector
//		right  *Vector
//		result *Vector
//		count  int
//		boolOp plan.BooleanOp
//
//		wantNull   bool
//		wantResult bool
//	}
//
//	/*
//		TRUE  AND TRUE   = TRUE
//
//		TRUE  AND FALSE  = FALSE
//		FALSE AND TRUE   = FALSE
//		FALSE AND FALSE  = FALSE
//	*/
//	arg1 := args{
//		left:       newBoolConstVector(true, false),
//		right:      newBoolConstVector(true, false),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   false,
//		wantResult: true,
//	}
//	arg2 := args{
//		left:       newBoolConstVector(true, false),
//		right:      newBoolConstVector(false, false),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   false,
//		wantResult: false,
//	}
//	arg3 := args{
//		left:       newBoolConstVector(false, false),
//		right:      newBoolConstVector(true, false),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   false,
//		wantResult: false,
//	}
//	arg4 := args{
//		left:       newBoolConstVector(false, false),
//		right:      newBoolConstVector(false, false),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   false,
//		wantResult: false,
//	}
//
//	/*
//		FALSE AND NULL   = FALSE
//		NULL  AND FALSE  = FALSE
//
//		TRUE  AND NULL   = NULL
//		NULL  AND TRUE   = NULL
//		NULL  AND NULL   = NULL
//	*/
//	arg5 := args{
//		left:       newBoolConstVector(false, false),
//		right:      newBoolConstVector(false, true),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   false,
//		wantResult: false,
//	}
//	arg6 := args{
//		left:       newBoolConstVector(false, true),
//		right:      newBoolConstVector(false, false),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   false,
//		wantResult: false,
//	}
//	arg7 := args{
//		left:       newBoolConstVector(true, false),
//		right:      newBoolConstVector(false, true),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   true,
//		wantResult: true,
//	}
//	arg8 := args{
//		left:       newBoolConstVector(false, true),
//		right:      newBoolConstVector(true, false),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   true,
//		wantResult: true,
//	}
//	arg9 := args{
//		left:       newBoolConstVector(false, true),
//		right:      newBoolConstVector(true, true),
//		result:     NewConstVector(common.BooleanType()),
//		count:      util.DefaultVectorSize,
//		boolOp:     plan.gAndOp,
//		wantNull:   true,
//		wantResult: true,
//	}
//
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			name: "t1",
//			args: arg1,
//		},
//		{
//			name: "t2",
//			args: arg2,
//		},
//		{
//			name: "t3",
//			args: arg3,
//		},
//		{
//			name: "t4",
//			args: arg4,
//		},
//		{
//			name: "t5",
//			args: arg5,
//		},
//		{
//			name: "t6",
//			args: arg6,
//		},
//		{
//			name: "t7",
//			args: arg7,
//		},
//		{
//			name: "t8",
//			args: arg8,
//		},
//		{
//			name: "t9",
//			args: arg9,
//		},
//		{
//			name: "t1_0",
//			args: arg9,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			plan.booleanNullMask(tt.args.left, tt.args.right, tt.args.result, tt.args.count, tt.args.boolOp)
//			if tt.args.wantNull {
//				assert.True(t, IsNullInPhyFormatConst(tt.args.result))
//			} else {
//				assert.False(t, IsNullInPhyFormatConst(tt.args.result))
//			}
//			data := GetSliceInPhyFormatConst[bool](tt.args.result)
//			assert.Equal(t, tt.args.wantResult, data[0])
//		})
//	}
//}
//
//func Test_booleanNullMask_Flat(t *testing.T) {
//	type args struct {
//		left   *Vector
//		right  *Vector
//		result *Vector
//		count  int
//		boolOp plan.BooleanOp
//
//		checkResult func(t *testing.T, arg *args)
//	}
//
//	//TRUE  AND TRUE   = TRUE
//	arg1 := args{
//		left:   newBoolFlatVectorEven(true, false, util.DefaultVectorSize),
//		right:  newBoolConstVector(true, false),
//		result: newBoolFlatVectorEven(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//				assert.True(t, result[i])
//			}
//		},
//	}
//	arg1_0 := args{
//		left:   newBoolFlatVectorEven(true, true, util.DefaultVectorSize),
//		right:  newBoolConstVector(true, false),
//		result: newBoolFlatVectorEven(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				if i%2 == 0 {
//					assert.False(t, arg.result.Mask.RowIsValid(uint64(i)))
//				} else {
//					assert.True(t, result[i])
//				}
//			}
//		},
//	}
//	arg1_1 := args{
//		left:   newBoolFlatVectorEven(true, true, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(true, true, util.DefaultVectorSize),
//		result: newBoolFlatVectorEven(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.False(t, arg.result.Mask.RowIsValid(uint64(i)))
//			}
//		},
//	}
//	//	TRUE  AND FALSE  = FALSE
//	arg2 := args{
//		left:   newBoolConstVector(true, false),
//		right:  newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//				assert.False(t, result[i])
//			}
//		},
//	}
//	//FALSE AND TRUE   = FALSE
//	arg3 := args{
//		left:   newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(true, false, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//				assert.False(t, result[i])
//			}
//		},
//	}
//	//FALSE AND FALSE  = FALSE
//	arg4 := args{
//		left:   newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(true, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//				assert.False(t, result[i])
//			}
//		},
//	}
//
//	//FALSE AND NULL   = FALSE
//	arg5 := args{
//		left:   newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(false, true, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//				assert.False(t, result[i])
//			}
//		},
//	}
//	//NULL  AND FALSE  = FALSE
//	arg6 := args{
//		left:   newBoolFlatVectorOdd(false, true, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//				assert.False(t, result[i])
//			}
//		},
//	}
//	//TRUE  AND NULL   = NULL
//	arg7 := args{
//		left:   newBoolFlatVectorOdd(true, false, util.DefaultVectorSize),
//		right:  newBoolFlatVectorEven(false, true, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				if i%2 == 0 {
//					assert.False(t, arg.result.Mask.RowIsValid(uint64(i)))
//				} else {
//					assert.False(t, result[i])
//				}
//			}
//		},
//	}
//	//NULL  AND TRUE   = NULL
//	arg8 := args{
//		left:   newBoolFlatVectorEven(false, true, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(true, false, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				if i%2 == 0 {
//					assert.False(t, arg.result.Mask.RowIsValid(uint64(i)))
//				} else {
//					assert.False(t, result[i])
//				}
//			}
//		},
//	}
//	//NULL  AND NULL   = NULL
//	arg9 := args{
//		left:   newBoolFlatVectorEven(false, true, util.DefaultVectorSize),
//		right:  newBoolFlatVectorEven(true, true, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				if i%2 == 0 {
//					assert.False(t, arg.result.Mask.RowIsValid(uint64(i)))
//				}
//			}
//		},
//	}
//	arg9_0 := args{
//		left:   newBoolFlatVectorEven(false, true, util.DefaultVectorSize),
//		right:  newBoolFlatVectorOdd(true, true, util.DefaultVectorSize),
//		result: newBoolFlatVectorOdd(false, false, util.DefaultVectorSize),
//		count:  util.DefaultVectorSize,
//		boolOp: plan.gAndOp,
//		checkResult: func(t *testing.T, arg *args) {
//			assert.True(t, arg.result.PhyFormat() == PF_FLAT)
//			result := GetSliceInPhyFormatFlat[bool](arg.result)
//			for i := 0; i < util.DefaultVectorSize; i++ {
//				if i%2 == 0 {
//					assert.False(t, arg.result.Mask.RowIsValid(uint64(i)))
//				} else {
//					assert.True(t, arg.result.Mask.RowIsValid(uint64(i)))
//					assert.False(t, result[i])
//				}
//			}
//		},
//	}
//
//	tests := []struct {
//		name string
//		args args
//	}{
//		{
//			name: "t1",
//			args: arg1,
//		},
//		{
//			name: "t1_0",
//			args: arg1_0,
//		},
//		{
//			name: "t1_1",
//			args: arg1_1,
//		},
//		{
//			name: "t2",
//			args: arg2,
//		},
//		{
//			name: "t3",
//			args: arg3,
//		},
//		{
//			name: "t4",
//			args: arg4,
//		},
//		{
//			name: "t5",
//			args: arg5,
//		},
//		{
//			name: "t6",
//			args: arg6,
//		},
//		{
//			name: "t7",
//			args: arg7,
//		},
//		{
//			name: "t8",
//			args: arg8,
//		},
//		{
//			name: "t9",
//			args: arg9,
//		},
//		{
//			name: "t9_0",
//			args: arg9_0,
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			plan.booleanNullMask(tt.args.left, tt.args.right, tt.args.result, tt.args.count, tt.args.boolOp)
//			if tt.args.checkResult != nil {
//				tt.args.checkResult(t, &tt.args)
//			}
//		})
//	}
//}
//
//func Test_randomVector(t *testing.T) {
//	typs := []common.LType{
//		common.IntegerType(),
//		common.DecimalType(common.DecimalMaxWidthInt64, 2),
//		common.VarcharType(),
//	}
//	pfs := []PhyFormat{
//		PF_FLAT,
//		PF_CONST,
//	}
//	for _, typ := range typs {
//		for _, pf := range pfs {
//			vec := plan.randomVector(typ, pf, 0.2)
//			fmt.Println("type", typ.String(), "phy_format", pf.String())
//			fmt.Println("vec")
//			vec.Print(util.DefaultVectorSize)
//		}
//	}
//}
