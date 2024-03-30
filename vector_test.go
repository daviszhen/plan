package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitmap(t *testing.T) {
	mask := &Bitmap{}
	mask.set(0, true)
	assert.True(t, mask._bits == nil)
	assert.True(t, mask.rowIsValid(0))
	mask.set(0, false)
	assert.True(t, !mask.rowIsValid(0))
	assert.True(t, mask._bits != nil)
	mask.set(100, false)
	assert.True(t, !mask.rowIsValid(100))
}

func newBoolConstVector(b bool, null bool) *Vector {
	vec := NewConstVector(boolean())
	data := getSliceInPhyFormatConst[bool](vec)
	data[0] = b
	setNullInPhyFormatConst(vec, null)
	return vec
}

func newBoolFlatVectorImpl(b bool, null bool, cnt int, fun func(i int) bool) *Vector {
	vec := NewFlatVector(boolean(), cnt)
	data := getSliceInPhyFormatFlat[bool](vec)
	for i := 0; i < cnt; i++ {
		data[i] = b
		if null && fun(i) {
			setNullInPhyFormatFlat(vec, uint64(i), null)
		}
	}
	return vec
}

func newBoolFlatVectorEven(b bool, null bool, cnt int) *Vector {
	return newBoolFlatVectorImpl(b, null, cnt, func(i int) bool {
		return i%2 == 0
	})
}

func newBoolFlatVectorOdd(b bool, null bool, cnt int) *Vector {
	return newBoolFlatVectorImpl(b, null, cnt, func(i int) bool {
		return i%2 != 0
	})
}

func Test_booleanNullMask(t *testing.T) {
	type args struct {
		left   *Vector
		right  *Vector
		result *Vector
		count  int
		boolOp BooleanOp

		wantNull   bool
		wantResult bool
	}

	/*
		TRUE  AND TRUE   = TRUE

		TRUE  AND FALSE  = FALSE
		FALSE AND TRUE   = FALSE
		FALSE AND FALSE  = FALSE
	*/
	arg1 := args{
		left:       newBoolConstVector(true, false),
		right:      newBoolConstVector(true, false),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   false,
		wantResult: true,
	}
	arg2 := args{
		left:       newBoolConstVector(true, false),
		right:      newBoolConstVector(false, false),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   false,
		wantResult: false,
	}
	arg3 := args{
		left:       newBoolConstVector(false, false),
		right:      newBoolConstVector(true, false),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   false,
		wantResult: false,
	}
	arg4 := args{
		left:       newBoolConstVector(false, false),
		right:      newBoolConstVector(false, false),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   false,
		wantResult: false,
	}

	/*
		FALSE AND NULL   = FALSE
		NULL  AND FALSE  = FALSE

		TRUE  AND NULL   = NULL
		NULL  AND TRUE   = NULL
		NULL  AND NULL   = NULL
	*/
	arg5 := args{
		left:       newBoolConstVector(false, false),
		right:      newBoolConstVector(false, true),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   false,
		wantResult: false,
	}
	arg6 := args{
		left:       newBoolConstVector(false, true),
		right:      newBoolConstVector(false, false),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   false,
		wantResult: false,
	}
	arg7 := args{
		left:       newBoolConstVector(true, false),
		right:      newBoolConstVector(false, true),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   true,
		wantResult: true,
	}
	arg8 := args{
		left:       newBoolConstVector(false, true),
		right:      newBoolConstVector(true, false),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   true,
		wantResult: true,
	}
	arg9 := args{
		left:       newBoolConstVector(false, true),
		right:      newBoolConstVector(true, true),
		result:     NewConstVector(boolean()),
		count:      defaultVectorSize,
		boolOp:     gAndOp,
		wantNull:   true,
		wantResult: true,
	}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "t1",
			args: arg1,
		},
		{
			name: "t2",
			args: arg2,
		},
		{
			name: "t3",
			args: arg3,
		},
		{
			name: "t4",
			args: arg4,
		},
		{
			name: "t5",
			args: arg5,
		},
		{
			name: "t6",
			args: arg6,
		},
		{
			name: "t7",
			args: arg7,
		},
		{
			name: "t8",
			args: arg8,
		},
		{
			name: "t9",
			args: arg9,
		},
		{
			name: "t1_0",
			args: arg9,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			booleanNullMask(tt.args.left, tt.args.right, tt.args.result, tt.args.count, tt.args.boolOp)
			if tt.args.wantNull {
				assert.True(t, isNullInPhyFormatConst(tt.args.result))
			} else {
				assert.False(t, isNullInPhyFormatConst(tt.args.result))
			}
			data := getSliceInPhyFormatConst[bool](tt.args.result)
			assert.Equal(t, tt.args.wantResult, data[0])
		})
	}
}

func Test_booleanNullMask_Flat(t *testing.T) {
	type args struct {
		left   *Vector
		right  *Vector
		result *Vector
		count  int
		boolOp BooleanOp

		checkResult func(t *testing.T, arg *args)
	}

	/*
		TRUE  AND TRUE   = TRUE

		TRUE  AND FALSE  = FALSE
		FALSE AND TRUE   = FALSE
		FALSE AND FALSE  = FALSE
	*/
	arg1 := args{
		left:   newBoolFlatVectorEven(true, false, defaultVectorSize),
		right:  newBoolConstVector(true, false),
		result: newBoolFlatVectorEven(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				assert.True(t, arg.result._mask.rowIsValid(uint64(i)))
				assert.True(t, result[i])
			}
		},
	}
	arg1_0 := args{
		left:   newBoolFlatVectorEven(true, true, defaultVectorSize),
		right:  newBoolConstVector(true, false),
		result: newBoolFlatVectorEven(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				if i%2 == 0 {
					assert.False(t, arg.result._mask.rowIsValid(uint64(i)))
				} else {
					assert.True(t, result[i])
				}
			}
		},
	}
	arg1_1 := args{
		left:   newBoolFlatVectorEven(true, true, defaultVectorSize),
		right:  newBoolFlatVectorOdd(true, true, defaultVectorSize),
		result: newBoolFlatVectorEven(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			for i := 0; i < defaultVectorSize; i++ {
				assert.False(t, arg.result._mask.rowIsValid(uint64(i)))
			}
		},
	}
	//	TRUE  AND FALSE  = FALSE
	arg2 := args{
		left:   newBoolConstVector(true, false),
		right:  newBoolFlatVectorOdd(false, false, defaultVectorSize),
		result: newBoolFlatVectorOdd(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				assert.True(t, arg.result._mask.rowIsValid(uint64(i)))
				assert.False(t, result[i])
			}
		},
	}
	//FALSE AND TRUE   = FALSE
	arg3 := args{
		left:   newBoolFlatVectorOdd(false, false, defaultVectorSize),
		right:  newBoolFlatVectorOdd(true, false, defaultVectorSize),
		result: newBoolFlatVectorOdd(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				assert.True(t, arg.result._mask.rowIsValid(uint64(i)))
				assert.False(t, result[i])
			}
		},
	}
	//FALSE AND FALSE  = FALSE
	arg4 := args{
		left:   newBoolFlatVectorOdd(false, false, defaultVectorSize),
		right:  newBoolFlatVectorOdd(false, false, defaultVectorSize),
		result: newBoolFlatVectorOdd(true, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				assert.True(t, arg.result._mask.rowIsValid(uint64(i)))
				assert.False(t, result[i])
			}
		},
	}

	/*
		FALSE AND NULL   = FALSE
		NULL  AND FALSE  = FALSE

		TRUE  AND NULL   = NULL
		NULL  AND TRUE   = NULL
		NULL  AND NULL   = NULL
	*/
	//arg5 := args{
	//	left:       newBoolConstVector(false, false),
	//	right:      newBoolConstVector(false, true),
	//	result:     NewConstVector(boolean()),
	//	count:      defaultVectorSize,
	//	boolOp:     gAndOp,
	//	wantNull:   false,
	//	wantResult: false,
	//}
	//arg6 := args{
	//	left:       newBoolConstVector(false, true),
	//	right:      newBoolConstVector(false, false),
	//	result:     NewConstVector(boolean()),
	//	count:      defaultVectorSize,
	//	boolOp:     gAndOp,
	//	wantNull:   false,
	//	wantResult: false,
	//}
	//arg7 := args{
	//	left:       newBoolConstVector(true, false),
	//	right:      newBoolConstVector(false, true),
	//	result:     NewConstVector(boolean()),
	//	count:      defaultVectorSize,
	//	boolOp:     gAndOp,
	//	wantNull:   true,
	//	wantResult: true,
	//}
	//arg8 := args{
	//	left:       newBoolConstVector(false, true),
	//	right:      newBoolConstVector(true, false),
	//	result:     NewConstVector(boolean()),
	//	count:      defaultVectorSize,
	//	boolOp:     gAndOp,
	//	wantNull:   true,
	//	wantResult: true,
	//}
	//arg9 := args{
	//	left:       newBoolConstVector(false, true),
	//	right:      newBoolConstVector(true, true),
	//	result:     NewConstVector(boolean()),
	//	count:      defaultVectorSize,
	//	boolOp:     gAndOp,
	//	wantNull:   true,
	//	wantResult: true,
	//}

	tests := []struct {
		name string
		args args
	}{
		{
			name: "t1",
			args: arg1,
		},
		{
			name: "t1_0",
			args: arg1_0,
		},
		{
			name: "t1_1",
			args: arg1_1,
		},
		{
			name: "t2",
			args: arg2,
		},
		{
			name: "t3",
			args: arg3,
		},
		{
			name: "t4",
			args: arg4,
		},
		//{
		//	name: "t5",
		//	args: arg5,
		//},
		//{
		//	name: "t6",
		//	args: arg6,
		//},
		//{
		//	name: "t7",
		//	args: arg7,
		//},
		//{
		//	name: "t8",
		//	args: arg8,
		//},
		//{
		//	name: "t9",
		//	args: arg9,
		//},
		//{
		//	name: "t1_0",
		//	args: arg9,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			booleanNullMask(tt.args.left, tt.args.right, tt.args.result, tt.args.count, tt.args.boolOp)
			if tt.args.checkResult != nil {
				tt.args.checkResult(t, &tt.args)
			}
		})
	}
}
