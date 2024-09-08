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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitmap(t *testing.T) {
	mask := &Bitmap{
		_bits: []uint8{0, 0},
	}
	mask.set(0, true)
	assert.True(t, len(mask._bits) == 2)
	assert.True(t, mask.rowIsValid(0))
	mask.set(0, false)
	assert.True(t, !mask.rowIsValid(0))
	assert.True(t, mask._bits != nil)
	mask.set(defaultVectorSize, false)
	assert.True(t, !mask.rowIsValid(defaultVectorSize))
}

func newInt32ConstVector(v int32, null bool) *Vector {
	vec := NewConstVector(integer())
	data := getSliceInPhyFormatConst[int32](vec)
	data[0] = v
	setNullInPhyFormatConst(vec, null)
	return vec
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

func newInt32FlatVectorImpl(null bool, cnt int, fill func(i int) int32, fun func(i int) bool) *Vector {
	vec := NewFlatVector(integer(), cnt)
	data := getSliceInPhyFormatFlat[int32](vec)
	for i := 0; i < cnt; i++ {
		data[i] = fill(i)
		if null && fun(i) {
			setNullInPhyFormatFlat(vec, uint64(i), null)
		}
	}
	return vec
}

func newInt32FlatVectorEven(null bool, cnt int) *Vector {
	return newInt32FlatVectorImpl(
		null,
		cnt,
		func(i int) int32 {
			return int32(i)
		},
		func(i int) bool {
			return i%2 == 0
		})
}

func newInt32FlatVectorOdd(null bool, cnt int) *Vector {
	return newInt32FlatVectorImpl(
		null,
		cnt,
		func(i int) int32 {
			return int32(i)
		},
		func(i int) bool {
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

	//TRUE  AND TRUE   = TRUE
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

	//FALSE AND NULL   = FALSE
	arg5 := args{
		left:   newBoolFlatVectorOdd(false, false, defaultVectorSize),
		right:  newBoolFlatVectorOdd(false, true, defaultVectorSize),
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
	//NULL  AND FALSE  = FALSE
	arg6 := args{
		left:   newBoolFlatVectorOdd(false, true, defaultVectorSize),
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
	//TRUE  AND NULL   = NULL
	arg7 := args{
		left:   newBoolFlatVectorOdd(true, false, defaultVectorSize),
		right:  newBoolFlatVectorEven(false, true, defaultVectorSize),
		result: newBoolFlatVectorOdd(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				if i%2 == 0 {
					assert.False(t, arg.result._mask.rowIsValid(uint64(i)))
				} else {
					assert.False(t, result[i])
				}
			}
		},
	}
	//NULL  AND TRUE   = NULL
	arg8 := args{
		left:   newBoolFlatVectorEven(false, true, defaultVectorSize),
		right:  newBoolFlatVectorOdd(true, false, defaultVectorSize),
		result: newBoolFlatVectorOdd(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				if i%2 == 0 {
					assert.False(t, arg.result._mask.rowIsValid(uint64(i)))
				} else {
					assert.False(t, result[i])
				}
			}
		},
	}
	//NULL  AND NULL   = NULL
	arg9 := args{
		left:   newBoolFlatVectorEven(false, true, defaultVectorSize),
		right:  newBoolFlatVectorEven(true, true, defaultVectorSize),
		result: newBoolFlatVectorOdd(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			for i := 0; i < defaultVectorSize; i++ {
				if i%2 == 0 {
					assert.False(t, arg.result._mask.rowIsValid(uint64(i)))
				}
			}
		},
	}
	arg9_0 := args{
		left:   newBoolFlatVectorEven(false, true, defaultVectorSize),
		right:  newBoolFlatVectorOdd(true, true, defaultVectorSize),
		result: newBoolFlatVectorOdd(false, false, defaultVectorSize),
		count:  defaultVectorSize,
		boolOp: gAndOp,
		checkResult: func(t *testing.T, arg *args) {
			assert.True(t, arg.result.phyFormat() == PF_FLAT)
			result := getSliceInPhyFormatFlat[bool](arg.result)
			for i := 0; i < defaultVectorSize; i++ {
				if i%2 == 0 {
					assert.False(t, arg.result._mask.rowIsValid(uint64(i)))
				} else {
					assert.True(t, arg.result._mask.rowIsValid(uint64(i)))
					assert.False(t, result[i])
				}
			}
		},
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
			name: "t9_0",
			args: arg9_0,
		},
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

func Test_randomVector(t *testing.T) {
	typs := []LType{
		integer(),
		decimal(DecimalMaxWidthInt64, 2),
		varchar(),
	}
	pfs := []PhyFormat{
		PF_FLAT,
		PF_CONST,
	}
	for _, typ := range typs {
		for _, pf := range pfs {
			vec := randomVector(typ, pf, 0.2)
			fmt.Println("type", typ.String(), "phy_format", pf.String())
			fmt.Println("vec")
			vec.print(defaultVectorSize)
		}
	}
}
