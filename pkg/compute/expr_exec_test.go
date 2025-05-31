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
	"fmt"
	"testing"
)

//
//func makeInt32Chunks(cnt int) []*Chunk {
//	ret := make([]*Chunk, 0)
//	for i := 0; i < 3; i++ {
//		data := &Chunk{}
//		ltyps := make([]common.LType, cnt)
//		for j := 0; j < cnt; j++ {
//			ltyps[j] = common.IntegerType()
//		}
//		data.init(ltyps, util.DefaultVectorSize)
//		for j := 0; j < cnt; j++ {
//			data.Data[j] = chunk.newInt32FlatVectorEven(false, util.DefaultVectorSize)
//		}
//		data.setCard(util.DefaultVectorSize)
//		ret = append(ret, data)
//	}
//
//	return ret
//}

//func Test_testExecute1(t *testing.T) {
//	pplan := runTest2(t, tpchQ20())
//	exprs := collectFilterExprs(pplan)
//	compExprs := findExpr(exprs, func(expr *Expr) bool {
//		if expr == nil {
//			return false
//		}
//		//if expr.Typ == ET_Func && expr.SubTyp == ET_Equal && expr.Children[0].DataTyp.LTyp.Id == LTID_INTEGER {
//		if expr.Typ == ET_Func && len(expr.Children) != 0 &&
//			(expr.Children[0].DataTyp.LTyp.Id == LTID_INTEGER || expr.Children[0].DataTyp.LTyp.Id == LTID_FLOAT) {
//			return true
//		}
//		return false
//	})
//	for _, expr := range compExprs {

//	}
//
//	eExec := NewExprExec(compExprs...)
//	{
//		//(partsupp.ps_suppkey,{(LTID_INTEGER INT32 0 0),not null},[-1 0],0) in ((supplier.s_suppkey,{(LTID_INTEGER INT32 0 0),not null},[-2 0],0))
//		data := makeInt32Chunks(4)
//		result := NewFlatVector(boolean(), defaultVectorSize)
//		err := eExec.executeExprI(data, 0, result)
//		assert.NoError(t, err)
//
//		//equal
//		assert.True(t, result.phyFormat() == PF_FLAT)
//		resSlice := getSliceInPhyFormatFlat[bool](result)
//		for i := 0; i < defaultVectorSize; i++ {
//			assert.True(t, result.Mask.rowIsValid(uint64(i)))
//			assert.True(t, resSlice[i])
//		}
//
//		//not equal
//		data[0].Data[0] = newInt32ConstVector(-1, false)
//		err = eExec.executeExprI(data, 0, result)
//		assert.NoError(t, err)
//		assert.True(t, result.phyFormat() == PF_FLAT)
//		resSlice = getSliceInPhyFormatFlat[bool](result)
//		for i := 0; i < defaultVectorSize; i++ {
//			assert.True(t, result.Mask.rowIsValid(uint64(i)))
//			assert.False(t, resSlice[i])
//		}
//	}
//
//	{
//		//filter 1
//		//│   └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  in
//		//│       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[-2 0],0)
//		//│       └── [ {(LTID_INTEGER INT32 0 0),null}]  cast
//		//│           ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 0],0)
//		//│           └── [ {(LTID_INTEGER INT32 0 0),null}]  (0)
//		data := makeInt32Chunks(4)
//		result := NewFlatVector(boolean(), defaultVectorSize)
//		err := eExec.executeExprI(data, 1, result)
//		assert.NoError(t, err)
//
//		//equal
//		assert.True(t, result.phyFormat() == PF_FLAT)
//		resSlice := getSliceInPhyFormatFlat[bool](result)
//		for i := 0; i < defaultVectorSize; i++ {
//			assert.True(t, result.Mask.rowIsValid(uint64(i)))
//			assert.True(t, resSlice[i])
//		}
//
//		//not equal
//		data[1].Data[0] = newInt32ConstVector(-1, false)
//		err = eExec.executeExprI(data, 1, result)
//		assert.NoError(t, err)
//		assert.True(t, result.phyFormat() == PF_FLAT)
//		resSlice = getSliceInPhyFormatFlat[bool](result)
//		for i := 0; i < defaultVectorSize; i++ {
//			assert.True(t, result.Mask.rowIsValid(uint64(i)))
//			assert.False(t, resSlice[i])
//		}
//	}
//	{
//		//filter 2
//		//└── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >
//		//    ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
//		//    └── [ {(LTID_INTEGER INT32 0 0),null}]  cast
//		//        ├── [ {(LTID_FLOAT FLOAT 0 0),not null}]  *
//		//        │   ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0.5)
//		//        │   └── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
//		//        │       ├── [ {(LTID_INTEGER INT32 0 0),null}]  (AggNode_16.sum(l_quantity),[16 0],0)
//		//        │       └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
//		//        └── [ {(LTID_INTEGER INT32 0 0),null}]  (0)
//		data := makeInt32Chunks(4)
//
//		///////////// const vector
//		//[-1 2]
//		data[0].Data[2] = newInt32ConstVector(2, false)
//		//[16 0]
//		data[2].Data[0] = newInt32ConstVector(16, false)
//		result := NewFlatVector(boolean(), defaultVectorSize)
//		err := eExec.executeExprI(data, 2, result)
//		assert.NoError(t, err)
//
//		//> : false
//		assert.True(t, result.phyFormat() == PF_CONST)
//		resSlice := getSliceInPhyFormatFlat[bool](result)
//		{
//			assert.True(t, result.Mask.rowIsValid(uint64(0)))
//			assert.False(t, resSlice[0])
//		}
//
//		//[-1 2]
//		data[0].Data[2] = newInt32ConstVector(8, false)
//		//[16 0]
//		data[2].Data[0] = newInt32ConstVector(6, false)
//		result = NewFlatVector(boolean(), defaultVectorSize)
//		err = eExec.executeExprI(data, 2, result)
//		assert.NoError(t, err)
//
//		//> : true
//		assert.True(t, result.phyFormat() == PF_CONST)
//		resSlice = getSliceInPhyFormatFlat[bool](result)
//		{
//			assert.True(t, result.Mask.rowIsValid(uint64(0)))
//			assert.True(t, resSlice[0])
//		}
//
//		///////////// flat vector
//		//[-1 2]
//		data[0].Data[2] = newInt32FlatVectorImpl(
//			false,
//			defaultVectorSize,
//			func(i int) int32 {
//				return 2
//			},
//			nil)
//		//[16 0]
//		data[2].Data[0] = newInt32FlatVectorImpl(
//			false,
//			defaultVectorSize,
//			func(i int) int32 {
//				return 16
//			},
//			nil)
//		result = NewFlatVector(boolean(), defaultVectorSize)
//		err = eExec.executeExprI(data, 2, result)
//		assert.NoError(t, err)
//
//		//> : false
//		assert.True(t, result.phyFormat() == PF_FLAT)
//		resSlice = getSliceInPhyFormatFlat[bool](result)
//		for i := 0; i < defaultVectorSize; i++ {
//			assert.True(t, result.Mask.rowIsValid(uint64(0)))
//			assert.False(t, resSlice[0])
//		}
//
//		//[-1 2]
//		data[0].Data[2] = newInt32FlatVectorImpl(
//			false,
//			defaultVectorSize,
//			func(i int) int32 {
//				return 8
//			},
//			nil)
//		//[16 0]
//		data[2].Data[0] = newInt32FlatVectorImpl(
//			false,
//			defaultVectorSize,
//			func(i int) int32 {
//				return 6
//			},
//			nil)
//		result = NewFlatVector(boolean(), defaultVectorSize)
//		err = eExec.executeExprI(data, 2, result)
//		assert.NoError(t, err)
//
//		//> : false
//		assert.True(t, result.phyFormat() == PF_FLAT)
//		resSlice = getSliceInPhyFormatFlat[bool](result)
//		for i := 0; i < defaultVectorSize; i++ {
//			assert.True(t, result.Mask.rowIsValid(uint64(0)))
//			assert.True(t, resSlice[0])
//		}
//	}
//}

func Test_testExecute2(t *testing.T) {
	_, pplan := preparePhyPlan(t, 20)
	exprs := collectFilterExprs(pplan)
	compExprs := findExpr(exprs, func(expr *Expr) bool {
		if expr == nil {
			return false
		}
		if expr.Typ == ET_Func && len(expr.Children) != 0 {
			return true
		}
		return false
	})
	for _, expr := range compExprs {
		fmt.Println(expr.String())
	}
}
