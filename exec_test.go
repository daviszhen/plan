package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func makeInt32Chunks(cnt int) []*Chunk {
	ret := make([]*Chunk, 0)
	for i := 0; i < 3; i++ {
		data := &Chunk{}
		ltyps := make([]LType, cnt)
		for j := 0; j < cnt; j++ {
			ltyps[j] = integer()
		}
		data.init(ltyps)
		for j := 0; j < cnt; j++ {
			data._data[j] = newInt32FlatVectorEven(false, defaultVectorSize)
		}
		data._count = defaultVectorSize
		ret = append(ret, data)
	}

	return ret
}

func Test_testExecute1(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	exprs := collectFilterExprs(pplan)
	compExprs := findExpr(exprs, func(expr *Expr) bool {
		if expr == nil {
			return false
		}
		//if expr.Typ == ET_Func && expr.SubTyp == ET_Equal && expr.Children[0].DataTyp.LTyp.id == LTID_INTEGER {
		if expr.Typ == ET_Func && len(expr.Children) != 0 &&
			(expr.Children[0].DataTyp.LTyp.id == LTID_INTEGER || expr.Children[0].DataTyp.LTyp.id == LTID_FLOAT) {
			return true
		}
		return false
	})
	for _, expr := range compExprs {
		fmt.Println(expr.String())
	}

	eExec := NewExprExec(compExprs...)
	{
		//filter 0
		//	│   └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  in
		//	│       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[-1 0],0)
		//	│       └── [ {(LTID_INTEGER INT32 0 0),null}]  cast
		//	│           ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 3],0)
		//	│           └── [ {(LTID_INTEGER INT32 0 0),null}]  (0)
		data := makeInt32Chunks(4)
		result := NewFlatVector(boolean(), defaultVectorSize)
		err := eExec.executeExprI(data, 0, result)
		assert.NoError(t, err)

		//equal
		assert.True(t, result.phyFormat() == PF_FLAT)
		resSlice := getSliceInPhyFormatFlat[bool](result)
		for i := 0; i < defaultVectorSize; i++ {
			assert.True(t, result._mask.rowIsValid(uint64(i)))
			assert.True(t, resSlice[i])
		}

		//not equal
		data[0]._data[3] = newInt32ConstVector(-1, false)
		err = eExec.executeExprI(data, 0, result)
		assert.NoError(t, err)
		assert.True(t, result.phyFormat() == PF_FLAT)
		resSlice = getSliceInPhyFormatFlat[bool](result)
		for i := 0; i < defaultVectorSize; i++ {
			assert.True(t, result._mask.rowIsValid(uint64(i)))
			assert.False(t, resSlice[i])
		}
	}

	{
		//filter 1
		//│   └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  in
		//│       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[-2 0],0)
		//│       └── [ {(LTID_INTEGER INT32 0 0),null}]  cast
		//│           ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 0],0)
		//│           └── [ {(LTID_INTEGER INT32 0 0),null}]  (0)
		data := makeInt32Chunks(4)
		result := NewFlatVector(boolean(), defaultVectorSize)
		err := eExec.executeExprI(data, 1, result)
		assert.NoError(t, err)

		//equal
		assert.True(t, result.phyFormat() == PF_FLAT)
		resSlice := getSliceInPhyFormatFlat[bool](result)
		for i := 0; i < defaultVectorSize; i++ {
			assert.True(t, result._mask.rowIsValid(uint64(i)))
			assert.True(t, resSlice[i])
		}

		//not equal
		data[1]._data[0] = newInt32ConstVector(-1, false)
		err = eExec.executeExprI(data, 1, result)
		assert.NoError(t, err)
		assert.True(t, result.phyFormat() == PF_FLAT)
		resSlice = getSliceInPhyFormatFlat[bool](result)
		for i := 0; i < defaultVectorSize; i++ {
			assert.True(t, result._mask.rowIsValid(uint64(i)))
			assert.False(t, resSlice[i])
		}
	}
	{
		//filter 2
		//└── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >
		//    ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		//    └── [ {(LTID_INTEGER INT32 0 0),null}]  cast
		//        ├── [ {(LTID_FLOAT FLOAT 0 0),not null}]  *
		//        │   ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0.5)
		//        │   └── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		//        │       ├── [ {(LTID_INTEGER INT32 0 0),null}]  (AggNode_16.sum(l_quantity),[16 0],0)
		//        │       └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		//        └── [ {(LTID_INTEGER INT32 0 0),null}]  (0)
		data := makeInt32Chunks(4)

		///////////// const vector
		//[-1 2]
		data[0]._data[2] = newInt32ConstVector(2, false)
		//[16 0]
		data[2]._data[0] = newInt32ConstVector(16, false)
		result := NewFlatVector(boolean(), defaultVectorSize)
		err := eExec.executeExprI(data, 2, result)
		assert.NoError(t, err)

		//> : false
		assert.True(t, result.phyFormat() == PF_CONST)
		resSlice := getSliceInPhyFormatFlat[bool](result)
		{
			assert.True(t, result._mask.rowIsValid(uint64(0)))
			assert.False(t, resSlice[0])
		}

		//[-1 2]
		data[0]._data[2] = newInt32ConstVector(8, false)
		//[16 0]
		data[2]._data[0] = newInt32ConstVector(6, false)
		result = NewFlatVector(boolean(), defaultVectorSize)
		err = eExec.executeExprI(data, 2, result)
		assert.NoError(t, err)

		//> : true
		assert.True(t, result.phyFormat() == PF_CONST)
		resSlice = getSliceInPhyFormatFlat[bool](result)
		{
			assert.True(t, result._mask.rowIsValid(uint64(0)))
			assert.True(t, resSlice[0])
		}

		///////////// flat vector
		//[-1 2]
		data[0]._data[2] = newInt32FlatVectorImpl(
			false,
			defaultVectorSize,
			func(i int) int32 {
				return 2
			},
			nil)
		//[16 0]
		data[2]._data[0] = newInt32FlatVectorImpl(
			false,
			defaultVectorSize,
			func(i int) int32 {
				return 16
			},
			nil)
		result = NewFlatVector(boolean(), defaultVectorSize)
		err = eExec.executeExprI(data, 2, result)
		assert.NoError(t, err)

		//> : false
		assert.True(t, result.phyFormat() == PF_FLAT)
		resSlice = getSliceInPhyFormatFlat[bool](result)
		for i := 0; i < defaultVectorSize; i++ {
			assert.True(t, result._mask.rowIsValid(uint64(0)))
			assert.False(t, resSlice[0])
		}

		//[-1 2]
		data[0]._data[2] = newInt32FlatVectorImpl(
			false,
			defaultVectorSize,
			func(i int) int32 {
				return 8
			},
			nil)
		//[16 0]
		data[2]._data[0] = newInt32FlatVectorImpl(
			false,
			defaultVectorSize,
			func(i int) int32 {
				return 6
			},
			nil)
		result = NewFlatVector(boolean(), defaultVectorSize)
		err = eExec.executeExprI(data, 2, result)
		assert.NoError(t, err)

		//> : false
		assert.True(t, result.phyFormat() == PF_FLAT)
		resSlice = getSliceInPhyFormatFlat[bool](result)
		for i := 0; i < defaultVectorSize; i++ {
			assert.True(t, result._mask.rowIsValid(uint64(0)))
			assert.True(t, resSlice[0])
		}
	}
}

func Test_testExecute2(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
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
