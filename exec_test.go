package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func makeInt32Chunks(cnt int) []*Chunk {
	ret := make([]*Chunk, 0)
	for i := 0; i < 2; i++ {
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

func Test_testExecute(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	exprs := collectFilterExprs(pplan)
	compExprs := findExpr(exprs, func(expr *Expr) bool {
		if expr == nil {
			return false
		}
		//if expr.Typ == ET_Func && expr.SubTyp == ET_Equal && expr.Children[0].DataTyp.LTyp.id == LTID_INTEGER {
		if expr.Typ == ET_Func && len(expr.Children) != 0 && expr.Children[0].DataTyp.LTyp.id == LTID_INTEGER {
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
	}

	//{
	//	//no null
	//	data := makeInt32Chunks()
	//	result := NewFlatVector(boolean(), defaultVectorSize)
	//	err := eExec.executeExpr(data, result)
	//	assert.NoError(t, err)
	//
	//	assert.True(t, result.phyFormat() == PF_FLAT)
	//	resSlice := getSliceInPhyFormatFlat[bool](result)
	//	for i := 0; i < defaultVectorSize; i++ {
	//		assert.True(t, result._mask.rowIsValid(uint64(i)))
	//		assert.True(t, resSlice[i])
	//	}
	//}
	//{
	//	//one null
	//	result := NewFlatVector(boolean(), defaultVectorSize)
	//
	//	data := make([]*Chunk, 2)
	//	data[0] = &Chunk{}
	//	data[0].init([]LType{integer()})
	//	data[0]._data[0] = newInt32FlatVectorEven(true, defaultVectorSize)
	//	data[0]._count = defaultVectorSize
	//
	//	data[1] = &Chunk{}
	//	data[1].init([]LType{integer()})
	//	data[1]._data[0] = newInt32FlatVectorEven(false, defaultVectorSize)
	//	data[1]._count = defaultVectorSize
	//
	//	for j := 0; j < 2; j++ {
	//		if j > 0 {
	//			data[0], data[1] = data[1], data[0]
	//		}
	//		err := eExec.executeExpr(data, result)
	//		assert.NoError(t, err)
	//
	//		assert.True(t, result.phyFormat() == PF_FLAT)
	//		resSlice := getSliceInPhyFormatFlat[bool](result)
	//		for i := 0; i < defaultVectorSize; i++ {
	//			if i%2 == 0 {
	//				assert.False(t, result._mask.rowIsValid(uint64(i)))
	//			} else {
	//				assert.True(t, result._mask.rowIsValid(uint64(i)))
	//				assert.True(t, resSlice[i])
	//			}
	//		}
	//	}
	//
	//}
	//{
	//	result := NewFlatVector(boolean(), defaultVectorSize)
	//
	//	data := make([]*Chunk, 2)
	//	data[0] = &Chunk{}
	//	data[0].init([]LType{integer()})
	//	data[0]._data[0] = newInt32FlatVectorEven(true, defaultVectorSize)
	//	data[0]._count = defaultVectorSize
	//
	//	data[1] = &Chunk{}
	//	data[1].init([]LType{integer()})
	//	data[1]._data[0] = newInt32FlatVectorOdd(true, defaultVectorSize)
	//	data[1]._count = defaultVectorSize
	//
	//	for j := 0; j < 2; j++ {
	//		if j > 0 {
	//			data[0], data[1] = data[1], data[0]
	//		}
	//		err := eExec.executeExpr(data, result)
	//		assert.NoError(t, err)
	//
	//		assert.True(t, result.phyFormat() == PF_FLAT)
	//		for i := 0; i < defaultVectorSize; i++ {
	//			assert.False(t, result._mask.rowIsValid(uint64(i)))
	//		}
	//	}
	//
	//}
}
