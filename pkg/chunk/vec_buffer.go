package chunk

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type VecBufferType int

const (
	//array of data
	VBT_STANDARD VecBufferType = iota
	VBT_DICT
	VBT_CHILD
	VBT_STRING
)

type VecBuffer struct {
	BufTyp VecBufferType
	Data   []byte
	Sel    *SelectVector
	Child  *Vector
}

func (buf *VecBuffer) GetSelVector() *SelectVector {
	util.AssertFunc(buf.BufTyp == VBT_DICT)
	return buf.Sel
}

func NewBuffer(sz int) *VecBuffer {
	return &VecBuffer{
		BufTyp: VBT_STANDARD,
		Data:   util.GAlloc.Alloc(sz),
	}
}

func NewStandardBuffer(lt common.LType, cap int) *VecBuffer {
	return NewBuffer(lt.GetInternalType().Size() * cap)
}

func NewDictBuffer(data []int) *VecBuffer {
	return &VecBuffer{
		BufTyp: VBT_DICT,
		Sel: &SelectVector{
			SelVec: data,
		},
	}
}

func NewDictBuffer2(sel *SelectVector) *VecBuffer {
	buf := &VecBuffer{
		BufTyp: VBT_DICT,
		Sel:    &SelectVector{},
	}
	buf.Sel.Init2(sel)
	return buf
}

func NewChildBuffer(child *Vector) *VecBuffer {
	return &VecBuffer{
		BufTyp: VBT_CHILD,
		Child:  child,
	}
}

func NewConstBuffer(typ common.LType) *VecBuffer {
	return NewStandardBuffer(typ, 1)
}
