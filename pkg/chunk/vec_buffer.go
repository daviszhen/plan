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
	_mem   util.CPtr // tracks ownership of Data
}

func (buf *VecBuffer) GetSelVector() *SelectVector {
	util.AssertFunc(buf.BufTyp == VBT_DICT)
	return buf.Sel
}

// Destroy releases C memory owned by this buffer. Idempotent.
// pTyp: physical type of data (needed to free embedded VARCHAR strings).
// count: number of valid elements (avoids freeing uninitialized slots).
func (buf *VecBuffer) Destroy(pTyp common.PhyType, count int) {
	if buf == nil {
		return
	}
	switch buf.BufTyp {
	case VBT_STANDARD, VBT_STRING:
		if pTyp == common.VARCHAR && count > 0 && !buf._mem.IsNil() {
			strings := util.ToSlice[common.String](buf.Data, common.VarcharSize)
			for i := 0; i < count; i++ {
				strings[i].Free()
			}
		}
		buf._mem.Destroy()
		buf.Data = nil
	case VBT_DICT:
		buf.Sel = nil
	case VBT_CHILD:
		if buf.Child != nil {
			buf.Child.Destroy()
			buf.Child = nil
		}
	}
}

func NewBuffer(sz int) *VecBuffer {
	mem := util.NewCPtr(sz)
	return &VecBuffer{
		BufTyp: VBT_STANDARD,
		Data:   mem.Bytes(),
		_mem:   mem,
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
