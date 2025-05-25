package chunk

import (
	"github.com/daviszhen/plan/pkg/util"
)

type SelectVector struct {
	SelVec []int
}

func NewSelectVector(count int) *SelectVector {
	vec := &SelectVector{}
	vec.Init(count)
	return vec
}

func NewSelectVector2(start, count int) *SelectVector {
	vec := &SelectVector{}
	vec.Init(util.DefaultVectorSize)
	for i := 0; i < count; i++ {
		vec.SetIndex(i, start+i)
	}
	return vec
}

func (svec *SelectVector) Invalid() bool {
	return len(svec.SelVec) == 0
}

func (svec *SelectVector) Init(cnt int) {
	svec.SelVec = make([]int, cnt)
}

func (svec *SelectVector) GetIndex(idx int) int {
	if svec.Invalid() {
		return idx
	} else {
		return svec.SelVec[idx]
	}
}

func (svec *SelectVector) SetIndex(idx int, index int) {
	svec.SelVec[idx] = index
}

func (svec *SelectVector) Slice(sel *SelectVector, count int) []int {
	data := make([]int, count)
	for i := 0; i < count; i++ {
		newIdx := sel.GetIndex(i)
		idx := svec.GetIndex(newIdx)
		data[i] = idx
	}
	return data
}

func (svec *SelectVector) Init2(sel *SelectVector) {
	svec.SelVec = sel.SelVec
}

func (svec *SelectVector) Init3(data []int) {
	svec.SelVec = data
}

func NewSelectVector3(tuples []int) *SelectVector {
	v := NewSelectVector(util.DefaultVectorSize)
	v.Init3(tuples)
	return v
}
