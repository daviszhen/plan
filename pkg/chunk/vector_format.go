package chunk

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// Format conversion methods for Vector
func (vec *Vector) Flatten(cnt int) {
	switch vec.PhyFormat() {
	case PF_FLAT:
	case PF_CONST:
		null := IsNullInPhyFormatConst(vec)
		oldData := vec.Data
		vec.Buf = NewStandardBuffer(vec._Typ, int(max(util.DefaultVectorSize, cnt)))
		vec.Data = vec.Buf.Data
		vec._PhyFormat = PF_FLAT
		if null {
			vec.Mask.SetAllInvalid(cnt)
			return
		}
		//fill flat vector
		pTyp := vec.Typ().GetInternalType()
		switch pTyp {
		case common.BOOL:
			FlattenConstVector[bool](vec.Data, oldData, pTyp.Size(), cnt)
		case common.UINT8:
			FlattenConstVector[uint8](vec.Data, oldData, pTyp.Size(), cnt)
		case common.INT8:
			FlattenConstVector[int8](vec.Data, oldData, pTyp.Size(), cnt)
		case common.UINT16:
			FlattenConstVector[uint16](vec.Data, oldData, pTyp.Size(), cnt)
		case common.INT16:
			FlattenConstVector[int16](vec.Data, oldData, pTyp.Size(), cnt)
		case common.UINT32:
			FlattenConstVector[uint32](vec.Data, oldData, pTyp.Size(), cnt)
		case common.INT32:
			FlattenConstVector[int32](vec.Data, oldData, pTyp.Size(), cnt)
		case common.UINT64:
			FlattenConstVector[uint64](vec.Data, oldData, pTyp.Size(), cnt)
		case common.INT64:
			FlattenConstVector[int64](vec.Data, oldData, pTyp.Size(), cnt)
		case common.FLOAT:
			FlattenConstVector[float32](vec.Data, oldData, pTyp.Size(), cnt)
		case common.DOUBLE:
			FlattenConstVector[float64](vec.Data, oldData, pTyp.Size(), cnt)
		case common.INTERVAL, common.LIST, common.STRUCT, common.VARCHAR, common.INT128, common.UNKNOWN, common.BIT, common.INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case PF_DICT:
		panic("usp")
	}
}

func (vec *Vector) Flatten2(sel *SelectVector, cnt int) {
	if vec.PhyFormat().IsFlat() {
		return
	}
	panic("usp")
}

func (vec *Vector) ToUnifiedFormat(count int, output *UnifiedFormat) {
	output.PTypSize = vec._Typ.GetInternalType().Size()
	switch vec.PhyFormat() {
	case PF_DICT:
		sel := GetSelVectorInPhyFormatDict(vec)
		child := GetChildInPhyFormatDict(vec)
		if child.PhyFormat().IsFlat() {
			output.Sel = sel
			output.Data = GetDataInPhyFormatFlat(child)
			output.Mask = GetMaskInPhyFormatFlat(child)
		} else {
			//flatten child
			childVec := &Vector{
				_Typ: child._Typ,
			}
			childVec.Reference(child)
			childVec.Flatten2(sel, count)
			childBuf := NewChildBuffer(childVec)
			output.Sel = sel
			output.Data = GetDataInPhyFormatFlat(childBuf.Child)
			output.Mask = GetMaskInPhyFormatFlat(childBuf.Child)
			vec.Aux = childVec.Aux
		}
	case PF_CONST:
		output.Sel = ZeroSelectVectorInPhyFormatConst(count, &output.InterSel)
		output.Data = GetDataInPhyFormatConst(vec)
		output.Mask = GetMaskInPhyFormatConst(vec)
	case PF_FLAT:
		vec.Flatten(count)
		output.Sel = IncrSelectVectorInPhyFormatFlat()
		output.Data = GetDataInPhyFormatFlat(vec)
		output.Mask = GetMaskInPhyFormatFlat(vec)
	}
}

func (vec *Vector) SliceOnSelf(sel *SelectVector, count int) {
	if vec.PhyFormat().IsConst() {
	} else if vec.PhyFormat().IsDict() {
		//dict
		curSel := GetSelVectorInPhyFormatDict(vec)
		buf := curSel.Slice(sel, count)
		vec.Buf = NewDictBuffer(buf)
	} else {
		//flat
		child := &Vector{
			_Typ: vec.Typ(),
		}
		child.Reference(vec)
		childRef := NewChildBuffer(child)
		dictBuf := NewDictBuffer2(sel)
		vec._PhyFormat = PF_DICT
		vec.Buf = dictBuf
		vec.Aux = childRef
	}
}

func (vec *Vector) Slice2(sel *SelectVector, count int) {
	vec.SliceOnSelf(sel, count)
}

func (vec *Vector) Slice(other *Vector, sel *SelectVector, count int) {
	vec.Reference(other)
	vec.SliceOnSelf(sel, count)
}

func (vec *Vector) Slice3(other *Vector, offset uint64, end uint64) {
	if other.PhyFormat().IsConst() {
		vec.Reference(other)
		return
	}
	util.AssertFunc(other.PhyFormat().IsFlat())
	interTyp := vec.Typ().GetInternalType()
	if interTyp == common.STRUCT {
		panic("usp")
	} else {
		vec.Reference(other)
		if offset > 0 {
			vec.Data = vec.Data[offset*uint64(interTyp.Size()):]
			vec.Mask.Slice(other.Mask, offset, end-offset)
		}
	}
}

// Helper functions for format conversion
func FlattenConstVector[T any](data []byte, srcData []byte, pSize int, cnt int) {
	src := util.ToSlice[T](srcData, pSize)
	dst := util.ToSlice[T](data, pSize)
	for i := 0; i < cnt; i++ {
		dst[i] = src[0]
	}
}
