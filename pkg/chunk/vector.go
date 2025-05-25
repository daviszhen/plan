package chunk

import (
	"fmt"
	"strings"
	"unsafe"

	"github.com/govalues/decimal"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type Vector struct {
	_PhyFormat PhyFormat
	_Typ       common.LType
	Data       []byte
	Mask       *util.Bitmap
	Buf        *VecBuffer
	Aux        *VecBuffer
}

func (vec *Vector) Init(cap int) {
	vec.Aux = nil
	vec.Mask.Reset()
	sz := vec.Typ().GetInternalType().Size()
	if sz > 0 {
		vec.Buf = NewStandardBuffer(vec.Typ(), cap)
		vec.Data = vec.Buf.Data
	}
	if cap > util.DefaultVectorSize {
		vec.Mask.Resize(util.DefaultVectorSize, cap)
	}
}

func (vec *Vector) Typ() common.LType {
	return vec._Typ
}

func (vec *Vector) PhyFormat() PhyFormat {
	return vec._PhyFormat
}

func (vec *Vector) SetPhyFormat(pf PhyFormat) {
	vec._PhyFormat = pf
	//
	if vec.Typ().GetInternalType().IsConstant() &&
		(vec.PhyFormat().IsConst() || vec.PhyFormat().IsFlat()) {
		vec.Aux = nil
	}
}

func (vec *Vector) Reference(other *Vector) {
	util.AssertFunc(vec.Typ().Equal(other.Typ()))
	vec.Reinterpret(other)
}

func (vec *Vector) ReferenceValue(val *Value) {
	util.AssertFunc(vec.Typ().Id == val.Typ.Id)
	vec.SetPhyFormat(PF_CONST)
	vec.Buf = NewConstBuffer(val.Typ)
	vec.Aux = nil
	vec.Data = GetDataInPhyFormatConst(vec)
	vec.SetValue(0, val)
}

func (vec *Vector) Reinterpret(other *Vector) {
	vec._PhyFormat = other._PhyFormat
	vec.Buf = other.Buf
	vec.Aux = other.Aux
	vec.Data = other.Data
	vec.Mask = other.Mask
}

func (vec *Vector) GetValue(idx int) *Value {
	switch vec.PhyFormat() {
	case PF_CONST:
		idx = 0
	case PF_FLAT:
	case PF_DICT:
		sel := GetSelVectorInPhyFormatDict(vec)
		child := GetChildInPhyFormatDict(vec)
		return child.GetValue(sel.GetIndex(idx))
	case PF_SEQUENCE:
		var start, incr int64
		GetSequenceInPhyFormatSequence2(vec, &start, &incr)
		return &Value{
			Typ: vec.Typ(),
			I64: start + int64(idx)*incr,
		}
	default:
		panic("usp")
	}
	if !vec.Mask.RowIsValid(uint64(idx)) {
		return &Value{
			Typ:    vec.Typ(),
			IsNull: true,
		}
	}

	switch vec.Typ().Id {
	case common.LTID_INTEGER:
		data := GetSliceInPhyFormatFlat[int32](vec)
		return &Value{
			Typ: vec.Typ(),
			I64: int64(data[idx]),
		}
	case common.LTID_BOOLEAN:
		data := GetSliceInPhyFormatFlat[bool](vec)
		return &Value{
			Typ:  vec.Typ(),
			Bool: data[idx],
		}
	case common.LTID_VARCHAR:
		data := GetSliceInPhyFormatFlat[common.String](vec)
		return &Value{
			Typ: vec.Typ(),
			Str: data[idx].String(),
		}
	case common.LTID_DECIMAL:
		data := GetSliceInPhyFormatFlat[common.Decimal](vec)
		d := data[idx]
		w, f, ok := d.Decimal.Int64(vec.Typ().Scale)
		if !ok {
			return &Value{
				Typ: vec.Typ(),
				Str: data[idx].String(),
			}
		} else {
			return &Value{
				Typ:   vec.Typ(),
				I64:   w,
				I64_1: f,
				Str:   "",
			}
		}
	case common.LTID_DATE:
		data := GetSliceInPhyFormatFlat[common.Date](vec)
		return &Value{
			Typ:   vec.Typ(),
			I64:   int64(data[idx].Year),
			I64_1: int64(data[idx].Month),
			I64_2: int64(data[idx].Day),
		}
	case common.LTID_UBIGINT:
		data := GetSliceInPhyFormatFlat[uint64](vec)
		return &Value{
			Typ: vec.Typ(),
			I64: int64(data[idx]),
		}
	case common.LTID_BIGINT:
		data := GetSliceInPhyFormatFlat[int64](vec)
		return &Value{
			Typ: vec.Typ(),
			I64: data[idx],
		}
	case common.LTID_DOUBLE:
		data := GetSliceInPhyFormatFlat[float64](vec)
		return &Value{
			Typ: vec.Typ(),
			F64: data[idx],
		}
	case common.LTID_FLOAT:
		data := GetSliceInPhyFormatFlat[float32](vec)
		return &Value{
			Typ: vec.Typ(),
			F64: float64(data[idx]),
		}
	case common.LTID_POINTER:
		data := GetSliceInPhyFormatFlat[unsafe.Pointer](vec)
		return &Value{
			Typ: vec.Typ(),
			I64: int64(uintptr(data[idx])),
		}
	case common.LTID_HUGEINT:
		data := GetSliceInPhyFormatFlat[common.Hugeint](vec)
		return &Value{
			Typ:   vec.Typ(),
			I64:   data[idx].Upper,
			I64_1: int64(data[idx].Lower),
		}
	default:
		panic("usp")
	}
}

func (vec *Vector) SetValue(idx int, val *Value) {
	if vec.PhyFormat().IsDict() {
		sel := GetSelVectorInPhyFormatDict(vec)
		child := GetChildInPhyFormatDict(vec)
		child.SetValue(sel.GetIndex(idx), val)
	}
	util.AssertFunc(val.Typ.Equal(vec.Typ()))
	util.AssertFunc(val.Typ.GetInternalType() == vec.Typ().GetInternalType())
	vec.Mask.Set(uint64(idx), !val.IsNull)
	pTyp := vec.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		slice := util.ToSlice[int32](vec.Data, pTyp.Size())
		slice[idx] = int32(val.I64)
	case common.INT64:
		slice := util.ToSlice[int64](vec.Data, pTyp.Size())
		slice[idx] = int64(val.I64)
	case common.FLOAT:
		slice := util.ToSlice[float32](vec.Data, pTyp.Size())
		slice[idx] = float32(val.F64)
	case common.VARCHAR:
		slice := util.ToSlice[common.String](vec.Data, pTyp.Size())
		byteSlice := []byte(val.Str)
		dstMem := util.CMalloc(len(byteSlice))
		dst := util.PointerToSlice[byte](dstMem, len(byteSlice))
		copy(dst, byteSlice)
		slice[idx] = common.String{
			Data: dstMem,
			Len:  len(dst),
		}
		again := slice[idx].String()
		util.AssertFunc(again == val.Str)

	case common.INTERVAL:
		slice := util.ToSlice[common.Interval](vec.Data, pTyp.Size())
		interVal := common.Interval{}
		switch strings.ToLower(val.Str) {
		case "year":
			interVal.Year = int32(val.I64)
			interVal.Unit = val.Str
		case "month":
			interVal.Months = int32(val.I64)
			interVal.Unit = val.Str
		case "day":
			interVal.Days = int32(val.I64)
			interVal.Unit = val.Str
			//default:
			//	panic("usp")
		}
		slice[idx] = interVal
	case common.DATE:
		slice := util.ToSlice[common.Date](vec.Data, pTyp.Size())
		slice[idx] = common.Date{
			Year:  int32(val.I64),
			Month: int32(val.I64_1),
			Day:   int32(val.I64_2),
		}
	case common.DECIMAL:
		slice := util.ToSlice[common.Decimal](vec.Data, pTyp.Size())
		if len(val.Str) != 0 {
			decVal, err := decimal.ParseExact(val.Str, vec.Typ().Scale)
			if err != nil {
				panic(err)
			}
			val.Str = ""
			slice[idx] = common.Decimal{
				decVal,
			}
		} else {
			nDec, err := decimal.NewFromInt64(val.I64, val.I64_1, vec._Typ.Scale)
			if err != nil {
				panic(err)
			}
			slice[idx] = common.Decimal{
				nDec,
			}
		}

	case common.DOUBLE:
		slice := util.ToSlice[float64](vec.Data, pTyp.Size())
		slice[idx] = val.F64
	case common.BOOL:
		slice := util.ToSlice[bool](vec.Data, pTyp.Size())
		slice[idx] = val.Bool
	case common.INT128:
		slice := util.ToSlice[common.Hugeint](vec.Data, pTyp.Size())
		slice[idx].Upper = val.I64
		slice[idx].Lower = uint64(val.I64_1)
	default:
		panic("usp")
	}
}

func (vec *Vector) Reset() {
	vec._PhyFormat = PF_FLAT
	vec.Mask.Reset()
}

func (vec *Vector) Print(rowCount int) {
	for j := 0; j < rowCount; j++ {
		val := vec.GetValue(j)
		fmt.Println(val)
	}
	fmt.Println()
}

func (vec *Vector) Print2(prefix string, rowCount int) {
	fields := make([]zap.Field, 0)
	for j := 0; j < rowCount; j++ {
		val := vec.GetValue(j)
		fields = append(fields, zap.String("", val.String()))
	}
	util.Info(prefix, fields...)
}

func (vec *Vector) Sequence(start uint64, incr uint64, count uint64) {
	vec._PhyFormat = PF_SEQUENCE
	vec.Buf = NewStandardBuffer(common.BigintType(), 3)
	dataSlice := GetSliceInPhyFormatSequence(vec)
	dataSlice[0] = int64(start)
	dataSlice[1] = int64(incr)
	dataSlice[2] = int64(count)
	vec.Mask = nil
	vec.Aux = nil
}

// sequence vector
func GetSliceInPhyFormatSequence(vec *Vector) []int64 {
	util.AssertFunc(vec.PhyFormat().IsSequence())
	pSize := vec.Typ().GetInternalType().Size()
	return util.ToSlice[int64](vec.Data, pSize)
}

// constant vector
func GetDataInPhyFormatConst(vec *Vector) []byte {
	util.AssertFunc(vec.PhyFormat().IsConst() || vec.PhyFormat().IsFlat())
	return vec.Data
}

func GetSliceInPhyFormatConst[T any](vec *Vector) []T {
	util.AssertFunc(vec.PhyFormat().IsConst() || vec.PhyFormat().IsFlat())
	pSize := vec.Typ().GetInternalType().Size()
	return util.ToSlice[T](vec.Data, pSize)
}

func IsNullInPhyFormatConst(vec *Vector) bool {
	util.AssertFunc(vec.PhyFormat().IsConst())
	return !vec.Mask.RowIsValid(0)
}

func SetNullInPhyFormatConst(vec *Vector, null bool) {
	util.AssertFunc(vec.PhyFormat().IsConst())
	vec.Mask.Set(0, !null)
}

func ZeroSelectVectorInPhyFormatConst(cnt int, sel *SelectVector) *SelectVector {
	sel.Init(cnt)
	return sel
}

func GetMaskInPhyFormatConst(vec *Vector) *util.Bitmap {
	util.AssertFunc(vec.PhyFormat().IsConst())
	return vec.Mask
}

func ReferenceInPhyFormatConst(
	vec *Vector,
	src *Vector,
	pos int,
	count int,
) {
	srcTyp := src.Typ().GetInternalType()
	switch srcTyp {
	case common.LIST:
		panic("usp")
	case common.STRUCT:
		panic("usp")
	default:
		value := src.GetValue(pos)
		vec.ReferenceValue(value)
		util.AssertFunc(vec.PhyFormat().IsConst())
	}
}

// flat vector
func GetDataInPhyFormatFlat(vec *Vector) []byte {
	return GetDataInPhyFormatConst(vec)
}

func GetSliceInPhyFormatFlat[T any](vec *Vector) []T {
	return GetSliceInPhyFormatConst[T](vec)
}

func SetMaskInPhyFormatFlat(vec *Vector, mask *util.Bitmap) {
	util.AssertFunc(vec.PhyFormat().IsFlat())
	vec.Mask.ShareWith(mask)
}

func GetMaskInPhyFormatFlat(vec *Vector) *util.Bitmap {
	util.AssertFunc(vec.PhyFormat().IsFlat())
	return vec.Mask
}

func SetNullInPhyFormatFlat(vec *Vector, idx uint64, null bool) {
	util.AssertFunc(vec.PhyFormat().IsFlat())
	vec.Mask.Set(idx, !null)
}

func IncrSelectVectorInPhyFormatFlat() *SelectVector {
	return &SelectVector{}
}

// dictionary vector
func GetSelVectorInPhyFormatDict(vec *Vector) *SelectVector {
	util.AssertFunc(vec.PhyFormat().IsDict())
	return vec.Buf.GetSelVector()
}

func GetChildInPhyFormatDict(vec *Vector) *Vector {
	util.AssertFunc(vec.PhyFormat().IsDict())
	return vec.Aux.Child
}

func GetSequenceInPhyFormatSequence2(vec *Vector, start, incr *int64) {
	var seqCount int64
	GetSequenceInPhyFormatSequence(vec, start, incr, &seqCount)
}

func SetData(vec *Vector, slice []byte) {
	util.AssertFunc(vec.PhyFormat().IsFlat())
	vec.Data = slice
}

func GenerateSequence(
	result *Vector,
	count uint64,
	start, increment uint64,
) {
	result.SetPhyFormat(PF_FLAT)
	switch result.Typ().GetInternalType() {
	case common.UINT64:
		data := GetSliceInPhyFormatFlat[uint64](result)
		value := start
		for i := uint64(0); i < count; i++ {
			if i > 0 {
				value += increment
			}
			data[i] = value
		}
	default:
		panic("usp")
	}
}

func NewUbigintFlatVector(v []uint64, sz int) *Vector {
	vec := NewFlatVector(common.UbigintType(), sz)
	data := GetSliceInPhyFormatFlat[uint64](vec)
	copy(data, v)
	return vec
}

func NewVarcharFlatVector(v []string, sz int) *Vector {
	vec := NewFlatVector(common.VarcharType(), sz)
	data := GetSliceInPhyFormatFlat[common.String](vec)
	for i := 0; i < len(v); i++ {
		dstMem := util.CMalloc(len(v[i]))
		dst := util.PointerToSlice[byte](dstMem, len(v[i]))
		copy(dst, v[i])
		data[i] = common.String{
			Data: dstMem,
			Len:  len(dst),
		}
	}
	return vec
}

func HasNull(input *Vector, count int) bool {
	if count == 0 {
		return false
	}

	if input.PhyFormat() == PF_CONST {
		return IsNullInPhyFormatConst(input)
	} else {
		var data UnifiedFormat
		input.ToUnifiedFormat(count, &data)

		if data.Mask.AllValid() {
			return false
		}
		for i := 0; i < count; i++ {
			idx := data.Sel.GetIndex(i)
			if !data.Mask.RowIsValid(uint64(idx)) {
				return true
			}
		}
		return false
	}
}
