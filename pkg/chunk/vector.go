package chunk

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"
	"unsafe"

	"github.com/govalues/decimal"
	"go.uber.org/zap"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type PhyFormat int

const (
	PF_FLAT PhyFormat = iota
	PF_CONST
	PF_DICT
	PF_SEQUENCE
)

func (f PhyFormat) String() string {
	switch f {
	case PF_FLAT:
		return "flat"
	case PF_CONST:
		return "constant"
	case PF_DICT:
		return "dictionary"
	}
	panic(fmt.Sprintf("usp %d", f))
}

func (f PhyFormat) IsConst() bool {
	return f == PF_CONST
}

func (f PhyFormat) IsFlat() bool {
	return f == PF_FLAT
}

func (f PhyFormat) IsDict() bool {
	return f == PF_DICT
}

func (f PhyFormat) IsSequence() bool {
	return f == PF_SEQUENCE
}

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

type VecBufferType int

const (
	//array of data
	VBT_STANDARD VecBufferType = iota
	VBT_DICT
	VBT_CHILD
	VBT_STRING
)

type UnifiedFormat struct {
	Sel      *SelectVector
	Data     []byte
	Mask     *util.Bitmap
	InterSel SelectVector
	PTypSize int
}

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

type Value struct {
	Typ    common.LType
	IsNull bool
	//value
	Bool  bool
	I64   int64
	I64_1 int64
	I64_2 int64
	U64   uint64
	F64   float64
	Str   string
}

func (val Value) String() string {
	if val.IsNull {
		return "NULL"
	}
	switch val.Typ.Id {
	case common.LTID_INTEGER:
		return fmt.Sprintf("%d", val.I64)
	case common.LTID_BOOLEAN:
		return fmt.Sprintf("%v", val.Bool)
	case common.LTID_VARCHAR:
		return val.Str
	case common.LTID_DECIMAL:
		if len(val.Str) != 0 {
			return val.Str
		} else {
			d, err := decimal.NewFromInt64(val.I64, val.I64_1, val.Typ.Scale)
			if err != nil {
				panic(err)
			}
			return d.String()
		}
	case common.LTID_DATE:
		dat := time.Date(int(val.I64), time.Month(val.I64_1), int(val.I64_2),
			0, 0, 0, 0, time.UTC)
		return dat.Format(time.DateOnly)
	case common.LTID_BIGINT:
		return fmt.Sprintf("%d", val.I64)
	case common.LTID_UBIGINT:
		return fmt.Sprintf("0x%x %d", val.I64, val.I64)
	case common.LTID_DOUBLE:
		return fmt.Sprintf("%v", val.F64)
	case common.LTID_FLOAT:
		return fmt.Sprintf("%v", val.F64)
	case common.LTID_POINTER:
		return fmt.Sprintf("0x%x", val.I64)
	case common.LTID_HUGEINT:
		h := big.NewInt(val.I64)
		l := big.NewInt(val.I64_1)
		h.Lsh(h, 64)
		h.Add(h, l)
		return fmt.Sprintf("%v", h.String())
	default:
		panic("usp")
	}
}

var (
	POWERS_OF_TEN = []int64{
		1,
		10,
		100,
		1000,
		10000,
		100000,
		1000000,
		10000000,
		100000000,
		1000000000,
		10000000000,
		100000000000,
		1000000000000,
		10000000000000,
		100000000000000,
		1000000000000000,
		10000000000000000,
		100000000000000000,
		1000000000000000000,
	}
)

func MaxValue(typ common.LType) *Value {
	ret := &Value{
		Typ: typ,
	}
	switch typ.Id {
	case common.LTID_BOOLEAN:
		ret.Bool = true
	case common.LTID_INTEGER:
		ret.I64 = math.MaxInt32
	case common.LTID_BIGINT:
		ret.I64 = math.MaxInt64
	case common.LTID_UBIGINT:
		ret.U64 = math.MaxUint64
	case common.LTID_DECIMAL:
		ret.I64 = POWERS_OF_TEN[typ.Width] - 1
	case common.LTID_DATE:
		ret.I64 = 5881580
		ret.I64_1 = 7
		ret.I64_2 = 10
	default:
		panic("usp")
	}
	return ret
}

func MinValue(typ common.LType) *Value {
	ret := &Value{
		Typ: typ,
	}
	switch typ.Id {
	case common.LTID_BOOLEAN:
		ret.Bool = false
	case common.LTID_INTEGER:
		ret.I64 = math.MinInt32
	case common.LTID_BIGINT:
		ret.I64 = math.MinInt64
	case common.LTID_UBIGINT:
		ret.I64 = 0
	case common.LTID_DECIMAL:
		ret.I64 = -POWERS_OF_TEN[typ.Width] + 1
	case common.LTID_DATE:
		ret.I64 = -5877641
		ret.I64_1 = 6
		ret.I64_2 = 25
	default:
		panic("usp")
	}
	return ret
}

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

func FlattenConstVector[T any](data []byte, srcData []byte, pSize int, cnt int) {
	src := util.ToSlice[T](srcData, pSize)
	dst := util.ToSlice[T](data, pSize)
	for i := 0; i < cnt; i++ {
		dst[i] = src[0]
	}
}

// to FLAT
func (vec *Vector) Flatten(cnt int) {
	switch vec.PhyFormat() {
	case PF_FLAT:
	case PF_CONST:
		null := IsNullInPhyFormatConst(vec)
		//oldBuffer := vec.Buf
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

func (vec *Vector) Serialize(count int, serial util.Serialize) error {
	var vdata UnifiedFormat
	vec.ToUnifiedFormat(count, &vdata)
	writeValidity := (count > 0) && !vdata.Mask.AllValid()
	err := util.Write[bool](writeValidity, serial)
	if err != nil {
		return err
	}
	if writeValidity {
		flatMask := &util.Bitmap{}
		flatMask.Init(count)
		for i := 0; i < count; i++ {
			rowIdx := vdata.Sel.GetIndex(i)
			flatMask.Set(uint64(i), vdata.Mask.RowIsValid(uint64(rowIdx)))
		}
		err = serial.WriteData(flatMask.Data(), flatMask.Bytes(count))
		if err != nil {
			return err
		}
	}
	typ := vec.Typ()
	if typ.GetInternalType().IsConstant() {
		writeSize := typ.GetInternalType().Size() * count
		buff := util.GAlloc.Alloc(writeSize)
		defer util.GAlloc.Free(buff)
		WriteToStorage(vec, count, util.BytesSliceToPointer(buff))
		err = serial.WriteData(buff, writeSize)
		if err != nil {
			return err
		}
	} else {
		switch typ.GetInternalType() {
		case common.VARCHAR:
			strSlice := GetSliceInPhyFormatUnifiedFormat[common.String](&vdata)
			for i := 0; i < count; i++ {
				idx := vdata.Sel.GetIndex(i)
				if !vdata.Mask.RowIsValid(uint64(idx)) {
					nVal := StringScatterOp{}.NullValue()
					err = common.WriteString(nVal, serial)
					if err != nil {
						return err
					}
				} else {
					val := strSlice[idx]
					err = common.WriteString(val, serial)
					if err != nil {
						return err
					}
				}
			}
		default:
			panic("usp")
		}
	}
	return err
}

func (vec *Vector) Deserialize(count int, deserial util.Deserialize) error {
	var mask *util.Bitmap
	switch vec.PhyFormat() {
	case PF_CONST:
		mask = GetMaskInPhyFormatConst(vec)
	case PF_FLAT:
		mask = GetMaskInPhyFormatFlat(vec)
	case PF_DICT:
		panic("usp")
	}
	mask.Reset()
	hasMask := false
	err := util.Read[bool](&hasMask, deserial)
	if err != nil {
		return err
	}
	if hasMask {
		mask.Init(count)
		err = deserial.ReadData(mask.Data(), mask.Bytes(count))
		if err != nil {
			return err
		}
	}

	typ := vec.Typ()
	if typ.GetInternalType().IsConstant() {
		readSize := typ.GetInternalType().Size() * count
		buf := util.GAlloc.Alloc(readSize)
		defer util.GAlloc.Free(buf)
		err = deserial.ReadData(buf, readSize)
		if err != nil {
			return err
		}
		ReadFromStorage(util.BytesSliceToPointer(buf), count, vec)
	} else {
		switch typ.GetInternalType() {
		case common.VARCHAR:
			strSlice := GetSliceInPhyFormatFlat[common.String](vec)
			for i := 0; i < count; i++ {
				var str common.String
				err = common.ReadString(&str, deserial)
				if err != nil {
					return err
				}
				if mask.RowIsValid(uint64(i)) {
					strSlice[i] = str
				}
			}
		default:
			panic("usp")
		}
	}
	return nil
}

func (vec *Vector) Sequence(
	start uint64, incr uint64, count uint64) {
	vec._PhyFormat = PF_SEQUENCE
	vec.Buf = NewStandardBuffer(common.BigintType(), 3)
	dataSlice := GetSliceInPhyFormatSequence(vec)
	dataSlice[0] = int64(start)
	dataSlice[1] = int64(incr)
	dataSlice[2] = int64(count)
	vec.Mask = nil
	vec.Aux = nil
}

func (vec *Vector) Slice3(
	other *Vector,
	offset uint64,
	end uint64) {
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

// unified format
func GetSliceInPhyFormatUnifiedFormat[T any](uni *UnifiedFormat) []T {
	return util.ToSlice[T](uni.Data, uni.PTypSize)
}

func GetSequenceInPhyFormatSequence(vec *Vector, start, incr, seqCount *int64) {
	util.AssertFunc(vec.PhyFormat().IsSequence())
	dSlice := GetSliceInPhyFormatSequence(vec)
	*start = dSlice[0]
	*incr = dSlice[1]
	*seqCount = dSlice[2]
}

func GetSequenceInPhyFormatSequence2(vec *Vector, start, incr *int64) {
	var seqCount int64
	GetSequenceInPhyFormatSequence(vec, start, incr, &seqCount)
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

func NewVector(lTyp common.LType, initData bool, cap int) *Vector {
	vec := &Vector{
		_PhyFormat: PF_FLAT,
		_Typ:       lTyp,
		Mask:       &util.Bitmap{},
	}
	if initData {
		vec.Init(cap)
	}
	return vec
}

func NewVector2(lTyp common.LType, cap int) *Vector {
	return NewVector(lTyp, true, cap)
}

func NewFlatVector(lTyp common.LType, cap int) *Vector {
	return NewVector2(lTyp, cap)
}

func NewConstVector(lTyp common.LType) *Vector {
	vec := NewVector2(lTyp, util.DefaultVectorSize)
	vec.SetPhyFormat(PF_CONST)
	return vec
}

func NewEmptyVector(typ common.LType, pf PhyFormat, cap int) *Vector {
	var vec *Vector
	switch pf {
	case PF_FLAT:
		vec = NewFlatVector(typ, cap)
	case PF_CONST:
		vec = NewConstVector(typ)
	default:
		panic("usp")
	}
	return vec
}

func Copy(
	srcP *Vector,
	dstP *Vector,
	selP *SelectVector,
	srcCount int,
	srcOffset int,
	dstOffset int,
) {
	util.AssertFunc(srcOffset <= srcCount)
	util.AssertFunc(srcP.Typ().Id == dstP.Typ().Id)
	copyCount := srcCount - srcOffset
	finished := false

	ownedSel := &SelectVector{}
	sel := selP
	src := srcP

	for !finished {
		switch src.PhyFormat() {
		case PF_DICT:
			//dict vector
			child := GetChildInPhyFormatDict(src)
			dictSel := GetSelVectorInPhyFormatDict(src)
			//
			newBuff := dictSel.Slice(sel, srcCount)
			ownedSel.Init3(newBuff)
			sel = ownedSel
			src = child
		case PF_CONST:
			sel = ZeroSelectVectorInPhyFormatConst(copyCount, ownedSel)
			finished = true
		case PF_FLAT:
			finished = true
		default:
			panic("usp")
		}
	}

	if copyCount == 0 {
		return
	}

	dstVecType := dstP.PhyFormat()
	if copyCount == 1 && dstVecType == PF_DICT {
		dstOffset = 0
		dstP.SetPhyFormat(PF_FLAT)
	}

	util.AssertFunc(dstP.PhyFormat().IsFlat())

	//copy bitmap
	dstBitmap := GetMaskInPhyFormatFlat(dstP)
	if src.PhyFormat().IsConst() {
		valid := !IsNullInPhyFormatConst(src)
		for i := 0; i < copyCount; i++ {
			dstBitmap.Set(uint64(dstOffset+i), valid)
		}
	} else {
		srcBitmap := CopyBitmap(src)
		if srcBitmap.IsMaskSet() {
			for i := 0; i < copyCount; i++ {
				idx := sel.GetIndex(srcOffset + i)

				if srcBitmap.RowIsValid(uint64(idx)) {
					if !dstBitmap.AllValid() {
						dstBitmap.SetValidUnsafe(uint64(dstOffset + i))
					}
				} else {
					if dstBitmap.AllValid() {
						initSize := max(util.DefaultVectorSize,
							dstOffset+copyCount)
						dstBitmap.Init(initSize)
					}
					dstBitmap.SetInvalidUnsafe(uint64(dstOffset + i))
				}
			}
		}
	}

	util.AssertFunc(sel != nil)

	//copy data
	switch src.Typ().GetInternalType() {
	case common.INT32:
		TemplatedCopy[int32](
			src,
			sel,
			dstP,
			srcOffset,
			dstOffset,
			copyCount,
		)
	case common.VARCHAR:
		srcSlice := GetSliceInPhyFormatFlat[common.String](src)
		dstSlice := GetSliceInPhyFormatFlat[common.String](dstP)

		for i := 0; i < copyCount; i++ {
			srcIdx := sel.GetIndex(srcOffset + i)
			dstIdx := dstOffset + i
			if dstBitmap.RowIsValid(uint64(dstIdx)) {
				srcStr := srcSlice[srcIdx]
				ptr := util.CMalloc(srcStr.Length())
				util.PointerCopy(ptr, srcStr.DataPtr(), srcStr.Length())
				dstSlice[dstIdx] = common.String{Data: ptr, Len: srcStr.Length()}
			}
		}
	default:
		panic("usp")
	}
}

func TemplatedCopy[T any](
	src *Vector,
	sel *SelectVector,
	dst *Vector,
	srcOffset int,
	dstOffset int,
	copyCount int,
) {
	srcSlice := GetSliceInPhyFormatFlat[T](src)
	dstSlice := GetSliceInPhyFormatFlat[T](dst)

	for i := 0; i < copyCount; i++ {
		srcIdx := sel.GetIndex(srcOffset + i)
		dstSlice[dstOffset+i] = srcSlice[srcIdx]
	}
}

func CopyBitmap(v *Vector) *util.Bitmap {
	switch v.PhyFormat() {
	case PF_FLAT:
		return GetMaskInPhyFormatFlat(v)
	default:
		panic("usp")
	}
}

func WriteToStorage(
	src *Vector,
	count int,
	ptr unsafe.Pointer,
) {
	if count == 0 {
		return
	}

	var vdata UnifiedFormat
	src.ToUnifiedFormat(count, &vdata)

	switch src.Typ().GetInternalType() {
	case common.INT32:
		SaveLoop[int32](&vdata, count, ptr, Int32ScatterOp{})
	case common.DECIMAL:
		SaveLoop[common.Decimal](&vdata, count, ptr, DecimalScatterOp{})
	case common.DATE:
		SaveLoop[common.Date](&vdata, count, ptr, DateScatterOp{})
	case common.INT64:
		SaveLoop[int64](&vdata, count, ptr, Int64ScatterOp{})
	case common.UINT64:
		SaveLoop[uint64](&vdata, count, ptr, Uint64ScatterOp{})
	default:
		panic("usp")
	}
}

func SaveLoop[T any](
	vdata *UnifiedFormat,
	count int,
	ptr unsafe.Pointer,
	nVal ScatterOp[T],
) {
	inSlice := GetSliceInPhyFormatUnifiedFormat[T](vdata)
	resSlice := util.PointerToSlice[T](ptr, count)
	for i := 0; i < count; i++ {
		idx := vdata.Sel.GetIndex(i)
		if !vdata.Mask.RowIsValid(uint64(idx)) {
			resSlice[i] = nVal.NullValue()
		} else {
			resSlice[i] = inSlice[idx]
		}
	}
}

func ReadFromStorage(
	ptr unsafe.Pointer,
	count int,
	res *Vector,
) {
	res.SetPhyFormat(PF_FLAT)
	switch res.Typ().GetInternalType() {
	case common.INT32:
		ReadLoop[int32](ptr, count, res)
	case common.DECIMAL:
		ReadLoop[common.Decimal](ptr, count, res)
	case common.DATE:
		ReadLoop[common.Date](ptr, count, res)
	case common.INT64:
		ReadLoop[int64](ptr, count, res)
	case common.UINT64:
		ReadLoop[uint64](ptr, count, res)
	default:
		panic("usp")
	}
}

func ReadLoop[T any](
	src unsafe.Pointer,
	count int,
	res *Vector,
) {
	srcSlice := util.PointerToSlice[T](src, count)
	resSlice := GetSliceInPhyFormatFlat[T](res)

	for i := 0; i < count; i++ {
		resSlice[i] = srcSlice[i]
	}
}

func SetData(vec *Vector, slice []byte) {
	util.AssertFunc(vec.PhyFormat().IsFlat())
	vec.Data = slice
}

func NewSelectVector3(tuples []int) *SelectVector {
	v := NewSelectVector(util.DefaultVectorSize)
	v.Init3(tuples)
	return v
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
