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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"
	"unsafe"

	dec "github.com/govalues/decimal"
)

type PhyFormat int

const (
	PF_FLAT PhyFormat = iota
	PF_CONST
	PF_DICT
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

func (f PhyFormat) isConst() bool {
	return f == PF_CONST
}

func (f PhyFormat) isFlat() bool {
	return f == PF_FLAT
}

func (f PhyFormat) isDict() bool {
	return f == PF_DICT
}

type UnifiedFormat struct {
	_sel      *SelectVector
	_data     []byte
	_mask     *Bitmap
	_interSel SelectVector
	_pTypSize int
}

type VecBufferType int

const (
	//array of data
	VBT_STANDARD VecBufferType = iota
	VBT_DICT
	VBT_CHILD
	VBT_STRING
)

type VecBuffer struct {
	_bufTyp VecBufferType
	_data   []byte
	_sel    *SelectVector
	_child  *Vector
}

func newBuffer(sz int) *VecBuffer {
	return &VecBuffer{
		_bufTyp: VBT_STANDARD,
		_data:   gAlloc.Alloc(sz),
	}
}

func NewStandardBuffer(lt LType, cap int) *VecBuffer {
	return newBuffer(lt.getInternalType().size() * cap)
}

func NewDictBuffer(data []int) *VecBuffer {
	return &VecBuffer{
		_bufTyp: VBT_DICT,
		_sel: &SelectVector{
			_selVec: data,
		},
	}
}

func NewDictBuffer2(sel *SelectVector) *VecBuffer {
	buf := &VecBuffer{
		_bufTyp: VBT_DICT,
		_sel:    &SelectVector{},
	}
	buf._sel.init2(sel)
	return buf
}

func NewChildBuffer(child *Vector) *VecBuffer {
	return &VecBuffer{
		_bufTyp: VBT_CHILD,
		_child:  child,
	}
}

func NewConstBuffer(typ LType) *VecBuffer {
	return NewStandardBuffer(typ, 1)
}

func (buf *VecBuffer) getSelVector() *SelectVector {
	assertFunc(buf._bufTyp == VBT_DICT)
	return buf._sel
}

const (
	defaultVectorSize = 8
)

type Vector struct {
	_phyFormat PhyFormat
	_typ       LType
	_data      []byte
	_mask      *Bitmap
	_buf       *VecBuffer
	_aux       *VecBuffer
}

func newVector(lTyp LType, initData bool, cap int) *Vector {
	vec := &Vector{
		_phyFormat: PF_FLAT,
		_typ:       lTyp,
		_mask:      &Bitmap{},
	}
	if initData {
		vec.init(cap)
	}
	return vec
}

func NewVector(lTyp LType, cap int) *Vector {
	return newVector(lTyp, true, cap)
}

func NewFlatVector(lTyp LType, cap int) *Vector {
	return NewVector(lTyp, cap)
}

func NewConstVector(lTyp LType) *Vector {
	vec := NewVector(lTyp, defaultVectorSize)
	vec.setPhyFormat(PF_CONST)
	return vec
}

func NewEmptyVector(typ LType, pf PhyFormat, cap int) *Vector {
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

func (vec *Vector) init(cap int) {
	vec._aux = nil
	vec._mask.reset()
	sz := vec.typ().getInternalType().size()
	if sz > 0 {
		vec._buf = NewStandardBuffer(vec.typ(), cap)
		vec._data = vec._buf._data
	}
	if cap > defaultVectorSize {
		vec._mask.resize(defaultVectorSize, cap)
	}
}

func (vec *Vector) typ() LType {
	return vec._typ
}

func (vec *Vector) phyFormat() PhyFormat {
	return vec._phyFormat
}

func (vec *Vector) setPhyFormat(pf PhyFormat) {
	vec._phyFormat = pf
	//
	if vec.typ().getInternalType().isConstant() &&
		(vec.phyFormat().isConst() || vec.phyFormat().isFlat()) {
		vec._aux = nil
	}
}
func (vec *Vector) getData() []byte {
	return vec._data
}

func flattenConstVector[T any](data []byte, srcData []byte, pSize int, cnt int) {
	src := toSlice[T](srcData, pSize)
	dst := toSlice[T](data, pSize)
	for i := 0; i < cnt; i++ {
		dst[i] = src[0]
	}
}

// to FLAT
func (vec *Vector) flatten(cnt int) {
	switch vec.phyFormat() {
	case PF_FLAT:
	case PF_CONST:
		null := isNullInPhyFormatConst(vec)
		//oldBuffer := vec._buf
		oldData := vec._data
		vec._buf = NewStandardBuffer(vec._typ, int(max(defaultVectorSize, cnt)))
		vec._data = vec._buf._data
		vec._phyFormat = PF_FLAT
		if null {
			vec._mask.setAllInvalid(cnt)
			return
		}
		//fill flat vector
		pTyp := vec.typ().getInternalType()
		switch pTyp {
		case BOOL:
			flattenConstVector[bool](vec._data, oldData, pTyp.size(), cnt)
		case UINT8:
			flattenConstVector[uint8](vec._data, oldData, pTyp.size(), cnt)
		case INT8:
			flattenConstVector[int8](vec._data, oldData, pTyp.size(), cnt)
		case UINT16:
			flattenConstVector[uint16](vec._data, oldData, pTyp.size(), cnt)
		case INT16:
			flattenConstVector[int16](vec._data, oldData, pTyp.size(), cnt)
		case UINT32:
			flattenConstVector[uint32](vec._data, oldData, pTyp.size(), cnt)
		case INT32:
			flattenConstVector[int32](vec._data, oldData, pTyp.size(), cnt)
		case UINT64:
			flattenConstVector[uint64](vec._data, oldData, pTyp.size(), cnt)
		case INT64:
			flattenConstVector[int64](vec._data, oldData, pTyp.size(), cnt)
		case FLOAT:
			flattenConstVector[float32](vec._data, oldData, pTyp.size(), cnt)
		case DOUBLE:
			flattenConstVector[float64](vec._data, oldData, pTyp.size(), cnt)
		case INTERVAL, LIST, STRUCT, VARCHAR, INT128, UNKNOWN, BIT, INVALID:
			panic("usp")
		default:
			panic("usp")
		}
	case PF_DICT:
		panic("usp")
	}
}

func (vec *Vector) flatten2(sel *SelectVector, cnt int) {
	if vec.phyFormat().isFlat() {
		return
	}
	panic("usp")
}

func (vec *Vector) toUnifiedFormat(count int, output *UnifiedFormat) {
	output._pTypSize = vec._typ.getInternalType().size()
	switch vec.phyFormat() {
	case PF_DICT:
		sel := getSelVectorInPhyFormatDict(vec)
		child := getChildInPhyFormatDict(vec)
		if child.phyFormat().isFlat() {
			output._sel = sel
			output._data = getDataInPhyFormatFlat(child)
			output._mask = getMaskInPhyFormatFlat(child)
		} else {
			//flatten child
			childVec := &Vector{
				_typ: child._typ,
			}
			childVec.reference(child)
			childVec.flatten2(sel, count)
			childBuf := NewChildBuffer(childVec)
			output._sel = sel
			output._data = getDataInPhyFormatFlat(childBuf._child)
			output._mask = getMaskInPhyFormatFlat(childBuf._child)
			vec._aux = childVec._aux
		}
	case PF_CONST:
		output._sel = zeroSelectVectorInPhyFormatConst(count, &output._interSel)
		output._data = getDataInPhyFormatConst(vec)
		output._mask = getMaskInPhyFormatConst(vec)
	case PF_FLAT:
		vec.flatten(count)
		output._sel = incrSelectVectorInPhyFormatFlat()
		output._data = getDataInPhyFormatFlat(vec)
		output._mask = getMaskInPhyFormatFlat(vec)
	}
}
func (vec *Vector) sliceOnSelf(sel *SelectVector, count int) {
	if vec.phyFormat().isConst() {
	} else if vec.phyFormat().isDict() {
		//dict
		curSel := getSelVectorInPhyFormatDict(vec)
		buf := curSel.slice(sel, count)
		vec._buf = NewDictBuffer(buf)
	} else {
		//flat
		child := &Vector{
			_typ: vec.typ(),
		}
		child.reference(vec)
		childRef := NewChildBuffer(child)
		dictBuf := NewDictBuffer2(sel)
		vec._phyFormat = PF_DICT
		vec._buf = dictBuf
		vec._aux = childRef
	}
}

func (vec *Vector) slice2(sel *SelectVector, count int) {
	vec.sliceOnSelf(sel, count)
}

func (vec *Vector) slice(other *Vector, sel *SelectVector, count int) {
	vec.reference(other)
	vec.sliceOnSelf(sel, count)
}

func (vec *Vector) reference(other *Vector) {
	assertFunc(vec.typ().equal(other.typ()))
	vec.reinterpret(other)
}

func (vec *Vector) referenceValue(val *Value) {
	assertFunc(vec.typ().id == val._typ.id)
	vec.setPhyFormat(PF_CONST)
	vec._buf = NewConstBuffer(val._typ)
	vec._aux = nil
	vec._data = getDataInPhyFormatConst(vec)
	vec.setValue(0, val)
}

func (vec *Vector) reinterpret(other *Vector) {
	vec._phyFormat = other._phyFormat
	vec._buf = other._buf
	vec._aux = other._aux
	vec._data = other._data
	vec._mask = other._mask
}

func (vec *Vector) getValue(idx int) *Value {
	switch vec.phyFormat() {
	case PF_CONST:
		idx = 0
	case PF_FLAT:
	case PF_DICT:
		sel := getSelVectorInPhyFormatDict(vec)
		child := getChildInPhyFormatDict(vec)
		return child.getValue(sel.getIndex(idx))
	default:
		panic("usp")
	}
	if !vec._mask.rowIsValid(uint64(idx)) {
		return &Value{
			_typ:    vec.typ(),
			_isNull: true,
		}
	}

	switch vec.typ().id {
	case LTID_INTEGER:
		data := getSliceInPhyFormatFlat[int32](vec)
		return &Value{
			_typ: vec.typ(),
			_i64: int64(data[idx]),
		}
	case LTID_BOOLEAN:
		data := getSliceInPhyFormatFlat[bool](vec)
		return &Value{
			_typ:  vec.typ(),
			_bool: data[idx],
		}
	case LTID_VARCHAR:
		data := getSliceInPhyFormatFlat[String](vec)
		return &Value{
			_typ: vec.typ(),
			_str: data[idx].String(),
		}
	case LTID_DECIMAL:
		data := getSliceInPhyFormatFlat[Decimal](vec)
		d := data[idx]
		w, f, ok := d.Decimal.Int64(vec.typ().scale)
		if !ok {
			return &Value{
				_typ: vec.typ(),
				_str: data[idx].String(),
			}
		} else {
			return &Value{
				_typ:   vec.typ(),
				_i64:   w,
				_i64_1: f,
				_str:   "",
			}
		}
	case LTID_DATE:
		data := getSliceInPhyFormatFlat[Date](vec)
		return &Value{
			_typ:   vec.typ(),
			_i64:   int64(data[idx]._year),
			_i64_1: int64(data[idx]._month),
			_i64_2: int64(data[idx]._day),
		}
	case LTID_UBIGINT:
		data := getSliceInPhyFormatFlat[uint64](vec)
		return &Value{
			_typ: vec.typ(),
			_i64: int64(data[idx]),
		}
	case LTID_DOUBLE:
		data := getSliceInPhyFormatFlat[float64](vec)
		return &Value{
			_typ: vec.typ(),
			_f64: data[idx],
		}
	case LTID_FLOAT:
		data := getSliceInPhyFormatFlat[float32](vec)
		return &Value{
			_typ: vec.typ(),
			_f64: float64(data[idx]),
		}
	default:
		panic("usp")
	}
}

func (vec *Vector) setValue(idx int, val *Value) {
	if vec.phyFormat().isDict() {
		sel := getSelVectorInPhyFormatDict(vec)
		child := getChildInPhyFormatDict(vec)
		child.setValue(sel.getIndex(idx), val)
	}
	assertFunc(val._typ.equal(vec.typ()))
	assertFunc(val._typ.getInternalType() == vec.typ().getInternalType())
	vec._mask.set(uint64(idx), !val._isNull)
	pTyp := vec.typ().getInternalType()
	switch pTyp {
	case INT32:
		slice := toSlice[int32](vec._data, pTyp.size())
		slice[idx] = int32(val._i64)
	case FLOAT:
		slice := toSlice[float32](vec._data, pTyp.size())
		slice[idx] = float32(val._f64)
	case VARCHAR:
		slice := toSlice[String](vec._data, pTyp.size())
		byteSlice := []byte(val._str)
		dstMem := cMalloc(len(byteSlice))
		dst := pointerToSlice[byte](dstMem, len(byteSlice))
		copy(dst, byteSlice)
		slice[idx] = String{
			_data: dstMem,
			_len:  len(dst),
		}
		again := slice[idx].String()
		assertFunc(again == val._str)

	case INTERVAL:
		slice := toSlice[Interval](vec._data, pTyp.size())
		interVal := Interval{}
		switch strings.ToLower(val._str) {
		case "year":
			interVal._year = int32(val._i64)
			interVal._unit = val._str
		case "month":
			interVal._months = int32(val._i64)
			interVal._unit = val._str
		case "day":
			interVal._days = int32(val._i64)
			interVal._unit = val._str
		default:
			panic("usp")
		}
		slice[idx] = interVal
	case DATE:
		slice := toSlice[Date](vec._data, pTyp.size())
		slice[idx] = Date{
			_year:  int32(val._i64),
			_month: int32(val._i64_1),
			_day:   int32(val._i64_2),
		}
	case DECIMAL:
		slice := toSlice[Decimal](vec._data, pTyp.size())
		if len(val._str) != 0 {
			decVal, err := dec.ParseExact(val._str, vec.typ().scale)
			if err != nil {
				panic(err)
			}
			val._str = ""
			slice[idx] = Decimal{
				decVal,
			}
		} else {
			nDec, err := dec.NewFromInt64(val._i64, val._i64_1, vec._typ.scale)
			if err != nil {
				panic(err)
			}
			slice[idx] = Decimal{
				nDec,
			}
		}

	case DOUBLE:
		slice := toSlice[float64](vec._data, pTyp.size())
		slice[idx] = val._f64
	case BOOL:
		slice := toSlice[bool](vec._data, pTyp.size())
		slice[idx] = val._bool
	default:
		panic("usp")
	}
}

func (vec *Vector) reset() {
	vec._mask.reset()
}

func (vec *Vector) print(rowCount int) {
	for j := 0; j < rowCount; j++ {
		val := vec.getValue(j)
		fmt.Println(val)
	}
	fmt.Println()
}

func (vec *Vector) serialize(count int, serial Serialize) error {
	var vdata UnifiedFormat
	vec.toUnifiedFormat(count, &vdata)
	writeValidity := (count > 0) && !vdata._mask.AllValid()
	err := Write[bool](writeValidity, serial)
	if err != nil {
		return err
	}
	if writeValidity {
		flatMask := &Bitmap{}
		flatMask.init(count)
		for i := 0; i < count; i++ {
			rowIdx := vdata._sel.getIndex(i)
			flatMask.set(uint64(i), vdata._mask.rowIsValid(uint64(rowIdx)))
		}
		err = serial.WriteData(flatMask.data(), flatMask.bytes(count))
		if err != nil {
			return err
		}
	}
	typ := vec.typ()
	if typ.getInternalType().isConstant() {
		writeSize := typ.getInternalType().size() * count
		buff := gAlloc.Alloc(writeSize)
		defer gAlloc.Free(buff)
		WriteToStorage(vec, count, bytesSliceToPointer(buff))
		err = serial.WriteData(buff, writeSize)
		if err != nil {
			return err
		}
	} else {
		switch typ.getInternalType() {
		case VARCHAR:
			strSlice := getSliceInPhyFormatUnifiedFormat[String](&vdata)
			for i := 0; i < count; i++ {
				idx := vdata._sel.getIndex(i)
				if !vdata._mask.rowIsValid(uint64(idx)) {
					nVal := stringScatterOp{}.nullValue()
					err = WriteString(nVal, serial)
					if err != nil {
						return err
					}
				} else {
					val := strSlice[idx]
					err = WriteString(val, serial)
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

func (vec *Vector) deserialize(count int, deserial Deserialize) error {
	var mask *Bitmap
	switch vec.phyFormat() {
	case PF_CONST:
		mask = getMaskInPhyFormatConst(vec)
	case PF_FLAT:
		mask = getMaskInPhyFormatFlat(vec)
	case PF_DICT:
		panic("usp")
	}
	mask.reset()
	hasMask := false
	err := Read[bool](&hasMask, deserial)
	if err != nil {
		return err
	}
	if hasMask {
		mask.init(count)
		err = deserial.ReadData(mask.data(), mask.bytes(count))
		if err != nil {
			return err
		}
	}

	typ := vec.typ()
	if typ.getInternalType().isConstant() {
		readSize := typ.getInternalType().size() * count
		buf := gAlloc.Alloc(readSize)
		defer gAlloc.Free(buf)
		err = deserial.ReadData(buf, readSize)
		if err != nil {
			return err
		}
		ReadFromStorage(bytesSliceToPointer(buf), count, vec)
	} else {
		switch typ.getInternalType() {
		case VARCHAR:
			strSlice := getSliceInPhyFormatFlat[String](vec)
			for i := 0; i < count; i++ {
				var str String
				err = ReadString(&str, deserial)
				if err != nil {
					return err
				}
				if mask.rowIsValid(uint64(i)) {
					strSlice[i] = str
				}
			}
		default:
			panic("usp")
		}
	}
	return nil
}

// constant vector
func getDataInPhyFormatConst(vec *Vector) []byte {
	assertFunc(vec.phyFormat().isConst() || vec.phyFormat().isFlat())
	return vec._data
}

func getSliceInPhyFormatConst[T any](vec *Vector) []T {
	assertFunc(vec.phyFormat().isConst() || vec.phyFormat().isFlat())
	pSize := vec.typ().getInternalType().size()
	return toSlice[T](vec._data, pSize)
}

func isNullInPhyFormatConst(vec *Vector) bool {
	assertFunc(vec.phyFormat().isConst())
	return !vec._mask.rowIsValid(0)
}

func setNullInPhyFormatConst(vec *Vector, null bool) {
	assertFunc(vec.phyFormat().isConst())
	vec._mask.set(0, !null)
}

func zeroSelectVectorInPhyFormatConst(cnt int, sel *SelectVector) *SelectVector {
	sel.init(cnt)
	return sel
}

func getMaskInPhyFormatConst(vec *Vector) *Bitmap {
	assertFunc(vec.phyFormat().isConst())
	return vec._mask
}

func referenceInPhyFormatConst(
	vec *Vector,
	src *Vector,
	pos int,
	count int,
) {
	srcTyp := src.typ().getInternalType()
	switch srcTyp {
	case LIST:
		panic("usp")
	case STRUCT:
		panic("usp")
	default:
		value := src.getValue(pos)
		vec.referenceValue(value)
		assertFunc(vec.phyFormat().isConst())
	}
}

// flat vector
func getDataInPhyFormatFlat(vec *Vector) []byte {
	return getDataInPhyFormatConst(vec)
}

func getSliceInPhyFormatFlat[T any](vec *Vector) []T {
	return getSliceInPhyFormatConst[T](vec)
}

func setMaskInPhyFormatFlat(vec *Vector, mask *Bitmap) {
	assertFunc(vec.phyFormat().isFlat())
	vec._mask.shareWith(mask)
}

func getMaskInPhyFormatFlat(vec *Vector) *Bitmap {
	assertFunc(vec.phyFormat().isFlat())
	return vec._mask
}

func setNullInPhyFormatFlat(vec *Vector, idx uint64, null bool) {
	assertFunc(vec.phyFormat().isFlat())
	vec._mask.set(idx, !null)
}

//func isNullInPhyFormatFlat(vec *Vector, idx uint64) bool {
//	assertFunc(vec.phyFormat().isFlat())
//	return !vec._mask.rowIsValid(idx)
//}

func incrSelectVectorInPhyFormatFlat() *SelectVector {
	return &SelectVector{}
}

// dictionary vector
func getSelVectorInPhyFormatDict(vec *Vector) *SelectVector {
	assertFunc(vec.phyFormat().isDict())
	return vec._buf.getSelVector()
}

func getChildInPhyFormatDict(vec *Vector) *Vector {
	assertFunc(vec.phyFormat().isDict())
	return vec._aux._child
}

// unified format
func getSliceInPhyFormatUnifiedFormat[T any](uni *UnifiedFormat) []T {
	return toSlice[T](uni._data, uni._pTypSize)
}

type Bitmap struct {
	_bits []uint8
}

func (bm *Bitmap) data() []uint8 {
	return bm._bits
}

func (bm *Bitmap) bytes(count int) int {
	return entryCount(count)
}

func (bm *Bitmap) init(count int) {
	cnt := entryCount(count)
	bm._bits = gAlloc.Alloc(cnt)
	for i := range bm._bits {
		bm._bits[i] = 0xFF
	}
}

func (bm *Bitmap) shareWith(other *Bitmap) {
	bm._bits = other._bits
}

func (bm *Bitmap) invalid() bool {
	return len(bm._bits) == 0
}

func (bm *Bitmap) getEntry(eIdx uint64) uint8 {
	if bm.invalid() {
		return 0xFF
	}
	return bm._bits[eIdx]
}

func getEntryIndex(idx uint64) (uint64, uint64) {
	return idx / 8, idx % 8
}

func entryIsSet(e uint8, pos uint64) bool {
	return e&(1<<pos) != 0
}

func (bm *Bitmap) combine(other *Bitmap, count int) {
	if other.AllValid() {
		return
	}
	if bm.AllValid() {
		bm.shareWith(other)
		return
	}
	oldData := bm._bits
	bm.init(count)
	eCnt := entryCount(count)
	for i := 0; i < eCnt; i++ {
		bm._bits[i] = oldData[i] & other._bits[i]
	}
}

func rowIsValidInEntry(e uint8, pos uint64) bool {
	return entryIsSet(e, pos)
}

func (bm *Bitmap) rowIsValidUnsafe(idx uint64) bool {
	eIdx, pos := getEntryIndex(idx)
	e := bm.getEntry(eIdx)
	return entryIsSet(e, pos)
}

func (bm *Bitmap) rowIsValid(idx uint64) bool {
	if bm.invalid() {
		return true
	}
	return bm.rowIsValidUnsafe(idx)
}

func (bm *Bitmap) setValid(ridx uint64) {
	if bm.invalid() {
		return
	}
	bm.setValidUnsafe(ridx)
}

func (bm *Bitmap) set(ridx uint64, valid bool) {
	if valid {
		bm.setValid(ridx)
	} else {
		bm.setInvalid(ridx)
	}
}

func (bm *Bitmap) setValidUnsafe(ridx uint64) {
	eIdx, pos := getEntryIndex(ridx)
	bm._bits[eIdx] |= 1 << pos
}

func (bm *Bitmap) setInvalid(ridx uint64) {
	if bm.invalid() {
		bm.init(defaultVectorSize)
	}
	bm.setInvalidUnsafe(ridx)
}

func (bm *Bitmap) setInvalidUnsafe(ridx uint64) {
	eIdx, pos := getEntryIndex(ridx)
	bm._bits[eIdx] &= ^(1 << pos)
}

func (bm *Bitmap) reset() {
	bm._bits = nil
}

func entryCount(cnt int) int {
	return (cnt + 7) / 8
}

func sizeInBytes(cnt int) int {
	return entryCount(cnt)
}

func (bm *Bitmap) resize(old int, new int) {
	if new <= old {
		return
	}
	if bm._bits != nil {
		ncnt := entryCount(new)
		ocnt := entryCount(old)
		newData := gAlloc.Alloc(ncnt)
		copy(newData, bm._bits)
		for i := ocnt; i < ncnt; i++ {
			newData[i] = 0xFF
		}
		bm._bits = newData
	} else {
		bm.init(new)
	}
}

func (bm *Bitmap) prepareSpace(cnt int) {
	if bm.invalid() {
		bm.init(int(cnt))
	}
}

func (bm *Bitmap) setAllInvalid(cnt int) {
	bm.prepareSpace(cnt)
	if cnt == 0 {
		return
	}
	lastEidx := entryCount(int(cnt)) - 1
	for i := 0; i < lastEidx; i++ {
		bm._bits[i] = 0
	}
	lastBits := cnt % 8
	if lastBits == 0 {
		bm._bits[lastEidx] = 0
	} else {
		bm._bits[lastEidx] = 0xFF << lastBits
	}
}
func NoneValidInEntry(entry uint8) bool {
	return entry == 0
}

func AllValidInEntry(entry uint8) bool {
	return entry == 0xFF
}

func (bm *Bitmap) AllValid() bool {
	return bm.invalid()
}

func (bm *Bitmap) copyFrom(other *Bitmap, count int) {
	if other.AllValid() {
		bm._bits = nil
	} else {
		eCnt := entryCount(count)
		bm._bits = make([]uint8, eCnt)
		copy(bm._bits, other._bits[:eCnt])
	}
}

func (bm *Bitmap) setAllValid(cnt int) {
	bm.prepareSpace(cnt)
	if cnt == 0 {
		return
	}
	lastEidx := entryCount(int(cnt)) - 1
	for i := 0; i < lastEidx; i++ {
		bm._bits[i] = 0xFF
	}
	lastBits := cnt % 8
	if lastBits == 0 {
		bm._bits[lastEidx] = 0xFF
	} else {
		bm._bits[lastEidx] = ^(0xFF << lastBits)
	}
}

func (bm *Bitmap) isMaskSet() bool {
	return bm._bits != nil
}

type SelectVector struct {
	_selVec []int
}

func NewSelectVector(count int) *SelectVector {
	vec := &SelectVector{}
	vec.init(count)
	return vec
}

func (svec *SelectVector) invalid() bool {
	return len(svec._selVec) == 0
}

func (svec *SelectVector) init(cnt int) {
	svec._selVec = make([]int, cnt)
}

func (svec *SelectVector) getIndex(idx int) int {
	if svec.invalid() {
		return idx
	} else {
		return svec._selVec[idx]
	}
}

func (svec *SelectVector) setIndex(idx int, index int) {
	svec._selVec[idx] = index
}

func (svec *SelectVector) slice(sel *SelectVector, count int) []int {
	data := make([]int, count)
	for i := 0; i < count; i++ {
		newIdx := sel.getIndex(i)
		idx := svec.getIndex(newIdx)
		data[i] = idx
	}
	return data
}

func (svec *SelectVector) init2(sel *SelectVector) {
	svec._selVec = sel._selVec
}

func (svec *SelectVector) init3(data []int) {
	svec._selVec = data
}

type Chunk struct {
	_data  []*Vector
	_count int
	_cap   int
}

func (c *Chunk) init(types []LType, cap int) {
	c._cap = cap
	c._data = nil
	for _, lType := range types {
		c._data = append(c._data, NewVector(lType, c._cap))
	}
}

func (c *Chunk) reset() {
	if len(c._data) == 0 {
		return
	}
	for _, vec := range c._data {
		vec.reset()
	}
	c._cap = defaultVectorSize
	c._count = 0
}

func (c *Chunk) cap() int {
	return c._cap
}

func (c *Chunk) setCap(cap int) {
	c._cap = cap
}

func (c *Chunk) setCard(count int) {
	assertFunc(c._count <= c._cap)
	c._count = count
}

func (c *Chunk) card() int {
	return c._count
}

func (c *Chunk) columnCount() int {
	return len(c._data)
}

func (c *Chunk) referenceIndice(other *Chunk, indice []int) {
	//assertFunc(other.columnCount() <= c.columnCount())
	c.setCard(other.card())
	for i, idx := range indice {
		c._data[i].reference(other._data[idx])
	}
}

func (c *Chunk) reference(other *Chunk) {
	assertFunc(other.columnCount() <= c.columnCount())
	c.setCap(other.cap())
	c.setCard(other.card())
	for i := 0; i < other.columnCount(); i++ {
		c._data[i].reference(other._data[i])
	}
}

func (c *Chunk) sliceIndice(other *Chunk, sel *SelectVector, count int, colOffset int, indice []int) {
	//assertFunc(other.columnCount() <= colOffset+c.columnCount())
	c.setCard(count)
	for i, idx := range indice {
		if other._data[i].phyFormat().isDict() {
			c._data[i+colOffset].reference(other._data[idx])
			c._data[i+colOffset].slice2(sel, count)
		} else {
			c._data[i+colOffset].slice(other._data[idx], sel, count)
		}
	}
}

func (c *Chunk) slice(other *Chunk, sel *SelectVector, count int, colOffset int) {
	assertFunc(other.columnCount() <= colOffset+c.columnCount())
	c.setCard(count)
	for i := 0; i < other.columnCount(); i++ {
		if other._data[i].phyFormat().isDict() {
			c._data[i+colOffset].reference(other._data[i])
			c._data[i+colOffset].slice2(sel, count)
		} else {
			c._data[i+colOffset].slice(other._data[i], sel, count)
		}
	}
}

func (c *Chunk) ToUnifiedFormat() []*UnifiedFormat {
	ret := make([]*UnifiedFormat, c.columnCount())
	for i := 0; i < c.columnCount(); i++ {
		ret[i] = &UnifiedFormat{}
		c._data[i].toUnifiedFormat(c.card(), ret[i])
	}
	return ret
}

func (c *Chunk) print() {
	for i := 0; i < c.card(); i++ {
		for j := 0; j < c.columnCount(); j++ {
			val := c._data[j].getValue(i)
			fmt.Print(val)
			fmt.Print(" | ")
		}
		fmt.Println()
	}
	if c.card() > 0 {
		fmt.Println()
	}
}

func (c *Chunk) sliceItself(sel *SelectVector, cnt int) {
	c._count = cnt
	for i := 0; i < c.columnCount(); i++ {
		c._data[i].sliceOnSelf(sel, cnt)
	}
}

func (c *Chunk) Hash(result *Vector) {
	assertFunc(result.typ().id == hashType().id)
	HashTypeSwitch(c._data[0], result, nil, c.card(), false)
	for i := 1; i < c.columnCount(); i++ {
		CombineHashTypeSwitch(result, c._data[i], nil, c.card(), false)
	}
}

func (c *Chunk) serialize(serial Serialize) error {
	//save row count
	err := Write[uint32](uint32(c.card()), serial)
	if err != nil {
		return err
	}
	//save column count
	err = Write[uint32](uint32(c.columnCount()), serial)
	if err != nil {
		return err
	}
	//save column types
	for i := 0; i < c.columnCount(); i++ {
		err = c._data[i].typ().serialize(serial)
		if err != nil {
			return err
		}
	}
	//save column data
	for i := 0; i < c.columnCount(); i++ {
		err = c._data[i].serialize(c.card(), serial)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Chunk) deserialize(deserial Deserialize) error {
	//read row count
	rowCnt := uint32(0)
	err := Read[uint32](&rowCnt, deserial)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	//read column count
	colCnt := uint32(0)
	err = Read[uint32](&colCnt, deserial)
	if err != nil {
		return err
	}
	//read column types
	typs := make([]LType, colCnt)
	for i := uint32(0); i < colCnt; i++ {
		typs[i], err = deserializeLType(deserial)
		if err != nil {
			return err
		}
	}
	c.init(typs, defaultVectorSize)
	c.setCard(int(rowCnt))
	//read column data
	for i := uint32(0); i < colCnt; i++ {
		err = c._data[i].deserialize(int(rowCnt), deserial)
		if err != nil {
			return err
		}
	}
	return err
}

type Value struct {
	_typ    LType
	_isNull bool
	//value
	_bool  bool
	_i64   int64
	_i64_1 int64
	_i64_2 int64
	_f64   float64
	_str   string
}

func (val Value) String() string {
	if val._isNull {
		return "NULL"
	}
	switch val._typ.id {
	case LTID_INTEGER:
		return fmt.Sprintf("%d", val._i64)
	case LTID_BOOLEAN:
		return fmt.Sprintf("%v", val._bool)
	case LTID_VARCHAR:
		return val._str
	case LTID_DECIMAL:
		if len(val._str) != 0 {
			return val._str
		} else {
			return fmt.Sprintf("%v.%v", val._i64, val._i64_1)
		}
	case LTID_DATE:
		dat := time.Date(int(val._i64), time.Month(val._i64_1), int(val._i64_2),
			0, 0, 0, 0, time.UTC)
		return dat.String()
	case LTID_UBIGINT:
		return fmt.Sprintf("0x%x %d", val._i64, val._i64)
	case LTID_DOUBLE:
		return fmt.Sprintf("%v", val._f64)
	case LTID_FLOAT:
		return fmt.Sprintf("%v", val._f64)
	default:
		panic("usp")
	}
}

func booleanNullMask(left, right, result *Vector, count int, boolOp BooleanOp) {
	assertFunc(left.typ().id == LTID_BOOLEAN &&
		right.typ().id == LTID_BOOLEAN &&
		result.typ().id == LTID_BOOLEAN)
	if left.phyFormat().isConst() && right.phyFormat().isConst() {
		result.setPhyFormat(PF_CONST)
		ldata := getSliceInPhyFormatConst[uint8](left)
		rdata := getSliceInPhyFormatConst[uint8](right)
		target := getSliceInPhyFormatConst[bool](result)
		null, res := boolOp.opWithNull(ldata[0] > 0, rdata[0] > 0, isNullInPhyFormatConst(left), isNullInPhyFormatConst(right))
		target[0] = res
		setNullInPhyFormatConst(result, null)
	} else {
		var ldata, rdata UnifiedFormat
		left.toUnifiedFormat(count, &ldata)
		right.toUnifiedFormat(count, &rdata)

		result.setPhyFormat(PF_FLAT)
		lSlice := getSliceInPhyFormatUnifiedFormat[uint8](&ldata)
		rSlice := getSliceInPhyFormatUnifiedFormat[uint8](&rdata)
		target := getSliceInPhyFormatFlat[bool](result)
		targetMask := getMaskInPhyFormatFlat(result)
		if !ldata._mask.AllValid() || !rdata._mask.AllValid() {
			for i := 0; i < count; i++ {
				lidx := ldata._sel.getIndex(i)
				ridx := rdata._sel.getIndex(i)
				null, res := boolOp.opWithNull(lSlice[lidx] > 0,
					rSlice[ridx] > 0,
					!ldata._mask.rowIsValid(uint64(lidx)),
					!rdata._mask.rowIsValid(uint64(ridx)),
				)
				target[i] = res
				targetMask.set(uint64(i), !null)
			}
		} else {
			for i := 0; i < count; i++ {
				lidx := ldata._sel.getIndex(i)
				ridx := rdata._sel.getIndex(i)
				res := boolOp.opWithoutNull(lSlice[lidx] > 0, rSlice[ridx] > 0)
				target[i] = res
			}
		}
	}
}

// AddInPlace left += delta
func AddInPlace(input *Vector, right int64, cnt int) {
	assertFunc(input.typ().id == LTID_POINTER)
	if right == 0 {
		return
	}
	switch input.phyFormat() {
	case PF_CONST:
		assertFunc(!isNullInPhyFormatConst(input))
		data := getSliceInPhyFormatConst[unsafe.Pointer](input)
		data[0] = pointerAdd(data[0], int(right))
	default:
		assertFunc(input.phyFormat().isFlat())
		data := getSliceInPhyFormatFlat[unsafe.Pointer](input)
		for i := 0; i < cnt; i++ {
			data[i] = pointerAdd(data[i], int(right))
		}
	}
}

func And(left, right, result *Vector, count int) {

}

func Copy(
	srcP *Vector,
	dstP *Vector,
	selP *SelectVector,
	srcCount int,
	srcOffset int,
	dstOffset int,
) {
	assertFunc(srcOffset <= srcCount)
	assertFunc(srcP.typ().id == dstP.typ().id)
	copyCount := srcCount - srcOffset
	finished := false

	ownedSel := &SelectVector{}
	sel := selP
	src := srcP

	for !finished {
		switch src.phyFormat() {
		case PF_DICT:
			//dict vector
			child := getChildInPhyFormatDict(src)
			dictSel := getSelVectorInPhyFormatDict(src)
			//
			newBuff := dictSel.slice(sel, srcCount)
			ownedSel.init3(newBuff)
			sel = ownedSel
			src = child
		case PF_CONST:
			sel = zeroSelectVectorInPhyFormatConst(copyCount, ownedSel)
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

	dstVecType := dstP.phyFormat()
	if copyCount == 1 && dstVecType == PF_DICT {
		dstOffset = 0
		dstP.setPhyFormat(PF_FLAT)
	}

	assertFunc(dstP.phyFormat().isFlat())

	//copy bitmap
	dstBitmap := getMaskInPhyFormatFlat(dstP)
	if src.phyFormat().isConst() {
		valid := !isNullInPhyFormatConst(src)
		for i := 0; i < copyCount; i++ {
			dstBitmap.set(uint64(dstOffset+i), valid)
		}
	} else {
		srcBitmap := CopyBitmap(src)
		if srcBitmap.isMaskSet() {
			for i := 0; i < copyCount; i++ {
				idx := sel.getIndex(srcOffset + i)

				if srcBitmap.rowIsValid(uint64(idx)) {
					if !dstBitmap.AllValid() {
						dstBitmap.setValidUnsafe(uint64(dstOffset + i))
					}
				} else {
					if dstBitmap.AllValid() {
						initSize := max(defaultVectorSize,
							dstOffset+copyCount)
						dstBitmap.init(initSize)
					}
					dstBitmap.setInvalidUnsafe(uint64(dstOffset + i))
				}
			}
		}
	}

	assertFunc(sel != nil)

	//copy data
	switch src.typ().getInternalType() {
	case INT32:
		TemplatedCopy[int32](
			src,
			sel,
			dstP,
			srcOffset,
			dstOffset,
			copyCount,
		)
	case VARCHAR:
		srcSlice := getSliceInPhyFormatFlat[String](src)
		dstSlice := getSliceInPhyFormatFlat[String](dstP)

		for i := 0; i < copyCount; i++ {
			srcIdx := sel.getIndex(srcOffset + i)
			dstIdx := dstOffset + i
			if dstBitmap.rowIsValid(uint64(dstIdx)) {
				srcStr := srcSlice[srcIdx]
				ptr := cMalloc(srcStr.len())
				pointerCopy(ptr, srcStr.data(), srcStr.len())
				dstSlice[dstIdx] = String{_data: ptr, _len: srcStr.len()}
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
	srcSlice := getSliceInPhyFormatFlat[T](src)
	dstSlice := getSliceInPhyFormatFlat[T](dst)

	for i := 0; i < copyCount; i++ {
		srcIdx := sel.getIndex(srcOffset + i)
		dstSlice[dstOffset+i] = srcSlice[srcIdx]
	}
}

func CopyBitmap(v *Vector) *Bitmap {
	switch v.phyFormat() {
	case PF_FLAT:
		return getMaskInPhyFormatFlat(v)
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
	src.toUnifiedFormat(count, &vdata)

	switch src.typ().getInternalType() {
	case INT32:
		saveLoop[int32](&vdata, count, ptr, int32ScatterOp{})
	case DECIMAL:
		saveLoop[Decimal](&vdata, count, ptr, decimalScatterOp{})
	default:
		panic("usp")
	}
}

func saveLoop[T any](
	vdata *UnifiedFormat,
	count int,
	ptr unsafe.Pointer,
	nVal ScatterOp[T],
) {
	inSlice := getSliceInPhyFormatUnifiedFormat[T](vdata)
	resSlice := pointerToSlice[T](ptr, count)
	for i := 0; i < count; i++ {
		idx := vdata._sel.getIndex(i)
		if !vdata._mask.rowIsValid(uint64(idx)) {
			resSlice[i] = nVal.nullValue()
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
	res.setPhyFormat(PF_FLAT)
	switch res.typ().getInternalType() {
	case INT32:
		readLoop[int32](ptr, count, res)
	case DECIMAL:
		readLoop[Decimal](ptr, count, res)
	default:
		panic("usp")
	}
}

func readLoop[T any](
	src unsafe.Pointer,
	count int,
	res *Vector,
) {
	srcSlice := pointerToSlice[T](src, count)
	resSlice := getSliceInPhyFormatFlat[T](res)

	for i := 0; i < count; i++ {
		resSlice[i] = srcSlice[i]
	}
}

func randomVector(
	typ LType,
	pf PhyFormat,
	nullRatio float32,
) *Vector {
	vec := NewEmptyVector(typ, pf, defaultVectorSize)
	switch pf {
	case PF_FLAT:
		fillFlatVector(vec, defaultVectorSize, nullRatio)
	case PF_CONST:
		fillConstVector(vec, nullRatio)
	default:
		panic("usp")
	}
	return vec
}

func fillConstVector(
	vec *Vector,
	nullRatio float32,
) {
	fillFlatVector(vec, 1, nullRatio)
}

func fillFlatVector(
	vec *Vector,
	count int,
	nullRatio float32,
) {
	assertFunc(vec.phyFormat().isFlat() || vec.phyFormat().isConst())
	switch vec.typ().getInternalType() {
	case INT32:
		dSlice := getSliceInPhyFormatFlat[int32](vec)
		fillLoop[int32](dSlice, vec._mask, count, nullRatio, int32ScatterOp{})
	case DECIMAL:
		dSlice := getSliceInPhyFormatFlat[Decimal](vec)
		fillLoop[Decimal](dSlice, vec._mask, count, nullRatio, decimalScatterOp{})
	case VARCHAR:
		dSlice := getSliceInPhyFormatFlat[String](vec)
		fillLoop[String](dSlice, vec._mask, count, nullRatio, stringScatterOp{})
	default:
		panic("usp")
	}
}

func fillLoop[T any](
	data []T,
	mask *Bitmap,
	count int,
	nullRatio float32,
	nVal ScatterOp[T],
) {
	for i := 0; i < count; i++ {
		if rand.Float32() <= nullRatio {
			mask.setInvalid(uint64(i))
		} else {
			data[i] = nVal.randValue()
		}
	}
}

func isSameVector(
	vecA, vecB *Vector,
	count int,
) bool {
	if vecA.typ().id != vecB.typ().id {
		return false
	}
	if vecA.phyFormat().isConst() && vecB.phyFormat().isConst() {
		return checkConstVector(vecA, vecB)
	} else if vecA.phyFormat().isFlat() && vecB.phyFormat().isFlat() {
		return checkFlatVector(vecA, vecB, count)
	} else {

		var vDataA, vDataB UnifiedFormat
		vecA.toUnifiedFormat(count, &vDataA)
		vecB.toUnifiedFormat(count, &vDataB)
		switch vecA.typ().getInternalType() {
		case INT32:
			return checkLoop2[int32](&vDataA, &vDataB, count, equalOp[int32]{})
		case DECIMAL:
			return checkLoop2[Decimal](&vDataA, &vDataB, count, equalDecimalOp{})
		case VARCHAR:
			return checkLoop2[String](&vDataA, &vDataB, count, equalStrOp{})
		default:
			panic("usp")
		}
	}
}
func checkConstVector(
	vecA, vecB *Vector,
) bool {
	return checkFlatVector(vecA, vecB, 1)
}

func checkFlatVector(
	vecA, vecB *Vector,
	count int,
) bool {
	assertFunc(
		vecA.phyFormat().isConst() && vecB.phyFormat().isConst() ||
			vecA.phyFormat().isFlat() && vecB.phyFormat().isFlat())
	assertFunc(vecA.typ().id == vecB.typ().id)
	ret := false
	var maskA, maskB *Bitmap
	switch vecA.phyFormat() {
	case PF_FLAT:
		maskA = getMaskInPhyFormatFlat(vecA)
		maskB = getMaskInPhyFormatFlat(vecB)
	case PF_CONST:
		maskA = getMaskInPhyFormatConst(vecA)
		maskB = getMaskInPhyFormatConst(vecB)
	case PF_DICT:
		panic("usp")
	}

	switch vecA.typ().getInternalType() {
	case INT32:
		dataA := getSliceInPhyFormatFlat[int32](vecA)
		dataB := getSliceInPhyFormatFlat[int32](vecB)
		ret = checkLoop[int32](
			dataA,
			maskA,
			dataB,
			maskB,
			count,
			equalOp[int32]{},
		)
	case DECIMAL:
		dataA := getSliceInPhyFormatFlat[Decimal](vecA)
		dataB := getSliceInPhyFormatFlat[Decimal](vecB)
		ret = checkLoop[Decimal](
			dataA,
			maskA,
			dataB,
			maskB,
			count,
			equalDecimalOp{},
		)
	case VARCHAR:
		dataA := getSliceInPhyFormatFlat[String](vecA)
		dataB := getSliceInPhyFormatFlat[String](vecB)
		ret = checkLoop[String](
			dataA,
			maskA,
			dataB,
			maskB,
			count,
			equalStrOp{},
		)
	default:
		panic("usp")
	}
	return ret
}

func checkLoop[T any](
	dataA []T,
	maskA *Bitmap,
	dataB []T,
	maskB *Bitmap,
	count int,
	eOp CompareOp[T],
) bool {
	for i := 0; i < count; i++ {
		mA := maskA.rowIsValid(uint64(i))
		mB := maskB.rowIsValid(uint64(i))
		if mA && mB {
			ret := eOp.operation(&dataA[i], &dataB[i])
			if !ret {
				return false
			}
		} else if !mA && !mB {
			//NULL && NULL
		} else {
			return false
		}
	}
	return true
}

func checkLoop2[T any](
	vdataA, vdataB *UnifiedFormat,
	count int,
	eOp CompareOp[T],
) bool {
	dataA := getSliceInPhyFormatUnifiedFormat[T](vdataA)
	dataB := getSliceInPhyFormatUnifiedFormat[T](vdataB)
	for i := 0; i < count; i++ {
		idxA := vdataA._sel.getIndex(i)
		idxB := vdataB._sel.getIndex(i)
		mA := vdataA._mask.rowIsValid(uint64(idxA))
		mB := vdataB._mask.rowIsValid(uint64(idxB))

		if mA && mB {
			ret := eOp.operation(&dataA[idxA], &dataB[idxB])
			if !ret {
				return false
			}
		} else if !mA && !mB {
			//NULL && NULL
		} else {
			return false
		}
	}
	return true
}

func isSameChunk(
	chA, chB *Chunk,
	count int,
) bool {
	if chA.card() != chB.card() {
		return false
	}
	if chA.columnCount() != chB.columnCount() {
		return false
	}
	for i := 0; i < chA.columnCount(); i++ {
		if !isSameVector(chA._data[i], chB._data[i], count) {
			return false
		}
	}
	return true
}
