package main

import (
	"fmt"
	"strings"
	"unsafe"
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
	defaultVectorSize = 5
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
	vec := NewVector(lTyp, 1)
	vec.setPhyFormat(PF_CONST)
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
		(vec.phyFormat().isConst() || vec.phyFormat().isConst()) {
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
			_str: data[idx]._data,
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
		slice[idx] = String{_data: val._str}
	case INTERVAL:
		slice := toSlice[Interval](vec._data, pTyp.size())
		interVal := Interval{}
		switch strings.ToLower(val._str) {
		case "year":
			interVal._year = int32(val._i64)
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
	default:
		panic("usp")
	}
}

func toSlice[T any](data []byte, pSize int) []T {
	slen := len(data) / pSize
	return unsafe.Slice((*T)(unsafe.Pointer(&data[0])), slen)
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

func isNullInPhyFormatFlat(vec *Vector, idx uint64) bool {
	assertFunc(vec.phyFormat().isFlat())
	return !vec._mask.rowIsValid(idx)
}

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
	return toSlice[T](uni._data, 1)
}

type Bitmap struct {
	_bits []uint8
}

func (bm *Bitmap) init(count int) {
	cnt := entryCount(count)
	bm._bits = gAlloc.Alloc(cnt)
	for i, _ := range bm._bits {
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

type Chunk struct {
	_data  []*Vector
	_count int
	_cap   int
}

func (c *Chunk) init(types []LType, cap int) {
	c._cap = cap
	for _, lType := range types {
		c._data = append(c._data, NewVector(lType, c._cap))
	}
}

func (c *Chunk) reset() {
	if len(c._data) == 0 {
		return
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
	assertFunc(c._count == 0)
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
			fmt.Print(" ")
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
	switch val._typ.id {
	case LTID_INTEGER:
		return fmt.Sprintf("%d", val._i64)
	case LTID_BOOLEAN:
		return fmt.Sprintf("%v", val._bool)
	case LTID_VARCHAR:
		return fmt.Sprintf("%v", val._str)
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

func And(left, right, result *Vector, count int) {

}
