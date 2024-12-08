package storage

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"unsafe"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/tidwall/btree"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/encode"
	"github.com/daviszhen/plan/pkg/util"
)

const (
	IndexTypeInvalid uint8 = 0
	IndexTypeArt     uint8 = 1
	IndexTypeBPlus   uint8 = 2
)

const (
	IndexConstraintTypeNone    uint8 = 0
	IndexConstraintTypeUnique  uint8 = 1
	IndexConstraintTypePrimary uint8 = 2
	IndexConstraintTypeForeign uint8 = 3
)

const (
	ExprTypeInvalid              uint8 = 0
	ExprTypeEqual                uint8 = 1
	ExprTypeGreaterThanOrEqualTo uint8 = 2
	ExprTypeGreaterThan          uint8 = 3
	ExprTypeLessThanOrEqualTo    uint8 = 4
	ExprTypeLessThan             uint8 = 5
)

type IndexLock struct {
	_indexLock sync.Locker
}

type IndexScanState struct {
	_typ      uint8
	_values   [2]*chunk.Value
	_exprTyps [2]uint8
}

type Index struct {
	_typ                   uint8
	_blockMgr              BlockMgr
	_columnIds             []IdxType
	_columnIdSet           map[IdxType]bool
	_unboundExprs          []*pg_query.Node
	_types                 []common.PhyType
	_logicalTypes          []common.LType
	_constraintType        uint8
	_lock                  sync.Mutex
	_serializedDataPointer BlockPointer
	//_boundExprs []*common.Expr
	//_executor
	_btree   *btree.BTreeG[*IndexKey]
	_blockId BlockID
	_offset  uint32
}

type IndexKey struct {
	_data unsafe.Pointer
	_len  uint32
	_val  uint64
}

func (k *IndexKey) Empty() bool {
	return k._len == 0
}

func (k *IndexKey) ConcatenateKey(okey *IndexKey) {
	oldData := k._data
	tData := util.CMalloc(int(k._len + okey._len))
	defer util.CFree(oldData)
	util.PointerCopy(tData, k._data, int(k._len))
	util.PointerCopy(
		util.PointerAdd(tData, int(k._len)),
		okey._data,
		int(okey._len),
	)
	k._len += okey._len
	k._data = tData
}

func IndexKeyLess(a, b *IndexKey) bool {
	if a._len == 0 && b._len == 0 {
		return false
	} else if a._len == 0 {
		return false
	} else if b._len == 0 {
		return true
	}
	return util.PointerMemcmp2(a._data, b._data, int(a._len), int(b._len)) < 0
}

func NewIndex(
	typ uint8,
	blockMgr BlockMgr,
	columnIds []IdxType,
	lTyps []common.LType,
	constraintTyp uint8,
	blkPtr *BlockPointer,
) *Index {
	util.AssertFunc(typ == IndexTypeBPlus)
	ret := &Index{
		_typ:            typ,
		_blockMgr:       blockMgr,
		_columnIds:      columnIds,
		_logicalTypes:   lTyps,
		_constraintType: constraintTyp,
		_columnIdSet:    make(map[IdxType]bool),
		_btree:          btree.NewBTreeG[*IndexKey](IndexKeyLess),
	}

	for _, id := range columnIds {
		ret._columnIdSet[id] = true
	}

	for _, lTyp := range ret._logicalTypes {
		ret._types = append(ret._types, lTyp.GetInternalType())
	}

	if blkPtr != nil && blkPtr._blockId != -1 {
		ret._blockId = blkPtr._blockId
		ret._offset = blkPtr._offset
		//deserialize the index
		err := ret.Deserialize()
		if err != nil {
			panic(err)
		}
	}

	return ret
}

func (idx *Index) InitializeScanSinglePredicate(
	txn *Txn,
	value *chunk.Value,
	exprTyp uint8,
) *IndexScanState {
	ret := &IndexScanState{}
	ret._values[0] = value
	ret._exprTyps[0] = exprTyp
	return ret
}

func (idx *Index) InitializeScanTwoPredicates(
	txn *Txn,
	lowValue *chunk.Value,
	lowExprTyp uint8,
	highValue *chunk.Value,
	highExprTyp uint8,
) *IndexScanState {
	ret := &IndexScanState{}
	ret._values[0] = lowValue
	ret._values[1] = highValue
	ret._exprTyps[0] = lowExprTyp
	ret._exprTyps[1] = highExprTyp
	return ret
}

func (idx *Index) Scan(
	txn *Txn,
	table *DataTable,
	state *IndexScanState,
	maxCount int,
	resultIds *[]uint64,
) bool {
	var rowIds []uint64
	var success bool
	util.AssertFunc(state._values[0].Typ.GetInternalType() == idx._types[0])
	key := CreateKey(idx._types[0], state._values[0])

	if state._values[1].IsNull {
		//single predicate
		f := func() {
			idx._lock.Lock()
			defer idx._lock.Unlock()
			switch state._exprTyps[0] {
			case ExprTypeEqual: //=
				success = idx.SearchEqual(key, maxCount, &rowIds)
			case ExprTypeGreaterThanOrEqualTo: //>=
				success = idx.SearchGreater(state, key, true, maxCount, &rowIds)
			case ExprTypeGreaterThan: //>
				success = idx.SearchGreater(state, key, false, maxCount, &rowIds)
			case ExprTypeLessThanOrEqualTo: //<=
				success = idx.SearchLess(state, key, true, maxCount, &rowIds)
			case ExprTypeLessThan: //<
				success = idx.SearchLess(state, key, false, maxCount, &rowIds)
			default:
				panic("usp")
			}
		}
		f()
	} else {
		//two predicates
		f := func() {
			idx._lock.Lock()
			defer idx._lock.Unlock()
			util.AssertFunc(state._values[1].Typ.GetInternalType() == idx._types[0])
			upKey := CreateKey(idx._types[0], state._values[1])
			leftInclusive := state._exprTyps[0] == ExprTypeGreaterThanOrEqualTo
			rightInclusive := state._exprTyps[1] == ExprTypeLessThanOrEqualTo
			success = idx.SearchCloseRange(
				state,
				key,
				upKey,
				leftInclusive,
				rightInclusive,
				maxCount,
				&rowIds,
			)
		}
		f()
	}

	if !success {
		return false
	}

	if len(rowIds) == 0 {
		return true
	}

	slices.Sort(rowIds)

	*resultIds = append(*resultIds, rowIds[0])
	for i := 1; i < len(rowIds); i++ {
		if rowIds[i] != rowIds[i-1] {
			*resultIds = append(*resultIds, rowIds[i])
		}
	}

	return true
}

func CreateKey(
	pType common.PhyType,
	value *chunk.Value) *IndexKey {
	util.AssertFunc(pType == value.Typ.GetInternalType())
	switch pType {
	case common.INT32:
		val32 := int32(value.I64)
		return CreateIndexKey2[int32](value.Typ, &val32, encode.Int32Encoder{})
	default:
		panic("usp")
	}
}

func (idx *Index) Append(
	entries *chunk.Chunk,
	rowIds *chunk.Vector,
) error {
	state := &IndexLock{}
	idx.InitLock(state)
	defer state._indexLock.Unlock()
	return idx.Append2(state, entries, rowIds)
}

func (idx *Index) InitLock(state *IndexLock) {
	state._indexLock = &idx._lock
	state._indexLock.Lock()
}

func (idx *Index) Append2(
	lock *IndexLock,
	entries *chunk.Chunk,
	rowIds *chunk.Vector) error {
	//TODO:execute exprs
	temp := &chunk.Chunk{}
	temp.Init(idx._logicalTypes, STANDARD_VECTOR_SIZE)
	for i, colIdx := range idx._columnIds {
		temp.Data[i].Reference(entries.Data[colIdx])
	}
	temp.SetCard(entries.Card())
	return idx.Insert(lock, temp, rowIds)
}

func (idx *Index) Insert(
	lock *IndexLock,
	input *chunk.Chunk,
	rowIds *chunk.Vector) error {
	util.AssertFunc(rowIds.Typ().GetInternalType() == common.UINT64)
	util.AssertFunc(
		idx._logicalTypes[0] == input.Data[0].Typ(),
	)
	//one key for one row
	keys := make([]*IndexKey, input.Card())
	for i := 0; i < input.Card(); i++ {
		keys[i] = &IndexKey{}
	}
	idx.GenerateKeys(input, keys)

	rowIds.Flatten(input.Card())
	rowIdsSlice := chunk.GetSliceInPhyFormatFlat[uint64](rowIds)

	failedIndex := -1
	for i := 0; i < input.Card(); i++ {
		if keys[i].Empty() {
			continue
		}

		rowId := rowIdsSlice[i]
		keys[i]._val = rowId
		_, has := idx._btree.Set(keys[i])
		if has {
			failedIndex = i
			break
		}
	}

	//delete already inserted rows
	if failedIndex != -1 {
		for i := 0; i < failedIndex; i++ {
			if keys[i].Empty() {
				continue
			}

			//rowId := rowIdsSlice[i]
			idx._btree.Delete(keys[i])
		}
		return fmt.Errorf("duplicate key")
	}

	return nil
}

func (idx *Index) GenerateKeys(
	input *chunk.Chunk,
	keys []*IndexKey,
) {
	switch input.Data[0].Typ().GetInternalType() {
	case common.INT32:
		TemplatedGenerateKeys[int32](
			input.Data[0],
			input.Card(),
			keys, encode.Int32Encoder{})
	case common.INT64:
		TemplatedGenerateKeys[int64](
			input.Data[0],
			input.Card(),
			keys, encode.Int64Encoder{})
	case common.UINT64:
		TemplatedGenerateKeys[uint64](
			input.Data[0],
			input.Card(),
			keys, encode.Uint64Encoder{})
	case common.VARCHAR:
		GenerateStringKeys(
			input.Data[0],
			input.Card(),
			keys,
		)
	default:
		panic("usp")
	}

	for i := 1; i < input.ColumnCount(); i++ {
		switch input.Data[i].Typ().GetInternalType() {
		case common.INT32:
			ConcatenateKeys[int32](
				input.Data[i],
				input.Card(),
				keys, encode.Int32Encoder{})
		case common.UINT64:
			ConcatenateKeys[uint64](
				input.Data[i],
				input.Card(),
				keys, encode.Uint64Encoder{})
		case common.VARCHAR:
			ConcatenateStringKeys(
				input.Data[i],
				input.Card(),
				keys,
			)
		default:
			panic("usp")
		}
	}
}

func GenerateStringKeys(
	input *chunk.Vector,
	count int,
	keys []*IndexKey,
) {
	var idata chunk.UnifiedFormat
	input.ToUnifiedFormat(count, &idata)

	util.AssertFunc(len(keys) >= count)
	inputData := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](&idata)
	for i := 0; i < count; i++ {
		idx := idata.Sel.GetIndex(i)
		if idata.Mask.RowIsValid(uint64(idx)) {
			CreateStringIndexKey(
				input.Typ(),
				keys[i],
				&inputData[idx])
		} else {
			keys[i] = &IndexKey{}
		}
	}
}

func TemplatedGenerateKeys[T any](
	input *chunk.Vector,
	count int,
	keys []*IndexKey,
	enc encode.Encoder[T],
) {
	var idata chunk.UnifiedFormat
	input.ToUnifiedFormat(count, &idata)

	util.AssertFunc(len(keys) >= count)
	inputData := chunk.GetSliceInPhyFormatUnifiedFormat[T](&idata)
	for i := 0; i < count; i++ {
		idx := idata.Sel.GetIndex(i)
		if idata.Mask.RowIsValid(uint64(idx)) {
			CreateIndexKey(
				input.Typ(),
				keys[i],
				&inputData[idx],
				enc)
		} else {
			keys[i] = &IndexKey{}
		}
	}
}

func CreateStringIndexKey(
	typ common.LType,
	key *IndexKey,
	value *common.String,
) {
	l := value.Length() + 1
	dst := util.CMalloc(l)
	util.PointerCopy(dst, value.DataPtr(), l-1)

	data := util.PointerToSlice[byte](dst, l)
	if typ.Id == common.LTID_VARCHAR {
		for i := 0; i < l-1; i++ {
			if data[i] == 0 {
				panic("index key has null-terminated bytes")
			}
		}
	}

	data[l-1] = 0
	key._data = dst
	key._len = uint32(l)
}

func CreateIndexKey[T any](
	typ common.LType,
	key *IndexKey,
	val *T,
	enc encode.Encoder[T],
) {
	key._data = CreateData[T](typ, val, enc)
	key._len = uint32(typ.GetInternalType().Size())
}

func CreateIndexKey2[T any](
	typ common.LType,
	val *T,
	enc encode.Encoder[T],
) *IndexKey {
	key := &IndexKey{}
	key._data = CreateData[T](typ, val, enc)
	key._len = uint32(typ.GetInternalType().Size())
	return key
}

func CreateData[T any](
	typ common.LType,
	val *T,
	enc encode.Encoder[T],
) unsafe.Pointer {
	pSize := typ.GetInternalType().Size()
	data := util.CMalloc(pSize)
	enc.EncodeData(data, val)
	return data
}

func ConcatenateKeys[T any](
	input *chunk.Vector,
	count int,
	keys []*IndexKey,
	enc encode.Encoder[T],
) {
	var idata chunk.UnifiedFormat
	input.ToUnifiedFormat(count, &idata)

	inputData := chunk.GetSliceInPhyFormatUnifiedFormat[T](&idata)
	for i := 0; i < count; i++ {
		idx := idata.Sel.GetIndex(i)

		//no previous column is NULL
		if !keys[i].Empty() {
			if idata.Mask.RowIsValid(uint64(idx)) {
				okey := CreateIndexKey2[T](
					input.Typ(),
					&inputData[idx],
					enc,
				)
				keys[i].ConcatenateKey(okey)
			} else {
				//set whole key is NULL
				keys[i] = &IndexKey{}
			}
		}
	}
}

func ConcatenateStringKeys(
	input *chunk.Vector,
	count int,
	keys []*IndexKey,
) {
	var idata chunk.UnifiedFormat
	input.ToUnifiedFormat(count, &idata)

	inputData := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](&idata)
	for i := 0; i < count; i++ {
		idx := idata.Sel.GetIndex(i)

		//no previous column is NULL
		if !keys[i].Empty() {
			if idata.Mask.RowIsValid(uint64(idx)) {
				nKey := &IndexKey{}
				CreateStringIndexKey(
					input.Typ(),
					nKey,
					&inputData[idx])
				keys[i].ConcatenateKey(nKey)
			} else {
				//set whole key is NULL
				keys[i] = &IndexKey{}
			}
		}
	}
}

func (idx *Index) Delete(
	entries *chunk.Chunk,
	rowIds *chunk.Vector,
) error {
	state := &IndexLock{}
	idx.InitLock(state)
	defer state._indexLock.Unlock()
	return idx.Delete2(
		state,
		entries,
		rowIds,
	)
}

func (idx *Index) Delete2(
	state *IndexLock,
	input *chunk.Chunk,
	rowIds *chunk.Vector) error {
	//TODO:execute exprs
	//one key for one row
	keys := make([]*IndexKey, input.Card())
	for i := 0; i < input.Card(); i++ {
		keys[i] = &IndexKey{}
	}
	idx.GenerateKeys(input, keys)

	//rowIds.Flatten(input.Card())
	//rowIdsSlice := chunk.GetSliceInPhyFormatFlat[uint64](rowIds)

	for i := 0; i < input.Card(); i++ {
		if keys[i].Empty() {
			continue
		}
		idx._btree.Delete(keys[i])
	}
	return nil
}

func (idx *Index) Vacuum() {
	state := &IndexLock{}
	idx.InitLock(state)
	defer state._indexLock.Unlock()
	//do nothing
}

func (idx *Index) IsUnique() bool {
	return idx._constraintType == IndexConstraintTypeUnique ||
		idx._constraintType == IndexConstraintTypePrimary
}

func (idx *Index) IsPrimary() bool {
	return idx._constraintType == IndexConstraintTypePrimary
}

func (idx *Index) IsForeign() bool {
	return idx._constraintType == IndexConstraintTypeForeign
}

func (idx *Index) SearchEqual(
	key *IndexKey,
	maxCount int,
	resultIds *[]uint64) bool {
	item, has := idx._btree.Get(key)
	if has {
		*resultIds = append(*resultIds, item._val)
		return false
	}
	return true
}

func (idx *Index) SearchGreater(
	state *IndexScanState,
	key *IndexKey,
	inclusive bool,
	maxCount int,
	resultIds *[]uint64) bool {
	cnt := 0
	idx._btree.Ascend(key, func(item *IndexKey) bool {
		if cnt >= maxCount {
			return false
		}
		if inclusive && cnt == 0 {
			//equal
			if !IndexKeyLess(key, item) &&
				!IndexKeyLess(item, key) {
				*resultIds = append(*resultIds, item._val)
				inclusive = false
				cnt++
				return true
			}
		}
		*resultIds = append(*resultIds, item._val)
		cnt++
		return true
	})
	if cnt < maxCount {
		return true
	}
	return false
}

func (idx *Index) SearchLess(
	state *IndexScanState,
	key *IndexKey,
	inclusive bool,
	maxCount int,
	resultIds *[]uint64) bool {
	cnt := 0
	idx._btree.Descend(key, func(item *IndexKey) bool {
		if cnt >= maxCount {
			return false
		}
		if inclusive && cnt == 0 {
			//equal
			if !IndexKeyLess(key, item) &&
				!IndexKeyLess(item, key) {
				*resultIds = append(*resultIds, item._val)
				inclusive = false
				cnt++
				return true
			}
		}
		*resultIds = append(*resultIds, item._val)
		cnt++
		return true
	})
	//reverse result ids
	slices.Reverse(*resultIds)
	if cnt < maxCount {
		return true
	}
	return false
}

func (idx *Index) SearchCloseRange(
	state *IndexScanState,
	key *IndexKey,
	upKey *IndexKey,
	leftInclusive bool,
	rightInclusive bool,
	maxCount int,
	resultIds *[]uint64) bool {
	cnt := 0
	idx._btree.Ascend(key, func(item *IndexKey) bool {
		if cnt >= maxCount {
			return false
		}
		if leftInclusive && cnt == 0 {
			//equal
			if !IndexKeyLess(key, item) &&
				!IndexKeyLess(item, key) {
				*resultIds = append(*resultIds, item._val)
				leftInclusive = false
				cnt++
				return true
			}
		}
		//item >= upkey
		if !IndexKeyLess(item, upKey) {
			if rightInclusive {
				if !IndexKeyLess(upKey, item) {
					*resultIds = append(*resultIds, item._val)
					rightInclusive = false
					cnt++
				}
			}
			return false
		}

		*resultIds = append(*resultIds, item._val)
		cnt++
		return true
	})
	if cnt < maxCount {
		return true
	}
	return false
}

func AppendToIndexes(
	indexes *TableIndexList,
	data *chunk.Chunk,
	rowStart uint64,
) error {
	if indexes.Empty() {
		return nil
	}
	var err error

	rowIds := chunk.NewFlatVector(common.UbigintType(), STANDARD_VECTOR_SIZE)
	chunk.GenerateSequence(rowIds, uint64(data.Card()), rowStart, 1)

	var alreadyAppended []*Index
	appendFailed := false
	indexes.Scan(func(index *Index) bool {
		err = index.Append(data, rowIds)
		if err != nil {
			appendFailed = true
			return true
		}
		alreadyAppended = append(alreadyAppended, index)
		return false
	})

	if appendFailed {
		var err2 error
		//constraint violation
		//remove append data from indexes
		for _, index := range alreadyAppended {
			err2 = index.Delete(data, rowIds)
			if err2 != nil {
				err = errors.Join(err, err2)
				break
			}
		}
	}

	return err
}

func (idx *Index) Serialize(writer *MetaBlockWriter) (BlockPointer, error) {
	blkPtr := writer.GetBlockPointer()
	cnt := idx._btree.Len()
	//write total count of items
	err := util.Write[uint32](uint32(cnt), writer)
	if err != nil {
		return BlockPointer{}, err
	}
	//
	idx._btree.Scan(func(item *IndexKey) bool {
		//write value first
		err = util.Write[uint64](item._val, writer)
		if err != nil {
			return false
		}
		//write data
		err = util.WritePtrBytes(item._data, item._len, writer)
		if err != nil {
			return false
		}
		return true
	})
	return blkPtr, nil
}

func (idx *Index) Deserialize() error {
	reader, err := NewMetaBlockReader(
		GStorageMgr._blockMgr,
		idx._blockId,
		true,
	)
	if err != nil {
		return err
	}
	defer reader.Close()
	reader._offset = uint64(idx._offset)
	var cnt uint32
	err = util.Read[uint32](&cnt, reader)
	if err != nil {
		return err
	}
	for i := uint32(0); i < cnt; i++ {
		//read value
		item := &IndexKey{}
		err = util.Read[uint64](&item._val, reader)
		if err != nil {
			return err
		}
		//read data
		item._data, item._len, err = util.ReadPtrBytes(reader)
		if err != nil {
			return err
		}
		_, has := idx._btree.Set(item)
		if has {
			return fmt.Errorf("duplicate key after deserialized")
		}
	}
	return err
}

func (idx *Index) VerifyAppend(input *chunk.Chunk) bool {
	idx._lock.Lock()
	defer idx._lock.Unlock()
	//one key for one row
	keys := make([]*IndexKey, input.Card())
	for i := 0; i < input.Card(); i++ {
		keys[i] = &IndexKey{}
	}
	//reference real columns
	temp := &chunk.Chunk{}
	temp.Init(idx._logicalTypes, STANDARD_VECTOR_SIZE)
	for i, colIdx := range idx._columnIds {
		temp.Data[i].Reference(input.Data[colIdx])
	}
	temp.SetCard(input.Card())
	idx.GenerateKeys(temp, keys)

	for _, key := range keys {
		_, has := idx._btree.Get(key)
		if has { //duplicate
			return true
		}
	}
	return false
}
