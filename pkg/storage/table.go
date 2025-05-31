package storage

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ColumnDefinition struct {
	Name        string
	Type        common.LType
	Constraints []*Constraint
}

func (colDef *ColumnDefinition) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := WriteString(colDef.Name, writer)
	if err != nil {
		return err
	}
	err = WriteField[common.LType](colDef.Type, writer)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func (colDef *ColumnDefinition) Deserialize(source util.Deserialize) error {
	reader, err := NewFieldReader(source)
	if err != nil {
		return err
	}
	colDef.Name, err = ReadString(reader)
	if err != nil {
		return err
	}
	err = ReadRequired[common.LType](&colDef.Type, reader)
	if err != nil {
		return err
	}
	reader.Finalize()
	return nil
}

type DataTableInfo struct {
	_schema      string
	_table       string
	_card        atomic.Uint64
	_data        *PersistentTableData
	_indexes     *TableIndexList
	_colDefs     []*ColumnDefinition
	_constraints []Constraint
	//loaded on read table data
	_indexesBlkPtrs []BlockPointer
}

func NewDataTableInfo() *DataTableInfo {
	ret := &DataTableInfo{
		_indexes: &TableIndexList{},
	}
	return ret
}

func NewDataTableInfo2(schema, table string) *DataTableInfo {
	ret := &DataTableInfo{
		_schema:  schema,
		_table:   table,
		_indexes: &TableIndexList{},
	}
	return ret
}

func NewDataTableInfo3(schema, table string,
	colDefs []*ColumnDefinition,
	constraints []*Constraint,
) *DataTableInfo {
	convert := func(inCons []*Constraint) []Constraint {
		conv := make([]Constraint, 0)
		for _, cons := range inCons {
			c := Constraint{}
			c = *cons
			conv = append(conv, c)
		}
		return conv
	}
	ret := &DataTableInfo{
		_schema:      schema,
		_table:       table,
		_indexes:     &TableIndexList{},
		_colDefs:     colDefs,
		_constraints: convert(constraints),
	}
	return ret
}

type TableIndexList struct {
	_indexesLock sync.Mutex
	_indexes     []*Index
}

func (list *TableIndexList) Scan(callback func(index *Index) bool) {
	list._indexesLock.Lock()
	defer list._indexesLock.Unlock()
	for _, index := range list._indexes {
		if callback(index) {
			break
		}
	}
}

func (list *TableIndexList) AddIndex(index *Index) {
	list._indexesLock.Lock()
	defer list._indexesLock.Unlock()
	list._indexes = append(list._indexes, index)
}

func (list *TableIndexList) Empty() bool {
	list._indexesLock.Lock()
	defer list._indexesLock.Unlock()
	return len(list._indexes) == 0
}

func (list *TableIndexList) Count() int {
	list._indexesLock.Lock()
	defer list._indexesLock.Unlock()
	return len(list._indexes)
}

func (list *TableIndexList) GetRequiredColumns() []IdxType {
	list._indexesLock.Lock()
	defer list._indexesLock.Unlock()
	uIdx := make(map[IdxType]bool)
	for _, index := range list._indexes {
		for _, colId := range index._columnIds {
			uIdx[colId] = true
		}
	}
	result := make([]IdxType, len(uIdx))
	for colIdx := range uIdx {
		result = append(result, colIdx)
	}
	slices.Sort(result)
	return result
}

func (list *TableIndexList) SerializeIndexes(
	writer *MetaBlockWriter) ([]BlockPointer, error) {
	blkPtrs := make([]BlockPointer, 0)
	for _, index := range list._indexes {
		blkPtr, err := index.Serialize(writer)
		if err != nil {
			return nil, err
		}
		blkPtrs = append(blkPtrs, blkPtr)
	}
	return blkPtrs, nil
}

func (list *TableIndexList) HasUniqueIndexes() bool {
	has := false
	list.Scan(func(index *Index) bool {
		if index.IsUnique() {
			has = true
			return has
		}
		return false
	})
	return has
}

func (info *DataTableInfo) comp(o *DataTableInfo) int {
	if info._schema < o._schema {
		return -1
	} else if info._schema > o._schema {
		return 1
	} else {
		if info._table < o._table {
			return -1
		} else if info._table > o._table {
			return 1
		} else {
			return 0
		}
	}
}

type DataTable struct {
	_info       *DataTableInfo
	_colDefs    []*ColumnDefinition
	_appendLock sync.Mutex
	_rowGroups  *RowGroupCollection
}

func NewDataTable(
	schema, table string,
	colDefs []*ColumnDefinition) *DataTable {
	info := NewDataTableInfo2(schema, table)

	dTable := &DataTable{
		_info:    info,
		_colDefs: colDefs,
	}

	types := make([]common.LType, 0)
	for _, colDef := range colDefs {
		types = append(types, colDef.Type)
	}
	dTable._rowGroups = NewRowGroupCollection(
		info,
		GStorageMgr._blockMgr,
		types,
		0,
		0,
	)
	dTable._rowGroups.InitializeEmpty()

	return dTable
}

func NewDataTable2(
	info *DataTableInfo,
) (*DataTable, error) {
	dTable := &DataTable{
		_info:    info,
		_colDefs: info._colDefs,
	}

	types := make([]common.LType, 0)
	for _, colDef := range info._colDefs {
		types = append(types, colDef.Type)
	}
	dTable._rowGroups = NewRowGroupCollection(
		info,
		GStorageMgr._blockMgr,
		types,
		0,
		0,
	)
	if info._data != nil && info._data._rowGroupCount > 0 {
		err := dTable._rowGroups.InitWithData(info._data)
		if err != nil {
			return nil, err
		}
	} else {
		dTable._rowGroups.InitializeEmpty()
	}

	return dTable, nil
}

func (table *DataTable) InitWithData() error {
	types := make([]common.LType, 0)
	for _, colDef := range table._colDefs {
		types = append(types, colDef.Type)
	}
	info := table._info
	table._rowGroups = NewRowGroupCollection(
		info,
		GStorageMgr._blockMgr,
		types,
		0,
		0,
	)
	if info._data != nil && info._data._rowGroupCount > 0 {
		err := table._rowGroups.InitWithData(info._data)
		if err != nil {
			return err
		}
	}
	//init indexes
	indexesIds := 0
	for _, cons := range info._constraints {
		if cons._typ == ConstraintTypeUnique {
			indexConsType := IndexConstraintTypeUnique
			if cons._isPrimaryKey {
				indexConsType = IndexConstraintTypePrimary
			}
			if len(info._indexesBlkPtrs) == 0 {
				AddDataTableIndex(table, &cons, indexConsType, nil)
			} else {
				AddDataTableIndex(table, &cons, indexConsType, &(info._indexesBlkPtrs[indexesIds]))
				indexesIds++
			}
		} else {

		}
	}

	return nil
}

func AddDataTableIndex(
	table *DataTable,
	cons *Constraint,
	indexConsType uint8,
	indexBlkPtr *BlockPointer,
) {
	colIds := make([]IdxType, 0)
	colTyps := make([]common.LType, 0)
	for _, name := range cons._uniqueNames {
		found := false
		for i2, def := range table._colDefs {
			if name == def.Name {
				colIds = append(colIds, IdxType(i2))
				colTyps = append(colTyps, def.Type)
				found = true
				break
			}
		}
		if !found {
			panic(fmt.Sprintf("no column %s in table", name))
		}
	}

	idx := NewIndex(
		IndexTypeBPlus,
		GStorageMgr._blockMgr,
		colIds,
		colTyps,
		indexConsType,
		indexBlkPtr,
	)
	table._info._indexes.AddIndex(idx)
}

func (table *DataTable) GetTypes() []common.LType {
	types := make([]common.LType, 0)
	for _, colDef := range table._colDefs {
		types = append(types, colDef.Type)
	}
	return types
}

func (table *DataTable) LocalAppend2(txn *Txn, data *chunk.Chunk) {
	state := &LocalAppendState{}
	table.InitLocalAppend(txn, state)
	err := table.LocalAppend(txn, state, data, false)
	if err != nil {
		panic(err)
	}
	table.FinalizeLocalAppend(txn, state)
}

func (table *DataTable) InitLocalAppend(
	txn *Txn,
	state *LocalAppendState) {
	txn._storage.InitializeAppend(state, table)
}

func (table *DataTable) LocalAppend(
	txn *Txn,
	state *LocalAppendState,
	data *chunk.Chunk,
	unsafe bool,
) error {
	var err error
	if data.Card() == 0 {
		return nil
	}

	if !unsafe {
		if err = table.VerifyAppendConstraints(data); err != nil {
			return err
		}
	}
	txn._storage.Append(state, data)
	return err
}

func (table *DataTable) AppendLock(state *TableAppendState) func() {
	state._appendLock = &table._appendLock
	state._appendLock.Lock()
	state._rowStart = RowType(table._rowGroups._totalRows.Load())
	state._currentRow = state._rowStart
	return func() {
		state._appendLock.Unlock()
	}
}

func (table *DataTable) MergeStorage(data *RowGroupCollection) {
	table._rowGroups.MergeStorage(data)
}

func (table *DataTable) CommitAppend(
	commitTd TxnType,
	rowStart IdxType,
	count IdxType) {
	table._appendLock.Lock()
	defer table._appendLock.Unlock()
	table._rowGroups.CommitAppend(commitTd, rowStart, count)
	table._info._card.Add(uint64(count))
}

func (table *DataTable) FinalizeLocalAppend(txn *Txn, state *LocalAppendState) {
	txn._storage.FinalizeAppend(state)
}

func (table *DataTable) InitScan(
	txn *Txn,
	state *TableScanState,
	columnIds []IdxType,
) {
	state.Init(columnIds)
	table._rowGroups.InitScan(state._tableState, columnIds)
	txn._storage.InitScan(table, state._localState)
}

func (table *DataTable) Scan(
	txn *Txn,
	result *chunk.Chunk,
	state *TableScanState,
) {
	if state._tableState.Scan(txn, result) {
		util.AssertFunc(result.Card() > 0)
		return
	}
	txn._storage.Scan(state._localState, state.GetColumnIds(), result)
}

func (table *DataTable) Fetch(
	txn *Txn,
	result *chunk.Chunk,
	colIdx []IdxType,
	rowIds *chunk.Vector,
	fetchCount IdxType,
	state *ColumnFetchState,
) {

}

func (table *DataTable) Delete(
	txn *Txn,
	rowIds *chunk.Vector,
	count IdxType,
) IdxType {
	util.AssertFunc(rowIds.Typ().GetInternalType() == common.INT64)
	if count == 0 {
		return 0
	}
	lstorage := txn._storage
	//has_delete_constraints := false
	rowIds.Flatten(int(count))
	ids := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)

	pos := IdxType(0)
	deleteCount := IdxType(0)
	for pos < count {
		start := pos
		isTxnDelete := ids[pos] >= MAX_ROW_ID
		//
		for pos++; pos < count; pos++ {
			rowIsTxnDelete := ids[pos] >= MAX_ROW_ID
			if rowIsTxnDelete != isTxnDelete {
				break
			}
		}
		currentOffset := start
		currentCount := pos - start
		offsetIds := chunk.NewFlatVector(common.BigintType(), util.DefaultVectorSize)
		offsetIds.Slice3(rowIds, uint64(currentOffset), uint64(pos))
		if isTxnDelete {
			deleteCount += lstorage.Delete(table, offsetIds, currentCount)
		} else {
			deleteCount += table._rowGroups.Delete(
				txn, table, ids[currentOffset:], currentCount)
		}
	}
	return deleteCount
}

func (table *DataTable) Update(
	txn *Txn,
	rowIds *chunk.Vector,
	colIds []IdxType,
	updates *chunk.Chunk,
) {
	util.AssertFunc(rowIds.Typ().GetInternalType() == common.INT64)
	util.AssertFunc(len(colIds) == updates.ColumnCount())
	count := updates.Card()
	if count == 0 {
		return
	}

	err := table.VerifyUpdateConstraints(updates, colIds)
	if err != nil {
		panic(err)
	}

	updates.Flatten()
	rowIds.Flatten(count)
	ids := chunk.GetSliceInPhyFormatFlat[RowType](rowIds)
	firstId := ids[0]
	if RowType(firstId) >= MAX_ROW_ID {
		txn._storage.Update(table, rowIds, colIds, updates)
		return
	}
	table._rowGroups.Update(txn, ids, colIds, updates)
}

func (table *DataTable) InitAppend(
	txn *Txn,
	state *TableAppendState,
	count IdxType) {
	if state._appendLock == nil {
		panic("appendLock should be called before InitAppend")
	}
	table._rowGroups.InitAppend(txn, state, count)
}

func (table *DataTable) Append(data *chunk.Chunk, state *TableAppendState) {
	table._rowGroups.Append(data, state)
}

func (table *DataTable) RevertAppend(
	row IdxType,
	count IdxType) {
	table._appendLock.Lock()
	defer table._appendLock.Unlock()
	var err error
	if !table._info._indexes.Empty() {
		currentRowBase := row
		rowIds := chunk.NewFlatVector(common.UbigintType(), STANDARD_VECTOR_SIZE)
		err = table.ScanTableSegment(
			row,
			count,
			func(data *chunk.Chunk) error {
				dSlice := chunk.GetSliceInPhyFormatFlat[uint64](rowIds)
				for i := 0; i < data.Card(); i++ {
					dSlice[i] = uint64(currentRowBase) + uint64(i)
				}
				table._info._indexes.Scan(func(index *Index) bool {
					err2 := index.Delete(data, rowIds)
					if err2 != nil {
						err = errors.Join(err, err2)
						return true
					}
					return false
				})
				currentRowBase += IdxType(data.Card())
				return err
			})
	}
	table.RevertAppendInternal(row, count)
}

func (table *DataTable) RevertAppendInternal(row IdxType, count IdxType) {
	if count == 0 {
		return
	}
	table._info._card.Store(uint64(row))
	table._rowGroups.RevertAppendInternal(row, count)
}

func (table *DataTable) WriteToLog(
	log *WriteAheadLog,
	rowStart IdxType,
	count IdxType) error {
	if log == nil || log._skipWriting {
		return nil
	}
	err := log.WriteSetTable(table._info._schema, table._info._table)
	if err != nil {
		return err
	}
	err = table.ScanTableSegment(rowStart, count,
		func(data *chunk.Chunk) error {
			return log.WriteInsert(data)
		})
	return err
}

func (table *DataTable) ScanTableSegment(
	rowStart IdxType,
	count IdxType,
	f func(data *chunk.Chunk) error) error {
	end := rowStart + count

	colIds := make([]IdxType, 0)
	types := make([]common.LType, 0)
	for i := 0; i < len(table._colDefs); i++ {
		col := table._colDefs[i]
		colIds = append(colIds, IdxType(i))
		types = append(types, col.Type)
	}

	data := &chunk.Chunk{}
	data.Init(types, STANDARD_VECTOR_SIZE)

	state := NewTableScanState()
	table.InitScanWithOffset(state, colIds, rowStart, rowStart+count)

	rowStartAligned := state._tableState._rowGroup.Start() +
		state._tableState._vectorIdx*STANDARD_VECTOR_SIZE

	currentRow := rowStartAligned
	for currentRow < end {
		state._tableState.ScanCommitted(data, TableScanTypeCommittedRows)
		if data.Card() == 0 {
			break
		}

		endRow := currentRow + IdxType(data.Card())
		//part of data or not
		chunkStart := max(currentRow, rowStart)
		chunkEnd := min(endRow, end)
		chunkCount := chunkEnd - chunkStart
		if chunkCount != IdxType(data.Card()) {
			startInChunk := IdxType(0)
			if currentRow >= rowStart {
				startInChunk = 0
			} else {
				startInChunk = rowStart - currentRow
			}
			sel := chunk.NewSelectVector2(int(startInChunk), int(chunkCount))
			data.SliceItself(sel, int(chunkCount))
		}
		err := f(data)
		if err != nil {
			return err
		}
		data.Reset()
		currentRow = endRow
	}
	return nil
}

func (table *DataTable) InitScanWithOffset(
	state *TableScanState,
	colIds []IdxType,
	startRow IdxType,
	endRow IdxType) {
	state.Init(colIds)
	table._rowGroups.InitScanWithOffset(
		state._tableState,
		colIds,
		startRow,
		endRow)
}

func (table *DataTable) UpdateColumn(
	txn *Txn,
	rowIds *chunk.Vector,
	colPath []IdxType,
	updates *chunk.Chunk) {
	util.AssertFunc(rowIds.Typ().GetInternalType() == common.INT64)
	util.AssertFunc(updates.ColumnCount() == 1)
	if updates.Card() == 0 {
		return
	}

	updates.Flatten()
	rowIds.Flatten(updates.Card())
	table._rowGroups.UpdateColumn(txn, rowIds, colPath, updates)
}

func (table *DataTable) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := WriteString(table._info._schema, writer)
	if err != nil {
		return err
	}
	err = WriteString(table._info._table, writer)
	if err != nil {
		return err
	}
	//write columns
	err = WriteColDefs(table._colDefs, writer)
	if err != nil {
		return err
	}
	//write constraints
	err = WriteConstraints(table._info._constraints, writer)
	if err != nil {
		return err
	}
	return writer.Finalize()
}

func (table *DataTable) Checkpoint(writer *TableDataWriter) error {
	globalStats := &TableStats{}
	table._rowGroups.CopyStats2(globalStats)
	err := table._rowGroups.Checkpoint(writer, globalStats)
	if err != nil {
		return err
	}
	return writer.FinalizeTable(globalStats, table._info)
}

func (table *DataTable) Deserialize(deserial util.Deserialize) error {
	fReader, err := NewFieldReader(deserial)
	if err != nil {
		return err
	}
	table._info._schema, err = ReadString(fReader)
	if err != nil {
		return err
	}
	table._info._table, err = ReadString(fReader)
	if err != nil {
		return err
	}

	table._colDefs, err = ReadColDefs(fReader)
	if err != nil {
		return err
	}
	//read constraints
	table._info._constraints, err = ReadConstraints(fReader)
	if err != nil {
		return err
	}

	fReader.Finalize()
	return nil
}

func (table *DataTable) AppendToIndexes(
	data *chunk.Chunk,
	rowStart uint64) error {
	return AppendToIndexes(table._info._indexes, data, rowStart)
}

func (table *DataTable) RemoveFromIndexes(
	state *TableAppendState,
	data *chunk.Chunk, rowStart RowType) error {
	if table._info._indexes.Empty() {
		return nil
	}
	rowIds := chunk.NewFlatVector(common.UbigintType(), STANDARD_VECTOR_SIZE)
	chunk.GenerateSequence(rowIds, uint64(data.Card()), uint64(rowStart), 1)
	return table.RemoveFromIndexes2(state, data, rowIds)
}

func (table *DataTable) RemoveFromIndexes2(
	state *TableAppendState,
	data *chunk.Chunk,
	rowIds *chunk.Vector) error {
	var err error
	table._info._indexes.Scan(func(index *Index) bool {
		err = index.Delete(data, rowIds)
		return err != nil
	})
	return err
}

func (table *DataTable) VerifyAppendConstraints(
	data *chunk.Chunk) error {
	if table._info._indexes.HasUniqueIndexes() {
		violated := false
		table._info._indexes.Scan(func(index *Index) bool {
			if !index.IsUnique() {
				return false
			}
			violated = index.VerifyAppend(data)
			return violated
		})
		if violated {
			return fmt.Errorf("violate unique")
		}
	}

	for _, cons := range table._info._constraints {
		if cons._typ == ConstraintTypeUnique {
			continue
		}
		if cons._typ == ConstraintTypeNotNull {
			if chunk.HasNull(data.Data[cons._notNullIndex], data.Card()) {
				return fmt.Errorf("violate not null")
			}
		}
	}
	return nil
}

func (table *DataTable) VerifyUpdateConstraints(
	updates *chunk.Chunk,
	colIds []IdxType) error {
	for _, cons := range table._info._constraints {
		if cons._typ == ConstraintTypeNotNull {
			for i2, colId := range colIds {
				if colId == IdxType(cons._notNullIndex) {
					if chunk.HasNull(updates.Data[i2], updates.Card()) {
						return fmt.Errorf("violate not null")
					}
				}
			}
		}
	}
	return nil
}

func (table *DataTable) GetStats(colIdx int) *BaseStats {
	if colIdx == -1 {
		return nil
	}
	return table._rowGroups.CopyStats(colIdx)
}

type PersistentTableData struct {
	_totalRows     IdxType
	_rowGroupCount IdxType
	_blockId       IdxType
	_offset        IdxType
	_tableStats    TableStats
}

func NewPersistentTableData() *PersistentTableData {
	return &PersistentTableData{
		_blockId: math.MaxUint64,
	}
}

type ColumnFetchState struct{}

type SegmentScanState struct {
	_handle *BufferHandle
}

type ColumnScanState struct {
	_current        *ColumnSegment
	_segmentTree    *ColumnSegmentTree
	_rowIdx         IdxType
	_internalIdx    IdxType
	_scanState      *SegmentScanState
	_childStates    []ColumnScanState
	_initialized    bool
	_segmentChecked bool
	_version        IdxType
	_previousStates []*SegmentScanState
	_lastOffset     IdxType
}

func (state *ColumnScanState) Init(typ common.LType) {
	if typ.Id == common.LTID_VALIDITY {
		return
	}
	state._childStates = make([]ColumnScanState, 1)
}

func (state *ColumnScanState) NextInternal(count IdxType) {
	if state._current == nil {
		return
	}
	state._rowIdx += count
	for state._rowIdx >=
		state._current.Start()+IdxType(state._current.Count()) {
		next := state._segmentTree.GetNextSegment(nil, state._current)
		if next == nil {
			break
		}
		state._current = next.(*ColumnSegment)
		state._initialized = false
		state._segmentChecked = false
		if state._current == nil {
			break
		}
	}
}

func (state *ColumnScanState) Next(count IdxType) {
	state.NextInternal(count)
	for _, childState := range state._childStates {
		childState.Next(count)
	}
}

type CollectionScanState struct {
	_rowGroup       *RowGroup
	_vectorIdx      IdxType
	_maxRowGroupRow IdxType
	_columnScans    []*ColumnScanState
	_rowGroups      *RowGroupSegmentTree
	_maxRow         IdxType
	_batchIdx       IdxType
	_parent         *TableScanState
}

func NewCollectionScanState(parent *TableScanState) *CollectionScanState {
	return &CollectionScanState{
		_parent: parent,
	}
}

func (state *CollectionScanState) Init(types []common.LType) {
	colIds := state.GetColumnIds()
	state._columnScans = make([]*ColumnScanState, len(colIds))
	for i := 0; i < len(colIds); i++ {
		state._columnScans[i] = &ColumnScanState{}
	}
	for i := 0; i < len(colIds); i++ {
		if colIds[i] == IdxType(COLUMN_IDENTIFIER_ROW_ID) {
			continue
		}
		state._columnScans[i].Init(types[colIds[i]])
	}
}

func (state *CollectionScanState) GetColumnIds() []IdxType {
	return state._parent.GetColumnIds()
}

func (state *CollectionScanState) Scan(
	txn *Txn,
	result *chunk.Chunk) bool {
	for state._rowGroup != nil {
		state._rowGroup.Scan(txn, state, result)
		if result.Card() > 0 {
			return true
		} else if state._maxRow <=
			state._rowGroup.Start()+IdxType(state._rowGroup.Count()) {
			state._rowGroup = nil
			return false
		} else {
			fun := func() {
				for {
					state._rowGroup =
						state._rowGroups.GetNextSegment(nil, state._rowGroup).(*RowGroup)
					if state._rowGroup != nil {
						if state._rowGroup.Start() >= state._maxRow {
							state._rowGroup = nil
							break
						}
						scanRg := state._rowGroup.InitScan(state)
						if scanRg {
							break
						}
					} else {
						break
					}
				}
			}
			fun()
		}
	}
	return false
}

func (state *CollectionScanState) ScanCommitted(
	result *chunk.Chunk,
	typ TableScanType) bool {
	for state._rowGroup != nil {
		state._rowGroup.ScanCommitted(state, result, typ)
		if result.Card() > 0 {
			return true
		} else {
			next := state._rowGroups.GetNextSegment(nil, state._rowGroup)
			if next == nil {
				state._rowGroup = nil
				return false
			} else {
				state._rowGroup = next.(*RowGroup)
				state._rowGroup.InitScan(state)
			}
		}
	}
	return false
}

type TableScanState struct {
	_tableState *CollectionScanState
	_localState *CollectionScanState
	_columnIds  []IdxType
}

func NewTableScanState() *TableScanState {
	ret := &TableScanState{}
	ret._tableState = NewCollectionScanState(ret)
	ret._localState = NewCollectionScanState(ret)

	return ret
}

func (state *TableScanState) Init(ids []IdxType) {
	state._columnIds = ids
}

func (state *TableScanState) GetColumnIds() []IdxType {
	return state._columnIds
}

func ReadTable(table *DataTable, txn *Txn, limit int, callback func(result *chunk.Chunk)) int {
	scanState := NewTableScanState()
	colIdx := make([]IdxType, 1+len(table._colDefs))
	colIdx[0] = COLUMN_IDENTIFIER_ROW_ID
	for i := 0; i < len(table._colDefs); i++ {
		colIdx[i+1] = IdxType(i)
	}
	table.InitScan(txn, scanState, colIdx)
	tCount := 0
	colTyps := make([]common.LType, 0)
	colTyps = append(colTyps, common.BigintType())
	colTyps = append(colTyps, table.GetTypes()...)
	for {
		result := &chunk.Chunk{}
		result.Init(colTyps, STANDARD_VECTOR_SIZE)
		table.Scan(txn, result, scanState)
		if result.Card() == 0 {
			break
		}
		tCount += result.Card()

		if callback != nil {
			callback(result)
		}
	}
	return tCount
}
