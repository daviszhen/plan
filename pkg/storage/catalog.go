package storage

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type Catalog struct {
	_writeLock sync.Mutex
	_dependMgr *DependMgr
	_schemas   *CatalogSet
}

type CatalogEntryLookup struct {
	_schema *CatalogEntry
	_entry  *CatalogEntry
}

func (lookup *CatalogEntryLookup) Found() bool {
	return lookup._entry != nil
}

func NewCatalog() *Catalog {
	cat := &Catalog{}
	cat._dependMgr = NewDependMgr(cat)
	cat._schemas = NewCatalogSet(cat)
	return cat
}

func (cat *Catalog) Init() error {
	txn, err := GTxnMgr.NewTxn("create schema internal")
	if err != nil {
		return err
	}
	BeginQuery(txn)
	defer func() {
		if err != nil {
			GTxnMgr.Rollback(txn)
		} else {
			err2 := GTxnMgr.Commit(txn)
			if err2 != nil {
				err = errors.Join(err, err2)
			}
		}
	}()
	err = cat.createSchemaInternal(txn, []string{"public"})
	return err
}

func (cat *Catalog) CreateSchema(txn *Txn, schema string) (*CatalogEntry, error) {
	ent := NewSchemaEntry(cat, schema)
	list := NewDependList()
	ret, err := cat._schemas.CreateEntry(txn, schema, ent, list)
	if err != nil {
		return nil, err
	}
	if !ret {
		return nil, nil
	}
	return ent, nil
}

func (cat *Catalog) CreateTable(txn *Txn,
	info *DataTableInfo,
) (*CatalogEntry, error) {
	schEnt := cat.GetSchema(txn, info._schema)
	if schEnt == nil {
		return nil, fmt.Errorf("no schema %s", info._schema)
	}
	return schEnt.CreateTable(txn, info)
}

func (cat *Catalog) GetSchema(txn *Txn, schema string) *CatalogEntry {
	ent := cat._schemas.GetEntry(txn, schema)
	return ent
}

func (cat *Catalog) ScanSchemas(fun func(ent *CatalogEntry)) {
	cat._schemas.Scan(fun)
}

func (cat *Catalog) createSchemaInternal(txn *Txn, schemas []string) error {
	var err error
	for _, schema := range schemas {
		_, err = cat.CreateSchema(txn, schema)
		if err != nil {
			return err
		}
	}
	return err
}

func (cat *Catalog) CreateSchema2(txn *Txn, schema string) error {
	return cat.createSchemaInternal(txn, []string{schema})
}

func (cat *Catalog) GetEntry(
	txn *Txn,
	typ uint8,
	schema string, name string) *CatalogEntry {
	ret := cat.LookupEntry(txn, typ, schema, name)
	return ret._entry
}

func (cat *Catalog) LookupEntry(
	txn *Txn,
	typ uint8,
	schema string,
	name string) *CatalogEntryLookup {
	result := cat.LookupEntryInternal(txn, typ, schema, name)
	if result.Found() {
		return result
	}
	return result
}

func (cat *Catalog) LookupEntryInternal(
	txn *Txn,
	typ uint8,
	schema string,
	name string) *CatalogEntryLookup {
	schEnt := cat.GetSchema(txn, schema)
	if schEnt == nil {
		return &CatalogEntryLookup{}
	}

	ent := schEnt.GetEntry(txn, typ, name)
	if ent == nil {
		return &CatalogEntryLookup{
			_schema: schEnt,
		}
	}
	return &CatalogEntryLookup{
		_schema: schEnt,
		_entry:  ent,
	}
}

type CatalogSet struct {
	_catalog      *Catalog
	_catalogLock  sync.Mutex
	_entries      map[IdxType]*EntryValue
	_mapping      map[string]*MappingValue
	_currentEntry IdxType
}

func NewCatalogSet(catalog *Catalog) *CatalogSet {
	set := &CatalogSet{
		_catalog: catalog,
		_entries: make(map[IdxType]*EntryValue),
		_mapping: make(map[string]*MappingValue),
	}

	return set
}

func (set *CatalogSet) CreateEntry(
	txn *Txn,
	name string,
	value *CatalogEntry,
	list *DependList,
) (bool, error) {
	set._catalog._writeLock.Lock()
	defer set._catalog._writeLock.Unlock()

	set._catalogLock.Lock()
	defer set._catalogLock.Unlock()
	var index IdxType
	mapping := set.GetMapping(txn, name, false)
	if mapping == nil || mapping._deleted {
		//create dummy node
		dummy := &CatalogEntry{
			_typ:     CatalogTypeInvalid,
			_parent:  value._parent,
			_catalog: value._catalog,
			_name:    name,
			_deleted: true,
			_set:     set,
		}
		entIdx := set.PutEntry(set._currentEntry, dummy)
		set._currentEntry++
		index = entIdx._index
		set.PutMapping(txn, name, entIdx)
	} else {
		index = mapping._index._index
		curEnt := mapping._index.GetEntry()
		if set.HasConflict(txn, TxnType(curEnt._timestamp.Load())) {
			panic("write-write conflicts on " + curEnt._name)
		}
		if !curEnt._deleted {
			return false, nil
		}
	}
	value._timestamp.Store(uint64(txn._id))
	value._set = set

	err := set._catalog._dependMgr.AddObject(txn, value, list)
	if err != nil {
		return false, err
	}

	entIdx := NewEntryIndex(set, index)
	set.PutEntry2(entIdx, value)

	//put old entry to the undo buffer
	txn.PushCatalogEntry(value._child)
	return true, nil
}

func (set *CatalogSet) GetEntryInternal(
	txn *Txn,
	name string,
	entIdex *EntryIndex) *CatalogEntry {
	maping := set.GetMapping(txn, name, false)
	if maping == nil || maping._deleted {
		return nil
	}
	if entIdex != nil {
		*entIdex = maping._index.Copy()
	}
	return set.GetEntryInternal2(txn, maping._index)
}

func (set *CatalogSet) GetMapping(
	txn *Txn,
	name string, getLatest bool) *MappingValue {
	if ent, has := set._mapping[name]; has {
		if getLatest {
			return ent
		}
		util.AssertFunc(ent != nil)
		//visit
		for ent._child != nil {
			if set.UseTimestamp(txn, ent._timestamp) {
				break
			}
			ent = ent._child
		}
		return ent
	} else {
		return nil
	}

}

func (set *CatalogSet) UseTimestamp(
	txn *Txn,
	ts TxnType) bool {
	if txn._id == ts {
		//i created
		return true
	}
	if ts < txn._startTime {
		return true
	}
	return false
}

func (set *CatalogSet) GetEntryInternal2(
	txn *Txn,
	entIdx EntryIndex) *CatalogEntry {
	ent := entIdx.GetEntry()
	if set.HasConflict(txn, TxnType(ent._timestamp.Load())) {
		panic("write-write conflict on " + ent._name)
	}
	if ent._deleted {
		return nil
	}
	return ent
}

func (set *CatalogSet) HasConflict(txn *Txn, ts TxnType) bool {
	return ts >= TxnType(TRANSACTION_ID_START) && ts != txn._id ||
		ts < TxnType(TRANSACTION_ID_START) && ts > txn._startTime
}

func (set *CatalogSet) PutEntry(
	entIdx IdxType,
	ent *CatalogEntry) EntryIndex {
	if _, has := set._entries[entIdx]; has {
		panic("entry index " + fmt.Sprint(entIdx) + " already exists")
	}
	set._entries[entIdx] = &EntryValue{
		_entry: ent,
	}
	return NewEntryIndex(set, entIdx)
}

func (set *CatalogSet) PutMapping(
	txn *Txn,
	name string,
	entIdx EntryIndex) {
	newVal := &MappingValue{
		_index: entIdx,
	}
	newVal._timestamp = txn._id
	if val, has := set._mapping[name]; has {
		if set.HasConflict(txn, val._timestamp) {
			panic("write-write conflicts on " + name)
		}
		newVal._child = val
		newVal._child._parent = newVal
	}
	set._mapping[name] = newVal
}

func (set *CatalogSet) PutEntry2(
	entIdx EntryIndex,
	ent *CatalogEntry) {
	if oldEnt, has := set._entries[entIdx._index]; !has {
		panic("entry index " + fmt.Sprint(entIdx._index) + " does not exists")
	} else {
		ent._child = oldEnt._entry
		ent._child._parent = ent
		oldEnt._entry = ent
	}
}

func (set *CatalogSet) UpdateTimestamp(
	ent *CatalogEntry,
	ts TxnType) {
	ent._timestamp.Store(uint64(ts))
	set._mapping[ent._name]._timestamp = ts
}

func (set *CatalogSet) Undo(ent *CatalogEntry) {
	set._catalog._writeLock.Lock()
	defer set._catalog._writeLock.Unlock()

	set._catalogLock.Lock()
	defer set._catalogLock.Unlock()

	//restore entry
	//remove parent
	toBeRemovedNode := ent._parent
	set.AdjustTableDependencies(ent)
	if !toBeRemovedNode._deleted {
		depMgr := set._catalog._dependMgr
		depMgr.EraseObject(toBeRemovedNode)
	}
	if ent._name != toBeRemovedNode._name {
		panic("usp : rename")
	}

	if toBeRemovedNode._parent != nil {
		//reconnect its parent with ent
		toBeRemovedNode._parent._child = toBeRemovedNode._child
		ent._parent = toBeRemovedNode._parent
	} else {
		//update base entry tables
		name := ent._name
		toBeRemovedNode._child.SetAsRoot()
		set._mapping[name]._index.Replace(toBeRemovedNode._child)
		ent._parent = nil
	}

	//restore the name
	restoreEnt := set._mapping[ent._name]
	if restoreEnt._deleted ||
		ent._typ == CatalogTypeInvalid {
		if restoreEnt._child != nil {
			restoreEnt._child._parent = nil
			set._mapping[ent._name] = restoreEnt._child
		} else {
			delete(set._mapping, ent._name)
		}
	}
}

func (set *CatalogSet) AdjustTableDependencies(ent *CatalogEntry) {
	if ent._typ == CatalogTypeTable &&
		ent._parent._typ == CatalogTypeTable {
		panic("usp")
	}
}

func (set *CatalogSet) GetEntry(txn *Txn, name string) *CatalogEntry {
	set._catalogLock.Lock()
	defer set._catalogLock.Unlock()
	mapping := set.GetMapping(txn, name, false)
	if mapping != nil && !mapping._deleted {
		//get right version for txn
		ent := mapping._index.GetEntry()
		cur := set.GetEntryForTxn(txn, ent)
		if cur._deleted ||
			cur._name != name &&
				!set.UseTimestamp(txn, mapping._timestamp) {
			return nil
		}
		return cur
	}
	return nil
}

func (set *CatalogSet) GetEntryForTxn(txn *Txn, ent *CatalogEntry) *CatalogEntry {
	cur := ent
	for cur._child != nil {
		if set.UseTimestamp(txn, TxnType(cur._timestamp.Load())) {
			break
		}
		cur = cur._child
	}
	return cur
}

// Scan commited entry
func (set *CatalogSet) Scan(fun func(ent *CatalogEntry)) {
	set._catalogLock.Lock()
	defer set._catalogLock.Unlock()
	for _, value := range set._entries {
		ent := value._entry
		committed := set.GetCommittedEntry(ent)
		if !committed._deleted {
			fun(committed)
		}
	}
}

func (set *CatalogSet) GetCommittedEntry(ent *CatalogEntry) *CatalogEntry {
	cur := ent
	for cur._child != nil {
		if cur._timestamp.Load() < TRANSACTION_ID_START {
			//commited
			break
		}
		cur = cur._child
	}
	return cur
}

const (
	CatalogTypeInvalid uint8 = 0
	CatalogTypeTable   uint8 = 1
	CatalogTypeSchema  uint8 = 2
	CatalogTypeDeleted uint8 = 50
)

type CatalogEntry struct {
	_typ       uint8
	_set       *CatalogSet
	_name      string
	_deleted   bool
	_timestamp atomic.Uint64
	_child     *CatalogEntry
	_parent    *CatalogEntry

	//for schema entry
	_catalog *Catalog
	_tables  *CatalogSet

	//for table entry
	_schema      *CatalogEntry
	_schName     string
	_storage     *DataTable
	_colDefs     []*ColumnDefinition
	_constraints []Constraint
}

func (ent *CatalogEntry) GetStorage() *DataTable {
	return ent._storage
}

func (ent *CatalogEntry) SetAsRoot() {
}

func (ent *CatalogEntry) CreateTable(
	txn *Txn,
	info *DataTableInfo) (*CatalogEntry, error) {
	//must be schema entry
	util.AssertFunc(ent._typ == CatalogTypeSchema)
	tabEnt, err := NewTableEntry(
		ent._catalog,
		ent,
		nil,
		info)
	if err != nil {
		return nil, err
	}
	storage := tabEnt._storage
	storage._info._card.Store(storage._rowGroups._totalRows.Load())

	list := NewDependList()
	retEnt, err := ent.AddEntryInternal(
		txn,
		tabEnt,
		list)
	if err != nil {
		return nil, err
	}
	return retEnt, nil
}

func (ent *CatalogEntry) AddEntryInternal(
	txn *Txn,
	tabEnt *CatalogEntry,
	list *DependList) (*CatalogEntry, error) {
	set := ent.GetCatalogSet(tabEnt._typ)
	list.AddDepend(ent)
	ret, err := set.CreateEntry(txn, tabEnt._name, tabEnt, list)
	if err != nil {
		return nil, err
	}
	if ret {
		return tabEnt, nil
	} else {
		return nil, fmt.Errorf("entry with name %s already exists", tabEnt._name)
	}
}

func (ent *CatalogEntry) GetCatalogSet(typ uint8) *CatalogSet {
	switch typ {
	case CatalogTypeTable:
		return ent._tables
	default:
		panic("usp")
	}
}

func (ent *CatalogEntry) Serialize(serial util.Serialize) error {
	writer := NewFieldWriter(serial)
	err := WriteField[uint8](ent._typ, writer)
	if err != nil {
		return err
	}
	switch ent._typ {
	case CatalogTypeTable:
		//schema
		err = WriteString(ent._schName, writer)
		if err != nil {
			return err
		}
		//table
		err = WriteString(ent._name, writer)
		if err != nil {
			return err
		}
		//colDefs
		err = WriteColDefs(ent._colDefs, writer)
		if err != nil {
			return err
		}
		//constraints
		err = WriteConstraints(ent._constraints, writer)
		if err != nil {
			return err
		}
	case CatalogTypeSchema:
		//schema
		err = WriteString(ent._name, writer)
		if err != nil {
			return err
		}
	default:
		panic("usp")
	}
	return writer.Finalize()
}

func (ent *CatalogEntry) Deserialize(source util.Deserialize) error {
	reader, err := NewFieldReader(source)
	if err != nil {
		return err
	}
	err = ReadRequired[uint8](&ent._typ, reader)
	if err != nil {
		return err
	}
	switch ent._typ {
	case CatalogTypeTable:
		var schema, name string
		var colDefs []*ColumnDefinition
		var constraints []Constraint
		//schema
		schema, err = ReadString(reader)
		if err != nil {
			return err
		}
		//table
		name, err = ReadString(reader)
		if err != nil {
			return err
		}
		//colDefs
		colDefs, err = ReadColDefs(reader)
		if err != nil {
			return err
		}
		//constraints
		constraints, err = ReadConstraints(reader)
		if err != nil {
			return err
		}
		ent._schName = schema
		ent._name = name
		ent._colDefs = colDefs
		ent._constraints = constraints
	case CatalogTypeSchema:
		//schema
		ent._name, err = ReadString(reader)
		if err != nil {
			return err
		}
	default:
		panic("usp")
	}
	reader.Finalize()
	return nil
}

func (ent *CatalogEntry) Scan(typ uint8, fun func(ent *CatalogEntry)) {
	set := ent.GetCatalogSet(typ)
	set.Scan(fun)
}

func (ent *CatalogEntry) GetEntry(
	txn *Txn,
	typ uint8,
	name string) *CatalogEntry {
	set := ent.GetCatalogSet(typ)
	return set.GetEntry(txn, name)
}

func (ent *CatalogEntry) GetColumnIndex(name string) int {
	for i, def := range ent._colDefs {
		if def.Name == name {
			return i
		}
	}
	return -1
}

func (ent *CatalogEntry) GetColumn(idx int) *ColumnDefinition {
	return ent._colDefs[idx]
}

func (ent *CatalogEntry) GetColumns() []*ColumnDefinition {
	return ent._colDefs
}

func (ent *CatalogEntry) GetColumnNames() []string {
	names := make([]string, 0)
	for _, colDef := range ent._colDefs {
		names = append(names, colDef.Name)
	}
	return names
}

func (ent *CatalogEntry) GetTypes() []common.LType {
	typs := make([]common.LType, 0)
	for _, colDef := range ent._colDefs {
		typs = append(typs, colDef.Type)
	}
	return typs
}

func (ent *CatalogEntry) GetStats() *TableStats {
	stats := &TableStats{}
	for i := range ent._colDefs {
		stats._columnStats = append(stats._columnStats, &ColumnStats{
			_stats: *ent.GetStats2(i),
		})
	}
	return stats
}

func (ent *CatalogEntry) GetStats2(colIdx int) *BaseStats {
	if colIdx == -1 {
		return nil
	}
	return ent._storage.GetStats(colIdx)
}

func (ent *CatalogEntry) GetColumn2Idx() map[string]int {
	ret := make(map[string]int)
	for i, colDef := range ent._colDefs {
		ret[colDef.Name] = i
	}
	return ret
}

func NewSchemaEntry(
	catalog *Catalog,
	schema string,
) *CatalogEntry {
	ret := &CatalogEntry{
		_typ:     CatalogTypeSchema,
		_catalog: catalog,
		_name:    schema,
		_tables:  NewCatalogSet(catalog),
	}

	return ret
}

func NewTableEntry(
	catalog *Catalog,
	schEnt *CatalogEntry,
	inheritedStorage *DataTable,
	info *DataTableInfo,
) (*CatalogEntry, error) {
	ret := &CatalogEntry{
		_typ:         CatalogTypeTable,
		_catalog:     catalog,
		_schema:      schEnt,
		_schName:     info._schema,
		_name:        info._table,
		_tables:      NewCatalogSet(catalog),
		_storage:     inheritedStorage,
		_colDefs:     info._colDefs,
		_constraints: info._constraints,
	}

	if inheritedStorage == nil {
		var err error
		ret._storage, err = NewDataTable2(info)
		if err != nil {
			return nil, err
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
					AddDataTableIndex(ret._storage, &cons, indexConsType, nil)
				} else {
					AddDataTableIndex(ret._storage, &cons, indexConsType, &(info._indexesBlkPtrs[indexesIds]))
					indexesIds++
				}
			} else {

			}
		}
	}

	return ret, nil
}

func catalogEntryLess(a, b *CatalogEntry) bool {
	if a._typ < b._typ {
		return true
	}
	if a._typ > b._typ {
		return false
	}

	if util.PointerLess(
		unsafe.Pointer(a._set),
		unsafe.Pointer(b._set)) {
		return true
	}
	if util.PointerLess(
		unsafe.Pointer(b._set),
		unsafe.Pointer(a._set)) {
		return false
	}
	sret := strings.Compare(a._name, b._name)
	if sret < 0 {
		return true
	}
	if sret > 0 {
		return false
	}
	if a._deleted && !b._deleted {
		return true
	}
	if !a._deleted && b._deleted {
		return false
	}
	if a._timestamp.Load() < b._timestamp.Load() {
		return true
	}
	if a._timestamp.Load() > b._timestamp.Load() {
		return false
	}
	if util.PointerLess(
		unsafe.Pointer(a._child),
		unsafe.Pointer(b._child)) {
		return true
	}
	if util.PointerLess(
		unsafe.Pointer(b._child),
		unsafe.Pointer(a._child)) {
		return false
	}
	if util.PointerLess(
		unsafe.Pointer(a._parent),
		unsafe.Pointer(b._parent)) {
		return true
	}
	if util.PointerLess(
		unsafe.Pointer(b._parent),
		unsafe.Pointer(a._parent)) {
		return false
	}
	return false
}

type EntryValue struct {
	_entry    *CatalogEntry
	_refCount atomic.Int64
}

type EntryIndex struct {
	_catalog *CatalogSet
	_index   IdxType
}

func NewEntryIndex(set *CatalogSet, idx IdxType) EntryIndex {
	ret := EntryIndex{
		_catalog: set,
		_index:   idx,
	}
	if entVal, has := set._entries[idx]; !has {
		panic(fmt.Sprintf("catalog entry not found %d", idx))
	} else {
		entVal._refCount.Add(1)
	}

	return ret
}

func (ent *EntryIndex) Close() {
	if ent._catalog == nil {
		return
	}
	if entVal, has := ent._catalog._entries[ent._index]; has {
		entVal._refCount.Add(-1)
		if entVal._refCount.Load() == 0 {
			delete(ent._catalog._entries, ent._index)
		}
	}
	ent._catalog = nil
}

func (ent *EntryIndex) Copy() EntryIndex {
	if ent._catalog != nil {
		return EntryIndex{
			_catalog: ent._catalog,
			_index:   ent._index,
		}
	} else {
		return EntryIndex{
			_index: math.MaxUint64,
		}
	}
}

func (ent *EntryIndex) GetEntry() *CatalogEntry {
	if res, has := ent._catalog._entries[ent._index]; !has {
		panic("catalog entry not found")
	} else {
		return res._entry
	}
}

func (ent *EntryIndex) Replace(newEnt *CatalogEntry) {
	if res, has := ent._catalog._entries[ent._index]; !has {
		panic("catalog entry not found")
	} else {
		res._entry = newEnt
	}
}

type MappingValue struct {
	_index     EntryIndex
	_timestamp TxnType
	_deleted   bool
	_child     *MappingValue
	_parent    *MappingValue
}
