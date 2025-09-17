package v2

import (
	"path/filepath"
	"strings"

	treemap "github.com/liyue201/gostl/ds/map"
)

type StorageMgr struct {
	_path string
	_wal  *WAL
}

func NewStorageMgr(path string) *StorageMgr {
	walPath := filepath.Join(path, "wal")
	return &StorageMgr{
		_path: path,
		_wal:  NewWAL(walPath),
	}
}

func (mgr *StorageMgr) Close() {
	mgr._wal.Close()
}

// txn storage for every txn
type TxnStorage struct {
	_txn    *Txn
	_tables *treemap.Map[*TableDef, *TableStorage]
}

func NewTxnStorage(txn *Txn) *TxnStorage {
	cmp := func(a, b *TableDef) int {
		return strings.Compare(a._id, b._id)
	}
	return &TxnStorage{
		_txn:    txn,
		_tables: treemap.New[*TableDef, *TableStorage](cmp),
	}
}

// main storage for all txns
type MainStorage struct {
	_tables *treemap.Map[*TableDef, *TableStorage]
}

func NewMainStorage() *MainStorage {
	return &MainStorage{
		_tables: treemap.New[*TableDef, *TableStorage](func(a, b *TableDef) int {
			return strings.Compare(a._id, b._id)
		}),
	}
}

// Delta storage for updates for all txns
type DeltaStorage struct {
	_tables *treemap.Map[*TableDef, *TableStorage]
}

func NewDeltaStorage() *DeltaStorage {
	return &DeltaStorage{
		_tables: treemap.New[*TableDef, *TableStorage](func(a, b *TableDef) int {
			return strings.Compare(a._id, b._id)
		}),
	}
}

type TableStorage struct {
	_tableDef *TableDef
	_memRg    *RowGroup
}

func NewTableStorage(table *TableDef) *TableStorage {
	ret := &TableStorage{
		_tableDef: table,
	}
	ret._memRg = NewRowGroup(table._cols)
	return ret
}

func (table *TableStorage) KeyExists(txn *Txn, pks []T_PK, result []bool) error {
	return table._memRg.KeyExists(txn, pks, result)
}

func (table *TableStorage) Scan(txn *Txn, result *[]T_ROW) error {
	return nil
}

func (table *TableStorage) Append(txn *Txn, data []T_ROW) error {
	return nil
}

func (table *TableStorage) Update(txn *Txn, pks []T_PK, colIds []int, updates []T_ROW) error {
	return nil
}

func (table *TableStorage) Delete(txn *Txn, pks []T_PK) error {
	return nil
}
