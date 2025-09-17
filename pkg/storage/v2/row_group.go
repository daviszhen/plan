package v2

import (
	"fmt"
	"sync"

	"github.com/daviszhen/plan/pkg/util"
	"github.com/tidwall/btree"
)

const (
	STANDARD_VECTOR_SIZE     = util.DefaultVectorSize
	STANDARD_ROW_GROUPS_SIZE = 122880
	ROW_GROUP_SIZE           = STANDARD_ROW_GROUPS_SIZE
	ROW_GROUP_VECTOR_COUNT   = ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE
	MERGE_THRESHOLD          = ROW_GROUP_SIZE
)

type RowGroupItem struct {
	_pk T_PK
	//column data
	_colsData []T_COL
	//txn version
	_rowVersion *ChunkInfo
	_colUpdates []*UpdateList
}

func RowGroupItemLess(a, b *RowGroupItem) bool {
	return a._pk.Less(b._pk)
}

type RowGroup struct {
	_lock   sync.Mutex
	_schema *Schema
	//data
	_rows *btree.BTreeG[*RowGroupItem]
}

func NewRowGroup(schema *Schema) *RowGroup {
	ret := &RowGroup{
		_schema: schema,
	}
	ret._rows = btree.NewBTreeG(RowGroupItemLess)
	return ret
}

func (rg *RowGroup) KeyExists(txn *Txn, pks []T_PK, result []bool) error {
	tmpItem := RowGroupItem{}
	for i, pk := range pks {
		tmpItem._pk = pk
		rowItem, ok := rg._rows.Get(&tmpItem)
		if !ok {
			result[i] = false
			continue
		}
		if rowItem == nil {
			return fmt.Errorf("row item is nil")
		}
		//version check
		if rowItem._rowVersion.UseInsertedVersion(txn) &&
			rowItem._rowVersion.UseDeletedVersion(txn) {
			result[i] = true
		} else {
			result[i] = false
		}
	}
	return nil
}

func (rg *RowGroup) Scan(txn *Txn, result *[]T_ROW) error {
	return nil
}

func (rg *RowGroup) Append(txn *Txn, pks []T_PK, data []T_ROW) error {
	for i, pk := range pks {
		tmpItem := &RowGroupItem{}
		tmpItem._pk = pk
		rowItem, ok := rg._rows.Get(tmpItem)
		if !ok {
			//no such key
		} else {
			//key exists
			if rowItem == nil {
				return fmt.Errorf("row item is nil 2")
			}
			//version check
			//TODO:
		}

	}
	return nil
}

func (rg *RowGroup) Update(txn *Txn, pks []T_PK, colIds []int, updates []T_ROW) error {
	return nil
}

func (rg *RowGroup) Delete(txn *Txn, pks []T_PK) error {
	return nil
}
