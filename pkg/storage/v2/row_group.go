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
	_pk T_COL
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
	_rows     *btree.BTreeG[*RowGroupItem]
	_rowsLock sync.RWMutex
}

func NewRowGroup(schema *Schema) *RowGroup {
	ret := &RowGroup{
		_schema: schema,
	}
	ret._rows = btree.NewBTreeGOptions(
		RowGroupItemLess,
		btree.Options{
			NoLocks: true,
		})
	return ret
}

func (rg *RowGroup) Scan(
	runCtx *RunContext,
	txn *Txn,
	resultPks *[]T_COL,
	resultRows *[]T_ROW) error {
	util.ScopeLock(&rg._rowsLock, func() {
		rg._rows.Ascend(nil,
			func(item *RowGroupItem) bool {
				//FIXME:
				*resultPks = append(*resultPks, item._pk)
				*resultRows = append(*resultRows, item._colsData)
				return true
			})
	})
	return nil
}

func (rg *RowGroup) Find(
	runCtx *RunContext,
	txn *Txn,
	pks []T_COL,
	resultExists *[]bool,
	resultRows *[][]*T_COL,
) {
	colIds := make([]int, rg._schema.ColumnCount())
	for i := 0; i < rg._schema.ColumnCount(); i++ {
		colIds[i] = i
	}
	rg.find(runCtx, txn, pks, colIds, resultExists, resultRows)
}

func (rg *RowGroup) find(
	runCtx *RunContext,
	txn *Txn,
	pks []T_COL,
	colIds []int,
	resultExists *[]bool,
	resultRows *[][]*T_COL,
) {
	if len(pks) == 0 || len(colIds) == 0 {
		return
	}
	util.AssertFunc(resultExists != nil)
	util.AssertFunc(resultRows != nil)
	util.AssertFunc(len(*resultExists) == len(pks))
	util.AssertFunc(len(*resultRows) == len(pks) &&
		len((*resultRows)[0]) == len(colIds))
	tmpItem := &RowGroupItem{}
	has := false
	for rowIdx, pk := range pks {
		tmpItem._pk = pk
		util.ScopeLock(&rg._rowsLock, func() {
			item, ok := rg._rows.Get(tmpItem)
			if !ok {
				(*resultExists)[rowIdx] = false
				return
			}
			if item == nil {
				panic("oldItem is nil")
			}
			//check if the row is visible and get the right version
			has, _ = CheckRowVisiable(
				runCtx.TxnMgr(),
				txn,
				item._rowVersion)
			if has {
				(*resultExists)[rowIdx] = true
				//visible,mvcc read
				for idx, colId := range colIds {
					colData := (*resultRows)[rowIdx][idx]
					//lastest version
					*colData = item._colsData[colId]
					MVCCRead(
						runCtx.TxnMgr(),
						txn,
						item._colUpdates[colId],
						colData,
					)
				}
			} else {
				//invisible, no exist
				(*resultExists)[rowIdx] = false
			}
		})
	}
}

func (rg *RowGroup) Insert(
	runCtx *RunContext,
	txn *Txn,
	pks []T_COL,
	rows []T_ROW) error {
	txnId := txn.Id()
	has, wwConflictsOnChange := false, false
	tmpItem := &RowGroupItem{}
	for rowIdx, pk := range pks {
		tmpItem._pk = pk
		util.ScopeLock(&rg._rowsLock, func() {
			iter := rg._rows.Iter()
			defer iter.Release()
			var item *RowGroupItem
			if iter.Seek(tmpItem) {
				item = iter.Item()
				//want tmpItem == item
				if !item._pk.Less(tmpItem._pk) {
					//must be equal
					has = true
				}
			}
			var itemToChange *RowGroupItem
			if has {
				//find the key, check it visible or not
				has, wwConflictsOnChange = CheckRowVisiable(
					runCtx.TxnMgr(),
					txn,
					item._rowVersion)
				if has || wwConflictsOnChange {
					return
				}
				//possible cases:
				//case 1: txn of insertion aborted, invisible
				// data is invalid, can be overwritten
				//case 2: current txn makes insertion and deletion, invisible
				// data of current txn is invalid, can be overwritten
				//case 3: current txn makes deletion, invisible
				// need to generate old version, then insert new data
				itemToChange = item
			} else {
				//add the new key
				itemToChange = &RowGroupItem{}
				itemToChange._pk = pk
				itemToChange._colsData = make([]T_COL, rg._schema.ColumnCount())
				itemToChange._rowVersion = &ChunkInfo{}
				itemToChange._rowVersion._inserted.Store(&txnId)
				itemToChange._colUpdates = make([]*UpdateList, rg._schema.ColumnCount())
				for i := 0; i < rg._schema.ColumnCount(); i++ {
					itemToChange._colUpdates[i] = &UpdateList{_info: nil}
				}
				rg._rows.Set(itemToChange)
			}

			//generate column version
			colIds := make([]int, rg._schema.ColumnCount())
			for i := 0; i < rg._schema.ColumnCount(); i++ {
				colIds[i] = i
			}
			if ChangeOnColumns(runCtx, txn, itemToChange, colIds, rows[rowIdx]) {
				wwConflictsOnChange = true
				return
			}
		})

		if has {
			return fmt.Errorf("duplicated key")
		}
		if wwConflictsOnChange {
			return fmt.Errorf("ww-conflicts")
		}
	}
	return nil
}

func ChangeOnColumns(
	runCtx *RunContext,
	txn *Txn,
	item *RowGroupItem,
	updateColIds []int,
	newColData T_ROW,
) bool {
	for idx, colIdx := range updateColIds {
		oldCol := item._colsData[colIdx]
		newCol := newColData[idx]
		txnId := txn.Id()
		newInfo := &UpdateInfo{
			_colIdx:  colIdx,
			_colData: oldCol,
		}
		newInfo._version.Store(&txnId)
		if CheckUpdatesForConflicts(runCtx.TxnMgr(), txn, item._colUpdates[colIdx], newInfo) {
			return true
		}
		//no conflict, update the column data in place
		item._colsData[colIdx] = newCol
		//TODO: generate wal
	}
	return false
}

func (rg *RowGroup) Update(txn *Txn, pks []T_COL, colIds []int, updates []T_ROW) error {
	return nil
}

func (rg *RowGroup) Delete(txn *Txn, pks []T_COL) error {
	return nil
}
