package v2

import (
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/util"
)

// undo log
type UndoFlags uint32

const (
	EMPTY_ENTRY   UndoFlags = 0
	INSERT_TUPLE  UndoFlags = 1
	DELETE_TUPLE  UndoFlags = 2
	UPDATE_TUPLE  UndoFlags = 3
	CATALOG_ENTRY UndoFlags = 4
)

type UndoLog struct {
	_typ  UndoFlags
	_data []byte
}

type UndoBuffer struct {
	_logs []UndoLog
}

// row version
type ChunkInfo struct {
	_inserted atomic.Pointer[TxnId]
	_deleted  atomic.Pointer[TxnId]
}

// column version
type UpdateList struct {
	_lock sync.RWMutex
	_info *UpdateInfo
}

type UpdateInfo struct {
	_colIdx  int
	_version atomic.Pointer[TxnId]
	_colData T_COL
	_prev    *UpdateInfo
	_next    *UpdateInfo
}

// CheckRowVisible check the row is visible to the txn and
// maybe ww-conflicts with another txn
// return:
//
//	visible (yes/no)
//	ww-conflicts if change the row(insert/delete/update) (yes/no)
func CheckRowVisiable(
	mgr *TxnMgr,
	txn *Txn,
	rowInfo *ChunkInfo) (bool, bool) {
	if rowInfo._inserted.Load() == nil {
		panic("insert txn id is nil")
	}
	insertTxnId := *rowInfo._inserted.Load()
	deleteTxnId := TxnIdInvalid
	if rowInfo._deleted.Load() != nil {
		deleteTxnId = *rowInfo._deleted.Load()
	}

	insertTxnStatus := mgr.TxnState(insertTxnId)
	//case 1: insert txn id is aborted.
	if insertTxnStatus == TxnStateAborted {
		if deleteTxnId != TxnIdInvalid &&
			deleteTxnId != insertTxnId {
			panic("delete txn id is valid and not the same as insert txn id")
		}
		//插入的事务已经回滚，看不见
		return false, false
	} else if insertTxnStatus == TxnStateActive {
		//case 2: insert txn id is active.
		if insertTxnId == txn.Id() {
			//自己事务插入的。
			if deleteTxnId == TxnIdInvalid {
				//没有删除，可见
				return true, false
			} else {
				if deleteTxnId != txn.Id() {
					panic("delete txn id is valid and not the same as insert txn id 2")
				}
				//有删除，不可见
				return false, false
			}
		} else { //其它事务插入的，看不见
			return false, true
		}
	} else if insertTxnStatus == TxnStateCommitted {
		//case 3: insert txn id is committed.
		insertTxnInSS := txn._snapshot.IsActiveInSnapshot(insertTxnId)
		deleteTxnStatus := TxnStateAborted
		if deleteTxnId != TxnIdInvalid {
			deleteTxnStatus = mgr.TxnState(deleteTxnId)
		}
		if insertTxnInSS {
			//插入的事务在快照active事务中，不可见
			return false, true
		} else if deleteTxnId == TxnIdInvalid ||
			deleteTxnStatus == TxnStateAborted {
			//没有删除，或者删除的事务已经回滚，可见
			return true, false
		} else if deleteTxnStatus == TxnStateActive {
			if deleteTxnId == txn.Id() {
				//删除的事务是自己，不可见
				return false, false
			} else {
				//删除的事务是其它事务，可见
				return true, true
			}
		} else if deleteTxnStatus == TxnStateCommitted {
			deleteTxnInSS := txn._snapshot.IsActiveInSnapshot(deleteTxnId)
			if deleteTxnInSS {
				//删除的事务在快照active事务中，可见
				return true, true
			} else {
				//删除的事务不在快照active事务中，不可见
				return false, true
			}
		}
	}
	panic("unreachable")
}

func CheckUpdateVisiable(mgr *TxnMgr, txn *Txn, updateInfo *UpdateInfo) (bool, bool) {
	if updateInfo._version.Load() == nil {
		panic("version is nil")
	}
	rowInfo := &ChunkInfo{}
	rowInfo._inserted.Store(updateInfo._version.Load())
	rowInfo._deleted.Store(nil)
	return CheckRowVisiable(mgr, txn, rowInfo)
}

func CheckUpdatesForConflicts(
	mgr *TxnMgr,
	txn *Txn,
	updates *UpdateList,
	newInfo *UpdateInfo,
) bool {
	wwConflictsOnChange := false
	util.ScopeLock(
		&updates._lock,
		func() {
			ptr := updates._info
			var oldInfo *UpdateInfo
			for ptr != nil {
				if ptr._version.Load() == newInfo._version.Load() {
					if oldInfo == nil {
						oldInfo = ptr
					} else {
						panic("txn has more than one update info")
					}
				}
				_, wwConflictsOnChange = CheckUpdateVisiable(
					mgr,
					txn,
					ptr)
				if wwConflictsOnChange {
					return
				}
				ptr = ptr._next
			}
			if oldInfo != nil {
				util.AssertFunc(oldInfo._version.Load() == newInfo._version.Load())
				util.AssertFunc(oldInfo._colIdx == newInfo._colIdx)
				oldInfo._colData = newInfo._colData
			} else {
				newInfo._next = updates._info
				if updates._info != nil {
					updates._info._prev = newInfo
				}
				updates._info = newInfo
				newInfo._prev = nil
			}
		})
	return wwConflictsOnChange
}

func MVCCRead(
	mgr *TxnMgr,
	txn *Txn,
	updates *UpdateList,
	colData *T_COL,
) {
	if updates == nil {
		return
	}
	visible := false
	util.ScopeLock(
		&updates._lock,
		func() {
			ptr := updates._info
			for ptr != nil {
				visible, _ = CheckUpdateVisiable(
					mgr,
					txn,
					ptr)
				if !visible {
					*colData = ptr._colData
				} else {
					break
				}
				ptr = ptr._next
			}
		})
}
