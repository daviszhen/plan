package v2

import (
	"sync/atomic"
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
	_root *UpdateNode
}

type UpdateNode struct {
	_info *UpdateNodeData
}

type UpdateNodeData struct {
	_info      *UpdateInfo
	_tupleData []byte
}

type UpdateInfo struct {
	_columnIndex uint64
	_version     atomic.Pointer[TxnId]
	_tupleData   []byte
	_prev        *UpdateInfo
	_next        *UpdateInfo
}
