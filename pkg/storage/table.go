package storage

import (
	"sync"
)

type DataTableInfo struct {
}

type DataTable struct {
	_info       *DataTableInfo
	_appendLock sync.Mutex
}
