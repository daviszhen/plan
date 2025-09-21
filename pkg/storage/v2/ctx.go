package v2

import hlc "github.com/cockroachdb/cockroachdb-parser/pkg/util/hlc"

var (
	gRunCtx *RunContext
)

func InitRunContext(storagePath string) *RunContext {
	gRunCtx = NewRunContext(storagePath)
	return gRunCtx
}

func GetRunContext() *RunContext {
	return gRunCtx
}

func CloseRunContext() {
	gRunCtx.Close()
}

type RunContext struct {
	_clock        *hlc.Clock
	_storageMgr   *StorageMgr
	_txnMgr       *TxnMgr
	_mainStorage  *MainStorage
	_deltaStorage *DeltaStorage
}

func NewRunContext(storagePath string) *RunContext {
	clock := hlc.NewClockForTesting(nil)
	return &RunContext{
		_clock:        clock,
		_storageMgr:   NewStorageMgr(storagePath),
		_txnMgr:       NewTxnMgr(clock),
		_mainStorage:  NewMainStorage(),
		_deltaStorage: NewDeltaStorage(),
	}
}

func (ctx *RunContext) TxnMgr() *TxnMgr {
	return ctx._txnMgr
}

func (ctx *RunContext) Close() {
	ctx._storageMgr.Close()
	ctx._txnMgr.Close()
	//TODO: close main storage and delta storage
}
