package v2

import (
	"fmt"
	"testing"

	hlc "github.com/cockroachdb/cockroachdb-parser/pkg/util/hlc"
)

func TestHLC(t *testing.T) {
	clock := hlc.NewClockForTesting(nil)

	ts := clock.Now()
	fmt.Println("hlc now", ts)
}

func TestTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txn := rctx._txnMgr.NewTxn("t1", TxnIsolationLevelReadCommitted)
	txn.Commit(TxnId(rctx._clock.Now()))
}
