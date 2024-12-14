package storage

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func Test_table1(t *testing.T) {
	colDefs := []*ColumnDefinition{
		{
			_name: "a",
			_type: common.IntegerType(),
		},
	}
	table := NewDataTable("test", "t1", colDefs)
	txnMgr := NewTxnMgr()
	txn, err := txnMgr.NewTxn()
	require.NoError(t, err)
	lAState := &LocalAppendState{}
	table.InitializeLocalAppend(txn, lAState)

	data := &chunk.Chunk{}
	data.Init(table.GetTypes(), util.DefaultVectorSize)
	table.LocalAppend(txn, lAState, data, false)

	err = txnMgr.Commit(txn)
	require.NoError(t, err)
}
