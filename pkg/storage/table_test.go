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
		{
			_name: "b",
			_type: common.IntegerType(),
		},
		{
			_name: "c",
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
	for i := 0; i < len(colDefs); i++ {
		data.Data[i] = NewInt32ConstVector(10, false)
	}
	data.Count = util.DefaultVectorSize
	for i := 0; i < 3; i++ {
		table.LocalAppend(txn, lAState, data, false)
	}
	table.FinalizeLocalAppend(txn, lAState)

	err = txnMgr.Commit(txn)
	require.NoError(t, err)
}

func NewInt32ConstVector(v int32, null bool) *chunk.Vector {
	vec := chunk.NewConstVector(common.IntegerType())
	data := chunk.GetSliceInPhyFormatConst[int32](vec)
	data[0] = v
	chunk.SetNullInPhyFormatConst(vec, null)
	return vec
}
