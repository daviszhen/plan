package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestChangeOnColumns_EmptyUpdateColIds tests ChangeOnColumns with empty update column IDs
func TestChangeOnColumns_EmptyUpdateColIds(t *testing.T) {
	t.Run("EmptyUpdateColIds", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("col0"), T_COL("col1"), T_COL("col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Test with empty update column IDs
		updateColIds := []int{}
		newColData := T_ROW{}

		hasConflict := ChangeOnColumns(rctx, txn, item, updateColIds, newColData)

		assert.False(t, hasConflict, "Expected no conflict with empty update column IDs")
	})
}

// TestChangeOnColumns_SingleColumnNoConflict tests ChangeOnColumns with single column update, no conflict
func TestChangeOnColumns_SingleColumnNoConflict(t *testing.T) {
	t.Run("SingleColumnNoConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Test updating single column
		updateColIds := []int{1}
		newColData := T_ROW{T_COL("new_col1")}

		hasConflict := ChangeOnColumns(rctx, txn, item, updateColIds, newColData)

		assert.False(t, hasConflict, "Expected no conflict for single column update")
		assert.Equal(t, T_COL("new_col1"), item._colsData[1], "Expected column 1 to be updated")
		assert.Equal(t, T_COL("old_col0"), item._colsData[0], "Expected column 0 to remain unchanged")
		assert.Equal(t, T_COL("old_col2"), item._colsData[2], "Expected column 2 to remain unchanged")
	})
}

// TestChangeOnColumns_MultipleColumnsNoConflict tests ChangeOnColumns with multiple column updates, no conflict
func TestChangeOnColumns_MultipleColumnsNoConflict(t *testing.T) {
	t.Run("MultipleColumnsNoConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Test updating multiple columns
		updateColIds := []int{0, 2}
		newColData := T_ROW{T_COL("new_col0"), T_COL("new_col2")}

		hasConflict := ChangeOnColumns(rctx, txn, item, updateColIds, newColData)

		assert.False(t, hasConflict, "Expected no conflict for multiple column updates")
		assert.Equal(t, T_COL("new_col0"), item._colsData[0], "Expected column 0 to be updated")
		assert.Equal(t, T_COL("old_col1"), item._colsData[1], "Expected column 1 to remain unchanged")
		assert.Equal(t, T_COL("new_col2"), item._colsData[2], "Expected column 2 to be updated")
	})
}

// TestChangeOnColumns_SingleColumnWithConflict tests ChangeOnColumns with single column update, with conflict
func TestChangeOnColumns_SingleColumnWithConflict(t *testing.T) {
	t.Run("SingleColumnWithConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create two transactions
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Commit txn2 to create conflict
		txn2.Commit(TxnId(clock.Now()))

		// Create RowGroupItem with existing update from committed transaction
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Add existing update from committed transaction to column 1
		existingInfo := &UpdateInfo{
			_colIdx:  1,
			_colData: T_COL("existing_data"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		existingInfo._version.Store(&txn2Id)
		item._colUpdates[1]._info = existingInfo

		// Test updating column 1 (should conflict)
		updateColIds := []int{1}
		newColData := T_ROW{T_COL("new_col1")}

		hasConflict := ChangeOnColumns(rctx, txn1, item, updateColIds, newColData)

		assert.True(t, hasConflict, "Expected conflict for column update with existing committed transaction")
		// Column data should not be updated due to conflict
		assert.Equal(t, T_COL("old_col1"), item._colsData[1], "Expected column 1 to remain unchanged due to conflict")
	})
}

// TestChangeOnColumns_MultipleColumnsWithConflict tests ChangeOnColumns with multiple column updates, conflict on first column
func TestChangeOnColumns_MultipleColumnsWithConflict(t *testing.T) {
	t.Run("MultipleColumnsWithConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create two transactions
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Commit txn2 to create conflict
		txn2.Commit(TxnId(clock.Now()))

		// Create RowGroupItem with existing update from committed transaction
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Add existing update from committed transaction to column 0
		existingInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("existing_data"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		existingInfo._version.Store(&txn2Id)
		item._colUpdates[0]._info = existingInfo

		// Test updating multiple columns (conflict on first column)
		updateColIds := []int{0, 2}
		newColData := T_ROW{T_COL("new_col0"), T_COL("new_col2")}

		hasConflict := ChangeOnColumns(rctx, txn1, item, updateColIds, newColData)

		assert.True(t, hasConflict, "Expected conflict for multiple column updates with conflict on first column")
		// No columns should be updated due to conflict
		assert.Equal(t, T_COL("old_col0"), item._colsData[0], "Expected column 0 to remain unchanged due to conflict")
		assert.Equal(t, T_COL("old_col1"), item._colsData[1], "Expected column 1 to remain unchanged")
		assert.Equal(t, T_COL("old_col2"), item._colsData[2], "Expected column 2 to remain unchanged due to conflict")
	})
}

// TestChangeOnColumns_MultipleColumnsWithConflictOnSecondColumn tests ChangeOnColumns with multiple column updates, conflict on second column
func TestChangeOnColumns_MultipleColumnsWithConflictOnSecondColumn(t *testing.T) {
	t.Run("MultipleColumnsWithConflictOnSecondColumn", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create two transactions
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Commit txn2 to create conflict
		txn2.Commit(TxnId(clock.Now()))

		// Create RowGroupItem with existing update from committed transaction
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Add existing update from committed transaction to column 2
		existingInfo := &UpdateInfo{
			_colIdx:  2,
			_colData: T_COL("existing_data"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		existingInfo._version.Store(&txn2Id)
		item._colUpdates[2]._info = existingInfo

		// Test updating multiple columns (conflict on second column)
		updateColIds := []int{0, 2}
		newColData := T_ROW{T_COL("new_col0"), T_COL("new_col2")}

		hasConflict := ChangeOnColumns(rctx, txn1, item, updateColIds, newColData)

		assert.True(t, hasConflict, "Expected conflict for multiple column updates with conflict on second column")
		// First column should be updated, second should not due to conflict
		assert.Equal(t, T_COL("new_col0"), item._colsData[0], "Expected column 0 to be updated")
		assert.Equal(t, T_COL("old_col1"), item._colsData[1], "Expected column 1 to remain unchanged")
		assert.Equal(t, T_COL("old_col2"), item._colsData[2], "Expected column 2 to remain unchanged due to conflict")
	})
}

// TestChangeOnColumns_AllColumnsNoConflict tests ChangeOnColumns with all columns update, no conflict
func TestChangeOnColumns_AllColumnsNoConflict(t *testing.T) {
	t.Run("AllColumnsNoConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Test updating all columns
		updateColIds := []int{0, 1, 2}
		newColData := T_ROW{T_COL("new_col0"), T_COL("new_col1"), T_COL("new_col2")}

		hasConflict := ChangeOnColumns(rctx, txn, item, updateColIds, newColData)

		assert.False(t, hasConflict, "Expected no conflict for all columns update")
		assert.Equal(t, T_COL("new_col0"), item._colsData[0], "Expected column 0 to be updated")
		assert.Equal(t, T_COL("new_col1"), item._colsData[1], "Expected column 1 to be updated")
		assert.Equal(t, T_COL("new_col2"), item._colsData[2], "Expected column 2 to be updated")
	})
}

// TestChangeOnColumns_AllColumnsWithConflict tests ChangeOnColumns with all columns update, with conflict
func TestChangeOnColumns_AllColumnsWithConflict(t *testing.T) {
	t.Run("AllColumnsWithConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create two transactions
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Commit txn2 to create conflict
		txn2.Commit(TxnId(clock.Now()))

		// Create RowGroupItem with existing update from committed transaction
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Add existing update from committed transaction to column 1
		existingInfo := &UpdateInfo{
			_colIdx:  1,
			_colData: T_COL("existing_data"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		existingInfo._version.Store(&txn2Id)
		item._colUpdates[1]._info = existingInfo

		// Test updating all columns (conflict on column 1)
		updateColIds := []int{0, 1, 2}
		newColData := T_ROW{T_COL("new_col0"), T_COL("new_col1"), T_COL("new_col2")}

		hasConflict := ChangeOnColumns(rctx, txn1, item, updateColIds, newColData)

		assert.True(t, hasConflict, "Expected conflict for all columns update with conflict on column 1")
		// First column should be updated, others should not due to conflict
		assert.Equal(t, T_COL("new_col0"), item._colsData[0], "Expected column 0 to be updated")
		assert.Equal(t, T_COL("old_col1"), item._colsData[1], "Expected column 1 to remain unchanged due to conflict")
		assert.Equal(t, T_COL("old_col2"), item._colsData[2], "Expected column 2 to remain unchanged due to conflict")
	})
}

// TestChangeOnColumns_UpdateInfoCreation tests that UpdateInfo is created correctly
func TestChangeOnColumns_UpdateInfoCreation(t *testing.T) {
	t.Run("UpdateInfoCreation", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Test updating single column
		updateColIds := []int{1}
		newColData := T_ROW{T_COL("new_col1")}

		hasConflict := ChangeOnColumns(rctx, txn, item, updateColIds, newColData)

		assert.False(t, hasConflict, "Expected no conflict")
		
		// Verify that UpdateInfo was added to the column updates list
		assert.NotNil(t, item._colUpdates[1]._info, "Expected UpdateInfo to be added to column 1 updates")
		updateInfo := item._colUpdates[1]._info
		assert.Equal(t, 1, updateInfo._colIdx, "Expected _colIdx to be 1")
		assert.Equal(t, T_COL("old_col1"), updateInfo._colData, "Expected _colData to be old column data")
		assert.NotNil(t, updateInfo._version.Load(), "Expected _version to be set")
		assert.Equal(t, txn.Id(), *updateInfo._version.Load(), "Expected _version to be transaction ID")
	})
}

// TestChangeOnColumns_EmptyNewColData tests ChangeOnColumns with empty new column data
func TestChangeOnColumns_EmptyNewColData(t *testing.T) {
	t.Run("EmptyNewColData", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// Test with empty new column data (should not cause panic)
		updateColIds := []int{}
		newColData := T_ROW{}

		hasConflict := ChangeOnColumns(rctx, txn, item, updateColIds, newColData)

		assert.False(t, hasConflict, "Expected no conflict with empty new column data")
		// All columns should remain unchanged
		assert.Equal(t, T_COL("old_col0"), item._colsData[0], "Expected column 0 to remain unchanged")
		assert.Equal(t, T_COL("old_col1"), item._colsData[1], "Expected column 1 to remain unchanged")
		assert.Equal(t, T_COL("old_col2"), item._colsData[2], "Expected column 2 to remain unchanged")
	})
}
// TestChangeOnColumns_SameTransactionUpdate tests ChangeOnColumns with same transaction updating same column
func TestChangeOnColumns_SameTransactionUpdate(t *testing.T) {
	t.Run("SameTransactionUpdate", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create RowGroupItem with some data
		item := &RowGroupItem{
			_pk:         T_COL("pk1"),
			_colsData:   []T_COL{T_COL("old_col0"), T_COL("old_col1"), T_COL("old_col2")},
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
				&UpdateList{_info: nil},
			},
		}

		// First update
		updateColIds1 := []int{1}
		newColData1 := T_ROW{T_COL("first_update")}
		hasConflict1 := ChangeOnColumns(rctx, txn, item, updateColIds1, newColData1)
		assert.False(t, hasConflict1, "Expected no conflict for first update")

		// Second update to same column by same transaction
		updateColIds2 := []int{1}
		newColData2 := T_ROW{T_COL("second_update")}
		hasConflict2 := ChangeOnColumns(rctx, txn, item, updateColIds2, newColData2)

		assert.False(t, hasConflict2, "Expected no conflict for same transaction updating same column")
		assert.Equal(t, T_COL("second_update"), item._colsData[1], "Expected column 1 to be updated to second value")
	})
}
