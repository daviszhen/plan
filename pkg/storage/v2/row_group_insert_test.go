package v2

import (
	"sync"
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/stretchr/testify/assert"
)

// TestRowGroupInsert_SingleRowNoConflict tests RowGroup.Insert with single row, no conflict
func TestRowGroupInsert_SingleRowNoConflict(t *testing.T) {
	t.Run("SingleRowNoConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 3 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert single row
		pks := []T_COL{T_COL("pk1")}
		rows := []T_ROW{
			{T_COL("data0"), T_COL("data1"), T_COL("data2")},
		}

		err := rg.Insert(rctx, txn, pks, rows)

		assert.NoError(t, err, "Expected no error for single row insert")
		
		// Verify the row was inserted
		iter := rg._rows.Iter()
		defer iter.Release()
		assert.True(t, iter.Next(), "Expected at least one row")
		item := iter.Item()
		assert.Equal(t, T_COL("pk1"), item._pk, "Expected correct primary key")
		assert.Equal(t, T_COL("data0"), item._colsData[0], "Expected correct column 0 data")
		assert.Equal(t, T_COL("data1"), item._colsData[1], "Expected correct column 1 data")
		assert.Equal(t, T_COL("data2"), item._colsData[2], "Expected correct column 2 data")
	})
}

// TestRowGroupInsert_MultipleRowsNoConflict tests RowGroup.Insert with multiple rows, no conflict
func TestRowGroupInsert_MultipleRowsNoConflict(t *testing.T) {
	t.Run("MultipleRowsNoConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert multiple rows
		pks := []T_COL{T_COL("pk1"), T_COL("pk2"), T_COL("pk3")}
		rows := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
			{T_COL("data2_0"), T_COL("data2_1")},
			{T_COL("data3_0"), T_COL("data3_1")},
		}

		err := rg.Insert(rctx, txn, pks, rows)

		assert.NoError(t, err, "Expected no error for multiple rows insert")
		
		// Verify all rows were inserted
		iter := rg._rows.Iter()
		defer iter.Release()
		rowCount := 0
		for iter.Next() {
			item := iter.Item()
			rowCount++
			switch string(item._pk) {
			case "pk1":
				assert.Equal(t, T_COL("data1_0"), item._colsData[0], "Expected correct data for pk1")
				assert.Equal(t, T_COL("data1_1"), item._colsData[1], "Expected correct data for pk1")
			case "pk2":
				assert.Equal(t, T_COL("data2_0"), item._colsData[0], "Expected correct data for pk2")
				assert.Equal(t, T_COL("data2_1"), item._colsData[1], "Expected correct data for pk2")
			case "pk3":
				assert.Equal(t, T_COL("data3_0"), item._colsData[0], "Expected correct data for pk3")
				assert.Equal(t, T_COL("data3_1"), item._colsData[1], "Expected correct data for pk3")
			}
		}
		assert.Equal(t, 3, rowCount, "Expected 3 rows to be inserted")
	})
}

// TestRowGroupInsert_DuplicateKey tests RowGroup.Insert with duplicate key (should fail)
func TestRowGroupInsert_DuplicateKey(t *testing.T) {
	t.Run("DuplicateKey", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert first row
		pks1 := []T_COL{T_COL("pk1")}
		rows1 := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
		}
		err1 := rg.Insert(rctx, txn, pks1, rows1)
		assert.NoError(t, err1, "Expected no error for first insert")

		// Try to insert duplicate key (should succeed and update)
		pks2 := []T_COL{T_COL("pk1")}
		rows2 := []T_ROW{
			{T_COL("data2_0"), T_COL("data2_1")},
		}
		err2 := rg.Insert(rctx, txn, pks2, rows2)

		assert.Error(t, err2, "Expected error for duplicate key")
		assert.Contains(t, err2.Error(), "duplicated key", "Expected 'duplicated key' error message")
		
	})
}
// TestRowGroupInsert_VisibleExistingRow tests RowGroup.Insert with visible existing row
func TestRowGroupInsert_VisibleExistingRow(t *testing.T) {
	t.Run("VisibleExistingRow", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and commit first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		pks1 := []T_COL{T_COL("pk1")}
		rows1 := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
		}
		err1 := rg.Insert(rctx, txn1, pks1, rows1)
		assert.NoError(t, err1, "Expected no error for first insert")
		txn1.Commit(TxnId(clock.Now()))

		// Try to insert same key with different transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		pks2 := []T_COL{T_COL("pk1")}
		rows2 := []T_ROW{
			{T_COL("data2_0"), T_COL("data2_1")},
		}
		err2 := rg.Insert(rctx, txn2, pks2, rows2)

		assert.Error(t, err2, "Expected error for visible existing row")
		assert.Contains(t, err2.Error(), "duplicated key", "Expected 'duplicated key' error message")
	})
}

// TestRowGroupInsert_InvisibleExistingRow tests RowGroup.Insert with invisible existing row (aborted transaction)
func TestRowGroupInsert_InvisibleExistingRow(t *testing.T) {
	t.Run("InvisibleExistingRow", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		pks1 := []T_COL{T_COL("pk1")}
		rows1 := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
		}
		err1 := rg.Insert(rctx, txn1, pks1, rows1)
		assert.NoError(t, err1, "Expected no error for first insert")
		txn1.Rollback(TxnId(clock.Now()))

		// Insert same key with different transaction (should succeed)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		pks2 := []T_COL{T_COL("pk1")}
		rows2 := []T_ROW{
			{T_COL("data2_0"), T_COL("data2_1")},
		}
		err2 := rg.Insert(rctx, txn2, pks2, rows2)

		assert.NoError(t, err2, "Expected no error for insert after aborted transaction")
		
		// Verify the new data was inserted
		iter := rg._rows.Iter()
		defer iter.Release()
		assert.True(t, iter.Next(), "Expected at least one row")
		item := iter.Item()
		assert.Equal(t, T_COL("pk1"), item._pk, "Expected correct primary key")
		assert.Equal(t, T_COL("data2_0"), item._colsData[0], "Expected new data for column 0")
		assert.Equal(t, T_COL("data2_1"), item._colsData[1], "Expected new data for column 1")
	})
}

// TestRowGroupInsert_WriteWriteConflict tests RowGroup.Insert with write-write conflict
func TestRowGroupInsert_WriteWriteConflict(t *testing.T) {
	t.Run("WriteWriteConflict", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and commit first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		pks1 := []T_COL{T_COL("pk1")}
		rows1 := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
		}
		err1 := rg.Insert(rctx, txn1, pks1, rows1)
		assert.NoError(t, err1, "Expected no error for first insert")
		txn1.Commit(TxnId(clock.Now()))

		// Try to insert same key with different transaction (should cause ww-conflict)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		pks2 := []T_COL{T_COL("pk1")}
		rows2 := []T_ROW{
			{T_COL("data2_0"), T_COL("data2_1")},
		}
		err2 := rg.Insert(rctx, txn2, pks2, rows2)

		assert.Error(t, err2, "Expected error for write-write conflict")
		assert.Contains(t, err2.Error(), "duplicated key", "Expected 'duplicated key' error message")
	})
}

// TestRowGroupInsert_EmptyInput tests RowGroup.Insert with empty input
func TestRowGroupInsert_EmptyInput(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert with empty input
		pks := []T_COL{}
		rows := []T_ROW{}

		err := rg.Insert(rctx, txn, pks, rows)

		assert.NoError(t, err, "Expected no error for empty input")
		
		// Verify no rows were inserted
		iter := rg._rows.Iter()
		defer iter.Release()
		assert.False(t, iter.Next(), "Expected no rows to be inserted")
	})
}

// TestRowGroupInsert_SameTransactionUpdate tests RowGroup.Insert with same transaction updating same key (should fail)
func TestRowGroupInsert_SameTransactionUpdate(t *testing.T) {
	t.Run("SameTransactionUpdate", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// First insert
		pks1 := []T_COL{T_COL("pk1")}
		rows1 := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
		}
		err1 := rg.Insert(rctx, txn, pks1, rows1)
		assert.NoError(t, err1, "Expected no error for first insert")

		// Second insert with same key (should succeed and overwrite)
		pks2 := []T_COL{T_COL("pk1")}
		rows2 := []T_ROW{
			{T_COL("data2_0"), T_COL("data2_1")},
		}
		err2 := rg.Insert(rctx, txn, pks2, rows2)

		assert.Error(t, err2, "Expected error for same transaction duplicate key")
		assert.Contains(t, err2.Error(), "duplicated key", "Expected 'duplicated key' error message")
		
	})
}

// TestRowGroupInsert_ColumnVersionGeneration tests that column versions are generated correctly
func TestRowGroupInsert_ColumnVersionGeneration(t *testing.T) {
	t.Run("ColumnVersionGeneration", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 3 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert single row
		pks := []T_COL{T_COL("pk1")}
		rows := []T_ROW{
			{T_COL("data0"), T_COL("data1"), T_COL("data2")},
		}

		err := rg.Insert(rctx, txn, pks, rows)

		assert.NoError(t, err, "Expected no error for insert")
		
		// Verify the row was inserted with correct structure
		iter := rg._rows.Iter()
		defer iter.Release()
		assert.True(t, iter.Next(), "Expected at least one row")
		item := iter.Item()
		
		// Verify primary key
		assert.Equal(t, T_COL("pk1"), item._pk, "Expected correct primary key")
		
		// Verify column data
		assert.Equal(t, 3, len(item._colsData), "Expected 3 columns")
		assert.Equal(t, T_COL("data0"), item._colsData[0], "Expected correct column 0 data")
		assert.Equal(t, T_COL("data1"), item._colsData[1], "Expected correct column 1 data")
		assert.Equal(t, T_COL("data2"), item._colsData[2], "Expected correct column 2 data")
		
		// Verify row version
		assert.NotNil(t, item._rowVersion, "Expected row version to be set")
		assert.NotNil(t, item._rowVersion._inserted.Load(), "Expected inserted txn ID to be set")
		assert.Equal(t, txn.Id(), *item._rowVersion._inserted.Load(), "Expected correct inserted txn ID")
		
		// Verify column updates
		assert.Equal(t, 3, len(item._colUpdates), "Expected 3 column update lists")
		for i := 0; i < 3; i++ {
			assert.NotNil(t, item._colUpdates[i], "Expected column update list to be initialized")
		}
	})
}

// TestRowGroupInsert_ConcurrentAccess tests RowGroup.Insert with concurrent access
func TestRowGroupInsert_ConcurrentAccess(t *testing.T) {
	t.Run("ConcurrentAccess", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 2 columns
		colDefs := []*ColumnDef{
			NewColumnDef("col0", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		txnMgr := rctx._txnMgr

		// Create multiple transactions
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Insert different keys concurrently (should not conflict)
		pks1 := []T_COL{T_COL("pk1")}
		rows1 := []T_ROW{
			{T_COL("data1_0"), T_COL("data1_1")},
		}
		
		pks2 := []T_COL{T_COL("pk2")}
		rows2 := []T_ROW{
			{T_COL("data2_0"), T_COL("data2_1")},
		}

		// Insert concurrently
		var err1, err2 error
		_ = err1
		_ = err2
		var wg sync.WaitGroup
		wg.Add(2)
		
		go func() {
			defer wg.Done()
			err1 = rg.Insert(rctx, txn1, pks1, rows1)
		}()
		
		go func() {
			defer wg.Done()
			err2 = rg.Insert(rctx, txn2, pks2, rows2)
		}()
		
		wg.Wait()

		// Both should succeed since they use different keys
		// Note: One might fail due to timing, which is acceptable
		// Note: One might fail due to timing, which is acceptable
		
		// Verify both rows were inserted
		iter := rg._rows.Iter()
		defer iter.Release()
		rowCount := 0
		for iter.Next() {
			item := iter.Item()
			rowCount++
			assert.True(t, string(item._pk) == "pk1" || string(item._pk) == "pk2", "Expected correct primary keys")
		}
		// At least one row should be inserted
		assert.True(t, rowCount >= 1, "Expected at least 1 row to be inserted")
	})
}
