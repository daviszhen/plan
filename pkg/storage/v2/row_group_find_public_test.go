package v2

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/assert"
)

// TestRowGroupFindPublic_EmptyInput tests Find with empty input
func TestRowGroupFindPublic_EmptyInput(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		// Test with empty pks
		var resultExists []bool
		var resultRows [][]*T_COL
		rg.Find(rctx, rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted), []T_COL{}, &resultExists, &resultRows)

		// Should not modify result arrays when input is empty
		assert.Nil(t, resultExists, "Expected resultExists to remain nil")
		assert.Nil(t, resultRows, "Expected resultRows to remain nil")
	})
}

// TestRowGroupFindPublic_NonExistentRows tests Find with non-existent rows
func TestRowGroupFindPublic_NonExistentRows(t *testing.T) {
	t.Run("NonExistentRows", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		// Test with non-existent primary keys
		resultExists := make([]bool, 2)
		resultRows := make([][]*T_COL, 2)
		for i := range resultRows {
			resultRows[i] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
			for j := range resultRows[i] {
				resultRows[i][j] = &T_COL{}
			}
		}

		rg.Find(rctx, rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted), []T_COL{T_COL{100}, T_COL{200}}, &resultExists, &resultRows)

		// Should mark all as non-existent
		assert.Equal(t, []bool{false, false}, resultExists, "Expected all rows to be non-existent")
	})
}

// TestRowGroupFindPublic_VisibleRows tests Find with visible rows
func TestRowGroupFindPublic_VisibleRows(t *testing.T) {
	t.Run("VisibleRows", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		txn := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert a row - _colsData should have 3 elements (pk, col1, col2)
		item := &RowGroupItem{
			_pk:         T_COL{1},
			_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}}, // pk, col1, col2
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements: nil for pk, then col1, col2
		}
		txnId := txn.Id()
		item._rowVersion._inserted.Store(&txnId)

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			rg._rows.Set(item)
		})

		// Test Find
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.Find(rctx, txn, []T_COL{T_COL{1}}, &resultExists, &resultRows)

		// Should find the row and return all columns
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{1}, *resultRows[0][0], "Expected pk data to be {1}")
		assert.Equal(t, T_COL{10}, *resultRows[0][1], "Expected col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][2], "Expected col2 data to be {20}")
	})
}

// TestRowGroupFindPublic_InvisibleRows tests Find with invisible rows
func TestRowGroupFindPublic_InvisibleRows(t *testing.T) {
	t.Run("InvisibleRows", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		clock := rctx._clock

		// Create and abort a transaction
		txn1 := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create another transaction
		txn2 := rctx._txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Insert a row with aborted transaction (invisible to txn2)
		item := &RowGroupItem{
			_pk:         T_COL{1},
			_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}}, // pk, col1, col2
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
		}
		txn1Id := txn1.Id()
		item._rowVersion._inserted.Store(&txn1Id)

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			rg._rows.Set(item)
		})

		// Test Find with txn2
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.Find(rctx, txn2, []T_COL{T_COL{1}}, &resultExists, &resultRows)

		// Should not find the row (invisible)
		assert.False(t, resultExists[0], "Expected row to be invisible")
	})
}

// TestRowGroupFindPublic_MultipleRows tests Find with multiple rows
func TestRowGroupFindPublic_MultipleRows(t *testing.T) {
	t.Run("MultipleRows", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		txn := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert multiple rows
		items := []*RowGroupItem{
			{
				_pk:         T_COL{1},
				_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}}, // pk, col1, col2
				_rowVersion: &ChunkInfo{},
				_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
			},
			{
				_pk:         T_COL{2},
				_colsData:   []T_COL{T_COL{2}, T_COL{11}, T_COL{21}}, // pk, col1, col2
				_rowVersion: &ChunkInfo{},
				_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
			},
		}

		txnId := txn.Id()
		for _, item := range items {
			item._rowVersion._inserted.Store(&txnId)
		}

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			for _, item := range items {
				rg._rows.Set(item)
			}
		})

		// Test Find
		resultExists := make([]bool, 2)
		resultRows := make([][]*T_COL, 2)
		for i := range resultRows {
			resultRows[i] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
			for j := range resultRows[i] {
				resultRows[i][j] = &T_COL{}
			}
		}

		rg.Find(rctx, txn, []T_COL{T_COL{1}, T_COL{2}}, &resultExists, &resultRows)

		// Should find both rows and return all columns
		assert.True(t, resultExists[0], "Expected first row to exist")
		assert.True(t, resultExists[1], "Expected second row to exist")
		
		// First row
		assert.Equal(t, T_COL{1}, *resultRows[0][0], "Expected first row pk data to be {1}")
		assert.Equal(t, T_COL{10}, *resultRows[0][1], "Expected first row col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][2], "Expected first row col2 data to be {20}")
		
		// Second row
		assert.Equal(t, T_COL{2}, *resultRows[1][0], "Expected second row pk data to be {2}")
		assert.Equal(t, T_COL{11}, *resultRows[1][1], "Expected second row col1 data to be {11}")
		assert.Equal(t, T_COL{21}, *resultRows[1][2], "Expected second row col2 data to be {21}")
	})
}

// TestRowGroupFindPublic_MixedVisibility tests Find with mixed visible and invisible rows
func TestRowGroupFindPublic_MixedVisibility(t *testing.T) {
	t.Run("MixedVisibility", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		clock := rctx._clock

		// Create and abort a transaction
		txn1 := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create another transaction
		txn2 := rctx._txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Insert rows with different visibility
		items := []*RowGroupItem{
			{
				_pk:         T_COL{1},
				_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}}, // pk, col1, col2
				_rowVersion: &ChunkInfo{},
				_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
			},
			{
				_pk:         T_COL{2},
				_colsData:   []T_COL{T_COL{2}, T_COL{11}, T_COL{21}}, // pk, col1, col2
				_rowVersion: &ChunkInfo{},
				_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
			},
		}

		// First row visible to txn2, second row invisible (from aborted txn1)
		txn2Id := txn2.Id()
		txn1Id := txn1.Id()
		items[0]._rowVersion._inserted.Store(&txn2Id)
		items[1]._rowVersion._inserted.Store(&txn1Id)

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			for _, item := range items {
				rg._rows.Set(item)
			}
		})

		// Test Find
		resultExists := make([]bool, 2)
		resultRows := make([][]*T_COL, 2)
		for i := range resultRows {
			resultRows[i] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
			for j := range resultRows[i] {
				resultRows[i][j] = &T_COL{}
			}
		}

		rg.Find(rctx, txn2, []T_COL{T_COL{1}, T_COL{2}}, &resultExists, &resultRows)

		// Should find only the visible row
		assert.True(t, resultExists[0], "Expected first row to be visible")
		assert.False(t, resultExists[1], "Expected second row to be invisible")
		
		// First row should have all columns
		assert.Equal(t, T_COL{1}, *resultRows[0][0], "Expected first row pk data to be {1}")
		assert.Equal(t, T_COL{10}, *resultRows[0][1], "Expected first row col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][2], "Expected first row col2 data to be {20}")
	})
}

// TestRowGroupFindPublic_WithMVCCRead tests Find with MVCC read for column updates
func TestRowGroupFindPublic_WithMVCCRead(t *testing.T) {
	t.Run("WithMVCCRead", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		clock := rctx._clock

		// Create and abort a transaction
		txn1 := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create another transaction
		txn2 := rctx._txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Insert a row with column updates from aborted transaction
		item := &RowGroupItem{
			_pk:         T_COL{1},
			_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}}, // pk, col1, col2
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
		}
		txn2Id := txn2.Id()
		item._rowVersion._inserted.Store(&txn2Id)

		// Add column update from aborted transaction (invisible to txn2)
		updateInfo := &UpdateInfo{
			_colIdx:  1,
			_colData: T_COL{100}, // Updated value
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo._version.Store(&txn1Id)
		item._colUpdates[1]._info = updateInfo

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			rg._rows.Set(item)
		})

		// Test Find
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.Find(rctx, txn2, []T_COL{T_COL{1}}, &resultExists, &resultRows)

		// Should find the row and read the old version data from MVCC
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{1}, *resultRows[0][0], "Expected pk data to be {1}")
		assert.Equal(t, T_COL{100}, *resultRows[0][1], "Expected col1 data to be old version from MVCC read")
		assert.Equal(t, T_COL{20}, *resultRows[0][2], "Expected col2 data to be original value")
	})
}

// TestRowGroupFindPublic_ConcurrentAccess tests Find with concurrent access
func TestRowGroupFindPublic_ConcurrentAccess(t *testing.T) {
	t.Run("ConcurrentAccess", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with column definitions
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		txn := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert a row
		item := &RowGroupItem{
			_pk:         T_COL{1},
			_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}}, // pk, col1, col2
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{nil, &UpdateList{_info: nil}, &UpdateList{_info: nil}}, // 3 elements
		}
		txnId := txn.Id()
		item._rowVersion._inserted.Store(&txnId)

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			rg._rows.Set(item)
		})

		// Test concurrent Find
		done := make(chan bool, 2)
		var resultExists1, resultExists2 []bool
		var resultRows1, resultRows2 [][]*T_COL

		go func() {
			resultExists1 = make([]bool, 1)
			resultRows1 = make([][]*T_COL, 1)
			resultRows1[0] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
			for j := range resultRows1[0] {
				resultRows1[0][j] = &T_COL{}
			}
			rg.Find(rctx, txn, []T_COL{T_COL{1}}, &resultExists1, &resultRows1)
			done <- true
		}()

		go func() {
			resultExists2 = make([]bool, 1)
			resultRows2 = make([][]*T_COL, 1)
			resultRows2[0] = make([]*T_COL, 3) // 3 columns: pk, col1, col2
			for j := range resultRows2[0] {
				resultRows2[0][j] = &T_COL{}
			}
			rg.Find(rctx, txn, []T_COL{T_COL{1}}, &resultExists2, &resultRows2)
			done <- true
		}()

		// Wait for both goroutines to complete
		<-done
		<-done

		// Both should find the same row with all columns
		assert.True(t, resultExists1[0], "Expected first concurrent find to succeed")
		assert.True(t, resultExists2[0], "Expected second concurrent find to succeed")
		
		// Both should get the same data for all columns
		assert.Equal(t, T_COL{1}, *resultRows1[0][0], "Expected first concurrent find to get correct pk data")
		assert.Equal(t, T_COL{10}, *resultRows1[0][1], "Expected first concurrent find to get correct col1 data")
		assert.Equal(t, T_COL{20}, *resultRows1[0][2], "Expected first concurrent find to get correct col2 data")
		
		assert.Equal(t, T_COL{1}, *resultRows2[0][0], "Expected second concurrent find to get correct pk data")
		assert.Equal(t, T_COL{10}, *resultRows2[0][1], "Expected second concurrent find to get correct col1 data")
		assert.Equal(t, T_COL{20}, *resultRows2[0][2], "Expected second concurrent find to get correct col2 data")
	})
}

// TestRowGroupFindPublic_SingleColumnSchema tests Find with single column schema
func TestRowGroupFindPublic_SingleColumnSchema(t *testing.T) {
	t.Run("SingleColumnSchema", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with only primary key
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		txn := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert a row
		item := &RowGroupItem{
			_pk:         T_COL{1},
			_colsData:   []T_COL{T_COL{1}}, // only pk
			_rowVersion: &ChunkInfo{},
			_colUpdates: []*UpdateList{nil}, // only pk column
		}
		txnId := txn.Id()
		item._rowVersion._inserted.Store(&txnId)

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			rg._rows.Set(item)
		})

		// Test Find
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 1) // only pk column
		resultRows[0][0] = &T_COL{}

		rg.Find(rctx, txn, []T_COL{T_COL{1}}, &resultExists, &resultRows)

		// Should find the row and return only the pk column
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{1}, *resultRows[0][0], "Expected pk data to be {1}")
	})
}

// TestRowGroupFindPublic_LargeSchema tests Find with large schema
func TestRowGroupFindPublic_LargeSchema(t *testing.T) {
	t.Run("LargeSchema", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with many columns
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
		}
		// Add 10 more columns
		for i := 1; i <= 10; i++ {
			colDefs = append(colDefs, NewColumnDef("col"+string(rune('0'+i)), common.IntegerType()))
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)
		txn := rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Insert a row with many columns
		item := &RowGroupItem{
			_pk:         T_COL{1},
			_colsData:   []T_COL{T_COL{1}, T_COL{10}, T_COL{20}, T_COL{30}, T_COL{40}, T_COL{50}, T_COL{60}, T_COL{70}, T_COL{80}, T_COL{90}, T_COL{100}}, // pk + 10 columns
			_rowVersion: &ChunkInfo{},
			_colUpdates: make([]*UpdateList, 11), // 11 columns
		}
		// Initialize all column updates
		for i := range item._colUpdates {
			item._colUpdates[i] = &UpdateList{_info: nil}
		}
		
		txnId := txn.Id()
		item._rowVersion._inserted.Store(&txnId)

		// Add to row group
		util.ScopeLock(&rg._rowsLock, func() {
			rg._rows.Set(item)
		})

		// Test Find
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 11) // 11 columns: pk + 10 columns
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.Find(rctx, txn, []T_COL{T_COL{1}}, &resultExists, &resultRows)

		// Should find the row and return all columns
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{1}, *resultRows[0][0], "Expected pk data to be {1}")
		assert.Equal(t, T_COL{10}, *resultRows[0][1], "Expected col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][2], "Expected col2 data to be {20}")
		assert.Equal(t, T_COL{100}, *resultRows[0][10], "Expected col10 data to be {100}")
	})
}

// TestRowGroupFindPublic_ColumnCountVerification tests that Find uses correct column count
func TestRowGroupFindPublic_ColumnCountVerification(t *testing.T) {
	t.Run("ColumnCountVerification", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		// Create schema with 5 columns
		colDefs := []*ColumnDef{
			NewColumnDef("pk", common.IntegerType()),
			NewColumnDef("col1", common.IntegerType()),
			NewColumnDef("col2", common.IntegerType()),
			NewColumnDef("col3", common.IntegerType()),
			NewColumnDef("col4", common.IntegerType()),
		}
		schema := NewSchema(colDefs)
		rg := NewRowGroup(schema)

		// Verify that the schema has 5 columns
		assert.Equal(t, 5, schema.ColumnCount(), "Expected schema to have 5 columns")

		// Test that Find creates colIds array with all column indices
		// This is tested indirectly by ensuring the result has the correct number of columns
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 5) // 5 columns
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		// This should not panic and should handle the empty case correctly
		rg.Find(rctx, rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted), []T_COL{T_COL{1}}, &resultExists, &resultRows)

		// Should mark as non-existent since no row was inserted
		assert.False(t, resultExists[0], "Expected row to be non-existent")
	})
}
