package v2

import (
	"testing"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/assert"
)

// TestRowGroupFind_EmptyInput tests find with empty input
func TestRowGroupFind_EmptyInput(t *testing.T) {
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
		rg.find(rctx, rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted), []T_COL{}, []int{1, 2}, &resultExists, &resultRows)

		// Should not modify result arrays when input is empty
		assert.Nil(t, resultExists, "Expected resultExists to remain nil")
		assert.Nil(t, resultRows, "Expected resultRows to remain nil")
	})
}

// TestRowGroupFind_EmptyColIds tests find with empty column IDs
func TestRowGroupFind_EmptyColIds(t *testing.T) {
	t.Run("EmptyColIds", func(t *testing.T) {
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

		// Test with empty colIds
		resultExists := make([]bool, 2)
		resultRows := make([][]*T_COL, 2)
		rg.find(rctx, rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted), []T_COL{T_COL{1}, T_COL{2}}, []int{}, &resultExists, &resultRows)

		// Should not modify result arrays when colIds is empty
		assert.Equal(t, []bool{false, false}, resultExists, "Expected resultExists to remain unchanged")
		assert.Equal(t, [][]*T_COL{nil, nil}, resultRows, "Expected resultRows to remain unchanged")
	})
}

// TestRowGroupFind_NonExistentRows tests find with non-existent rows
func TestRowGroupFind_NonExistentRows(t *testing.T) {
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
			resultRows[i] = make([]*T_COL, 2)
			for j := range resultRows[i] {
				resultRows[i][j] = &T_COL{}
			}
		}

		rg.find(rctx, rctx._txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted), []T_COL{T_COL{100}, T_COL{200}}, []int{1, 2}, &resultExists, &resultRows)

		// Should mark all as non-existent
		assert.Equal(t, []bool{false, false}, resultExists, "Expected all rows to be non-existent")
	})
}

// TestRowGroupFind_VisibleRows tests find with visible rows
func TestRowGroupFind_VisibleRows(t *testing.T) {
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

		// Test find
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 2)
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.find(rctx, txn, []T_COL{T_COL{1}}, []int{1, 2}, &resultExists, &resultRows)

		// Should find the row
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{10}, *resultRows[0][0], "Expected col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][1], "Expected col2 data to be {20}")
	})
}

// TestRowGroupFind_InvisibleRows tests find with invisible rows
func TestRowGroupFind_InvisibleRows(t *testing.T) {
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

		// Test find with txn2
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 2)
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.find(rctx, txn2, []T_COL{T_COL{1}}, []int{1, 2}, &resultExists, &resultRows)

		// Should not find the row (invisible)
		assert.False(t, resultExists[0], "Expected row to be invisible")
	})
}

// TestRowGroupFind_MultipleRows tests find with multiple rows
func TestRowGroupFind_MultipleRows(t *testing.T) {
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

		// Test find
		resultExists := make([]bool, 2)
		resultRows := make([][]*T_COL, 2)
		for i := range resultRows {
			resultRows[i] = make([]*T_COL, 2)
			for j := range resultRows[i] {
				resultRows[i][j] = &T_COL{}
			}
		}

		rg.find(rctx, txn, []T_COL{T_COL{1}, T_COL{2}}, []int{1, 2}, &resultExists, &resultRows)

		// Should find both rows
		assert.True(t, resultExists[0], "Expected first row to exist")
		assert.True(t, resultExists[1], "Expected second row to exist")
		assert.Equal(t, T_COL{10}, *resultRows[0][0], "Expected first row col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][1], "Expected first row col2 data to be {20}")
		assert.Equal(t, T_COL{11}, *resultRows[1][0], "Expected second row col1 data to be {11}")
		assert.Equal(t, T_COL{21}, *resultRows[1][1], "Expected second row col2 data to be {21}")
	})
}

// TestRowGroupFind_MixedVisibility tests find with mixed visible and invisible rows
func TestRowGroupFind_MixedVisibility(t *testing.T) {
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

		// Test find
		resultExists := make([]bool, 2)
		resultRows := make([][]*T_COL, 2)
		for i := range resultRows {
			resultRows[i] = make([]*T_COL, 2)
			for j := range resultRows[i] {
				resultRows[i][j] = &T_COL{}
			}
		}

		rg.find(rctx, txn2, []T_COL{T_COL{1}, T_COL{2}}, []int{1, 2}, &resultExists, &resultRows)

		// Should find only the visible row
		assert.True(t, resultExists[0], "Expected first row to be visible")
		assert.False(t, resultExists[1], "Expected second row to be invisible")
		assert.Equal(t, T_COL{10}, *resultRows[0][0], "Expected first row col1 data to be {10}")
		assert.Equal(t, T_COL{20}, *resultRows[0][1], "Expected first row col2 data to be {20}")
	})
}

// TestRowGroupFind_WithMVCCRead tests find with MVCC read for column updates
func TestRowGroupFind_WithMVCCRead(t *testing.T) {
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

		// Test find
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 2)
		for j := range resultRows[0] {
			resultRows[0][j] = &T_COL{}
		}

		rg.find(rctx, txn2, []T_COL{T_COL{1}}, []int{1, 2}, &resultExists, &resultRows)

		// Should find the row and read the old version data from MVCC
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{100}, *resultRows[0][0], "Expected col1 data to be old version from MVCC read")
		assert.Equal(t, T_COL{20}, *resultRows[0][1], "Expected col2 data to be original value")
	})
}

// TestRowGroupFind_ConcurrentAccess tests find with concurrent access
func TestRowGroupFind_ConcurrentAccess(t *testing.T) {
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

		// Test concurrent find
		done := make(chan bool, 2)
		var resultExists1, resultExists2 []bool
		var resultRows1, resultRows2 [][]*T_COL

		go func() {
			resultExists1 = make([]bool, 1)
			resultRows1 = make([][]*T_COL, 1)
			resultRows1[0] = make([]*T_COL, 2)
			for j := range resultRows1[0] {
				resultRows1[0][j] = &T_COL{}
			}
			rg.find(rctx, txn, []T_COL{T_COL{1}}, []int{1, 2}, &resultExists1, &resultRows1)
			done <- true
		}()

		go func() {
			resultExists2 = make([]bool, 1)
			resultRows2 = make([][]*T_COL, 1)
			resultRows2[0] = make([]*T_COL, 2)
			for j := range resultRows2[0] {
				resultRows2[0][j] = &T_COL{}
			}
			rg.find(rctx, txn, []T_COL{T_COL{1}}, []int{1, 2}, &resultExists2, &resultRows2)
			done <- true
		}()

		// Wait for both goroutines to complete
		<-done
		<-done

		// Both should find the same row
		assert.True(t, resultExists1[0], "Expected first concurrent find to succeed")
		assert.True(t, resultExists2[0], "Expected second concurrent find to succeed")
		assert.Equal(t, T_COL{10}, *resultRows1[0][0], "Expected first concurrent find to get correct data")
		assert.Equal(t, T_COL{10}, *resultRows2[0][0], "Expected second concurrent find to get correct data")
	})
}

// TestRowGroupFind_SingleColumn tests find with single column
func TestRowGroupFind_SingleColumn(t *testing.T) {
	t.Run("SingleColumn", func(t *testing.T) {
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

		// Test find with single column
		resultExists := make([]bool, 1)
		resultRows := make([][]*T_COL, 1)
		resultRows[0] = make([]*T_COL, 1)
		resultRows[0][0] = &T_COL{}

		rg.find(rctx, txn, []T_COL{T_COL{1}}, []int{1}, &resultExists, &resultRows)

		// Should find the row and return only the requested column
		assert.True(t, resultExists[0], "Expected row to exist")
		assert.Equal(t, T_COL{10}, *resultRows[0][0], "Expected col1 data to be {10}")
	})
}
