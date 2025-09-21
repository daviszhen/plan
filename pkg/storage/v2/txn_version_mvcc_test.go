package v2

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMVCCRead_EmptyUpdateList tests MVCCRead with empty update list
func TestMVCCRead_EmptyUpdateList(t *testing.T) {
	t.Run("EmptyUpdateList", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create empty UpdateList
		updates := &UpdateList{
			_info: nil,
		}

		// Test MVCCRead with empty list
		var colData T_COL
		MVCCRead(txnMgr, txn, updates, &colData)

		// colData should remain unchanged (nil)
		assert.Nil(t, colData, "Expected colData to remain nil with empty update list")
	})
}

// TestMVCCRead_SingleInvisibleUpdate tests MVCCRead with single invisible update
func TestMVCCRead_SingleInvisibleUpdate(t *testing.T) {
	t.Run("SingleInvisibleUpdate", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList with single invisible update
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo from aborted transaction (invisible to txn2)
		updateInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version_data"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo._version.Store(&txn1Id)
		updates._info = updateInfo

		// Test MVCCRead with txn2
		var colData T_COL
		MVCCRead(txnMgr, txn2, updates, &colData)

		// Should read the old version data (invisible transaction's pre-modification data)
		assert.Equal(t, T_COL("old_version_data"), colData, "Expected to read old version data from invisible transaction")
	})
}

// TestMVCCRead_SingleVisibleUpdate tests MVCCRead with single visible update
func TestMVCCRead_SingleVisibleUpdate(t *testing.T) {
	t.Run("SingleVisibleUpdate", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create UpdateList with single visible update
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo from same transaction (visible)
		updateInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version_data"),
			_prev:    nil,
			_next:    nil,
		}
		txnId := txn.Id()
		updateInfo._version.Store(&txnId)
		updates._info = updateInfo

		// Test MVCCRead
		var colData T_COL
		MVCCRead(txnMgr, txn, updates, &colData)

		// Should not read any data (visible transaction means current txn has correct data)
		assert.Nil(t, colData, "Expected colData to remain nil with visible transaction")
	})
}

// TestMVCCRead_MultipleUpdatesNewestVisible tests MVCCRead with multiple updates, newest visible
func TestMVCCRead_MultipleUpdatesNewestVisible(t *testing.T) {
	t.Run("MultipleUpdatesNewestVisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create UpdateList with multiple updates
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo1 (visible - same transaction)
		updateInfo1 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version1"),
			_prev:    nil,
			_next:    nil,
		}
		txnId := txn.Id()
		updateInfo1._version.Store(&txnId)

		// Create UpdateInfo2 (invisible - aborted transaction)
		updateInfo2 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version2"),
			_prev:    nil,
			_next:    nil,
		}
		clock := rctx._clock
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		txn2.Rollback(TxnId(clock.Now()))
		txn2Id := txn2.Id()
		updateInfo2._version.Store(&txn2Id)

		// Link updates: updateInfo1 (newest) -> updateInfo2 (oldest)
		updateInfo1._next = updateInfo2
		updateInfo2._prev = updateInfo1
		updates._info = updateInfo1

		// Test MVCCRead
		var colData T_COL
		MVCCRead(txnMgr, txn, updates, &colData)

		// Should not read any data (newest transaction is visible)
		assert.Nil(t, colData, "Expected colData to remain nil when newest transaction is visible")
	})
}

// TestMVCCRead_MultipleUpdatesNewestInvisible tests MVCCRead with multiple updates, newest invisible
func TestMVCCRead_MultipleUpdatesNewestInvisible(t *testing.T) {
	t.Run("MultipleUpdatesNewestInvisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create and abort second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		txn2.Rollback(TxnId(clock.Now()))

		// Create third transaction
		txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

		// Create UpdateList with multiple updates
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo1 (invisible - from aborted txn1)
		updateInfo1 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version1"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo1._version.Store(&txn1Id)

		// Create UpdateInfo2 (invisible - from aborted txn2)
		updateInfo2 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version2"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		updateInfo2._version.Store(&txn2Id)

		// Link updates: updateInfo1 (newest) -> updateInfo2 (oldest)
		updateInfo1._next = updateInfo2
		updateInfo2._prev = updateInfo1
		updates._info = updateInfo1

		// Test MVCCRead with txn3
		var colData T_COL
		MVCCRead(txnMgr, txn3, updates, &colData)

		// Should read the last invisible data (old_version2)
		assert.Equal(t, T_COL("old_version2"), colData, "Expected to read last invisible data")
	})
}

// TestMVCCRead_MixedVisibilityNewestVisible tests MVCCRead with mixed visibility, newest visible
func TestMVCCRead_MixedVisibilityNewestVisible(t *testing.T) {
	t.Run("MixedVisibilityNewestVisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList with mixed visibility updates
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo1 (visible - from txn2)
		updateInfo1 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version1"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		updateInfo1._version.Store(&txn2Id)

		// Create UpdateInfo2 (invisible - from aborted txn1)
		updateInfo2 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version2"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo2._version.Store(&txn1Id)

		// Link updates: updateInfo1 (newest) -> updateInfo2 (oldest)
		updateInfo1._next = updateInfo2
		updateInfo2._prev = updateInfo1
		updates._info = updateInfo1

		// Test MVCCRead with txn2
		var colData T_COL
		MVCCRead(txnMgr, txn2, updates, &colData)

		// Should not read any data (newest transaction is visible)
		assert.Nil(t, colData, "Expected colData to remain nil when newest transaction is visible")
	})
}

// TestMVCCRead_MixedVisibilityNewestInvisible tests MVCCRead with mixed visibility, newest invisible
func TestMVCCRead_MixedVisibilityNewestInvisible(t *testing.T) {
	t.Run("MixedVisibilityNewestInvisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList with mixed visibility updates
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo1 (invisible - from aborted txn1)
		updateInfo1 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version1"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo1._version.Store(&txn1Id)

		// Create UpdateInfo2 (visible - from txn2)
		updateInfo2 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version2"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		updateInfo2._version.Store(&txn2Id)

		// Link updates: updateInfo1 (newest) -> updateInfo2 (oldest)
		updateInfo1._next = updateInfo2
		updateInfo2._prev = updateInfo1
		updates._info = updateInfo1

		// Test MVCCRead with txn2
		var colData T_COL
		MVCCRead(txnMgr, txn2, updates, &colData)

		// Should read the newest invisible data (old_version1)
		assert.Equal(t, T_COL("old_version1"), colData, "Expected to read newest invisible data")
	})
}

// TestMVCCRead_AllVisible tests MVCCRead with all visible updates
func TestMVCCRead_AllVisible(t *testing.T) {
	t.Run("AllVisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		txn := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

		// Create UpdateList with all visible updates
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo1 (visible - same transaction)
		updateInfo1 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version1"),
			_prev:    nil,
			_next:    nil,
		}
		txnId := txn.Id()
		updateInfo1._version.Store(&txnId)

		// Create UpdateInfo2 (visible - same transaction)
		updateInfo2 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version2"),
			_prev:    nil,
			_next:    nil,
		}
		updateInfo2._version.Store(&txnId)

		// Link updates: updateInfo1 (newest) -> updateInfo2 (oldest)
		updateInfo1._next = updateInfo2
		updateInfo2._prev = updateInfo1
		updates._info = updateInfo1

		// Test MVCCRead
		var colData T_COL
		MVCCRead(txnMgr, txn, updates, &colData)

		// Should not read any data (all transactions are visible)
		assert.Nil(t, colData, "Expected colData to remain nil when all transactions are visible")
	})
}

// TestMVCCRead_AbortedTransactionUpdate tests MVCCRead with aborted transaction update
func TestMVCCRead_AbortedTransactionUpdate(t *testing.T) {
	t.Run("AbortedTransactionUpdate", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList with aborted transaction update
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo from aborted transaction
		updateInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("aborted_old_version"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo._version.Store(&txn1Id)
		updates._info = updateInfo

		// Test MVCCRead with txn2
		var colData T_COL
		MVCCRead(txnMgr, txn2, updates, &colData)

		// Should read the aborted transaction's old version data
		assert.Equal(t, T_COL("aborted_old_version"), colData, "Expected to read aborted transaction's old version data")
	})
}

// TestMVCCRead_ConcurrentAccess tests MVCCRead with concurrent access
func TestMVCCRead_ConcurrentAccess(t *testing.T) {
	t.Run("ConcurrentAccess", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo from aborted transaction (invisible to txn2)
		updateInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("concurrent_old_version"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo._version.Store(&txn1Id)
		updates._info = updateInfo

		// Test MVCCRead concurrently
		var colData1, colData2 T_COL
		done := make(chan bool, 2)

		go func() {
			MVCCRead(txnMgr, txn2, updates, &colData1)
			done <- true
		}()

		go func() {
			MVCCRead(txnMgr, txn2, updates, &colData2)
			done <- true
		}()

		// Wait for both goroutines to complete
		<-done
		<-done

		// Both should read the same invisible data
		assert.Equal(t, T_COL("concurrent_old_version"), colData1, "Expected first read to get correct data")
		assert.Equal(t, T_COL("concurrent_old_version"), colData2, "Expected second read to get correct data")
	})
}

// TestMVCCRead_EmptyColData tests MVCCRead with empty column data
func TestMVCCRead_EmptyColData(t *testing.T) {
	t.Run("EmptyColData", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList with empty column data
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo with empty data
		updateInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL(nil),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo._version.Store(&txn1Id)
		updates._info = updateInfo

		// Test MVCCRead
		var colData T_COL
		MVCCRead(txnMgr, txn2, updates, &colData)

		// Should read the empty data
		assert.Equal(t, T_COL(nil), colData, "Expected to read empty data")
	})
}

// TestMVCCRead_LongChainNewestVisible tests MVCCRead with long chain, newest visible
func TestMVCCRead_LongChainNewestVisible(t *testing.T) {
	t.Run("LongChainNewestVisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

		// Create UpdateList with long chain
		updates := &UpdateList{
			_info: nil,
		}

		// Create chain: visible1 -> invisible2 -> invisible3 -> invisible4 -> invisible5
		var prev *UpdateInfo
		for i := 1; i <= 5; i++ {
			updateInfo := &UpdateInfo{
				_colIdx:  0,
				_colData: T_COL(fmt.Sprintf("old_version%d", i)),
				_prev:    prev,
				_next:    nil,
			}
			
			if i == 1 {
				// Visible update (from txn2)
				txn2Id := txn2.Id()
				updateInfo._version.Store(&txn2Id)
			} else {
				// Invisible updates (from aborted txn1)
				txn1Id := txn1.Id()
				updateInfo._version.Store(&txn1Id)
			}
			
			if prev != nil {
				prev._next = updateInfo
			} else {
				updates._info = updateInfo
			}
			prev = updateInfo
		}

		// Test MVCCRead with txn2
		var colData T_COL
		MVCCRead(txnMgr, txn2, updates, &colData)

		// Should not read any data (newest transaction is visible)
		assert.Nil(t, colData, "Expected colData to remain nil when newest transaction is visible")
	})
}

// TestMVCCRead_LongChainNewestInvisible tests MVCCRead with long chain, newest invisible
func TestMVCCRead_LongChainNewestInvisible(t *testing.T) {
	t.Run("LongChainNewestInvisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create and abort second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		txn2.Rollback(TxnId(clock.Now()))

		// Create third transaction
		txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

		// Create UpdateList with long chain
		updates := &UpdateList{
			_info: nil,
		}

		// Create chain: invisible1 -> invisible2 -> visible3 -> invisible4 -> invisible5
		var prev *UpdateInfo
		for i := 1; i <= 5; i++ {
			updateInfo := &UpdateInfo{
				_colIdx:  0,
				_colData: T_COL(fmt.Sprintf("old_version%d", i)),
				_prev:    prev,
				_next:    nil,
			}
			
			if i == 3 {
				// Visible update (from txn3)
				txn3Id := txn3.Id()
				updateInfo._version.Store(&txn3Id)
			} else if i <= 2 {
				// Invisible updates (from aborted txn1)
				txn1Id := txn1.Id()
				updateInfo._version.Store(&txn1Id)
			} else {
				// Invisible updates (from aborted txn2)
				txn2Id := txn2.Id()
				updateInfo._version.Store(&txn2Id)
			}
			
			if prev != nil {
				prev._next = updateInfo
			} else {
				updates._info = updateInfo
			}
			prev = updateInfo
		}

		// Test MVCCRead with txn3
		var colData T_COL
		MVCCRead(txnMgr, txn3, updates, &colData)

		// Should read the last invisible data before visible one (old_version2)
		assert.Equal(t, T_COL("old_version2"), colData, "Expected to read last invisible data before visible one in long chain")
	})
}

// TestMVCCRead_SnapshotIsolation tests MVCCRead with snapshot isolation
func TestMVCCRead_SnapshotIsolation(t *testing.T) {
	t.Run("SnapshotIsolation", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create snapshot isolation transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelSnapshotIsolation)

		// Create and abort second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		txn2.Rollback(TxnId(clock.Now()))

		// Create UpdateList
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo from aborted transaction (invisible to snapshot txn1)
		updateInfo := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("snapshot_old_version"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		updateInfo._version.Store(&txn2Id)
		updates._info = updateInfo

		// Test MVCCRead with snapshot isolation
		var colData T_COL
		MVCCRead(txnMgr, txn1, updates, &colData)

		// Should read the old version data (invisible to snapshot)
		assert.Equal(t, T_COL("snapshot_old_version"), colData, "Expected to read old version data with snapshot isolation")
	})
}

// TestMVCCRead_AllInvisible tests MVCCRead with all invisible updates
func TestMVCCRead_AllInvisible(t *testing.T) {
	t.Run("AllInvisible", func(t *testing.T) {
		rctx := InitRunContext("test")
		defer rctx.Close()

		txnMgr := rctx._txnMgr
		clock := rctx._clock

		// Create and abort first transaction
		txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
		txn1.Rollback(TxnId(clock.Now()))

		// Create and abort second transaction
		txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
		txn2.Rollback(TxnId(clock.Now()))

		// Create third transaction
		txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

		// Create UpdateList with all invisible updates
		updates := &UpdateList{
			_info: nil,
		}

		// Create UpdateInfo1 (invisible - from aborted txn1)
		updateInfo1 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version1"),
			_prev:    nil,
			_next:    nil,
		}
		txn1Id := txn1.Id()
		updateInfo1._version.Store(&txn1Id)

		// Create UpdateInfo2 (invisible - from aborted txn2)
		updateInfo2 := &UpdateInfo{
			_colIdx:  0,
			_colData: T_COL("old_version2"),
			_prev:    nil,
			_next:    nil,
		}
		txn2Id := txn2.Id()
		updateInfo2._version.Store(&txn2Id)

		// Link updates: updateInfo1 (newest) -> updateInfo2 (oldest)
		updateInfo1._next = updateInfo2
		updateInfo2._prev = updateInfo1
		updates._info = updateInfo1

		// Test MVCCRead with txn3
		var colData T_COL
		MVCCRead(txnMgr, txn3, updates, &colData)

		// Should read the last invisible data (old_version2)
		assert.Equal(t, T_COL("old_version2"), colData, "Expected to read last invisible data when all are invisible")
	})
}
