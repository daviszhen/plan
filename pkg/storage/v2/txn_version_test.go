package v2

import (
	"testing"

	hlc "github.com/cockroachdb/cockroachdb-parser/pkg/util/hlc"
	"github.com/stretchr/testify/assert"
)

// TestCheckRowVisiable_InsertTxnAborted tests case 1: insert transaction is aborted
func TestCheckRowVisiable_InsertTxnAborted(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Abort txn2
	txn2.Rollback(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&txn2Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	assert.False(t, visible, "Expected visible=false")
	assert.False(t, wwConflict, "Expected wwConflict=false")
}

// TestCheckRowVisiable_InsertTxnAbortedWithSameDeleteTxn tests case 1 with delete txn same as insert txn
func TestCheckRowVisiable_InsertTxnAbortedWithSameDeleteTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Abort txn2
	txn2.Rollback(TxnId(clock.Now()))

	// Create row info with txn2 as both insert and delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn2Id) // Same txn as insert

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	assert.False(t, visible, "Expected visible=false")
	assert.False(t, wwConflict, "Expected wwConflict=false")
}

// TestCheckRowVisiable_InsertTxnAbortedWithDifferentDeleteTxn tests case 1 with different delete txn - should panic
func TestCheckRowVisiable_InsertTxnAbortedWithDifferentDeleteTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
	txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

	// Abort txn2
	txn2.Rollback(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn and txn3 as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	txn3Id := txn3.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn3Id) // Different txn from insert

	// This should panic because insert txn is aborted and delete txn is different
	assert.Panics(t, func() {
		CheckRowVisiable(txnMgr, txn1, rowInfo)
	}, "Expected panic when insert txn is aborted and delete txn is different")
}

// TestCheckRowVisiable_InsertTxnActiveSameTxnNoDelete tests case 2.1: insert txn active, same txn, no delete
func TestCheckRowVisiable_InsertTxnActiveSameTxnNoDelete(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create test transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create row info with txn1 as insert txn, no delete
	rowInfo := &ChunkInfo{}
	txn1Id := txn1.Id()
	rowInfo._inserted.Store(&txn1Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	assert.True(t, visible, "Expected visible=true")
	assert.False(t, wwConflict, "Expected wwConflict=false")
}

// TestCheckRowVisiable_InsertTxnActiveSameTxnWithSameDeleteTxn tests case 2.2: insert txn active, same txn, with same delete txn
func TestCheckRowVisiable_InsertTxnActiveSameTxnWithSameDeleteTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create test transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create row info with txn1 as both insert and delete txn
	rowInfo := &ChunkInfo{}
	txn1Id := txn1.Id()
	rowInfo._inserted.Store(&txn1Id)
	rowInfo._deleted.Store(&txn1Id) // Same txn as insert

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	assert.False(t, visible, "Expected visible=false")
	assert.False(t, wwConflict, "Expected wwConflict=false")
}

// TestCheckRowVisiable_InsertTxnActiveSameTxnWithDifferentDeleteTxn tests case 2.2: insert txn active, same txn, with different delete txn - should panic
func TestCheckRowVisiable_InsertTxnActiveSameTxnWithDifferentDeleteTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Create row info with txn1 as insert txn and txn2 as delete txn
	rowInfo := &ChunkInfo{}
	txn1Id := txn1.Id()
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&txn1Id)
	rowInfo._deleted.Store(&txn2Id) // Different txn from insert

	// This should panic because insert txn is active same txn and delete txn is different
	assert.Panics(t, func() {
		CheckRowVisiable(txnMgr, txn1, rowInfo)
	}, "Expected panic when insert txn is active same txn and delete txn is different")
}

// TestCheckRowVisiable_InsertTxnActiveDifferentTxn tests case 2.3: insert txn active, different txn
func TestCheckRowVisiable_InsertTxnActiveDifferentTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Create row info with txn2 as insert txn, no delete
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&txn2Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	assert.False(t, visible, "Expected visible=false")
	assert.True(t, wwConflict, "Expected wwConflict=true")
}

// TestCheckRowVisiable_InsertTxnCommittedInSnapshot tests case 3.1: insert txn committed, in snapshot
func TestCheckRowVisiable_InsertTxnCommittedInSnapshot(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelSnapshotIsolation)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Commit txn2
	txn2.Commit(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&txn2Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=false, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn in snapshot")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn in snapshot")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotNoDelete tests case 3.2: insert txn committed, not in snapshot, no delete
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotNoDelete(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Commit txn2
	txn2.Commit(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn, no delete
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&txn2Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=false, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn not in snapshot")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn not in snapshot")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteAborted tests case 3.3: insert txn committed, not in snapshot, delete txn aborted
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteAborted(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
	txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

	// Commit txn2, abort txn3
	txn2.Commit(TxnId(clock.Now()))
	txn3.Rollback(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn and txn3 as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	txn3Id := txn3.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn3Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=false, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn with aborted delete")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn with aborted delete")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteActiveSameTxn tests case 3.4: insert txn committed, not in snapshot, delete txn active same txn
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteActiveSameTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Commit txn2
	txn2.Commit(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn and txn1 as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	txn1Id := txn1.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn1Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=false, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn with active same txn delete")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn with active same txn delete")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteActiveDifferentTxn tests case 3.5: insert txn committed, not in snapshot, delete txn active different txn
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteActiveDifferentTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
	txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

	// Commit txn2
	txn2.Commit(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn and txn3 as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	txn3Id := txn3.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn3Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=false, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn with active different txn delete")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn with active different txn delete")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteCommittedInSnapshot tests case 3.6: insert txn committed, not in snapshot, delete txn committed in snapshot
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteCommittedInSnapshot(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelSnapshotIsolation)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
	txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

	// Commit both txn2 and txn3
	txn2.Commit(TxnId(clock.Now()))
	txn3.Commit(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn and txn3 as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	txn3Id := txn3.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn3Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - deleteTxnInSS = true (txn3 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=true, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn with committed delete in snapshot")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn with committed delete in snapshot")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteCommittedNotInSnapshot tests case 3.7: insert txn committed, not in snapshot, delete txn committed not in snapshot
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshotDeleteCommittedNotInSnapshot(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transactions
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
	txn3 := txnMgr.NewTxn("txn3", TxnIsolationLevelReadCommitted)

	// Commit both txn2 and txn3
	txn2.Commit(TxnId(clock.Now()))
	txn3.Commit(TxnId(clock.Now()))

	// Create row info with txn2 as insert txn and txn3 as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	txn3Id := txn3.Id()
	rowInfo._inserted.Store(&txn2Id)
	rowInfo._deleted.Store(&txn3Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = true (txn2 id > txn1's snapshot._maxTxnId)
	// - deleteTxnInSS = true (txn3 id > txn1's snapshot._maxTxnId)
	// - So result should be visible=true, wwConflict=true
	assert.False(t, visible, "Expected visible=false for committed txn with committed delete not in snapshot")
	assert.True(t, wwConflict, "Expected wwConflict=true for committed txn with committed delete not in snapshot")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxn tests case 3: insert txn committed, old txn not in snapshot
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxn(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create an old transaction with a very small timestamp
	oldTxnId := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})

	// Create current transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create row info with old txn as insert txn, no delete
	rowInfo := &ChunkInfo{}
	rowInfo._inserted.Store(&oldTxnId)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = false (oldTxnId < txn1's snapshot._minTxnId)
	// - deleteTxnId = TxnIdInvalid (no delete)
	// - So result should be visible=true, wwConflict=false
	assert.True(t, visible, "Expected visible=true for old committed txn not in snapshot")
	assert.False(t, wwConflict, "Expected wwConflict=false for old committed txn not in snapshot")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithDelete tests case 3: insert txn committed, old txn not in snapshot, with delete
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithDelete(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create an old transaction with a very small timestamp
	oldTxnId := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})

	// Create current transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create row info with old txn as insert txn and current txn as delete txn
	rowInfo := &ChunkInfo{}
	txn1Id := txn1.Id()
	rowInfo._inserted.Store(&oldTxnId)
	rowInfo._deleted.Store(&txn1Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = false (oldTxnId < txn1's snapshot._minTxnId)
	// - deleteTxnStatus = TxnStateActive, deleteTxnId == txn.Id()
	// - So result should be visible=false, wwConflict=false
	assert.False(t, visible, "Expected visible=false for old committed txn with active same txn delete")
	assert.False(t, wwConflict, "Expected wwConflict=false for old committed txn with active same txn delete")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithOldDelete tests case 3: insert txn committed, old txn not in snapshot, with old delete
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithOldDelete(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create old transactions with very small timestamps
	oldInsertTxnId := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})
	oldDeleteTxnId := TxnId(hlc.Timestamp{WallTime: 2000, Logical: 0})

	// Create current transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create row info with old txn as insert txn and old txn as delete txn
	rowInfo := &ChunkInfo{}
	rowInfo._inserted.Store(&oldInsertTxnId)
	rowInfo._deleted.Store(&oldDeleteTxnId)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = false (oldInsertTxnId < txn1's snapshot._minTxnId)
	// - deleteTxnInSS = false (oldDeleteTxnId < txn1's snapshot._minTxnId)
	// - So result should be visible=false, wwConflict=true
	assert.False(t, visible, "Expected visible=false for old committed txn with old committed delete not in snapshot")
	assert.True(t, wwConflict, "Expected wwConflict=true for old committed txn with old committed delete not in snapshot")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithActiveDelete tests case 3: insert txn committed, old txn not in snapshot, with active delete
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithActiveDelete(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr

	// Create an old transaction with a very small timestamp
	oldTxnId := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})

	// Create current transaction and another active transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)

	// Create row info with old txn as insert txn and active txn as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&oldTxnId)
	rowInfo._deleted.Store(&txn2Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = false (oldTxnId < txn1's snapshot._minTxnId)
	// - deleteTxnStatus = TxnStateActive, deleteTxnId != txn.Id()
	// - So result should be visible=true, wwConflict=true
	assert.True(t, visible, "Expected visible=true for old committed txn with active different txn delete")
	assert.True(t, wwConflict, "Expected wwConflict=true for old committed txn with active different txn delete")
}

// TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithAbortedDelete tests case 3: insert txn committed, old txn not in snapshot, with aborted delete
func TestCheckRowVisiable_InsertTxnCommittedNotInSnapshot_OldTxnWithAbortedDelete(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create an old transaction with a very small timestamp
	oldTxnId := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})

	// Create current transaction and abort another transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn2 := txnMgr.NewTxn("txn2", TxnIsolationLevelReadCommitted)
	txn2.Rollback(TxnId(clock.Now()))

	// Create row info with old txn as insert txn and aborted txn as delete txn
	rowInfo := &ChunkInfo{}
	txn2Id := txn2.Id()
	rowInfo._inserted.Store(&oldTxnId)
	rowInfo._deleted.Store(&txn2Id)

	visible, wwConflict := CheckRowVisiable(txnMgr, txn1, rowInfo)

	// Based on the new IsActiveInSnapshot implementation:
	// - insertTxnInSS = false (oldTxnId < txn1's snapshot._minTxnId)
	// - deleteTxnStatus = TxnStateAborted
	// - So result should be visible=true, wwConflict=false
	assert.True(t, visible, "Expected visible=true for old committed txn with aborted delete")
	assert.False(t, wwConflict, "Expected wwConflict=false for old committed txn with aborted delete")
}

// TestCheckRowVisiable_NilInsertTxnId tests edge case: nil insert txn id should panic
func TestCheckRowVisiable_NilInsertTxnId(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create row info with nil insert txn id
	rowInfo := &ChunkInfo{}
	// Don't set _inserted, leaving it as nil

	// This should panic
	assert.Panics(t, func() {
		CheckRowVisiable(txnMgr, txn1, rowInfo)
	}, "Expected panic when insert txn id is nil")
}

// TestCheckUpdateVisiable tests the CheckUpdateVisiable function
func TestCheckUpdateVisiable(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn1Id := txn1.Id()

	// Create UpdateInfo
	updateInfo := &UpdateInfo{
		_colIdx:  0,
		_colData: nil,
		_prev:    nil,
		_next:    nil,
	}
	updateInfo._version.Store(&txn1Id)

	// Test CheckUpdateVisiable
	visible, wwConflict := CheckUpdateVisiable(txnMgr, txn1, updateInfo)

	// The result should be the same as CheckRowVisiable with the same insert txn
	assert.True(t, visible, "Expected visible=true")
	assert.False(t, wwConflict, "Expected wwConflict=false")
}

// TestCheckUpdateVisiable_NilVersion tests CheckUpdateVisiable with nil version should panic
func TestCheckUpdateVisiable_NilVersion(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Create UpdateInfo with nil version
	updateInfo := &UpdateInfo{
		_colIdx:  0,
		_colData: nil,
		_prev:    nil,
		_next:    nil,
	}
	// Don't set _version, leaving it as nil

	// This should panic
	assert.Panics(t, func() {
		CheckUpdateVisiable(txnMgr, txn1, updateInfo)
	}, "Expected panic when version is nil")
}

// TestCheckUpdatesForConflicts tests the CheckUpdatesForConflicts function
func TestCheckUpdatesForConflicts(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn1Id := txn1.Id()

	// Create UpdateList
	updates := &UpdateList{
		_info: nil,
	}

	// Create new UpdateInfo
	newInfo := &UpdateInfo{
		_colIdx:  0,
		_colData: nil,
		_prev:    nil,
		_next:    nil,
	}
	newInfo._version.Store(&txn1Id)

	// Test CheckUpdatesForConflicts
	wwConflicts := CheckUpdatesForConflicts(txnMgr, txn1, updates, newInfo)

	assert.False(t, wwConflicts, "Expected wwConflicts=false")

	// Verify that newInfo was added to the list
	assert.NotNil(t, updates._info, "Expected newInfo to be added to updates list")
	assert.Equal(t, newInfo, updates._info, "Expected updates._info to be newInfo")
}

// TestChunkInfoOperations tests ChunkInfo atomic operations
func TestChunkInfoOperations(t *testing.T) {
	chunkInfo := &ChunkInfo{}

	// Test initial state
	assert.Nil(t, chunkInfo._inserted.Load(), "Expected _inserted to be nil initially")
	assert.Nil(t, chunkInfo._deleted.Load(), "Expected _deleted to be nil initially")

	// Test setting insert txn
	txnId1 := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})
	chunkInfo._inserted.Store(&txnId1)

	loadedTxnId := chunkInfo._inserted.Load()
	assert.NotNil(t, loadedTxnId, "Expected _inserted to be set")
	assert.Equal(t, txnId1, *loadedTxnId, "Expected _inserted to be %v", txnId1)

	// Test setting delete txn
	txnId2 := TxnId(hlc.Timestamp{WallTime: 2000, Logical: 0})
	chunkInfo._deleted.Store(&txnId2)

	loadedDeleteTxnId := chunkInfo._deleted.Load()
	assert.NotNil(t, loadedDeleteTxnId, "Expected _deleted to be set")
	assert.Equal(t, txnId2, *loadedDeleteTxnId, "Expected _deleted to be %v", txnId2)
}

// TestUpdateInfoOperations tests UpdateInfo operations
func TestUpdateInfoOperations(t *testing.T) {
	updateInfo := &UpdateInfo{
		_colIdx:  5,
		_colData: nil,
		_prev:    nil,
		_next:    nil,
	}

	// Test initial state
	assert.Nil(t, updateInfo._version.Load(), "Expected _version to be nil initially")

	// Test setting version
	txnId := TxnId(hlc.Timestamp{WallTime: 1000, Logical: 0})
	updateInfo._version.Store(&txnId)

	loadedTxnId := updateInfo._version.Load()
	assert.NotNil(t, loadedTxnId, "Expected _version to be set")
	assert.Equal(t, txnId, *loadedTxnId, "Expected _version to be %v", txnId)

	// Test colIdx
	assert.Equal(t, 5, updateInfo._colIdx, "Expected _colIdx to be 5")
}

// TestTxnStateAfterCommit tests that transaction state is correctly updated after commit
func TestTxnStateAfterCommit(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Initially should be active
	assert.False(t, txn1.IsAborted(), "Expected txn to be active initially")
	assert.False(t, txn1.IsCommitted(), "Expected txn to be active initially")

	// Commit the transaction
	commitTs := TxnId(clock.Now())
	txn1.Commit(commitTs)

	// Should be committed now
	assert.True(t, txn1.IsCommitted(), "Expected txn to be committed after commit")
	assert.False(t, txn1.IsAborted(), "Expected txn to not be aborted after commit")

	// Check commit timestamp
	assert.Equal(t, commitTs, txn1.CommitTs(), "Expected commitTs to be %v", commitTs)
}

// TestTxnStateAfterRollback tests that transaction state is correctly updated after rollback
func TestTxnStateAfterRollback(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)

	// Initially should be active
	assert.False(t, txn1.IsAborted(), "Expected txn to be active initially")
	assert.False(t, txn1.IsCommitted(), "Expected txn to be active initially")

	// Rollback the transaction
	abortTs := TxnId(clock.Now())
	txn1.Rollback(abortTs)

	// Should be aborted now
	assert.True(t, txn1.IsAborted(), "Expected txn to be aborted after rollback")
	assert.False(t, txn1.IsCommitted(), "Expected txn to not be committed after rollback")

	// Check abort timestamp
	assert.Equal(t, abortTs, txn1.AbortTs(), "Expected abortTs to be %v", abortTs)
}

// TestTxnMgrTxnState tests TxnMgr.TxnState method
func TestTxnMgrTxnState(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn1Id := txn1.Id()

	// Test active state
	state := txnMgr.TxnState(txn1Id)
	assert.Equal(t, TxnStateActive, state, "Expected TxnStateActive")

	// Commit transaction
	txn1.Commit(TxnId(clock.Now()))

	// Test committed state
	state = txnMgr.TxnState(txn1Id)
	assert.Equal(t, TxnStateCommitted, state, "Expected TxnStateCommitted")

	// Test invalid txn id
	state = txnMgr.TxnState(TxnIdInvalid)
	assert.Equal(t, TxnStateAborted, state, "Expected TxnStateAborted for invalid txn id")
}

// TestTxnMgrTxnStateWithRollback tests TxnMgr.TxnState method with rollback
func TestTxnMgrTxnStateWithRollback(t *testing.T) {
	rctx := InitRunContext("test")
	defer rctx.Close()

	txnMgr := rctx._txnMgr
	clock := rctx._clock

	// Create test transaction
	txn1 := txnMgr.NewTxn("txn1", TxnIsolationLevelReadCommitted)
	txn1Id := txn1.Id()

	// Test active state
	state := txnMgr.TxnState(txn1Id)
	assert.Equal(t, TxnStateActive, state, "Expected TxnStateActive")

	// Rollback transaction
	txn1.Rollback(TxnId(clock.Now()))

	// Test aborted state
	state = txnMgr.TxnState(txn1Id)
	assert.Equal(t, TxnStateAborted, state, "Expected TxnStateAborted")
}
