package sdk

import (
	"context"
	"testing"
	"time"

	"github.com/daviszhen/plan/pkg/storage2"
	"github.com/stretchr/testify/require"
)

// TestDetachedTransaction_SDK_CommitFlow tests the SDK layer detached transaction commit flow.
// This corresponds to Lance Java DatasetTest.testCommitTransactionDetachedTrue
func TestDetachedTransaction_SDK_CommitFlow(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create detached append transaction
	txnID, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, txnID)

	// Verify initial status
	state, err := dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, storage2.DetachedStatusPending, state.Status)
	require.Equal(t, txnID, state.ID)
	require.NotNil(t, state.Transaction)
	require.NotNil(t, state.ExpiresAt)

	// Commit the detached transaction
	version, err := dataset.CommitDetached(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Verify committed status
	state, err = dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusCommitted, state.Status)
	require.NotNil(t, state.ResultVersion)
	require.Equal(t, uint64(1), *state.ResultVersion)

	// Verify dataset version updated
	require.Equal(t, uint64(1), dataset.Version())
}

// TestDetachedTransaction_SDK_StatusQueries tests SDK status query functionality.
func TestDetachedTransaction_SDK_StatusQueries(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create multiple detached transactions
	id1, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)

	id2, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)

	id3, err := dataset.CreateDetachedDelete(ctx, "id > 10", nil)
	require.NoError(t, err)

	// Commit second transaction
	_, err = dataset.CommitDetached(ctx, id2)
	require.NoError(t, err)

	// Test GetDetachedStatus
	state1, err := dataset.GetDetachedStatus(ctx, id1)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusPending, state1.Status)

	state2, err := dataset.GetDetachedStatus(ctx, id2)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusCommitted, state2.Status)
	require.NotNil(t, state2.ResultVersion)

	state3, err := dataset.GetDetachedStatus(ctx, id3)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusPending, state3.Status)

	// Test ListDetached with different statuses
	all, err := dataset.ListDetached(ctx, "")
	require.NoError(t, err)
	require.Len(t, all, 3)

	pending, err := dataset.ListDetached(ctx, storage2.DetachedStatusPending)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	committed, err := dataset.ListDetached(ctx, storage2.DetachedStatusCommitted)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	require.Equal(t, id2, committed[0].ID)
}

// TestDetachedTransaction_SDK_Expiration tests SDK expiration functionality.
func TestDetachedTransaction_SDK_Expiration(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create transaction with short timeout
	timeout := 10 * time.Millisecond
	txnID, err := dataset.CreateDetachedAppend(ctx, nil, &timeout)
	require.NoError(t, err)

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Cleanup expired transactions
	count, err := dataset.CleanupExpiredDetached(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify status is expired
	state, err := dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusExpired, state.Status)

	// Attempting to commit should fail
	_, err = dataset.CommitDetached(ctx, txnID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expired")
}

// TestDetachedTransaction_SDK_RepeatedCommit tests repeated commit behavior.
func TestDetachedTransaction_SDK_RepeatedCommit(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create and commit transaction
	txnID, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)

	// First commit
	version1, err := dataset.CommitDetached(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version1)

	// Second commit should return same version
	version2, err := dataset.CommitDetached(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, version1, version2)

	// Third commit should also work
	version3, err := dataset.CommitDetached(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, version1, version3)

	// Status should remain committed
	state, err := dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusCommitted, state.Status)
	require.Equal(t, version1, *state.ResultVersion)
}

// TestDetachedTransaction_SDK_Delete tests deletion functionality.
func TestDetachedTransaction_SDK_Delete(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Create transaction
	txnID, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)

	// Verify it exists
	state, err := dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.NotNil(t, state)

	// Delete it
	err = dataset.DeleteDetached(txnID)
	require.NoError(t, err)

	// Verify it's gone
	state, err = dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.Nil(t, state)

	// Deleting again should not error
	err = dataset.DeleteDetached(txnID)
	require.NoError(t, err)
}

// TestDetachedTransaction_SDK_Workflow simulates a realistic detached transaction workflow.
func TestDetachedTransaction_SDK_Workflow(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Step 1: Create dataset
	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Step 2: Create detached transaction (simulating long-running process)
	txnID, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, txnID)

	// Step 3: Check status (could be done from another process/service)
	state, err := dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusPending, state.Status)

	// Step 4: Commit (could be done later, possibly from different service)
	version, err := dataset.CommitDetached(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Step 5: Verify final state
	state, err = dataset.GetDetachedStatus(ctx, txnID)
	require.NoError(t, err)
	require.Equal(t, storage2.DetachedStatusCommitted, state.Status)
	require.Equal(t, version, *state.ResultVersion)

	// Step 6: Verify dataset reflects the change
	require.Equal(t, version, dataset.Version())
	count, err := dataset.CountRows()
	require.NoError(t, err)
	require.Equal(t, uint64(0), count) // Empty append
}

// TestDetachedTransaction_SDK_AllOperations tests all detached transaction operations.
func TestDetachedTransaction_SDK_AllOperations(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	dataset, err := CreateDataset(ctx, tmpDir).Build()
	require.NoError(t, err)
	defer dataset.Close()

	// Test CreateDetachedAppend
	appendID, err := dataset.CreateDetachedAppend(ctx, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, appendID)

	// Test CreateDetachedDelete
	deleteID, err := dataset.CreateDetachedDelete(ctx, "id > 100", nil)
	require.NoError(t, err)
	require.NotEmpty(t, deleteID)

	// Test CreateDetachedOverwrite
	overwriteID, err := dataset.CreateDetachedOverwrite(ctx, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, overwriteID)

	// Verify all are pending
	all, err := dataset.ListDetached(ctx, storage2.DetachedStatusPending)
	require.NoError(t, err)
	require.Len(t, all, 3)

	// Commit one
	version, err := dataset.CommitDetached(ctx, appendID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Verify status changes
	pending, err := dataset.ListDetached(ctx, storage2.DetachedStatusPending)
	require.NoError(t, err)
	require.Len(t, pending, 2)

	committed, err := dataset.ListDetached(ctx, storage2.DetachedStatusCommitted)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	require.Equal(t, appendID, committed[0].ID)
}
