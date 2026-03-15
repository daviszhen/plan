package storage2

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDetachedTransaction_CommitFlow tests the complete detached transaction commit flow.
// Corresponds to Lance Java DatasetTest.testCommitTransactionDetachedTrue
func TestDetachedTransaction_CommitFlow(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset (version 0)
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create transaction
	txn := NewTransactionAppend(0, "test-detached-flow", nil)

	// Create detached transaction
	id, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn, nil)
	require.NoError(t, err)
	require.Equal(t, "test-detached-flow", id)

	// Verify initial state
	state, err := GetDetachedTransactionStatus(ctx, tmpDir, id)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, DetachedStatusPending, state.Status)
	require.Equal(t, id, state.ID)
	require.NotNil(t, state.Transaction)
	require.NotNil(t, state.ExpiresAt)
	require.WithinDuration(t, time.Now().Add(DefaultDetachedTimeout), *state.ExpiresAt, time.Minute)

	// Commit the detached transaction
	version, err := CommitDetachedTransaction(ctx, tmpDir, handler, id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Verify committed state
	state, err = GetDetachedTransactionStatus(ctx, tmpDir, id)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusCommitted, state.Status)
	require.NotNil(t, state.ResultVersion)
	require.Equal(t, uint64(1), *state.ResultVersion)
	require.True(t, state.UpdatedAt.After(state.CreatedAt))

	// Verify dataset version
	latestVersion, err := handler.ResolveLatestVersion(ctx, tmpDir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), latestVersion)
}

// TestDetachedTransaction_StatusQueries tests status query functionality.
// Corresponds to checking transaction status in Lance Java tests
func TestDetachedTransaction_StatusQueries(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create multiple transactions with different statuses
	txn1 := NewTransactionAppend(0, "txn-pending", nil)
	// txn2 will be an Overwrite that gets committed first
	txn2 := NewTransactionOverwrite(0, "txn-committed", nil, nil, nil)

	// txn3 will be an Append that conflicts with the committed Overwrite
	// Per Lance semantics: Append (current) vs Overwrite (committed) = CONFLICT
	txn3 := NewTransactionAppend(0, "txn-failed", nil)

	// Create detached transactions
	id1, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn1, nil)
	require.NoError(t, err)

	id2, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn2, nil)
	require.NoError(t, err)

	id3, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn3, nil)
	require.NoError(t, err)

	// Commit second transaction (Overwrite)
	_, err = CommitDetachedTransaction(ctx, tmpDir, handler, id2)
	require.NoError(t, err)

	// Now try to commit third transaction (Append) - should fail due to conflict with committed Overwrite
	_, err = CommitDetachedTransaction(ctx, tmpDir, handler, id3)
	require.Error(t, err) // Should fail due to conflict
	require.Contains(t, err.Error(), "conflict")

	// Test GetDetachedTransactionStatus for each
	state1, err := GetDetachedTransactionStatus(ctx, tmpDir, id1)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusPending, state1.Status)

	state2, err := GetDetachedTransactionStatus(ctx, tmpDir, id2)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusCommitted, state2.Status)
	require.NotNil(t, state2.ResultVersion)

	state3, err := GetDetachedTransactionStatus(ctx, tmpDir, id3)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusFailed, state3.Status)
	require.NotEmpty(t, state3.Error)

	// Test ListDetachedTransactions with different filters
	all, err := ListDetachedTransactions(ctx, tmpDir, "")
	require.NoError(t, err)
	require.Len(t, all, 3)

	pending, err := ListDetachedTransactions(ctx, tmpDir, DetachedStatusPending)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, id1, pending[0].ID)

	committed, err := ListDetachedTransactions(ctx, tmpDir, DetachedStatusCommitted)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	require.Equal(t, id2, committed[0].ID)

	failed, err := ListDetachedTransactions(ctx, tmpDir, DetachedStatusFailed)
	require.NoError(t, err)
	require.Len(t, failed, 1)
	require.Equal(t, id3, failed[0].ID)
}

// TestDetachedTransaction_Expiration tests expiration functionality.
// Corresponds to expiration behavior in Lance Java tests
func TestDetachedTransaction_Expiration(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create transaction with very short timeout
	shortTimeout := 10 * time.Millisecond
	opts := &DetachedTransactionOptions{
		Timeout: shortTimeout,
	}

	txn := NewTransactionAppend(0, "expiring-txn", nil)
	id, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn, opts)
	require.NoError(t, err)

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Test CleanupExpiredDetachedTransactions
	count, err := CleanupExpiredDetachedTransactions(ctx, tmpDir)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify status is now expired
	state, err := GetDetachedTransactionStatus(ctx, tmpDir, id)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusExpired, state.Status)

	// Attempting to commit expired transaction should fail
	_, err = CommitDetachedTransaction(ctx, tmpDir, handler, id)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expired")

	// Create another transaction with normal timeout
	normalTxn := NewTransactionAppend(0, "normal-txn", nil)
	normalID, err := CreateDetachedTransaction(ctx, tmpDir, handler, normalTxn, nil)
	require.NoError(t, err)

	// Cleanup should not affect non-expired transactions
	count, err = CleanupExpiredDetachedTransactions(ctx, tmpDir)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	// Normal transaction should still be pending
	state, err = GetDetachedTransactionStatus(ctx, tmpDir, normalID)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusPending, state.Status)
}

// TestDetachedTransaction_ConcurrentCommits tests concurrent commit scenarios.
// Tests race conditions and conflict handling
func TestDetachedTransaction_ConcurrentCommits(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create multiple detached transactions
	var ids []string
	for i := 0; i < 5; i++ {
		txn := NewTransactionAppend(0, "concurrent-txn-"+string(rune('A'+i)), nil)
		id, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn, nil)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	// Try to commit all concurrently
	type result struct {
		id      string
		version uint64
		err     error
	}

	results := make(chan result, len(ids))
	for _, id := range ids {
		go func(txnID string) {
			version, err := CommitDetachedTransaction(ctx, tmpDir, handler, txnID)
			results <- result{id: txnID, version: version, err: err}
		}(id)
	}

	// Collect results
	var successful []result
	var failed []result
	for i := 0; i < len(ids); i++ {
		res := <-results
		if res.err == nil {
			successful = append(successful, res)
		} else {
			failed = append(failed, res)
		}
	}

	// At least one should succeed (the first one)
	require.Greater(t, len(successful), 0, "at least one transaction should succeed")

	// Others may fail due to conflicts
	t.Logf("Successful commits: %d, Failed commits: %d", len(successful), len(failed))

	// Verify successful transactions are committed
	for _, res := range successful {
		state, err := GetDetachedTransactionStatus(ctx, tmpDir, res.id)
		require.NoError(t, err)
		require.Equal(t, DetachedStatusCommitted, state.Status)
		require.Equal(t, res.version, *state.ResultVersion)
	}

	// Verify failed transactions have error status
	for _, res := range failed {
		state, err := GetDetachedTransactionStatus(ctx, tmpDir, res.id)
		require.NoError(t, err)
		require.Equal(t, DetachedStatusFailed, state.Status)
		require.NotEmpty(t, state.Error)
		// The error might be "conflict" or file rename error, both are acceptable
	}
}

// TestDetachedTransaction_RepeatedCommit tests committing the same transaction multiple times.
// Corresponds to Lance Java testCommitTransactionDetachedTrueOnV1ManifestThrowsUnsupported behavior
func TestDetachedTransaction_RepeatedCommit(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create and commit a transaction
	txn := NewTransactionAppend(0, "repeat-commit-txn", nil)
	id, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn, nil)
	require.NoError(t, err)

	// First commit
	version1, err := CommitDetachedTransaction(ctx, tmpDir, handler, id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version1)

	// Second commit should succeed and return the same version
	version2, err := CommitDetachedTransaction(ctx, tmpDir, handler, id)
	require.NoError(t, err)
	require.Equal(t, version1, version2)

	// Third commit should also succeed
	version3, err := CommitDetachedTransaction(ctx, tmpDir, handler, id)
	require.NoError(t, err)
	require.Equal(t, version1, version3)

	// Verify status remains committed
	state, err := GetDetachedTransactionStatus(ctx, tmpDir, id)
	require.NoError(t, err)
	require.Equal(t, DetachedStatusCommitted, state.Status)
	require.Equal(t, version1, *state.ResultVersion)
}

// TestDetachedTransaction_NonExistent tests behavior with non-existent transactions.
func TestDetachedTransaction_NonExistent(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Test commit non-existent transaction
	_, err := CommitDetachedTransaction(ctx, tmpDir, handler, "non-existent-id")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")

	// Test get status for non-existent transaction
	state, err := GetDetachedTransactionStatus(ctx, tmpDir, "non-existent-id")
	require.NoError(t, err) // Should not error, just return nil
	require.Nil(t, state)

	// Test delete non-existent transaction
	err = DeleteDetachedTransaction(tmpDir, "non-existent-id")
	require.NoError(t, err) // Should not error for non-existent
}

// TestDetachedTransaction_StatePersistence tests that transaction state persists across restarts.
func TestDetachedTransaction_StatePersistence(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial dataset
	m0 := NewManifest(0)
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create detached transaction
	txn := NewTransactionAppend(0, "persistence-test", nil)
	id, err := CreateDetachedTransaction(ctx, tmpDir, handler, txn, nil)
	require.NoError(t, err)

	// Verify state exists
	state1, err := GetDetachedTransactionStatus(ctx, tmpDir, id)
	require.NoError(t, err)
	require.NotNil(t, state1)

	// "Restart" by creating new handler (same directory)
	handler2 := NewLocalRenameCommitHandler()

	// State should still be accessible
	state2, err := GetDetachedTransactionStatus(ctx, tmpDir, id)
	require.NoError(t, err)
	require.NotNil(t, state2)
	require.Equal(t, state1.ID, state2.ID)
	require.Equal(t, state1.Status, state2.Status)
	require.Equal(t, state1.CreatedAt.Unix(), state2.CreatedAt.Unix())

	// Commit with new handler
	version, err := CommitDetachedTransaction(ctx, tmpDir, handler2, id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
}
