// Copyright 2024 daviszhen <daviszhen@163.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage2

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MockExternalLocker is a mock implementation of ExternalLocker for testing
type MockExternalLocker struct {
	mu       sync.Mutex
	locks    map[string]bool
	lockTime map[string]time.Time
	ttl      time.Duration
	// FailAcquire causes Acquire to return false (simulating lock contention)
	FailAcquire bool
	// AcquireError causes Acquire to return an error
	AcquireError error
	// ReleaseError causes Release to return an error
	ReleaseError error
}

// NewMockExternalLocker creates a new mock locker
func NewMockExternalLocker() *MockExternalLocker {
	return &MockExternalLocker{
		locks:    make(map[string]bool),
		lockTime: make(map[string]time.Time),
		ttl:      30 * time.Second,
	}
}

func (m *MockExternalLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.AcquireError != nil {
		return false, m.AcquireError
	}
	if m.FailAcquire {
		return false, nil
	}

	// Check if already locked and not expired
	if locked, ok := m.locks[key]; ok && locked {
		if time.Since(m.lockTime[key]) < m.ttl {
			return false, nil
		}
	}

	m.locks[key] = true
	m.lockTime[key] = time.Now()
	return true, nil
}

func (m *MockExternalLocker) Release(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ReleaseError != nil {
		return m.ReleaseError
	}

	delete(m.locks, key)
	delete(m.lockTime, key)
	return nil
}

func (m *MockExternalLocker) IsLocked(ctx context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	locked, ok := m.locks[key]
	if !ok {
		return false, nil
	}
	if time.Since(m.lockTime[key]) >= m.ttl {
		return false, nil
	}
	return locked, nil
}

// TestS3CommitOptions_Default tests default options
func TestS3CommitOptions_Default(t *testing.T) {
	opts := DefaultS3CommitOptions()
	require.Equal(t, 3, opts.MaxRetries)
	require.Equal(t, 100*time.Millisecond, opts.RetryDelay)
	require.Equal(t, 1*time.Second, opts.MaxRetryDelay)
	require.False(t, opts.UseExternalLock)
}

// TestMockExternalLocker_Basic tests the mock locker basic operations
func TestMockExternalLocker_Basic(t *testing.T) {
	locker := NewMockExternalLocker()
	ctx := context.Background()

	// Test acquire
	acquired, err := locker.Acquire(ctx, "test-key", 30*time.Second)
	require.NoError(t, err)
	require.True(t, acquired)

	// Test is locked
	locked, err := locker.IsLocked(ctx, "test-key")
	require.NoError(t, err)
	require.True(t, locked)

	// Test re-acquire (should fail)
	acquired, err = locker.Acquire(ctx, "test-key", 30*time.Second)
	require.NoError(t, err)
	require.False(t, acquired)

	// Test release
	err = locker.Release(ctx, "test-key")
	require.NoError(t, err)

	// Test is unlocked
	locked, err = locker.IsLocked(ctx, "test-key")
	require.NoError(t, err)
	require.False(t, locked)

	// Test re-acquire after release
	acquired, err = locker.Acquire(ctx, "test-key", 30*time.Second)
	require.NoError(t, err)
	require.True(t, acquired)
}

// TestMockExternalLocker_FailAcquire tests lock contention simulation
func TestMockExternalLocker_FailAcquire(t *testing.T) {
	locker := NewMockExternalLocker()
	locker.FailAcquire = true
	ctx := context.Background()

	acquired, err := locker.Acquire(ctx, "test-key", 30*time.Second)
	require.NoError(t, err)
	require.False(t, acquired)
}

// TestMockExternalLocker_Errors tests error simulation
func TestMockExternalLocker_Errors(t *testing.T) {
	locker := NewMockExternalLocker()
	ctx := context.Background()

	// Test acquire error
	locker.AcquireError = fmt.Errorf("acquire failed")
	_, err := locker.Acquire(ctx, "test-key", 30*time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "acquire failed")
	locker.AcquireError = nil

	// Acquire for release test
	acquired, _ := locker.Acquire(ctx, "test-key", 30*time.Second)
	require.True(t, acquired)

	// Test release error
	locker.ReleaseError = fmt.Errorf("release failed")
	err = locker.Release(ctx, "test-key")
	require.Error(t, err)
	require.Contains(t, err.Error(), "release failed")
}

// TestIsConflictError tests conflict error detection
func TestIsConflictError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{ErrConflict, true},
		{fmt.Errorf("version already exists"), true},
		{fmt.Errorf("ConditionCheckFailedException"), true},
		{fmt.Errorf("PreconditionFailed"), true},
		{fmt.Errorf("409 Conflict"), true},
		{fmt.Errorf("some other error"), false},
		{fmt.Errorf("network timeout"), false},
	}

	for _, tt := range tests {
		result := isConflictError(tt.err)
		require.Equal(t, tt.expected, result, "error: %v", tt.err)
	}
}

// TestIsConditionalWriteError tests conditional write error detection
func TestIsConditionalWriteError(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{fmt.Errorf("ConditionCheckFailedException"), true},
		{fmt.Errorf("PreconditionFailed"), true},
		{fmt.Errorf("some other error"), false},
		{fmt.Errorf("409 Conflict"), false},
	}

	for _, tt := range tests {
		result := isConditionalWriteError(tt.err)
		require.Equal(t, tt.expected, result, "error: %v", tt.err)
	}
}

// MemoryCommitHandlerV2 is a test-friendly commit handler using MemoryObjectStore
// with optimistic locking and retry support
type MemoryCommitHandlerV2 struct {
	store   *MemoryObjectStore
	options S3CommitOptions
	locker  ExternalLocker
	data    map[string][]byte
	mu      sync.Mutex
}

// NewMemoryCommitHandlerV2 creates a new memory-based commit handler for testing
func NewMemoryCommitHandlerV2(store *MemoryObjectStore, options S3CommitOptions) *MemoryCommitHandlerV2 {
	return &MemoryCommitHandlerV2{
		store:   store,
		options: options,
		data:    make(map[string][]byte),
	}
}

func (h *MemoryCommitHandlerV2) SetLocker(locker ExternalLocker) {
	h.locker = locker
}

func (h *MemoryCommitHandlerV2) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	prefix := basePath + "/" + VersionsDir + "/"
	var maxVersion uint64

	for key := range h.data {
		if len(key) > len(prefix) && key[:len(prefix)] == prefix {
			name := key[len(prefix):]
			v, err := ParseVersion(name)
			if err != nil {
				continue
			}
			if v > maxVersion {
				maxVersion = v
			}
		}
	}

	return maxVersion, nil
}

func (h *MemoryCommitHandlerV2) ResolveVersion(_ context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

func (h *MemoryCommitHandlerV2) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	return h.commitWithRetry(ctx, basePath, version, manifest, 0)
}

func (h *MemoryCommitHandlerV2) commitWithRetry(ctx context.Context, basePath string, version uint64, manifest *Manifest, attempt int) error {
	err := h.commitOnce(ctx, basePath, version, manifest)
	if err == nil {
		return nil
	}

	if isConflictError(err) {
		if attempt >= h.options.MaxRetries {
			return fmt.Errorf("commit failed after %d retries: %w", attempt, err)
		}

		delay := h.options.RetryDelay * time.Duration(1<<uint(attempt))
		if delay > h.options.MaxRetryDelay {
			delay = h.options.MaxRetryDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		latest, err := h.ResolveLatestVersion(ctx, basePath)
		if err != nil {
			return err
		}
		if latest >= version {
			return ErrConflict
		}

		return h.commitWithRetry(ctx, basePath, version, manifest, attempt+1)
	}

	return err
}

func (h *MemoryCommitHandlerV2) commitOnce(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}

	key := basePath + "/" + ManifestPath(version)

	// Use external lock if available
	if h.locker != nil && h.options.UseExternalLock {
		lockKey := key
		acquired, err := h.locker.Acquire(ctx, lockKey, 30*time.Second)
		if err != nil {
			return fmt.Errorf("failed to acquire lock: %w", err)
		}
		if !acquired {
			return ErrConflict
		}
		defer h.locker.Release(ctx, lockKey)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if target version already exists
	if _, exists := h.data[key]; exists {
		return ErrConflict
	}

	h.data[key] = data
	return nil
}

// TestMemoryCommitHandlerV2_Basic tests basic operations
func TestMemoryCommitHandlerV2_Basic(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset"

	store := NewMemoryObjectStore("")
	handler := NewMemoryCommitHandlerV2(store, DefaultS3CommitOptions())

	// Test ResolveLatestVersion on empty dataset
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(0), version)

	// Test Commit first version
	m0 := NewManifest(1)
	m0.Version = 1
	err = handler.Commit(ctx, basePath, 1, m0)
	require.NoError(t, err)

	// Verify version is now 1
	version, err = handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Test commit conflict (try to commit version 1 again)
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.Error(t, err)
	require.True(t, isConflictError(err) || err == ErrConflict)
}

// TestMemoryCommitHandlerV2_ResolveVersion tests version resolution
func TestMemoryCommitHandlerV2_ResolveVersion(t *testing.T) {
	store := NewMemoryObjectStore("")
	handler := NewMemoryCommitHandlerV2(store, DefaultS3CommitOptions())
	ctx := context.Background()

	path, err := handler.ResolveVersion(ctx, "test-dataset", 5)
	require.NoError(t, err)
	require.Contains(t, path, "5.manifest")
}

// TestMemoryCommitHandlerV2_WithExternalLocker tests external lock integration
func TestMemoryCommitHandlerV2_WithExternalLocker(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset"

	store := NewMemoryObjectStore("")
	opts := DefaultS3CommitOptions()
	opts.UseExternalLock = true
	handler := NewMemoryCommitHandlerV2(store, opts)

	locker := NewMockExternalLocker()
	handler.SetLocker(locker)

	// Commit with external lock
	m0 := NewManifest(1)
	m0.Version = 1
	err := handler.Commit(ctx, basePath, 1, m0)
	require.NoError(t, err)

	// Verify lock was released
	locked, err := locker.IsLocked(ctx, basePath+"/"+ManifestPath(1))
	require.NoError(t, err)
	require.False(t, locked)
}

// TestMemoryCommitHandlerV2_ExternalLockContention tests external lock contention
func TestMemoryCommitHandlerV2_ExternalLockContention(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset"

	store := NewMemoryObjectStore("")
	opts := DefaultS3CommitOptions()
	opts.UseExternalLock = true
	opts.MaxRetries = 1
	handler := NewMemoryCommitHandlerV2(store, opts)

	locker := NewMockExternalLocker()
	locker.FailAcquire = true
	handler.SetLocker(locker)

	// Attempt to commit should fail due to lock contention
	m0 := NewManifest(1)
	m0.Version = 1
	err := handler.Commit(ctx, basePath, 1, m0)
	require.Error(t, err)
}

// TestMemoryCommitHandlerV2_Retry tests retry mechanism
func TestMemoryCommitHandlerV2_Retry(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset"

	store := NewMemoryObjectStore("")
	opts := S3CommitOptions{
		MaxRetries:      2,
		RetryDelay:      10 * time.Millisecond,
		MaxRetryDelay:   100 * time.Millisecond,
		UseExternalLock: false,
	}
	handler := NewMemoryCommitHandlerV2(store, opts)

	// First commit succeeds
	m0 := NewManifest(1)
	m0.Version = 1
	err := handler.Commit(ctx, basePath, 1, m0)
	require.NoError(t, err)

	// Try to commit same version - should fail with conflict
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.Error(t, err)
	// Should be a conflict error
	require.True(t, isConflictError(err) || err == ErrConflict, "expected conflict error, got: %v", err)
}

// TestMemoryCommitHandlerV2_NoRetries tests behavior with no retries
func TestMemoryCommitHandlerV2_NoRetries(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset-no-retry"

	store := NewMemoryObjectStore("")
	opts := S3CommitOptions{
		MaxRetries:      0, // No retries
		RetryDelay:      10 * time.Millisecond,
		MaxRetryDelay:   100 * time.Millisecond,
		UseExternalLock: false,
	}
	handler := NewMemoryCommitHandlerV2(store, opts)

	// Commit version 1
	m0 := NewManifest(1)
	m0.Version = 1
	err := handler.Commit(ctx, basePath, 1, m0)
	require.NoError(t, err)

	// Try to commit same version - should fail immediately
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.Error(t, err)
	// Error should contain conflict info
	require.True(t, isConflictError(err), "expected conflict error, got: %v", err)
}

// TestMemoryCommitHandlerV2_MultiVersion tests multiple version commits
func TestMemoryCommitHandlerV2_MultiVersion(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset"

	store := NewMemoryObjectStore("")
	handler := NewMemoryCommitHandlerV2(store, DefaultS3CommitOptions())

	// Commit multiple versions
	for i := 1; i <= 5; i++ {
		m := NewManifest(uint64(i))
		m.Version = uint64(i)
		err := handler.Commit(ctx, basePath, uint64(i), m)
		require.NoError(t, err, "version %d", i)
	}

	// Verify latest version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(5), version)
}

// TestMemoryCommitHandlerV2_ConcurrentCommits tests concurrent commit scenarios
func TestMemoryCommitHandlerV2_ConcurrentCommits(t *testing.T) {
	ctx := context.Background()
	basePath := "test-dataset"

	store := NewMemoryObjectStore("")
	opts := S3CommitOptions{
		MaxRetries:      0, // No retries for this test
		RetryDelay:      10 * time.Millisecond,
		MaxRetryDelay:   100 * time.Millisecond,
		UseExternalLock: false,
	}
	handler := NewMemoryCommitHandlerV2(store, opts)

	// Commit version 1
	m1 := NewManifest(1)
	m1.Version = 1
	err := handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err)

	// Two goroutines try to commit version 2 simultaneously
	var wg sync.WaitGroup
	var successCount int
	var mu sync.Mutex

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m := NewManifest(2)
			m.Version = 2
			err := handler.Commit(ctx, basePath, 2, m)
			if err == nil {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one should succeed
	require.Equal(t, 1, successCount, "exactly one commit should succeed")

	// Verify version 2 exists
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)
}

// TestCommitTransactionWithRetry_LocalHandler tests transaction commit with retry using local handler
func TestCommitTransactionWithRetry_LocalHandler(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()

	localHandler := NewLocalRenameCommitHandler()

	// Create initial manifest
	m0 := NewManifest(0)
	m0.Version = 0
	m0.Fragments = []*DataFragment{}
	err := localHandler.Commit(ctx, basePath, 0, m0)
	require.NoError(t, err)

	// First transaction: Overwrite (produces v1)
	txn1 := NewTransactionOverwrite(0, "txn1", nil, nil, nil)
	err = CommitTransaction(ctx, basePath, localHandler, txn1)
	require.NoError(t, err)

	// Second transaction: Append based on v0 should conflict with committed Overwrite
	// Per Lance semantics: Append (current) vs Overwrite (committed) = CONFLICT
	txn2 := NewTransactionAppend(0, "txn2", []*DataFragment{
		NewDataFragment(0, []*DataFile{NewDataFile("a.parquet", []int32{0}, 1, 0)}),
	})
	err = CommitTransaction(ctx, basePath, localHandler, txn2)
	require.Equal(t, ErrConflict, err)
}
