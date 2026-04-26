// Copyright 2024 - Licensed under the Apache License
// SPDX-License-Identifier: Apache-2.0

//go:build s3_integration

package storage2

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ============================================================================
// P3: S3CommitHandler integration tests with MinIO
// ============================================================================

func newMinIOStoreForCommit(t *testing.T, prefix string) *S3ObjectStore {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket:         bucket,
		Prefix:         prefix,
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Credentials: &S3Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	})
	require.NoError(t, err)

	_ = store.CreateBucket(ctx)

	return store
}

// TestS3CommitHandlerV2ResolveLatestVersion tests version resolution
func TestS3CommitHandlerV2ResolveLatestVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-resolve-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	handler := NewS3CommitHandlerV2(store, opts)

	basePath := "test-dataset"

	// Initially should be 0
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(0), version)

	// Commit version 1
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err)

	// Should now be 1
	version, err = handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Commit version 2
	m2 := NewManifest(2)
	m2.Version = 2
	err = handler.Commit(ctx, basePath, 2, m2)
	require.NoError(t, err)

	// Should now be 2
	version, err = handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)
}

// TestS3CommitHandlerV2ResolveVersion tests path resolution
func TestS3CommitHandlerV2ResolveVersion(t *testing.T) {
	ctx := context.Background()

	prefix := fmt.Sprintf("commit-path-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	handler := NewS3CommitHandlerV2(store, opts)

	// Test path resolution
	path, err := handler.ResolveVersion(ctx, "dataset", 5)
	require.NoError(t, err)
	require.Contains(t, path, "5.manifest")
}

// TestS3CommitHandlerV2CommitDuplicateVersion tests duplicate version rejection
// Note: MinIO doesn't support S3's IfNoneMatch conditional writes, so we use
// the external locker mechanism to ensure proper duplicate detection.
func TestS3CommitHandlerV2CommitDuplicateVersion(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-dup-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	opts.MaxRetries = 0 // No retries for this test
	opts.UseExternalLock = true
	handler := NewS3CommitHandlerV2(store, opts)
	locker := NewMockExternalLocker()
	handler.SetLocker(locker)

	basePath := "test-dataset"

	// Commit version 1
	m1 := NewManifest(1)
	m1.Version = 1
	err := handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err)

	// Try to commit version 1 again - should fail
	m1b := NewManifest(1)
	m1b.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1b)
	require.Error(t, err)
	require.True(t, isConflictError(err))
}

// TestS3CommitHandlerV2WithExternalLock tests commit with external lock
func TestS3CommitHandlerV2WithExternalLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-lock-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	opts.UseExternalLock = true
	handler := NewS3CommitHandlerV2(store, opts)

	locker := NewMockExternalLocker()
	handler.SetLocker(locker)

	basePath := "test-dataset"

	// Commit with lock
	m1 := NewManifest(1)
	m1.Version = 1
	err := handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err)

	// Verify version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
}

// TestS3CommitHandlerV2ExternalLockFailAcquire tests lock acquisition failure
func TestS3CommitHandlerV2ExternalLockFailAcquire(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-lock-fail-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	opts.UseExternalLock = true
	opts.MaxRetries = 0
	handler := NewS3CommitHandlerV2(store, opts)

	locker := NewMockExternalLocker()
	locker.FailAcquire = true
	handler.SetLocker(locker)

	basePath := "test-dataset"

	// Commit should fail because lock cannot be acquired
	m1 := NewManifest(1)
	m1.Version = 1
	err := handler.Commit(ctx, basePath, 1, m1)
	require.Error(t, err)
	require.True(t, isConflictError(err))
}

// TestS3CommitHandlerV2ExternalLockError tests lock error handling
func TestS3CommitHandlerV2ExternalLockError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-lock-err-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	opts.UseExternalLock = true
	opts.MaxRetries = 0
	handler := NewS3CommitHandlerV2(store, opts)

	locker := NewMockExternalLocker()
	locker.AcquireError = fmt.Errorf("lock service unavailable")
	handler.SetLocker(locker)

	basePath := "test-dataset"

	// Commit should fail with lock error
	m1 := NewManifest(1)
	m1.Version = 1
	err := handler.Commit(ctx, basePath, 1, m1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "lock")
}

// TestS3CommitHandlerV2MultipleVersions tests committing multiple versions sequentially
func TestS3CommitHandlerV2MultipleVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-multi-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	handler := NewS3CommitHandlerV2(store, opts)

	basePath := "test-dataset"

	// Commit versions 1-5
	for v := uint64(1); v <= 5; v++ {
		m := NewManifest(v)
		m.Version = v
		m.Fragments = []*DataFragment{
			NewDataFragmentWithRows(0, uint64(v*100), []*DataFile{
				NewDataFile(fmt.Sprintf("data_%d.parquet", v), []int32{0}, 1, 0),
			}),
		}
		err := handler.Commit(ctx, basePath, v, m)
		require.NoError(t, err, "Failed to commit version %d", v)
	}

	// Verify final version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(5), version)
}

// TestIsConflictErrorP3 tests conflict error detection
func TestIsConflictErrorP3(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{ErrConflict, true},
		{fmt.Errorf("transaction conflict detected"), true},
		{fmt.Errorf("manifest already exists"), true},
		{fmt.Errorf("ConditionCheckFailed"), true},
		{fmt.Errorf("PreconditionFailed"), true},
		{fmt.Errorf("HTTP 409 Conflict"), true},
		{fmt.Errorf("some other error"), false},
	}

	for _, tc := range tests {
		result := isConflictError(tc.err)
		if result != tc.expected {
			t.Errorf("isConflictError(%v): got %v, want %v", tc.err, result, tc.expected)
		}
	}
}

// TestIsConditionalWriteErrorP3 tests conditional write error detection
func TestIsConditionalWriteErrorP3(t *testing.T) {
	tests := []struct {
		err      error
		expected bool
	}{
		{nil, false},
		{fmt.Errorf("ConditionCheckFailed: blah"), true},
		{fmt.Errorf("PreconditionFailed: blah"), true},
		{fmt.Errorf("some other error"), false},
	}

	for _, tc := range tests {
		result := isConditionalWriteError(tc.err)
		if result != tc.expected {
			t.Errorf("isConditionalWriteError(%v): got %v, want %v", tc.err, result, tc.expected)
		}
	}
}

// TestS3CommitHandlerV2RetryBackoff tests retry backoff behavior
// Note: MinIO doesn't support S3's IfNoneMatch conditional writes, so we use
// the external locker mechanism to ensure proper duplicate detection.
func TestS3CommitHandlerV2RetryBackoff(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-backoff-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := S3CommitOptions{
		MaxRetries:      2,
		RetryDelay:      50 * time.Millisecond, // Fast for testing
		MaxRetryDelay:   100 * time.Millisecond,
		UseExternalLock: true,
	}
	handler := NewS3CommitHandlerV2(store, opts)
	locker := NewMockExternalLocker()
	handler.SetLocker(locker)

	basePath := "test-dataset"

	// First, commit version 1
	m1 := NewManifest(1)
	m1.Version = 1
	err := handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err)

	// Try to commit version 1 again with retries - should fail after retries
	m1b := NewManifest(1)
	m1b.Version = 1

	start := time.Now()
	err = handler.Commit(ctx, basePath, 1, m1b)
	elapsed := time.Since(start)

	require.Error(t, err)
	// Should have taken some time due to retries (at least initial delay)
	t.Logf("Retry elapsed time: %v", elapsed)
}

// TestS3CommitHandlerV2ManifestIntegrity tests that committed manifests are readable
func TestS3CommitHandlerV2ManifestIntegrity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("commit-integrity-%d", time.Now().UnixNano())
	store := newMinIOStoreForCommit(t, prefix)

	opts := DefaultS3CommitOptions()
	handler := NewS3CommitHandlerV2(store, opts)

	basePath := "test-dataset"

	// Create manifest with specific data
	m := NewManifest(1)
	m.Version = 1
	m.Tag = "test-tag"
	m.Config = map[string]string{"key": "value"}
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 1000, []*DataFile{
			NewDataFile("data.parquet", []int32{0, 1}, 1, 0),
		}),
	}

	err := handler.Commit(ctx, basePath, 1, m)
	require.NoError(t, err)

	// Read the manifest file directly from S3
	manifestPath := basePath + "/" + ManifestPath(1)
	data, err := store.Read(manifestPath)
	require.NoError(t, err)
	require.NotEmpty(t, data)

	// Unmarshal and verify
	loaded, err := UnmarshalManifest(data)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, uint64(1), loaded.Version)
	require.Equal(t, "test-tag", loaded.Tag)
	require.Equal(t, "value", loaded.Config["key"])
	require.Len(t, loaded.Fragments, 1)
	require.Equal(t, uint64(1000), loaded.Fragments[0].PhysicalRows)
}
