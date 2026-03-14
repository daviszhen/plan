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

//go:build s3_integration

package storage2

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// MinIO integration tests - run with: go test -tags=s3_integration -run TestMinIO
// Requires MinIO server running at localhost:9000 with bucket "test-bucket"
// Set environment variables:
//   - MINIO_ENDPOINT (default: http://localhost:9000)
//   - MINIO_ACCESS_KEY (default: minioadmin)
//   - MINIO_SECRET_KEY (default: minioadmin)
//   - MINIO_BUCKET (default: test-bucket)

func getMinIOConfig() (endpoint, accessKey, secretKey, bucket string) {
	endpoint = getEnvOrDefault("MINIO_ENDPOINT", "http://localhost:9000")
	accessKey = getEnvOrDefault("MINIO_ACCESS_KEY", "minioadmin")
	secretKey = getEnvOrDefault("MINIO_SECRET_KEY", "minioadmin")
	bucket = getEnvOrDefault("MINIO_BUCKET", "test-bucket")
	return
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

// TestMinIOConnection tests basic connection to MinIO
func TestMinIOConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket:         bucket,
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Credentials: &S3Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	})
	require.NoError(t, err, "Failed to connect to MinIO")

	// Try to create bucket (ignore if exists)
	_ = store.CreateBucket(ctx)

	// Test basic write
	testKey := fmt.Sprintf("test-%d.txt", time.Now().UnixNano())
	testData := []byte("hello minio")

	err = store.Write(testKey, testData)
	require.NoError(t, err, "Failed to write to MinIO")

	// Test read
	data, err := store.Read(testKey)
	require.NoError(t, err, "Failed to read from MinIO")
	require.Equal(t, testData, data)

	// Cleanup
	_ = store.Delete(ctx, testKey)
}

// TestMinIOCommitHandler tests S3CommitHandlerV2 with MinIO
func TestMinIOCommitHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket:         bucket,
		Prefix:         fmt.Sprintf("dataset-%d", time.Now().UnixNano()),
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Credentials: &S3Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	})
	require.NoError(t, err, "Failed to connect to MinIO")

	// Create bucket if needed
	_ = store.CreateBucket(ctx)

	// Create commit handler
	opts := DefaultS3CommitOptions()
	handler := NewS3CommitHandlerV2(store, opts)

	basePath := "test-dataset"

	// Test ResolveLatestVersion on empty dataset
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(0), version)

	// Commit version 1
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err, "Failed to commit version 1")

	// Verify version
	version, err = handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)

	// Commit version 2
	m2 := NewManifest(2)
	m2.Version = 2
	err = handler.Commit(ctx, basePath, 2, m2)
	require.NoError(t, err, "Failed to commit version 2")

	// Verify version
	version, err = handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)

	// Try to commit version 2 again (should fail)
	err = handler.Commit(ctx, basePath, 2, m2)
	require.Error(t, err, "Should fail on duplicate version")
	require.True(t, isConflictError(err), "Should be conflict error")
}

// TestMinIOCommitWithExternalLock tests S3CommitHandlerV2 with external lock
func TestMinIOCommitWithExternalLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket:         bucket,
		Prefix:         fmt.Sprintf("dataset-lock-%d", time.Now().UnixNano()),
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Credentials: &S3Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	})
	require.NoError(t, err, "Failed to connect to MinIO")

	_ = store.CreateBucket(ctx)

	opts := DefaultS3CommitOptions()
	opts.UseExternalLock = true
	handler := NewS3CommitHandlerV2(store, opts)

	// Use mock locker for testing
	locker := NewMockExternalLocker()
	handler.SetLocker(locker)

	basePath := "test-dataset"

	// Commit with external lock
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err, "Failed to commit with external lock")

	// Verify version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(1), version)
}

// TestMinIOConcurrentCommits tests concurrent commit scenarios
func TestMinIOConcurrentCommits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket:         bucket,
		Prefix:         fmt.Sprintf("dataset-concurrent-%d", time.Now().UnixNano()),
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Credentials: &S3Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	})
	require.NoError(t, err, "Failed to connect to MinIO")

	_ = store.CreateBucket(ctx)

	opts := DefaultS3CommitOptions()
	opts.MaxRetries = 3
	handler := NewS3CommitHandlerV2(store, opts)

	basePath := "test-dataset"

	// Commit version 1 first
	m1 := NewManifest(1)
	m1.Version = 1
	err = handler.Commit(ctx, basePath, 1, m1)
	require.NoError(t, err)

	// Now two concurrent commits for version 2
	done := make(chan error, 2)

	go func() {
		m := NewManifest(2)
		m.Version = 2
		done <- handler.Commit(ctx, basePath, 2, m)
	}()

	go func() {
		m := NewManifest(2)
		m.Version = 2
		done <- handler.Commit(ctx, basePath, 2, m)
	}()

	// One should succeed, one should fail
	err1 := <-done
	err2 := <-done

	// Exactly one should succeed
	successCount := 0
	if err1 == nil {
		successCount++
	}
	if err2 == nil {
		successCount++
	}
	require.Equal(t, 1, successCount, "Exactly one commit should succeed")

	// Verify final version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	require.NoError(t, err)
	require.Equal(t, uint64(2), version)
}
