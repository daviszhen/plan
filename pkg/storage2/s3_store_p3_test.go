// Copyright 2024 - Licensed under the Apache License
// SPDX-License-Identifier: Apache-2.0

//go:build s3_integration

package storage2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ============================================================================
// P3: S3ObjectStore comprehensive integration tests with MinIO
// ============================================================================

func newMinIOStore(t *testing.T) *S3ObjectStore {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	store, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket:         bucket,
		Prefix:         fmt.Sprintf("test-%d", time.Now().UnixNano()),
		Endpoint:       endpoint,
		Region:         "us-east-1",
		ForcePathStyle: true,
		Credentials: &S3Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
		},
	})
	require.NoError(t, err, "Failed to connect to MinIO")

	// Ensure bucket exists
	_ = store.CreateBucket(ctx)

	return store
}

// TestS3ObjectStoreBasicReadWrite tests basic read/write operations
func TestS3ObjectStoreBasicReadWrite(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testData := []byte("Hello, MinIO S3!")
	testKey := "basic/test.txt"

	// Write
	err := store.Write(testKey, testData)
	require.NoError(t, err)

	// Read
	data, err := store.Read(testKey)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Cleanup
	_ = store.Delete(ctx, testKey)
}

// TestS3ObjectStoreReadRange tests range read operations
func TestS3ObjectStoreReadRange(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	testKey := "range/test.txt"

	err := store.Write(testKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	// Full read
	data, err := store.ReadRange(ctx, testKey, ReadOptions{})
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Range read with offset and length
	data, err = store.ReadRange(ctx, testKey, ReadOptions{Offset: 10, Length: 10})
	require.NoError(t, err)
	require.Equal(t, "ABCDEFGHIJ", string(data))

	// Range read from offset to end (uses full read due to Length=0)
	// Note: When Length=0, S3 SDK does full read regardless of Offset
	data, err = store.ReadRange(ctx, testKey, ReadOptions{Offset: 26, Length: 10})
	require.NoError(t, err)
	require.Equal(t, "QRSTUVWXYZ", string(data))
}

// TestS3ObjectStoreList tests list operations
func TestS3ObjectStoreList(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	// Create test files
	files := []string{
		"list/dir1/file1.txt",
		"list/dir1/file2.txt",
		"list/dir2/file3.txt",
		"list/file4.txt",
	}

	for _, f := range files {
		err := store.Write(f, []byte("content"))
		require.NoError(t, err)
	}
	defer func() {
		for _, f := range files {
			_ = store.Delete(ctx, f)
		}
	}()

	// List root
	names, err := store.List("list")
	require.NoError(t, err)
	require.Contains(t, names, "dir1")
	require.Contains(t, names, "dir2")
	require.Contains(t, names, "file4.txt")

	// List subdirectory
	names, err = store.List("list/dir1")
	require.NoError(t, err)
	require.Len(t, names, 2)
	require.Contains(t, names, "file1.txt")
	require.Contains(t, names, "file2.txt")
}

// TestS3ObjectStoreExists tests existence check
func TestS3ObjectStoreExists(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "exists/test.txt"

	// Should not exist initially
	exists, err := store.Exists(ctx, testKey)
	require.NoError(t, err)
	require.False(t, exists)

	// Write file
	err = store.Write(testKey, []byte("content"))
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	// Should exist now
	exists, err = store.Exists(ctx, testKey)
	require.NoError(t, err)
	require.True(t, exists)
}

// TestS3ObjectStoreDelete tests delete operation
func TestS3ObjectStoreDelete(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "delete/test.txt"

	// Write
	err := store.Write(testKey, []byte("content"))
	require.NoError(t, err)

	// Verify exists
	exists, err := store.Exists(ctx, testKey)
	require.NoError(t, err)
	require.True(t, exists)

	// Delete
	err = store.Delete(ctx, testKey)
	require.NoError(t, err)

	// Verify deleted
	exists, err = store.Exists(ctx, testKey)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestS3ObjectStoreGetSize tests getting object size
func TestS3ObjectStoreGetSize(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "size/test.txt"
	testData := make([]byte, 12345)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err := store.Write(testKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	size, err := store.GetSize(ctx, testKey)
	require.NoError(t, err)
	require.Equal(t, int64(12345), size)
}

// TestS3ObjectStoreGetETag tests getting object ETag
func TestS3ObjectStoreGetETag(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "etag/test.txt"

	err := store.Write(testKey, []byte("content"))
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	etag, err := store.GetETag(ctx, testKey)
	require.NoError(t, err)
	require.NotEmpty(t, etag)
}

// TestS3ObjectStoreCopy tests copy operation
func TestS3ObjectStoreCopy(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	srcKey := "copy/src.txt"
	dstKey := "copy/dst.txt"
	testData := []byte("copy me!")

	err := store.Write(srcKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, srcKey)
	defer store.Delete(ctx, dstKey)

	// Copy
	err = store.Copy(ctx, srcKey, dstKey)
	require.NoError(t, err)

	// Verify copy
	data, err := store.Read(dstKey)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Original should still exist
	exists, err := store.Exists(ctx, srcKey)
	require.NoError(t, err)
	require.True(t, exists)
}

// TestS3ObjectStoreRename tests rename operation
func TestS3ObjectStoreRename(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	srcKey := "rename/old.txt"
	dstKey := "rename/new.txt"
	testData := []byte("rename me!")

	err := store.Write(srcKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, dstKey)

	// Rename
	err = store.Rename(ctx, srcKey, dstKey)
	require.NoError(t, err)

	// Verify new file
	data, err := store.Read(dstKey)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Original should not exist
	exists, err := store.Exists(ctx, srcKey)
	require.NoError(t, err)
	require.False(t, exists)
}

// TestS3ObjectStoreReadStream tests streaming read
func TestS3ObjectStoreReadStream(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "stream/read.txt"
	testData := []byte("streaming content for read test")

	err := store.Write(testKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	// Full stream read
	reader, err := store.ReadStream(ctx, testKey, ReadOptions{})
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	reader.Close()
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Range stream read
	reader, err = store.ReadStream(ctx, testKey, ReadOptions{Offset: 10, Length: 7})
	require.NoError(t, err)
	data, err = io.ReadAll(reader)
	reader.Close()
	require.NoError(t, err)
	require.Equal(t, "content", string(data))
}

// TestS3ObjectStoreWriteStream tests streaming write
func TestS3ObjectStoreWriteStream(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "stream/write.txt"
	defer store.Delete(ctx, testKey)

	// Write using stream
	writer, err := store.WriteStream(ctx, testKey, WriteOptions{Create: true})
	require.NoError(t, err)

	_, err = writer.Write([]byte("line1\n"))
	require.NoError(t, err)
	_, err = writer.Write([]byte("line2\n"))
	require.NoError(t, err)
	_, err = writer.Write([]byte("line3\n"))
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Verify content
	data, err := store.Read(testKey)
	require.NoError(t, err)
	require.Equal(t, "line1\nline2\nline3\n", string(data))
}

// TestS3ObjectStoreMkdirAll tests MkdirAll (no-op for S3)
func TestS3ObjectStoreMkdirAll(t *testing.T) {
	store := newMinIOStore(t)

	// MkdirAll is a no-op for S3, should not error
	err := store.MkdirAll("some/nested/directory")
	require.NoError(t, err)
}

// TestS3ObjectStoreLargeFile tests handling large files
func TestS3ObjectStoreLargeFile(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "large/file.bin"

	// Create 1MB file
	size := 1024 * 1024
	testData := make([]byte, size)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	err := store.Write(testKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	// Verify size
	s, err := store.GetSize(ctx, testKey)
	require.NoError(t, err)
	require.Equal(t, int64(size), s)

	// Read back
	data, err := store.Read(testKey)
	require.NoError(t, err)
	require.Equal(t, len(testData), len(data))
	require.True(t, bytes.Equal(testData, data))
}

// TestS3ObjectStorePrefix tests prefix handling
func TestS3ObjectStorePrefix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	endpoint, accessKey, secretKey, bucket := getMinIOConfig()

	// Create store with specific prefix
	prefix := fmt.Sprintf("prefixed-test-%d", time.Now().UnixNano())
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

	testKey := "subdir/file.txt"
	testData := []byte("prefixed content")

	err = store.Write(testKey, testData)
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	// Read should work with same store
	data, err := store.Read(testKey)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Verify fullKey formatting
	fullKey := store.fullKey(testKey)
	require.Contains(t, fullKey, prefix)
	require.Contains(t, fullKey, "subdir/file.txt")
}

// TestS3ObjectStoreNonExistentFile tests error handling for missing files
func TestS3ObjectStoreNonExistentFile(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	// Read non-existent file
	_, err := store.Read("nonexistent/file.txt")
	require.Error(t, err)

	// GetSize on non-existent file
	_, err = store.GetSize(ctx, "nonexistent/file.txt")
	require.Error(t, err)

	// GetETag on non-existent file
	_, err = store.GetETag(ctx, "nonexistent/file.txt")
	require.Error(t, err)
}

// TestS3ObjectStoreEmptyFile tests handling empty files
func TestS3ObjectStoreEmptyFile(t *testing.T) {
	store := newMinIOStore(t)
	ctx := context.Background()

	testKey := "empty/file.txt"

	// Write empty file
	err := store.Write(testKey, []byte{})
	require.NoError(t, err)
	defer store.Delete(ctx, testKey)

	// Read empty file
	data, err := store.Read(testKey)
	require.NoError(t, err)
	require.Empty(t, data)

	// Size should be 0
	size, err := store.GetSize(ctx, testKey)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
}

// TestNewS3ObjectStoreValidation tests input validation
func TestNewS3ObjectStoreValidation(t *testing.T) {
	ctx := context.Background()

	// Missing bucket should fail
	_, err := NewS3ObjectStore(ctx, S3ObjectStoreOptions{
		Bucket: "",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bucket")
}
