// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

// ============================================================================
// GSObjectStore Tests
// ============================================================================

func TestGSObjectStore_Basic(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
		Prefix: "test-prefix",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	// Test Write
	data := []byte("hello world")
	if err := store.Write("test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Test Read
	readData, err := store.Read("test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(readData), string(data))
	}
}

func TestGSObjectStore_List(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	// Write multiple files
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("dir/file_%d.txt", i)
		if err := store.Write(path, []byte(fmt.Sprintf("content %d", i))); err != nil {
			t.Fatalf("Write %s: %v", path, err)
		}
	}

	// List directory
	names, err := store.List("dir")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 5 {
		t.Fatalf("List count = %d, want 5", len(names))
	}
}

func TestGSObjectStore_Delete(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	// Write and delete
	if err := store.Write("delete_me.txt", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := store.Delete(ctx, "delete_me.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	exists, err := store.Exists(ctx, "delete_me.txt")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("Object should be deleted")
	}
}

func TestGSObjectStore_Exists(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	// Check non-existent
	exists, err := store.Exists(ctx, "nonexistent.txt")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("Non-existent object should not exist")
	}

	// Write and check
	if err := store.Write("exists.txt", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	exists, err = store.Exists(ctx, "exists.txt")
	if err != nil {
		t.Fatalf("Exists after write: %v", err)
	}
	if !exists {
		t.Fatal("Object should exist after write")
	}
}

func TestGSObjectStore_ReadRange(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("0123456789abcdef")
	if err := store.Write("range_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read from offset 5
	readData, err := store.ReadRange(ctx, "range_test.txt", ReadOptions{Offset: 5})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	expected := data[5:]
	if !bytes.Equal(readData, expected) {
		t.Fatalf("ReadRange offset: got %q, want %q", string(readData), string(expected))
	}

	// Read with offset and length
	readData, err = store.ReadRange(ctx, "range_test.txt", ReadOptions{Offset: 5, Length: 5})
	if err != nil {
		t.Fatalf("ReadRange with length: %v", err)
	}
	expected = data[5:10]
	if !bytes.Equal(readData, expected) {
		t.Fatalf("ReadRange offset+length: got %q, want %q", string(readData), string(expected))
	}
}

func TestGSObjectStore_ReadStream(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("stream test data")
	if err := store.Write("stream_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := store.ReadStream(ctx, "stream_test.txt", ReadOptions{})
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("ReadStream data: got %q, want %q", string(readData), string(data))
	}
}

func TestGSObjectStore_WriteStream(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	writer, err := store.WriteStream(ctx, "write_stream.txt", WriteOptions{})
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}

	data := []byte("write stream test")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write count = %d, want %d", n, len(data))
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify
	readData, err := store.Read("write_stream.txt")
	if err != nil {
		t.Fatalf("Read after WriteStream: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("WriteStream data: got %q, want %q", string(readData), string(data))
	}
}

func TestGSObjectStore_Copy(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("copy test data")
	if err := store.Write("copy_src.txt", data); err != nil {
		t.Fatalf("Write source: %v", err)
	}

	if err := store.Copy(ctx, "copy_src.txt", "copy_dst.txt"); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	// Verify destination
	readData, err := store.Read("copy_dst.txt")
	if err != nil {
		t.Fatalf("Read destination: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Copy data: got %q, want %q", string(readData), string(data))
	}

	// Source should still exist
	srcData, err := store.Read("copy_src.txt")
	if err != nil {
		t.Fatalf("Read source after copy: %v", err)
	}
	if !bytes.Equal(srcData, data) {
		t.Fatal("Source data should still exist after copy")
	}
}

func TestGSObjectStore_Rename(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("rename test data")
	if err := store.Write("rename_src.txt", data); err != nil {
		t.Fatalf("Write source: %v", err)
	}

	if err := store.Rename(ctx, "rename_src.txt", "rename_dst.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Verify destination
	readData, err := store.Read("rename_dst.txt")
	if err != nil {
		t.Fatalf("Read destination: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Rename data: got %q, want %q", string(readData), string(data))
	}

	// Source should not exist
	exists, err := store.Exists(ctx, "rename_src.txt")
	if err != nil {
		t.Fatalf("Exists source: %v", err)
	}
	if exists {
		t.Fatal("Source should not exist after rename")
	}
}

func TestGSObjectStore_GetSize(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("size test data")
	if err := store.Write("size_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	size, err := store.GetSize(ctx, "size_test.txt")
	if err != nil {
		t.Fatalf("GetSize: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", size, len(data))
	}
}

func TestGSObjectStore_GetETag(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("etag test data")
	if err := store.Write("etag_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	etag, err := store.GetETag(ctx, "etag_test.txt")
	if err != nil {
		t.Fatalf("GetETag: %v", err)
	}
	if etag == "" {
		t.Fatal("ETag should not be empty")
	}
}

func TestGSObjectStore_Prefix(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
		Prefix: "my/prefix",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	data := []byte("prefix test")
	if err := store.Write("test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the full key includes the prefix
	fullKey := store.fullKey("test.txt")
	expectedKey := "my/prefix/test.txt"
	if fullKey != expectedKey {
		t.Fatalf("fullKey = %q, want %q", fullKey, expectedKey)
	}
}

// ============================================================================
// AZObjectStore Tests
// ============================================================================

func TestAZObjectStore_Basic(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
		Prefix:    "test-prefix",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	// Test Write
	data := []byte("hello azure")
	if err := store.Write("test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Test Read
	readData, err := store.Read("test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Read data mismatch: got %q, want %q", string(readData), string(data))
	}
}

func TestAZObjectStore_List(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	// Write multiple files
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("azure_dir/file_%d.txt", i)
		if err := store.Write(path, []byte(fmt.Sprintf("azure content %d", i))); err != nil {
			t.Fatalf("Write %s: %v", path, err)
		}
	}

	// List directory
	names, err := store.List("azure_dir")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(names) != 5 {
		t.Fatalf("List count = %d, want 5", len(names))
	}
}

func TestAZObjectStore_Delete(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	// Write and delete
	if err := store.Write("delete_me.txt", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := store.Delete(ctx, "delete_me.txt"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify deleted
	exists, err := store.Exists(ctx, "delete_me.txt")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("Object should be deleted")
	}
}

func TestAZObjectStore_Exists(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	// Check non-existent
	exists, err := store.Exists(ctx, "nonexistent.txt")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Fatal("Non-existent object should not exist")
	}

	// Write and check
	if err := store.Write("exists.txt", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	exists, err = store.Exists(ctx, "exists.txt")
	if err != nil {
		t.Fatalf("Exists after write: %v", err)
	}
	if !exists {
		t.Fatal("Object should exist after write")
	}
}

func TestAZObjectStore_ReadRange(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("0123456789ABCDEF")
	if err := store.Write("range_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read from offset 5
	readData, err := store.ReadRange(ctx, "range_test.txt", ReadOptions{Offset: 5})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	expected := data[5:]
	if !bytes.Equal(readData, expected) {
		t.Fatalf("ReadRange offset: got %q, want %q", string(readData), string(expected))
	}

	// Read with offset and length
	readData, err = store.ReadRange(ctx, "range_test.txt", ReadOptions{Offset: 5, Length: 5})
	if err != nil {
		t.Fatalf("ReadRange with length: %v", err)
	}
	expected = data[5:10]
	if !bytes.Equal(readData, expected) {
		t.Fatalf("ReadRange offset+length: got %q, want %q", string(readData), string(expected))
	}
}

func TestAZObjectStore_ReadStream(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("azure stream test")
	if err := store.Write("stream_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	reader, err := store.ReadStream(ctx, "stream_test.txt", ReadOptions{})
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	defer reader.Close()

	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("ReadStream data: got %q, want %q", string(readData), string(data))
	}
}

func TestAZObjectStore_WriteStream(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	writer, err := store.WriteStream(ctx, "write_stream.txt", WriteOptions{})
	if err != nil {
		t.Fatalf("WriteStream: %v", err)
	}

	data := []byte("azure write stream")
	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(data) {
		t.Fatalf("Write count = %d, want %d", n, len(data))
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Verify
	readData, err := store.Read("write_stream.txt")
	if err != nil {
		t.Fatalf("Read after WriteStream: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("WriteStream data: got %q, want %q", string(readData), string(data))
	}
}

func TestAZObjectStore_Copy(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("azure copy test")
	if err := store.Write("copy_src.txt", data); err != nil {
		t.Fatalf("Write source: %v", err)
	}

	if err := store.Copy(ctx, "copy_src.txt", "copy_dst.txt"); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	// Verify destination
	readData, err := store.Read("copy_dst.txt")
	if err != nil {
		t.Fatalf("Read destination: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Copy data: got %q, want %q", string(readData), string(data))
	}

	// Source should still exist
	srcData, err := store.Read("copy_src.txt")
	if err != nil {
		t.Fatalf("Read source after copy: %v", err)
	}
	if !bytes.Equal(srcData, data) {
		t.Fatal("Source data should still exist after copy")
	}
}

func TestAZObjectStore_Rename(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("azure rename test")
	if err := store.Write("rename_src.txt", data); err != nil {
		t.Fatalf("Write source: %v", err)
	}

	if err := store.Rename(ctx, "rename_src.txt", "rename_dst.txt"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Verify destination
	readData, err := store.Read("rename_dst.txt")
	if err != nil {
		t.Fatalf("Read destination: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Rename data: got %q, want %q", string(readData), string(data))
	}

	// Source should not exist
	exists, err := store.Exists(ctx, "rename_src.txt")
	if err != nil {
		t.Fatalf("Exists source: %v", err)
	}
	if exists {
		t.Fatal("Source should not exist after rename")
	}
}

func TestAZObjectStore_GetSize(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("azure size test")
	if err := store.Write("size_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	size, err := store.GetSize(ctx, "size_test.txt")
	if err != nil {
		t.Fatalf("GetSize: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", size, len(data))
	}
}

func TestAZObjectStore_GetETag(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("azure etag test")
	if err := store.Write("etag_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	etag, err := store.GetETag(ctx, "etag_test.txt")
	if err != nil {
		t.Fatalf("GetETag: %v", err)
	}
	if etag == "" {
		t.Fatal("ETag should not be empty")
	}
}

func TestAZObjectStore_Prefix(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
		Prefix:    "my/azure/prefix",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	data := []byte("prefix test")
	if err := store.Write("test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify the full key includes the prefix
	fullKey := store.fullKey("test.txt")
	expectedKey := "my/azure/prefix/test.txt"
	if fullKey != expectedKey {
		t.Fatalf("fullKey = %q, want %q", fullKey, expectedKey)
	}
}

// ============================================================================
// Commit Handler Tests
// ============================================================================

func TestGSCommitHandler_Basic(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	handler := NewGSCommitHandler(store)

	// Commit version 0
	m0 := NewManifest(0)
	if err := handler.Commit(ctx, "dataset", 0, m0); err != nil {
		t.Fatalf("Commit v0: %v", err)
	}

	// Resolve latest
	latest, err := handler.ResolveLatestVersion(ctx, "dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion: %v", err)
	}
	if latest != 0 {
		t.Fatalf("latest = %d, want 0", latest)
	}

	// Commit version 1
	m1 := NewManifest(1)
	if err := handler.Commit(ctx, "dataset", 1, m1); err != nil {
		t.Fatalf("Commit v1: %v", err)
	}

	latest, err = handler.ResolveLatestVersion(ctx, "dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion: %v", err)
	}
	if latest != 1 {
		t.Fatalf("latest = %d, want 1", latest)
	}
}

func TestGSCommitHandlerWithLock_Conflict(t *testing.T) {
	ctx := context.Background()
	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "test-bucket",
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	handler := NewGSCommitHandlerWithLock(store)

	// Commit version 0
	m0 := NewManifest(0)
	if err := handler.Commit(ctx, "dataset", 0, m0); err != nil {
		t.Fatalf("Commit v0: %v", err)
	}

	// Try to commit version 0 again - should fail
	err = handler.Commit(ctx, "dataset", 0, m0)
	if err == nil {
		t.Fatal("expected conflict error for duplicate version")
	}
}

func TestAZCommitHandler_Basic(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	handler := NewAZCommitHandler(store)

	// Commit version 0
	m0 := NewManifest(0)
	if err := handler.Commit(ctx, "dataset", 0, m0); err != nil {
		t.Fatalf("Commit v0: %v", err)
	}

	// Resolve latest
	latest, err := handler.ResolveLatestVersion(ctx, "dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion: %v", err)
	}
	if latest != 0 {
		t.Fatalf("latest = %d, want 0", latest)
	}

	// Commit version 1
	m1 := NewManifest(1)
	if err := handler.Commit(ctx, "dataset", 1, m1); err != nil {
		t.Fatalf("Commit v1: %v", err)
	}

	latest, err = handler.ResolveLatestVersion(ctx, "dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion: %v", err)
	}
	if latest != 1 {
		t.Fatalf("latest = %d, want 1", latest)
	}
}

func TestAZCommitHandlerWithLock_Conflict(t *testing.T) {
	ctx := context.Background()
	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "test-container",
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	handler := NewAZCommitHandlerWithLock(store)

	// Commit version 0
	m0 := NewManifest(0)
	if err := handler.Commit(ctx, "dataset", 0, m0); err != nil {
		t.Fatalf("Commit v0: %v", err)
	}

	// Try to commit version 0 again - should fail
	err = handler.Commit(ctx, "dataset", 0, m0)
	if err == nil {
		t.Fatal("expected conflict error for duplicate version")
	}
}

// ============================================================================
// StoreFactory Tests
// ============================================================================

func TestStoreFactory_GS(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{
		EnableCache: true,
	})

	store, err := factory.GetStore(ctx, "gs://my-bucket/path/to/dataset")
	if err != nil {
		t.Fatalf("GetStore GS: %v", err)
	}
	if store == nil {
		t.Fatal("store is nil")
	}

	// Test basic operations
	data := []byte("factory test")
	if err := store.Write("test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	readData, err := store.Read("test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Read data mismatch")
	}
}

func TestStoreFactory_AZ(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{
		EnableCache: true,
	})

	store, err := factory.GetStore(ctx, "az://my-container/path/to/dataset")
	if err != nil {
		t.Fatalf("GetStore AZ: %v", err)
	}
	if store == nil {
		t.Fatal("store is nil")
	}

	// Test basic operations
	data := []byte("factory test")
	if err := store.Write("test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	readData, err := store.Read("test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Read data mismatch")
	}
}

func TestStoreFactory_GetCommitHandler_GS(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{})

	handler, err := factory.GetCommitHandler(ctx, "gs://my-bucket/dataset")
	if err != nil {
		t.Fatalf("GetCommitHandler GS: %v", err)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}

	// Test commit
	m := NewManifest(0)
	if err := handler.Commit(ctx, "dataset", 0, m); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestStoreFactory_GetCommitHandler_AZ(t *testing.T) {
	ctx := context.Background()
	factory := NewStoreFactory(StoreFactoryOptions{})

	handler, err := factory.GetCommitHandler(ctx, "az://my-container/dataset")
	if err != nil {
		t.Fatalf("GetCommitHandler AZ: %v", err)
	}
	if handler == nil {
		t.Fatal("handler is nil")
	}

	// Test commit
	m := NewManifest(0)
	if err := handler.Commit(ctx, "dataset", 0, m); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

// ============================================================================
// Integration Tests (require real cloud credentials)
// ============================================================================

// These tests are skipped by default and only run when the appropriate
// environment variables are set.

func TestGSObjectStore_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Check for credentials
	credsFile := "" // TODO: Get from environment variable
	if credsFile == "" {
		t.Skip("skipping GCS integration test: no credentials")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := NewGSObjectStore(ctx, GSObjectStoreOptions{
		Bucket: "integration-test-bucket",
		Credentials: &GSCredentials{
			CredentialsFile: credsFile,
		},
	})
	if err != nil {
		t.Fatalf("NewGSObjectStore: %v", err)
	}

	// Run basic operations
	data := []byte("integration test data")
	if err := store.Write("integration_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	readData, err := store.Read("integration_test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Data mismatch")
	}

	// Cleanup
	store.Delete(ctx, "integration_test.txt")
}

func TestAZObjectStore_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// Check for credentials
	connStr := "" // TODO: Get from environment variable
	if connStr == "" {
		t.Skip("skipping Azure integration test: no credentials")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := NewAZObjectStore(ctx, AZObjectStoreOptions{
		Container: "integration-test-container",
		Credentials: &AZCredentials{
			ConnectionString: connStr,
		},
	})
	if err != nil {
		t.Fatalf("NewAZObjectStore: %v", err)
	}

	// Run basic operations
	data := []byte("integration test data")
	if err := store.Write("integration_test.txt", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	readData, err := store.Read("integration_test.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(readData, data) {
		t.Fatalf("Data mismatch")
	}

	// Cleanup
	store.Delete(ctx, "integration_test.txt")
}
