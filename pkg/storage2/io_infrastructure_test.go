// Licensed to Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is part of the Plan project.

package storage2

import (
	"context"
	"sync"
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// TestStoreFactory_MemoryURI tests GetStore with "mem://test" URI
// to verify memory store is returned.
func TestStoreFactory_MemoryURI(t *testing.T) {
	ctx := context.Background()

	factory := NewStoreFactory(StoreFactoryOptions{})

	store, err := factory.GetStore(ctx, "mem://test")
	if err != nil {
		t.Fatalf("GetStore failed for mem:// URI: %v", err)
	}

	if store == nil {
		t.Fatalf("Store should not be nil")
	}

	// Verify it's a memory store by checking type
	_, ok := store.(*MemoryObjectStore)
	if !ok {
		t.Errorf("Expected MemoryObjectStore for mem:// URI, got %T", store)
	}
}

// TestStoreFactory_FileURI tests GetStore with "/tmp/test" URI
// to verify local store is returned.
func TestStoreFactory_FileURI(t *testing.T) {
	ctx := context.Background()

	factory := NewStoreFactory(StoreFactoryOptions{})

	// Use a temporary directory for testing
	store, err := factory.GetStore(ctx, "/tmp/test_storage")
	if err != nil {
		t.Fatalf("GetStore failed for file URI: %v", err)
	}

	if store == nil {
		t.Fatalf("Store should not be nil")
	}

	// Verify it's a local store
	_, ok := store.(*LocalObjectStoreExt)
	if !ok {
		t.Errorf("Expected LocalObjectStoreExt for file URI, got %T", store)
	}
}

// TestStoreFactory_InvalidURI tests GetStore with invalid URI
// to verify error is returned.
func TestStoreFactory_InvalidURI(t *testing.T) {
	ctx := context.Background()

	factory := NewStoreFactory(StoreFactoryOptions{})

	// Test with completely invalid URI
	_, err := factory.GetStore(ctx, "invalid://unsupported-scheme")
	if err == nil {
		t.Errorf("Expected error for invalid URI scheme, got nil")
	}

	// Test with malformed URI
	_, err = factory.GetStore(ctx, "://malformed")
	if err == nil {
		t.Errorf("Expected error for malformed URI, got nil")
	}
}

// TestMemoryObjectStore_FullCRUD tests the full Write, Read, List, Delete, Exists cycle.
func TestMemoryObjectStore_FullCRUD(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryObjectStore("test")

	// Test Write
	testData := []byte("hello, world!")
	err := store.Write("test/path/file.txt", testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Test Exists
	exists, err := store.Exists(ctx, "test/path/file.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Errorf("File should exist after Write")
	}

	// Test Read
	data, err := store.Read("test/path/file.txt")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(data) != string(testData) {
		t.Errorf("Read data mismatch: got %q, want %q", string(data), string(testData))
	}

	// Test GetSize
	size, err := store.GetSize(ctx, "test/path/file.txt")
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if size != int64(len(testData)) {
		t.Errorf("Size mismatch: got %d, want %d", size, len(testData))
	}

	// Test List
	files, err := store.List("test/path")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(files) != 1 || files[0] != "file.txt" {
		t.Errorf("List result unexpected: %v", files)
	}

	// Test Delete
	err = store.Delete(ctx, "test/path/file.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	exists, err = store.Exists(ctx, "test/path/file.txt")
	if err != nil {
		t.Fatalf("Exists after delete failed: %v", err)
	}
	if exists {
		t.Errorf("File should not exist after Delete")
	}
}

// TestMemoryObjectStore_ReadRange_Basic tests ReadRange with offset/length.
func TestMemoryObjectStore_ReadRange_Basic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryObjectStore("test")

	// Write test data
	testData := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	err := store.Write("test/file.bin", testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Test ReadRange with offset
	offset := int64(10)
	length := int64(10)
	data, err := store.ReadRange(ctx, "test/file.bin", ReadOptions{
		Offset: offset,
		Length: length,
	})
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	expected := testData[offset : offset+length]
	if string(data) != string(expected) {
		t.Errorf("ReadRange data mismatch: got %q, want %q", string(data), string(expected))
	}

	// Test ReadRange from beginning
	data, err = store.ReadRange(ctx, "test/file.bin", ReadOptions{
		Offset: 0,
		Length: 5,
	})
	if err != nil {
		t.Fatalf("ReadRange from start failed: %v", err)
	}
	if string(data) != "01234" {
		t.Errorf("ReadRange from start mismatch: got %q", string(data))
	}

	// Test ReadRange to end
	data, err = store.ReadRange(ctx, "test/file.bin", ReadOptions{
		Offset: 32,
		Length: 4,
	})
	if err != nil {
		t.Fatalf("ReadRange to end failed: %v", err)
	}
	if string(data) != "WXYZ" {
		t.Errorf("ReadRange to end mismatch: got %q", string(data))
	}
}

// TestMemoryObjectStore_CopyRename_Basic tests Copy and Rename operations.
func TestMemoryObjectStore_CopyRename_Basic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryObjectStore("test")

	// Write original file
	testData := []byte("original content")
	err := store.Write("test/original.txt", testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Test Copy
	err = store.Copy(ctx, "test/original.txt", "test/copy.txt")
	if err != nil {
		t.Fatalf("Copy failed: %v", err)
	}

	// Verify copy exists
	copyData, err := store.Read("test/copy.txt")
	if err != nil {
		t.Fatalf("Read copy failed: %v", err)
	}
	if string(copyData) != string(testData) {
		t.Errorf("Copy data mismatch")
	}

	// Verify original still exists
	originalData, err := store.Read("test/original.txt")
	if err != nil {
		t.Fatalf("Read original failed: %v", err)
	}
	if string(originalData) != string(testData) {
		t.Errorf("Original data changed after copy")
	}

	// Test Rename
	err = store.Rename(ctx, "test/original.txt", "test/renamed.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	// Verify renamed file exists
	renamedData, err := store.Read("test/renamed.txt")
	if err != nil {
		t.Fatalf("Read renamed failed: %v", err)
	}
	if string(renamedData) != string(testData) {
		t.Errorf("Renamed data mismatch")
	}

	// Verify original no longer exists
	exists, err := store.Exists(ctx, "test/original.txt")
	if err != nil {
		t.Fatalf("Exists check failed: %v", err)
	}
	if exists {
		t.Errorf("Original file should not exist after rename")
	}
}

// TestMemoryObjectStore_ConcurrentAccess tests concurrent reads and writes.
func TestMemoryObjectStore_ConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryObjectStore("test")

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // Readers and writers

	// Concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "test/concurrent/file_" + string(rune('A'+id))
				data := []byte("data from goroutine")
				_ = store.Write(key, data)
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "test/concurrent/file_" + string(rune('A'+(id%5)))
				_, _ = store.Read(key)
				_, _ = store.Exists(ctx, key)
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Verify store is still functional after concurrent access
	err := store.Write("test/final_check", []byte("final"))
	if err != nil {
		t.Fatalf("Store not functional after concurrent access: %v", err)
	}

	data, err := store.Read("test/final_check")
	if err != nil {
		t.Fatalf("Read failed after concurrent access: %v", err)
	}
	if string(data) != "final" {
		t.Errorf("Data mismatch after concurrent access")
	}
}

// TestMemoryCommitHandler_VersionTracking tests committing multiple versions
// and verifying latest version tracking.
func TestMemoryCommitHandler_VersionTracking(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryObjectStore("test")
	handler := NewMemoryCommitHandlerWithStore(store)

	// Commit version 1 (version param is the version to write)
	manifest1 := &Manifest{
		Version:    1,
		Fields:     []*storage2pb.Field{{Name: "col1", Id: 0}},
		Fragments:  []*DataFragment{},
	}
	err := handler.Commit(ctx, "mem://test/dataset", 1, manifest1)
	if err != nil {
		t.Fatalf("Commit version 1 failed: %v", err)
	}

	// Verify latest version
	latest, err := handler.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 1 {
		t.Errorf("Latest version mismatch: got %d, want 1", latest)
	}

	// Commit version 2
	manifest2 := &Manifest{
		Version:    2,
		Fields:     []*storage2pb.Field{{Name: "col1", Id: 0}, {Name: "col2", Id: 1}},
		Fragments:  []*DataFragment{},
	}
	err = handler.Commit(ctx, "mem://test/dataset", 2, manifest2)
	if err != nil {
		t.Fatalf("Commit version 2 failed: %v", err)
	}

	// Verify latest version updated
	latest, err = handler.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 2 {
		t.Errorf("Latest version mismatch: got %d, want 2", latest)
	}

	// Commit version 3
	manifest3 := &Manifest{
		Version:    3,
		Fields:     manifest2.Fields,
		Fragments:  []*DataFragment{},
	}
	err = handler.Commit(ctx, "mem://test/dataset", 3, manifest3)
	if err != nil {
		t.Fatalf("Commit version 3 failed: %v", err)
	}

	// Verify latest version is 3
	latest, err = handler.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 3 {
		t.Errorf("Latest version mismatch: got %d, want 3", latest)
	}

	// Verify we can resolve specific versions (returns path string)
	v1Path, err := handler.ResolveVersion(ctx, "mem://test/dataset", 1)
	if err != nil {
		t.Fatalf("ResolveVersion 1 failed: %v", err)
	}
	if v1Path == "" {
		t.Errorf("Version 1 path should not be empty")
	}

	v2Path, err := handler.ResolveVersion(ctx, "mem://test/dataset", 2)
	if err != nil {
		t.Fatalf("ResolveVersion 2 failed: %v", err)
	}
	if v2Path == "" {
		t.Errorf("Version 2 path should not be empty")
	}
}

// TestMemoryCommitHandler_ConflictDetection tests that MemoryCommitHandler
// allows overwriting versions (no built-in conflict detection).
func TestMemoryCommitHandler_ConflictDetection(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryObjectStore("test")
	handler := NewMemoryCommitHandlerWithStore(store)

	// Commit version 1
	manifest1 := &Manifest{
		Version:    1,
		Fields:     []*storage2pb.Field{{Name: "col1", Id: 0}},
		Fragments:  []*DataFragment{},
	}
	err := handler.Commit(ctx, "mem://test/dataset", 1, manifest1)
	if err != nil {
		t.Fatalf("Commit version 1 failed: %v", err)
	}

	// Verify latest version is 1
	latest, err := handler.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 1 {
		t.Errorf("Latest version should be 1, got %d", latest)
	}

	// Overwrite version 1 (MemoryCommitHandler allows this)
	manifest1Alt := &Manifest{
		Version:    1,
		Fields:     []*storage2pb.Field{{Name: "col1_alt", Id: 0}},
		Fragments:  []*DataFragment{},
	}
	err = handler.Commit(ctx, "mem://test/dataset", 1, manifest1Alt)
	if err != nil {
		t.Fatalf("Overwrite version 1 failed: %v", err)
	}

	// Verify we can read back the overwritten manifest
	manifestPath, err := handler.ResolveVersion(ctx, "mem://test/dataset", 1)
	if err != nil {
		t.Fatalf("ResolveVersion failed: %v", err)
	}
	if manifestPath == "" {
		t.Errorf("Manifest path should not be empty")
	}

	// Commit version 2
	manifest2 := &Manifest{
		Version:    2,
		Fields:     manifest1.Fields,
		Fragments:  []*DataFragment{},
	}
	err = handler.Commit(ctx, "mem://test/dataset", 2, manifest2)
	if err != nil {
		t.Fatalf("Commit version 2 failed: %v", err)
	}

	// Verify version 2 is now the latest
	latest, err = handler.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 2 {
		t.Errorf("Latest version should be 2, got %d", latest)
	}
}

// TestStoreFactory_CommitHandlerForMem tests GetCommitHandler for mem:// URI.
func TestStoreFactory_CommitHandlerForMem(t *testing.T) {
	ctx := context.Background()

	factory := NewStoreFactory(StoreFactoryOptions{})

	handler, err := factory.GetCommitHandler(ctx, "mem://test")
	if err != nil {
		t.Fatalf("GetCommitHandler failed for mem:// URI: %v", err)
	}

	if handler == nil {
		t.Fatalf("CommitHandler should not be nil")
	}

	// Verify it's a memory commit handler
	memHandler, ok := handler.(*MemoryCommitHandler)
	if !ok {
		t.Errorf("Expected MemoryCommitHandler for mem:// URI, got %T", handler)
	}

	// factory returns handler without store, so ResolveLatestVersion returns 0
	latest, err := memHandler.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 0 {
		t.Errorf("Expected version 0 for empty handler, got %d", latest)
	}

	// Test a handler created with a store works for commit
	store := NewMemoryObjectStore("test")
	handlerWithStore := NewMemoryCommitHandlerWithStore(store)

	manifest := &Manifest{
		Version:    1,
		Fields:     []*storage2pb.Field{{Name: "col1", Id: 0}},
		Fragments:  []*DataFragment{},
	}

	err = handlerWithStore.Commit(ctx, "mem://test/dataset", 1, manifest)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	latest, err = handlerWithStore.ResolveLatestVersion(ctx, "mem://test/dataset")
	if err != nil {
		t.Fatalf("ResolveLatestVersion failed: %v", err)
	}
	if latest != 1 {
		t.Errorf("Latest version mismatch: got %d, want 1", latest)
	}
}
