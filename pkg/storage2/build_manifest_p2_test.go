// Copyright 2024 - Licensed under the Apache License
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// ============================================================================
// P2-1: buildManifestRewrite tests
// ============================================================================

// TestBuildManifestRewriteBasic tests basic Rewrite (compaction) operation.
func TestBuildManifestRewriteBasic(t *testing.T) {
	// Setup: 3 fragments
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 1000, []*DataFile{NewDataFile("f0.parquet", []int32{0, 1}, 1, 0)}),
			NewDataFragmentWithRows(1, 1000, []*DataFile{NewDataFile("f1.parquet", []int32{0, 1}, 1, 0)}),
			NewDataFragmentWithRows(2, 1000, []*DataFile{NewDataFile("f2.parquet", []int32{0, 1}, 1, 0)}),
		},
		MaxFragmentId: ptrUint32(2),
	}

	// Rewrite: merge fragments 0 and 1 into a new fragment
	txn := NewTransactionRewrite(1, "rewrite-test",
		[]*DataFragment{current.Fragments[0], current.Fragments[1]}, // old fragments
		[]*DataFragment{NewDataFragmentWithRows(0, 2000, []*DataFile{NewDataFile("merged.parquet", []int32{0, 1}, 1, 0)})},
	)

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest Rewrite: %v", err)
	}

	// Verify version incremented
	if next.Version != 2 {
		t.Errorf("Version: got %d want 2", next.Version)
	}

	// Should have 2 fragments: fragment 2 (unchanged) + 1 new merged fragment
	if len(next.Fragments) != 2 {
		t.Fatalf("Fragments count: got %d want 2", len(next.Fragments))
	}

	// Fragment 2 should still exist
	found2 := false
	var newFragID uint64
	for _, f := range next.Fragments {
		if f.Id == 2 {
			found2 = true
		} else {
			newFragID = f.Id
		}
	}
	if !found2 {
		t.Error("Fragment 2 should still exist")
	}

	// New fragment should have ID > 2
	if newFragID <= 2 {
		t.Errorf("New fragment ID should be > 2, got %d", newFragID)
	}

	// MaxFragmentId should be updated
	if next.MaxFragmentId == nil || *next.MaxFragmentId < uint32(newFragID) {
		t.Errorf("MaxFragmentId should be at least %d", newFragID)
	}
}

// TestBuildManifestRewriteMultipleGroups tests Rewrite with multiple compaction groups.
func TestBuildManifestRewriteMultipleGroups(t *testing.T) {
	// Setup: 6 fragments
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 500, []*DataFile{NewDataFile("f0.parquet", []int32{0}, 1, 0)}),
			NewDataFragmentWithRows(1, 500, []*DataFile{NewDataFile("f1.parquet", []int32{0}, 1, 0)}),
			NewDataFragmentWithRows(2, 500, []*DataFile{NewDataFile("f2.parquet", []int32{0}, 1, 0)}),
			NewDataFragmentWithRows(3, 500, []*DataFile{NewDataFile("f3.parquet", []int32{0}, 1, 0)}),
			NewDataFragmentWithRows(4, 500, []*DataFile{NewDataFile("f4.parquet", []int32{0}, 1, 0)}),
			NewDataFragmentWithRows(5, 500, []*DataFile{NewDataFile("f5.parquet", []int32{0}, 1, 0)}),
		},
		MaxFragmentId: ptrUint32(5),
	}

	// Create rewrite with 2 groups:
	// Group 1: fragments 0,1,2 -> one new fragment
	// Group 2: fragments 3,4 -> one new fragment
	rewriteOp := &storage2pb.Transaction_Rewrite{
		Groups: []*storage2pb.Transaction_Rewrite_RewriteGroup{
			{
				OldFragments: []*DataFragment{
					current.Fragments[0],
					current.Fragments[1],
					current.Fragments[2],
				},
				NewFragments: []*DataFragment{
					NewDataFragmentWithRows(0, 1500, []*DataFile{NewDataFile("merged_012.parquet", []int32{0}, 1, 0)}),
				},
			},
			{
				OldFragments: []*DataFragment{
					current.Fragments[3],
					current.Fragments[4],
				},
				NewFragments: []*DataFragment{
					NewDataFragmentWithRows(0, 1000, []*DataFile{NewDataFile("merged_34.parquet", []int32{0}, 1, 0)}),
				},
			},
		},
	}

	txn := &Transaction{
		ReadVersion: 1,
		Uuid:        "multi-group-rewrite",
		Operation:   &storage2pb.Transaction_Rewrite_{Rewrite: rewriteOp},
	}

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest Rewrite: %v", err)
	}

	// Should have 3 fragments: fragment 5 (unchanged) + 2 new merged fragments
	if len(next.Fragments) != 3 {
		t.Errorf("Fragments count: got %d want 3", len(next.Fragments))
	}

	// Fragment 5 should still exist (not in any rewrite group)
	found5 := false
	for _, f := range next.Fragments {
		if f.Id == 5 {
			found5 = true
			break
		}
	}
	if !found5 {
		t.Error("Fragment 5 should still exist")
	}
}

// TestBuildManifestRewriteEmpty tests Rewrite with empty groups.
func TestBuildManifestRewriteEmpty(t *testing.T) {
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 1000, []*DataFile{NewDataFile("f0.parquet", []int32{0}, 1, 0)}),
		},
		MaxFragmentId: ptrUint32(0),
	}

	// Empty rewrite (no groups) - should just increment version
	rewriteOp := &storage2pb.Transaction_Rewrite{
		Groups: []*storage2pb.Transaction_Rewrite_RewriteGroup{},
	}

	txn := &Transaction{
		ReadVersion: 1,
		Uuid:        "empty-rewrite",
		Operation:   &storage2pb.Transaction_Rewrite_{Rewrite: rewriteOp},
	}

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest empty Rewrite: %v", err)
	}

	// Fragments should be unchanged
	if len(next.Fragments) != 1 {
		t.Errorf("Fragments count: got %d want 1", len(next.Fragments))
	}
	if next.Version != 2 {
		t.Errorf("Version: got %d want 2", next.Version)
	}
}

// TestBuildManifestRewriteDeleteAll tests Rewrite that removes all fragments (compacts to empty).
func TestBuildManifestRewriteDeleteAll(t *testing.T) {
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 100, []*DataFile{NewDataFile("f0.parquet", []int32{0}, 1, 0)}),
			NewDataFragmentWithRows(1, 100, []*DataFile{NewDataFile("f1.parquet", []int32{0}, 1, 0)}),
		},
		MaxFragmentId: ptrUint32(1),
	}

	// Rewrite all fragments to empty (e.g., all rows were deleted during compaction)
	rewriteOp := &storage2pb.Transaction_Rewrite{
		Groups: []*storage2pb.Transaction_Rewrite_RewriteGroup{
			{
				OldFragments: current.Fragments,
				NewFragments: []*DataFragment{}, // No new fragments
			},
		},
	}

	txn := &Transaction{
		ReadVersion: 1,
		Uuid:        "delete-all-rewrite",
		Operation:   &storage2pb.Transaction_Rewrite_{Rewrite: rewriteOp},
	}

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest delete-all Rewrite: %v", err)
	}

	// Should have 0 fragments
	if len(next.Fragments) != 0 {
		t.Errorf("Fragments count: got %d want 0", len(next.Fragments))
	}
}

// TestBuildManifestRewritePreservesOtherFields tests that Rewrite preserves manifest fields.
func TestBuildManifestRewritePreservesOtherFields(t *testing.T) {
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 1000, []*DataFile{NewDataFile("f0.parquet", []int32{0, 1}, 1, 0)}),
		},
		MaxFragmentId:      ptrUint32(0),
		Fields:             []*storage2pb.Field{{Id: 1, Name: "col1"}, {Id: 2, Name: "col2"}},
		Config:             map[string]string{"key": "value"},
		Tag:                "release-1.0",
		ReaderFeatureFlags: 1,
		WriterFeatureFlags: 3,
	}

	// Rewrite the fragment
	txn := NewTransactionRewrite(1, "preserve-fields",
		current.Fragments,
		[]*DataFragment{NewDataFragmentWithRows(0, 1000, []*DataFile{NewDataFile("merged.parquet", []int32{0, 1}, 1, 0)})},
	)

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest Rewrite: %v", err)
	}

	// Verify fields are preserved
	if len(next.Fields) != 2 {
		t.Errorf("Fields count: got %d want 2", len(next.Fields))
	}
	if next.Config["key"] != "value" {
		t.Errorf("Config: got %v", next.Config)
	}
	if next.Tag != "release-1.0" {
		t.Errorf("Tag: got %q want release-1.0", next.Tag)
	}
	if next.ReaderFeatureFlags != 1 || next.WriterFeatureFlags != 3 {
		t.Errorf("FeatureFlags: reader=%d writer=%d", next.ReaderFeatureFlags, next.WriterFeatureFlags)
	}
}

// TestBuildManifestUpdateConfigReplace tests UpdateConfig with replace mode.
func TestBuildManifestUpdateConfigReplace(t *testing.T) {
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 100, []*DataFile{NewDataFile("f.parquet", []int32{0}, 1, 0)}),
		},
		Config: map[string]string{
			"key1": "val1",
			"key2": "val2",
		},
	}

	// Create UpdateConfig with replace mode
	updateOp := &storage2pb.Transaction_UpdateConfig{
		ConfigUpdates: &storage2pb.Transaction_UpdateMap{
			Replace: true,
			UpdateEntries: []*storage2pb.Transaction_UpdateMapEntry{
				{Key: "newKey", Value: strPtr("newVal")},
			},
		},
	}

	txn := &Transaction{
		ReadVersion: 1,
		Uuid:        "config-replace",
		Operation:   &storage2pb.Transaction_UpdateConfig_{UpdateConfig: updateOp},
	}

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest UpdateConfig: %v", err)
	}

	// Replace mode should clear existing keys
	if _, exists := next.Config["key1"]; exists {
		t.Error("key1 should have been removed in replace mode")
	}
	if _, exists := next.Config["key2"]; exists {
		t.Error("key2 should have been removed in replace mode")
	}
	if next.Config["newKey"] != "newVal" {
		t.Errorf("newKey: got %q want newVal", next.Config["newKey"])
	}
}

// TestBuildManifestUpdateConfigDelete tests UpdateConfig with delete entries.
func TestBuildManifestUpdateConfigDelete(t *testing.T) {
	current := &Manifest{
		Version: 1,
		Fragments: []*DataFragment{
			NewDataFragmentWithRows(0, 100, []*DataFile{NewDataFile("f.parquet", []int32{0}, 1, 0)}),
		},
		Config: map[string]string{
			"keep":   "value1",
			"delete": "value2",
		},
	}

	// Create UpdateConfig with delete entry (nil value)
	updateOp := &storage2pb.Transaction_UpdateConfig{
		ConfigUpdates: &storage2pb.Transaction_UpdateMap{
			UpdateEntries: []*storage2pb.Transaction_UpdateMapEntry{
				{Key: "delete", Value: nil}, // nil value = delete
				{Key: "add", Value: strPtr("new")},
			},
		},
	}

	txn := &Transaction{
		ReadVersion: 1,
		Uuid:        "config-delete",
		Operation:   &storage2pb.Transaction_UpdateConfig_{UpdateConfig: updateOp},
	}

	next, err := BuildManifest(current, txn)
	if err != nil {
		t.Fatalf("BuildManifest UpdateConfig: %v", err)
	}

	if next.Config["keep"] != "value1" {
		t.Errorf("keep: got %q want value1", next.Config["keep"])
	}
	if _, exists := next.Config["delete"]; exists {
		t.Error("delete key should have been removed")
	}
	if next.Config["add"] != "new" {
		t.Errorf("add: got %q want new", next.Config["add"])
	}
}

func strPtr(s string) *string { return &s }

// ============================================================================
// P2-2: RetryableObjectStoreExt tests
// ============================================================================

// TestRetryableObjectStoreExtReadRange tests RetryableObjectStoreExt.ReadRange.
func TestRetryableObjectStoreExtReadRange(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStoreExt(dir, nil)
	ctx := context.Background()

	// Write test data
	data := []byte("0123456789abcdefghij") // 20 bytes
	if err := inner.Write("test.bin", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Create retryable wrapper
	config := &RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1, // 1ms for fast testing
		MaxDelay:      10,
		BackoffFactor: 2.0,
	}
	store := NewRetryableObjectStoreExt(inner, config)

	// Test ReadRange
	result, err := store.ReadRange(ctx, "test.bin", ReadOptions{Offset: 5, Length: 10})
	if err != nil {
		t.Fatalf("ReadRange: %v", err)
	}
	if string(result) != "56789abcde" {
		t.Errorf("ReadRange: got %q want 56789abcde", result)
	}
}

// TestRetryableObjectStoreExtGetSize tests RetryableObjectStoreExt.GetSize.
func TestRetryableObjectStoreExtGetSize(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStoreExt(dir, nil)
	ctx := context.Background()

	// Write test data
	data := make([]byte, 12345)
	if err := inner.Write("size.bin", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	store := NewRetryableObjectStoreExt(inner, nil)

	size, err := store.GetSize(ctx, "size.bin")
	if err != nil {
		t.Fatalf("GetSize: %v", err)
	}
	if size != 12345 {
		t.Errorf("GetSize: got %d want 12345", size)
	}
}

// TestRetryableObjectStoreExtGetETag tests RetryableObjectStoreExt.GetETag.
func TestRetryableObjectStoreExtGetETag(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStoreExt(dir, nil)
	ctx := context.Background()

	// Write test data
	if err := inner.Write("etag.bin", []byte("content")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	store := NewRetryableObjectStoreExt(inner, nil)

	etag, err := store.GetETag(ctx, "etag.bin")
	if err != nil {
		t.Fatalf("GetETag: %v", err)
	}
	if etag == "" {
		t.Error("GetETag should return non-empty string")
	}
}

// TestRetryableObjectStoreExtCopy tests RetryableObjectStoreExt.Copy.
func TestRetryableObjectStoreExtCopy(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStoreExt(dir, nil)
	ctx := context.Background()

	// Write source
	data := []byte("source content")
	if err := inner.Write("src.bin", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	store := NewRetryableObjectStoreExt(inner, nil)

	// Copy
	if err := store.Copy(ctx, "src.bin", "dst.bin"); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	// Verify destination
	dst, err := inner.Read("dst.bin")
	if err != nil {
		t.Fatalf("Read dst: %v", err)
	}
	if string(dst) != string(data) {
		t.Errorf("Copy content: got %q want %q", dst, data)
	}
}

// TestRetryableObjectStoreExtRename tests RetryableObjectStoreExt.Rename.
func TestRetryableObjectStoreExtRename(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStoreExt(dir, nil)
	ctx := context.Background()

	// Write source
	data := []byte("rename content")
	if err := inner.Write("old.bin", data); err != nil {
		t.Fatalf("Write: %v", err)
	}

	store := NewRetryableObjectStoreExt(inner, nil)

	// Rename
	if err := store.Rename(ctx, "old.bin", "new.bin"); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	// Verify old file doesn't exist
	if _, err := inner.Read("old.bin"); err == nil {
		t.Error("old.bin should not exist after rename")
	}

	// Verify new file exists
	newData, err := inner.Read("new.bin")
	if err != nil {
		t.Fatalf("Read new: %v", err)
	}
	if string(newData) != string(data) {
		t.Errorf("Rename content: got %q want %q", newData, data)
	}
}

// TestRetryableObjectStoreExtNotFound tests that non-transient errors are not retried.
func TestRetryableObjectStoreExtNotFound(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStoreExt(dir, nil)
	ctx := context.Background()

	store := NewRetryableObjectStoreExt(inner, nil)

	// Read non-existent file - should fail immediately (not retry)
	_, err := store.ReadRange(ctx, "nonexistent.bin", ReadOptions{})
	if err == nil {
		t.Error("ReadRange should fail for non-existent file")
	}
}

// TestDefaultRetryConfig tests DefaultRetryConfig returns valid config.
func TestDefaultRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()
	if config == nil {
		t.Fatal("DefaultRetryConfig returned nil")
	}
	if config.MaxRetries < 1 {
		t.Errorf("MaxRetries: got %d, should be >= 1", config.MaxRetries)
	}
	if config.InitialDelay <= 0 {
		t.Errorf("InitialDelay: got %v, should be > 0", config.InitialDelay)
	}
	if config.BackoffFactor <= 1.0 {
		t.Errorf("BackoffFactor: got %v, should be > 1.0", config.BackoffFactor)
	}
}
