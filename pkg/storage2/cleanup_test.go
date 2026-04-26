// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestCleanupOldVersions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create multiple versions
	for i := uint64(0); i < 5; i++ {
		m := NewManifest(i)
		m.Fragments = []*storage2pb.DataFragment{
			{
				Id:           i,
				PhysicalRows: 100,
				Files: []*storage2pb.DataFile{
					{Path: filepath.Join("data", "frag"+string(rune('0'+i)), "0.lance")},
				},
			},
		}
		if err := handler.Commit(ctx, basePath, i, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}

		// Create data file
		dataDir := filepath.Join(basePath, "data", "frag"+string(rune('0'+i)))
		os.MkdirAll(dataDir, 0755)
		os.WriteFile(filepath.Join(dataDir, "0.lance"), []byte("data"), 0644)
	}

	// Verify all versions exist
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 5 {
		t.Fatalf("expected 5 versions, got %d", len(versions))
	}

	// Clean up, retain only 2 versions
	policy := NewCleanupPolicyBuilder().RetainVersions(2).Build()
	store := NewLocalObjectStoreExt(basePath, nil)
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify stats
	if stats.ManifestsRemoved != 3 {
		t.Errorf("expected 3 manifests removed, got %d", stats.ManifestsRemoved)
	}
	if stats.DataFilesRemoved != 3 {
		t.Errorf("expected 3 data files removed, got %d", stats.DataFilesRemoved)
	}

	// Verify remaining versions
	versions, err = listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions after cleanup: %v", err)
	}
	if len(versions) != 2 {
		t.Errorf("expected 2 versions after cleanup, got %d", len(versions))
	}

	// Verify the latest versions are kept
	for _, v := range versions {
		if v.version < 3 {
			t.Errorf("version %d should have been deleted", v.version)
		}
	}
}

func TestCleanupPolicyBuilder(t *testing.T) {
	// Test default policy
	policy := DefaultCleanupPolicy()
	if policy.RetainVersions != 1 {
		t.Errorf("expected RetainVersions=1, got %d", policy.RetainVersions)
	}
	if policy.DeleteUnverified {
		t.Error("expected DeleteUnverified=false")
	}
	if policy.UnverifiedThresholdDays != 7 {
		t.Errorf("expected UnverifiedThresholdDays=7, got %d", policy.UnverifiedThresholdDays)
	}

	// Test builder
	builder := NewCleanupPolicyBuilder()
	policy = builder.
		RetainVersions(5).
		DeleteUnverified(14).
		Build()

	if policy.RetainVersions != 5 {
		t.Errorf("expected RetainVersions=5, got %d", policy.RetainVersions)
	}
	if !policy.DeleteUnverified {
		t.Error("expected DeleteUnverified=true")
	}
	if policy.UnverifiedThresholdDays != 14 {
		t.Errorf("expected UnverifiedThresholdDays=14, got %d", policy.UnverifiedThresholdDays)
	}

	// Test OlderThan
	builder2 := NewCleanupPolicyBuilder()
	policy2 := builder2.OlderThan(24 * time.Hour).Build()
	if policy2.BeforeTimestamp == nil {
		t.Error("expected BeforeTimestamp to be set")
	}

	// Test BeforeVersion
	builder3 := NewCleanupPolicyBuilder()
	version := uint64(10)
	policy3 := builder3.BeforeVersion(version).Build()
	if policy3.BeforeVersion == nil || *policy3.BeforeVersion != version {
		t.Errorf("expected BeforeVersion=%d", version)
	}
}

func TestClassifyVersions(t *testing.T) {
	now := time.Now()
	versions := []versionInfo{
		{version: 5, path: "v5", timestamp: now},
		{version: 4, path: "v4", timestamp: now.Add(-1 * time.Hour)},
		{version: 3, path: "v3", timestamp: now.Add(-2 * time.Hour)},
		{version: 2, path: "v2", timestamp: now.Add(-3 * time.Hour)},
		{version: 1, path: "v1", timestamp: now.Add(-4 * time.Hour)},
	}

	// Test RetainVersions
	policy := CleanupPolicy{RetainVersions: 2}
	retain, delete := classifyVersions(versions, policy)
	if len(retain) != 2 {
		t.Errorf("expected 2 retained, got %d", len(retain))
	}
	if len(delete) != 3 {
		t.Errorf("expected 3 deleted, got %d", len(delete))
	}
	if retain[0].version != 5 || retain[1].version != 4 {
		t.Error("expected versions 5 and 4 to be retained")
	}

	// Test RetainVersions >= total
	policy2 := CleanupPolicy{RetainVersions: 10}
	retain2, delete2 := classifyVersions(versions, policy2)
	if len(retain2) != 5 {
		t.Errorf("expected all 5 retained, got %d", len(retain2))
	}
	if len(delete2) != 0 {
		t.Errorf("expected 0 deleted, got %d", len(delete2))
	}

	// Test BeforeVersion
	beforeV := uint64(3)
	policy3 := CleanupPolicy{BeforeVersion: &beforeV}
	retain3, delete3 := classifyVersions(versions, policy3)
	if len(retain3) != 3 {
		t.Errorf("expected 3 retained (v5,v4,v3), got %d", len(retain3))
	}
	if len(delete3) != 2 {
		t.Errorf("expected 2 deleted (v2,v1), got %d", len(delete3))
	}
}

func TestCollectReferencedFiles(t *testing.T) {
	manifest := &Manifest{
		Version: 1,
		Fragments: []*storage2pb.DataFragment{
			{
				Id: 0,
				Files: []*storage2pb.DataFile{
					{Path: "data/0/file0.lance"},
					{Path: "data/0/file1.lance"},
				},
				DeletionFile: &storage2pb.DeletionFile{
					FileType:       storage2pb.DeletionFile_ARROW_ARRAY,
					ReadVersion:    1,
					Id:             0,
					NumDeletedRows: 5,
				},
			},
			{
				Id: 1,
				Files: []*storage2pb.DataFile{
					{Path: "data/1/file0.lance"},
				},
			},
		},
		TransactionFile: "_transactions/1-abc.txn",
	}

	refs := collectReferencedFiles(manifest)

	// Check data files
	expectedDataFiles := []string{
		"data/0/file0.lance",
		"data/0/file1.lance",
		"data/1/file0.lance",
	}
	for _, f := range expectedDataFiles {
		if _, ok := refs.dataFiles[f]; !ok {
			t.Errorf("expected data file %s to be referenced", f)
		}
	}
	if len(refs.dataFiles) != len(expectedDataFiles) {
		t.Errorf("expected %d data files, got %d", len(expectedDataFiles), len(refs.dataFiles))
	}

	// Check deletion files (path is computed: _deletions/{frag_id}-{read_version}-{id}.arrow)
	expectedDelPath := "_deletions/0-1-0.arrow"
	if _, ok := refs.deleteFiles[expectedDelPath]; !ok {
		t.Errorf("expected deletion file %s to be referenced", expectedDelPath)
	}
	if len(refs.deleteFiles) != 1 {
		t.Errorf("expected 1 deletion file, got %d", len(refs.deleteFiles))
	}

	// Check transaction files
	if _, ok := refs.txFiles["_transactions/1-abc.txn"]; !ok {
		t.Error("expected transaction file to be referenced")
	}
}

func TestMergeReferencedFiles(t *testing.T) {
	refs1 := &referencedFiles{
		dataFiles:   map[string]struct{}{"a.lance": {}, "b.lance": {}},
		deleteFiles: map[string]struct{}{"del1.arrow": {}},
		indexFiles:  map[string]struct{}{},
		txFiles:     map[string]struct{}{"tx1.txn": {}},
	}
	refs2 := &referencedFiles{
		dataFiles:   map[string]struct{}{"b.lance": {}, "c.lance": {}},
		deleteFiles: map[string]struct{}{"del2.arrow": {}},
		indexFiles:  map[string]struct{}{"idx1": {}},
		txFiles:     map[string]struct{}{},
	}

	merged := mergeReferencedFiles(refs1, refs2)

	if len(merged.dataFiles) != 3 {
		t.Errorf("expected 3 data files, got %d", len(merged.dataFiles))
	}
	if len(merged.deleteFiles) != 2 {
		t.Errorf("expected 2 delete files, got %d", len(merged.deleteFiles))
	}
	if len(merged.indexFiles) != 1 {
		t.Errorf("expected 1 index file, got %d", len(merged.indexFiles))
	}
	if len(merged.txFiles) != 1 {
		t.Errorf("expected 1 tx file, got %d", len(merged.txFiles))
	}
}

func TestCleanupWithNoVersions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	policy := DefaultCleanupPolicy()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	if stats.ManifestsRemoved != 0 {
		t.Errorf("expected 0 manifests removed, got %d", stats.ManifestsRemoved)
	}
}

func TestCleanupRetainsAtLeastOneVersion(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create a single version
	m := NewManifest(0)
	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Try to delete with BeforeVersion that would delete everything
	beforeV := uint64(10)
	policy := CleanupPolicy{BeforeVersion: &beforeV}
	store := NewLocalObjectStoreExt(basePath, nil)
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Should retain at least one version
	if stats.ManifestsRemoved != 0 {
		t.Errorf("expected 0 manifests removed (retain at least one), got %d", stats.ManifestsRemoved)
	}

	versions, _ := listManifestVersions(basePath, nil)
	if len(versions) != 1 {
		t.Errorf("expected 1 version retained, got %d", len(versions))
	}
}

func TestCleanupOldVersionsSimple(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create 3 versions
	for i := uint64(0); i < 3; i++ {
		m := NewManifest(i)
		if err := handler.Commit(ctx, basePath, i, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}
	}

	// Use simple API
	stats, err := CleanupOldVersionsSimple(ctx, basePath, handler, 1)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	if stats.ManifestsRemoved != 2 {
		t.Errorf("expected 2 manifests removed, got %d", stats.ManifestsRemoved)
	}
}
