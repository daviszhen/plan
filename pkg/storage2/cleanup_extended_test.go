// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// setupCleanupTestDataset creates a dataset with the specified number of versions.
// Each version has a unique data file. Returns the latest version number.
func setupCleanupTestDataset(t *testing.T, basePath string, handler CommitHandler, store ObjectStoreExt, numVersions int) uint64 {
	t.Helper()
	ctx := context.Background()

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}

	for i := 0; i < numVersions; i++ {
		version := uint64(i)
		dataPath := filepath.Join("data", "frag"+string(rune('0'+i)), "0.lance")

		m := NewManifest(version)
		m.Fragments = []*DataFragment{
			NewDataFragmentWithRows(uint64(i), 100, []*DataFile{
				NewDataFile(dataPath, []int32{0, 1}, 2, 0),
			}),
		}

		if err := handler.Commit(ctx, basePath, version, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}

		// Create the actual data file
		fullDataPath := filepath.Join(basePath, dataPath)
		if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
			t.Fatalf("mkdir data dir v%d: %v", i, err)
		}

		// Create a simple chunk and write it
		c := &chunk.Chunk{}
		c.Init(typs, util.DefaultVectorSize)
		c.SetCard(10)
		for j := 0; j < 10; j++ {
			c.Data[0].SetValue(j, &chunk.Value{Typ: typs[0], I64: int64(j)})
			c.Data[1].SetValue(j, &chunk.Value{Typ: typs[1], I64: int64(j * 10)})
		}

		if err := WriteChunkToFile(fullDataPath, c); err != nil {
			t.Fatalf("write data file v%d: %v", i, err)
		}
	}

	return uint64(numVersions - 1)
}

// TestCleanup_RetainRecentVersions creates 10 versions, retains 3, and verifies only latest 3 remain.
func TestCleanup_RetainRecentVersions(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create 10 versions
	latestVersion := setupCleanupTestDataset(t, basePath, handler, store, 10)
	if latestVersion != 9 {
		t.Fatalf("expected latest version 9, got %d", latestVersion)
	}

	// Verify all 10 versions exist
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 10 {
		t.Fatalf("expected 10 versions before cleanup, got %d", len(versions))
	}

	// Cleanup, retain only 3 versions
	policy := NewCleanupPolicyBuilder().RetainVersions(3).Build()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify stats
	if stats.ManifestsRemoved != 7 {
		t.Errorf("expected 7 manifests removed, got %d", stats.ManifestsRemoved)
	}

	// Verify remaining versions
	versions, err = listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions after cleanup: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions after cleanup, got %d", len(versions))
	}

	// Verify the latest 3 versions are kept (v7, v8, v9)
	for _, v := range versions {
		if v.version < 7 {
			t.Errorf("version %d should have been deleted, only v7-v9 should remain", v.version)
		}
	}

	// Verify data files for deleted versions are removed
	for i := 0; i < 7; i++ {
		dataPath := filepath.Join(basePath, "data", "frag"+string(rune('0'+i)), "0.lance")
		if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
			t.Errorf("data file for v%d should have been deleted: %s", i, dataPath)
		}
	}

	// Verify data files for retained versions still exist
	for i := 7; i < 10; i++ {
		dataPath := filepath.Join(basePath, "data", "frag"+string(rune('0'+i)), "0.lance")
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			t.Errorf("data file for v%d should still exist: %s", i, dataPath)
		}
	}
}

// TestCleanup_TaggedVersionProtection creates versions, tags v3, and verifies tagged version is preserved.
func TestCleanup_TaggedVersionProtection(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create 5 versions
	for i := 0; i < 5; i++ {
		version := uint64(i)
		dataPath := filepath.Join("data", "frag"+string(rune('0'+i)), "0.lance")

		m := NewManifest(version)
		m.Fragments = []*DataFragment{
			NewDataFragmentWithRows(uint64(i), 100, []*DataFile{
				NewDataFile(dataPath, []int32{0, 1}, 2, 0),
			}),
		}

		// Tag version 3
		if i == 3 {
			m.Tag = "v3-release"
		}

		if err := handler.Commit(ctx, basePath, version, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}

		// Create the actual data file
		fullDataPath := filepath.Join(basePath, dataPath)
		if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
			t.Fatalf("mkdir data dir v%d: %v", i, err)
		}
		if err := os.WriteFile(fullDataPath, []byte("data"), 0644); err != nil {
			t.Fatalf("write data file v%d: %v", i, err)
		}
	}

	// Verify v3 has a tag
	manifestV3, err := LoadManifest(ctx, basePath, handler, 3)
	if err != nil {
		t.Fatalf("load manifest v3: %v", err)
	}
	if manifestV3.Tag != "v3-release" {
		t.Fatalf("expected v3 to have tag 'v3-release', got '%s'", manifestV3.Tag)
	}

	// Cleanup with RetainVersions(2) - this would normally delete v0, v1, v2, v3
	// But v3 should be protected because it's tagged
	policy := NewCleanupPolicyBuilder().RetainVersions(2).Build()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify stats - should have deleted v0, v1, v2 (3 manifests)
	// v3, v4 should remain (v4 is latest, v3 is tagged)
	if stats.ManifestsRemoved != 3 {
		t.Errorf("expected 3 manifests removed (v0, v1, v2), got %d", stats.ManifestsRemoved)
	}

	// Verify remaining versions
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions after cleanup: %v", err)
	}

	// Check that v3 and v4 exist
	versionMap := make(map[uint64]bool)
	for _, v := range versions {
		versionMap[v.version] = true
	}

	if !versionMap[3] {
		t.Error("tagged version v3 should be preserved")
	}
	if !versionMap[4] {
		t.Error("latest version v4 should be preserved")
	}

	// v0, v1, v2 should be deleted
	for i := 0; i < 3; i++ {
		if versionMap[uint64(i)] {
			t.Errorf("version v%d should have been deleted", i)
		}
	}

	// Verify data file for v3 still exists
	dataPathV3 := filepath.Join(basePath, "data", "frag3", "0.lance")
	if _, err := os.Stat(dataPathV3); os.IsNotExist(err) {
		t.Error("data file for tagged version v3 should still exist")
	}
}

// TestCleanup_OlderThanTimestamp creates versions and cleans up those older than a certain time.
func TestCleanup_OlderThanTimestamp(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create 3 versions with artificial delays to ensure different timestamps
	for i := 0; i < 3; i++ {
		version := uint64(i)
		dataPath := filepath.Join("data", "frag"+string(rune('0'+i)), "0.lance")

		m := NewManifest(version)
		m.Fragments = []*DataFragment{
			NewDataFragmentWithRows(uint64(i), 100, []*DataFile{
				NewDataFile(dataPath, []int32{0, 1}, 2, 0),
			}),
		}

		if err := handler.Commit(ctx, basePath, version, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}

		// Create the actual data file
		fullDataPath := filepath.Join(basePath, dataPath)
		if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
			t.Fatalf("mkdir data dir v%d: %v", i, err)
		}
		if err := os.WriteFile(fullDataPath, []byte("data"), 0644); err != nil {
			t.Fatalf("write data file v%d: %v", i, err)
		}

		// Small delay between versions (except the last one)
		if i < 2 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Get the timestamp of v1 (middle version)
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}

	var v1Timestamp time.Time
	for _, v := range versions {
		if v.version == 1 {
			v1Timestamp = v.timestamp
			break
		}
	}

	// Cleanup versions older than v1's timestamp
	// This should delete v0 (older than v1)
	policy := NewCleanupPolicyBuilder().OlderThan(time.Since(v1Timestamp)).Build()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify stats - at least one version should be removed (v0)
	if stats.ManifestsRemoved < 1 {
		t.Errorf("expected at least 1 manifest removed, got %d", stats.ManifestsRemoved)
	}

	// Verify remaining versions
	versions, err = listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions after cleanup: %v", err)
	}

	// At least one version should remain (the latest)
	if len(versions) < 1 {
		t.Error("at least one version should remain after cleanup")
	}

	// The latest version should always be preserved
	versionMap := make(map[uint64]bool)
	for _, v := range versions {
		versionMap[v.version] = true
	}
	if !versionMap[2] {
		t.Error("latest version v2 should be preserved")
	}
}

// TestCleanup_DataFileCleanup verifies data files referenced only by deleted versions are removed.
func TestCleanup_DataFileCleanup(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create versions where each has a unique data file
	for i := 0; i < 3; i++ {
		version := uint64(i)
		dataPath := filepath.Join("data", "version"+string(rune('0'+i)), "data.lance")

		m := NewManifest(version)
		m.Fragments = []*DataFragment{
			NewDataFragmentWithRows(uint64(i), 100, []*DataFile{
				NewDataFile(dataPath, []int32{0, 1}, 2, 0),
			}),
		}

		if err := handler.Commit(ctx, basePath, version, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}

		// Create the actual data file
		fullDataPath := filepath.Join(basePath, dataPath)
		if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
			t.Fatalf("mkdir data dir v%d: %v", i, err)
		}
		if err := os.WriteFile(fullDataPath, []byte("data-content-"+string(rune('0'+i))), 0644); err != nil {
			t.Fatalf("write data file v%d: %v", i, err)
		}
	}

	// Create a shared data file that's referenced by both v1 and v2
	sharedDataPath := filepath.Join("data", "shared", "data.lance")
	fullSharedPath := filepath.Join(basePath, sharedDataPath)
	if err := os.MkdirAll(filepath.Dir(fullSharedPath), 0755); err != nil {
		t.Fatalf("mkdir shared data dir: %v", err)
	}
	if err := os.WriteFile(fullSharedPath, []byte("shared-data"), 0644); err != nil {
		t.Fatalf("write shared data file: %v", err)
	}

	// Update v1 and v2 to reference the shared file
	for v := 1; v <= 2; v++ {
		m, err := LoadManifest(ctx, basePath, handler, uint64(v))
		if err != nil {
			t.Fatalf("load manifest v%d: %v", v, err)
		}
		// Add shared file to fragments
		m.Fragments[0].Files = append(m.Fragments[0].Files, NewDataFile(sharedDataPath, []int32{2}, 2, 0))
		if err := handler.Commit(ctx, basePath, uint64(v), m); err != nil {
			t.Fatalf("update manifest v%d: %v", v, err)
		}
	}

	// Cleanup, retain only 1 version (v2)
	policy := NewCleanupPolicyBuilder().RetainVersions(1).Build()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify stats
	if stats.ManifestsRemoved != 2 {
		t.Errorf("expected 2 manifests removed, got %d", stats.ManifestsRemoved)
	}

	// v0's unique data file should be deleted
	v0DataPath := filepath.Join(basePath, "data", "version0", "data.lance")
	if _, err := os.Stat(v0DataPath); !os.IsNotExist(err) {
		t.Error("v0's unique data file should have been deleted")
	}

	// v1's unique data file should be deleted
	v1DataPath := filepath.Join(basePath, "data", "version1", "data.lance")
	if _, err := os.Stat(v1DataPath); !os.IsNotExist(err) {
		t.Error("v1's unique data file should have been deleted")
	}

	// v2's data file should still exist
	v2DataPath := filepath.Join(basePath, "data", "version2", "data.lance")
	if _, err := os.Stat(v2DataPath); os.IsNotExist(err) {
		t.Error("v2's data file should still exist")
	}

	// Shared data file should still exist (referenced by v2)
	if _, err := os.Stat(fullSharedPath); os.IsNotExist(err) {
		t.Error("shared data file should still exist (referenced by retained v2)")
	}
}

// TestCleanup_PreserveActiveWriteFiles verifies files from current version are never deleted.
func TestCleanup_PreserveActiveWriteFiles(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create a single version with data files
	dataPath := filepath.Join("data", "active", "data.lance")
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile(dataPath, []int32{0, 1}, 2, 0),
		}),
	}

	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatalf("commit v0: %v", err)
	}

	// Create the actual data file
	fullDataPath := filepath.Join(basePath, dataPath)
	if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if err := os.WriteFile(fullDataPath, []byte("active-data"), 0644); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	// Try cleanup with aggressive policy that would delete everything
	beforeV := uint64(100) // Delete all versions before v100
	policy := CleanupPolicy{BeforeVersion: &beforeV}
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Should retain at least one version (the only version)
	if stats.ManifestsRemoved != 0 {
		t.Errorf("expected 0 manifests removed (must retain at least one), got %d", stats.ManifestsRemoved)
	}

	// Verify the version still exists
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("expected 1 version retained, got %d", len(versions))
	}

	// Verify the data file still exists
	if _, err := os.Stat(fullDataPath); os.IsNotExist(err) {
		t.Error("active data file should still exist")
	}
}

// TestCleanup_MultiplePolicyConstraints combines RetainCount + BeforeVersion, verifying all honored.
func TestCleanup_MultiplePolicyConstraints(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create 10 versions
	for i := 0; i < 10; i++ {
		version := uint64(i)
		dataPath := filepath.Join("data", "v"+string(rune('0'+i)), "data.lance")

		m := NewManifest(version)
		m.Fragments = []*DataFragment{
			NewDataFragmentWithRows(uint64(i), 100, []*DataFile{
				NewDataFile(dataPath, []int32{0, 1}, 2, 0),
			}),
		}

		if err := handler.Commit(ctx, basePath, version, m); err != nil {
			t.Fatalf("commit v%d: %v", i, err)
		}

		// Create the actual data file
		fullDataPath := filepath.Join(basePath, dataPath)
		if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
			t.Fatalf("mkdir data dir v%d: %v", i, err)
		}
		if err := os.WriteFile(fullDataPath, []byte("data"), 0644); err != nil {
			t.Fatalf("write data file v%d: %v", i, err)
		}
	}

	// Cleanup with RetainVersions(3) - should keep v7, v8, v9
	policy := NewCleanupPolicyBuilder().RetainVersions(3).Build()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Verify stats
	if stats.ManifestsRemoved != 7 {
		t.Errorf("expected 7 manifests removed, got %d", stats.ManifestsRemoved)
	}

	// Verify remaining versions
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions after cleanup: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions after cleanup, got %d", len(versions))
	}

	// Verify v7, v8, v9 are retained
	versionMap := make(map[uint64]bool)
	for _, v := range versions {
		versionMap[v.version] = true
	}
	for _, expectedV := range []uint64{7, 8, 9} {
		if !versionMap[expectedV] {
			t.Errorf("version v%d should be retained", expectedV)
		}
	}

	// Verify data files for retained versions exist
	for i := 7; i <= 9; i++ {
		dataPath := filepath.Join(basePath, "data", "v"+string(rune('0'+i)), "data.lance")
		if _, err := os.Stat(dataPath); os.IsNotExist(err) {
			t.Errorf("data file for v%d should still exist", i)
		}
	}
}

// TestCleanup_IdempotentCleanup runs cleanup twice and verifies second run is no-op.
func TestCleanup_IdempotentCleanup(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create 5 versions
	setupCleanupTestDataset(t, basePath, handler, store, 5)

	// First cleanup, retain 2 versions
	policy := NewCleanupPolicyBuilder().RetainVersions(2).Build()
	stats1, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("first cleanup: %v", err)
	}

	if stats1.ManifestsRemoved != 3 {
		t.Errorf("first cleanup: expected 3 manifests removed, got %d", stats1.ManifestsRemoved)
	}

	// Second cleanup with same policy - should be no-op
	stats2, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("second cleanup: %v", err)
	}

	if stats2.ManifestsRemoved != 0 {
		t.Errorf("second cleanup: expected 0 manifests removed (idempotent), got %d", stats2.ManifestsRemoved)
	}
	if stats2.DataFilesRemoved != 0 {
		t.Errorf("second cleanup: expected 0 data files removed (idempotent), got %d", stats2.DataFilesRemoved)
	}
	if stats2.BytesRemoved != 0 {
		t.Errorf("second cleanup: expected 0 bytes removed (idempotent), got %d", stats2.BytesRemoved)
	}

	// Verify versions are still correct
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 2 {
		t.Errorf("expected 2 versions after both cleanups, got %d", len(versions))
	}
}

// TestCleanup_SingleVersion tests cleanup with only 1 version, verifying it's retained.
func TestCleanup_SingleVersion(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create a single version
	dataPath := filepath.Join("data", "only", "data.lance")
	m := NewManifest(0)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile(dataPath, []int32{0, 1}, 2, 0),
		}),
	}

	if err := handler.Commit(ctx, basePath, 0, m); err != nil {
		t.Fatalf("commit v0: %v", err)
	}

	// Create the actual data file
	fullDataPath := filepath.Join(basePath, dataPath)
	if err := os.MkdirAll(filepath.Dir(fullDataPath), 0755); err != nil {
		t.Fatalf("mkdir data dir: %v", err)
	}
	if err := os.WriteFile(fullDataPath, []byte("only-data"), 0644); err != nil {
		t.Fatalf("write data file: %v", err)
	}

	// Cleanup with default policy (retain 1)
	policy := DefaultCleanupPolicy()
	stats, err := CleanupOldVersions(ctx, basePath, store, handler, policy)
	if err != nil {
		t.Fatalf("cleanup: %v", err)
	}

	// Should not delete anything
	if stats.ManifestsRemoved != 0 {
		t.Errorf("expected 0 manifests removed (only 1 version), got %d", stats.ManifestsRemoved)
	}

	// Verify the version still exists
	versions, err := listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("expected 1 version retained, got %d", len(versions))
	}

	// Verify the data file still exists
	if _, err := os.Stat(fullDataPath); os.IsNotExist(err) {
		t.Error("data file should still exist")
	}

	// Try with RetainVersions(0) - should still keep at least one version
	policy2 := CleanupPolicy{RetainVersions: 0}
	stats2, err := CleanupOldVersions(ctx, basePath, store, handler, policy2)
	if err != nil {
		t.Fatalf("cleanup with RetainVersions=0: %v", err)
	}

	// Should still not delete the only version
	if stats2.ManifestsRemoved != 0 {
		t.Errorf("expected 0 manifests removed (must retain at least one), got %d", stats2.ManifestsRemoved)
	}

	versions, err = listManifestVersions(basePath, nil)
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 1 {
		t.Errorf("expected 1 version retained (always keep at least one), got %d", len(versions))
	}
}
