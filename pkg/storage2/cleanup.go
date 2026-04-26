// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// CleanupPolicy defines the policy for cleaning up old versions
type CleanupPolicy struct {
	// BeforeTimestamp cleans up versions before this timestamp
	BeforeTimestamp *time.Time
	// BeforeVersion cleans up versions before this version number
	BeforeVersion *uint64
	// RetainVersions retains the most recent N versions (takes precedence over timestamp/version)
	RetainVersions int
	// DeleteUnverified deletes files that cannot be verified (possibly from interrupted transactions)
	DeleteUnverified bool
	// UnverifiedThresholdDays is the minimum age (in days) for unverified files to be deleted
	UnverifiedThresholdDays int
}

// DefaultCleanupPolicy returns the default cleanup policy
func DefaultCleanupPolicy() CleanupPolicy {
	return CleanupPolicy{
		RetainVersions:          1,
		DeleteUnverified:        false,
		UnverifiedThresholdDays: 7,
	}
}

// CleanupPolicyBuilder provides a fluent interface for building cleanup policies
type CleanupPolicyBuilder struct {
	policy CleanupPolicy
}

// NewCleanupPolicyBuilder creates a new CleanupPolicyBuilder with default settings
func NewCleanupPolicyBuilder() *CleanupPolicyBuilder {
	return &CleanupPolicyBuilder{
		policy: DefaultCleanupPolicy(),
	}
}

// OlderThan sets the policy to clean versions older than the given duration
func (b *CleanupPolicyBuilder) OlderThan(duration time.Duration) *CleanupPolicyBuilder {
	t := time.Now().Add(-duration)
	b.policy.BeforeTimestamp = &t
	return b
}

// BeforeVersion sets the policy to clean versions before the given version
func (b *CleanupPolicyBuilder) BeforeVersion(version uint64) *CleanupPolicyBuilder {
	b.policy.BeforeVersion = &version
	return b
}

// RetainVersions sets the number of recent versions to retain
func (b *CleanupPolicyBuilder) RetainVersions(n int) *CleanupPolicyBuilder {
	b.policy.RetainVersions = n
	return b
}

// DeleteUnverified enables deletion of unverified files older than the given days
func (b *CleanupPolicyBuilder) DeleteUnverified(days int) *CleanupPolicyBuilder {
	b.policy.DeleteUnverified = true
	b.policy.UnverifiedThresholdDays = days
	return b
}

// Build returns the configured CleanupPolicy
func (b *CleanupPolicyBuilder) Build() CleanupPolicy {
	return b.policy
}

// CleanupStats contains statistics about the cleanup operation
type CleanupStats struct {
	// ManifestsRemoved is the number of manifest files removed
	ManifestsRemoved int
	// DataFilesRemoved is the number of data files removed
	DataFilesRemoved int
	// DeleteFilesRemoved is the number of deletion files removed
	DeleteFilesRemoved int
	// IndexFilesRemoved is the number of index files removed
	IndexFilesRemoved int
	// TxFilesRemoved is the number of transaction files removed
	TxFilesRemoved int
	// BytesRemoved is the total bytes removed
	BytesRemoved uint64
	// Errors contains any errors encountered during cleanup
	Errors []error
}

// referencedFiles tracks files referenced by manifests
type referencedFiles struct {
	dataFiles   map[string]struct{} // data/{fragment_id}/{file_idx}.lance
	deleteFiles map[string]struct{} // data/{fragment_id}/_deletions/{file}.arrow
	indexFiles  map[string]struct{} // _indices/{uuid}/...
	txFiles     map[string]struct{} // _transactions/{txn_id}.txn
}

// newReferencedFiles creates a new referencedFiles instance
func newReferencedFiles() *referencedFiles {
	return &referencedFiles{
		dataFiles:   make(map[string]struct{}),
		deleteFiles: make(map[string]struct{}),
		indexFiles:  make(map[string]struct{}),
		txFiles:     make(map[string]struct{}),
	}
}

// collectReferencedFiles collects all files referenced by the given manifest
func collectReferencedFiles(manifest *Manifest) *referencedFiles {
	refs := newReferencedFiles()
	if manifest == nil {
		return refs
	}

	// Collect data files and deletion files from fragments
	for _, frag := range manifest.Fragments {
		if frag == nil {
			continue
		}
		// Data files
		for _, df := range frag.Files {
			if df != nil && df.Path != "" {
				refs.dataFiles[df.Path] = struct{}{}
			}
		}
		// Deletion files - build path from fragment info
		if frag.DeletionFile != nil && frag.DeletionFile.NumDeletedRows > 0 {
			ext := "arrow"
			if frag.DeletionFile.FileType == 1 { // DeletionFile_BITMAP
				ext = "bin"
			}
			delPath := fmt.Sprintf("_deletions/%d-%d-%d.%s",
				frag.Id, frag.DeletionFile.ReadVersion, frag.DeletionFile.Id, ext)
			refs.deleteFiles[delPath] = struct{}{}
		}
	}

	// Collect index files (using index UUIDs)
	// Index files are stored under _indices/{uuid}/
	// The manifest stores index metadata with UUIDs

	// Collect transaction file reference
	if manifest.TransactionFile != "" {
		refs.txFiles[manifest.TransactionFile] = struct{}{}
	}

	return refs
}

// mergeReferencedFiles merges multiple referencedFiles into one
func mergeReferencedFiles(refsList ...*referencedFiles) *referencedFiles {
	merged := newReferencedFiles()
	for _, refs := range refsList {
		if refs == nil {
			continue
		}
		for k := range refs.dataFiles {
			merged.dataFiles[k] = struct{}{}
		}
		for k := range refs.deleteFiles {
			merged.deleteFiles[k] = struct{}{}
		}
		for k := range refs.indexFiles {
			merged.indexFiles[k] = struct{}{}
		}
		for k := range refs.txFiles {
			merged.txFiles[k] = struct{}{}
		}
	}
	return merged
}

// versionInfo holds information about a manifest version
type versionInfo struct {
	version   uint64
	path      string
	timestamp time.Time
}

// CleanupOldVersions cleans up old versions according to the given policy
//
// Cleanup flow:
// 1. List all manifest files
// 2. Determine which manifests to retain based on policy
// 3. Collect all files referenced by retained manifests
// 4. Delete unreferenced data files, deletion files, and index files
// 5. Delete old manifests
func CleanupOldVersions(ctx context.Context, basePath string, store ObjectStoreExt,
	handler CommitHandler, policy CleanupPolicy) (*CleanupStats, error) {

	stats := &CleanupStats{}

	// 1. List all manifest files and get their info
	versions, err := listManifestVersions(basePath, store)
	if err != nil {
		return stats, fmt.Errorf("list manifest versions: %w", err)
	}

	if len(versions) == 0 {
		return stats, nil // Nothing to clean
	}

	// Sort by version descending (newest first)
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].version > versions[j].version
	})

	// 2. Determine which versions to retain
	versionsToRetain, versionsToDelete := classifyVersions(versions, policy)

	if len(versionsToDelete) == 0 {
		return stats, nil // Nothing to clean
	}

	// 3. Collect files referenced by retained manifests
	retainedRefs := newReferencedFiles()
	for _, v := range versionsToRetain {
		manifest, err := LoadManifest(ctx, basePath, handler, v.version)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("load manifest v%d: %w", v.version, err))
			continue
		}
		refs := collectReferencedFiles(manifest)
		retainedRefs = mergeReferencedFiles(retainedRefs, refs)
	}

	// Collect files referenced by manifests to be deleted (for comparison)
	deleteRefs := newReferencedFiles()
	for _, v := range versionsToDelete {
		manifest, err := LoadManifest(ctx, basePath, handler, v.version)
		if err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("load manifest v%d: %w", v.version, err))
			continue
		}
		refs := collectReferencedFiles(manifest)
		deleteRefs = mergeReferencedFiles(deleteRefs, refs)
	}

	// 4. Delete unreferenced files
	// Data files
	for path := range deleteRefs.dataFiles {
		if _, retained := retainedRefs.dataFiles[path]; !retained {
			fullPath := filepath.Join(basePath, path)
			size := getFileSize(fullPath)
			if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
				stats.Errors = append(stats.Errors, fmt.Errorf("delete data file %s: %w", path, err))
			} else {
				stats.DataFilesRemoved++
				stats.BytesRemoved += uint64(size)
			}
		}
	}

	// Deletion files
	for path := range deleteRefs.deleteFiles {
		if _, retained := retainedRefs.deleteFiles[path]; !retained {
			fullPath := filepath.Join(basePath, path)
			size := getFileSize(fullPath)
			if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
				stats.Errors = append(stats.Errors, fmt.Errorf("delete deletion file %s: %w", path, err))
			} else {
				stats.DeleteFilesRemoved++
				stats.BytesRemoved += uint64(size)
			}
		}
	}

	// Transaction files
	for path := range deleteRefs.txFiles {
		if _, retained := retainedRefs.txFiles[path]; !retained {
			fullPath := filepath.Join(basePath, path)
			size := getFileSize(fullPath)
			if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
				stats.Errors = append(stats.Errors, fmt.Errorf("delete tx file %s: %w", path, err))
			} else {
				stats.TxFilesRemoved++
				stats.BytesRemoved += uint64(size)
			}
		}
	}

	// 5. Delete old manifests
	for _, v := range versionsToDelete {
		fullPath := filepath.Join(basePath, v.path)
		size := getFileSize(fullPath)
		if err := os.Remove(fullPath); err != nil && !os.IsNotExist(err) {
			stats.Errors = append(stats.Errors, fmt.Errorf("delete manifest v%d: %w", v.version, err))
		} else {
			stats.ManifestsRemoved++
			stats.BytesRemoved += uint64(size)
		}
	}

	// 6. Optionally clean up unverified files
	if policy.DeleteUnverified {
		cleanupUnverifiedFiles(ctx, basePath, store, retainedRefs, policy, stats)
	}

	return stats, nil
}

// listManifestVersions lists all manifest versions in the dataset
func listManifestVersions(basePath string, store ObjectStoreExt) ([]versionInfo, error) {
	versionsDir := filepath.Join(basePath, VersionsDir)
	entries, err := os.ReadDir(versionsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var versions []versionInfo
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, "."+ManifestExtension) {
			continue
		}

		version, _, err := ParseVersionEx(name)
		if err != nil {
			continue // Skip invalid files
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		versions = append(versions, versionInfo{
			version:   version,
			path:      filepath.Join(VersionsDir, name),
			timestamp: info.ModTime(),
		})
	}

	return versions, nil
}

// classifyVersions splits versions into those to retain and those to delete
func classifyVersions(versions []versionInfo, policy CleanupPolicy) (retain, delete []versionInfo) {
	if len(versions) == 0 {
		return nil, nil
	}

	// If RetainVersions is set, use it as the primary criterion
	if policy.RetainVersions > 0 {
		if policy.RetainVersions >= len(versions) {
			return versions, nil // Keep all
		}
		return versions[:policy.RetainVersions], versions[policy.RetainVersions:]
	}

	// Otherwise, use timestamp or version threshold
	for _, v := range versions {
		shouldDelete := false

		if policy.BeforeVersion != nil && v.version < *policy.BeforeVersion {
			shouldDelete = true
		}

		if policy.BeforeTimestamp != nil && v.timestamp.Before(*policy.BeforeTimestamp) {
			shouldDelete = true
		}

		if shouldDelete {
			delete = append(delete, v)
		} else {
			retain = append(retain, v)
		}
	}

	// Always retain at least one version
	if len(retain) == 0 && len(delete) > 0 {
		retain = append(retain, delete[0])
		delete = delete[1:]
	}

	return retain, delete
}

// cleanupUnverifiedFiles removes files that are not referenced by any manifest
// and are older than the threshold
func cleanupUnverifiedFiles(ctx context.Context, basePath string, store ObjectStoreExt,
	retainedRefs *referencedFiles, policy CleanupPolicy, stats *CleanupStats) {

	threshold := time.Now().AddDate(0, 0, -policy.UnverifiedThresholdDays)

	// Clean up orphaned data files in data/ directory
	dataDir := filepath.Join(basePath, "data")
	cleanupOrphanedFiles(dataDir, retainedRefs.dataFiles, threshold, stats, "data")

	// Clean up orphaned transaction files
	txDir := filepath.Join(basePath, TransactionsDir)
	cleanupOrphanedTxFiles(txDir, retainedRefs.txFiles, threshold, stats)
}

// cleanupOrphanedFiles removes orphaned files from a directory
func cleanupOrphanedFiles(dir string, referenced map[string]struct{}, threshold time.Time,
	stats *CleanupStats, fileType string) {

	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}

		// Check if file is referenced
		relPath, _ := filepath.Rel(filepath.Dir(dir), path)
		if _, ok := referenced[relPath]; ok {
			return nil // File is referenced, keep it
		}

		// Check if file is old enough to delete
		if info.ModTime().After(threshold) {
			return nil // Too recent, might be in use
		}

		// Delete orphaned file
		size := info.Size()
		if err := os.Remove(path); err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("delete orphaned %s file %s: %w", fileType, path, err))
		} else {
			stats.DataFilesRemoved++
			stats.BytesRemoved += uint64(size)
		}

		return nil
	})
}

// cleanupOrphanedTxFiles removes orphaned transaction files
func cleanupOrphanedTxFiles(dir string, referenced map[string]struct{}, threshold time.Time,
	stats *CleanupStats) {

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasSuffix(name, "."+TransactionExtension) {
			continue
		}

		// Check if file is referenced
		relPath := filepath.Join(TransactionsDir, name)
		if _, ok := referenced[relPath]; ok {
			continue // File is referenced, keep it
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Check if file is old enough to delete
		if info.ModTime().After(threshold) {
			continue // Too recent, might be in use
		}

		// Delete orphaned file
		fullPath := filepath.Join(dir, name)
		size := info.Size()
		if err := os.Remove(fullPath); err != nil {
			stats.Errors = append(stats.Errors, fmt.Errorf("delete orphaned tx file %s: %w", name, err))
		} else {
			stats.TxFilesRemoved++
			stats.BytesRemoved += uint64(size)
		}
	}
}

// getFileSize returns the size of a file, or 0 if it cannot be determined
func getFileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// CleanupTable is a convenience method on Table for cleanup
func (t *Table) Cleanup(ctx context.Context, policy CleanupPolicy) (*CleanupStats, error) {
	store := NewLocalObjectStoreExt(t.BasePath, nil)
	return CleanupOldVersions(ctx, t.BasePath, store, t.Handler, policy)
}

// CleanupOldVersionsSimple provides a simplified cleanup API
func CleanupOldVersionsSimple(ctx context.Context, basePath string, handler CommitHandler,
	retainVersions int) (*CleanupStats, error) {

	policy := NewCleanupPolicyBuilder().RetainVersions(retainVersions).Build()
	store := NewLocalObjectStoreExt(basePath, nil)
	return CleanupOldVersions(ctx, basePath, store, handler, policy)
}
