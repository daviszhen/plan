package storage2

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// CommitHandler abstracts how to resolve the latest version and commit a new manifest atomically.
type CommitHandler interface {
	// ResolveLatestVersion returns the latest manifest version number for the dataset at basePath.
	// Returns 0 if no version exists yet.
	ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error)
	// ResolveVersion returns the path to the manifest file for the given version (relative to basePath).
	ResolveVersion(ctx context.Context, basePath string, version uint64) (string, error)
	// Commit writes the manifest so that it becomes the given version at basePath.
	// It must be atomic: only one writer can succeed for the same version.
	Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error
}

// LocalRenameCommitHandler commits by writing to a temp file then renaming (atomic on most local FS).
type LocalRenameCommitHandler struct{}

// NewLocalRenameCommitHandler returns a CommitHandler for local filesystem.
func NewLocalRenameCommitHandler() *LocalRenameCommitHandler {
	return &LocalRenameCommitHandler{}
}

// ResolveLatestVersion lists _versions/*.manifest and returns the highest version number.
func (h *LocalRenameCommitHandler) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	dir := filepath.Join(basePath, VersionsDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var maxVersion uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		v, err := ParseVersion(e.Name())
		if err != nil {
			continue
		}
		if v > maxVersion {
			maxVersion = v
		}
	}
	return maxVersion, nil
}

// ResolveVersion returns the relative path to the manifest file for the given version.
func (h *LocalRenameCommitHandler) ResolveVersion(_ context.Context, basePath string, version uint64) (string, error) {
	return ManifestPath(version), nil
}

// Commit writes the manifest to _versions/{version}.manifest using a temp file + rename.
func (h *LocalRenameCommitHandler) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}
	versionsDir := filepath.Join(basePath, VersionsDir)
	if err := os.MkdirAll(versionsDir, 0755); err != nil {
		return err
	}
	finalPath := filepath.Join(versionsDir, fmt.Sprintf("%d.%s", version, ManifestExtension))
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}
