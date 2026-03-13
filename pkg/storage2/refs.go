package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const MainBranch = "main"

// Ref represents a reference to a version in the dataset
type Ref struct {
	// Branch name (nil for main branch)
	Branch *string
	// Version number (nil for latest)
	Version *uint64
	// Tag name (if this is a tag reference)
	Tag string
}

// Refs manages tags and branches for a dataset
type Refs struct {
	basePath    string
	handler     CommitHandler
	objectStore ObjectStore
}

// NewRefs creates a new Refs manager
func NewRefs(basePath string, handler CommitHandler, store ObjectStore) *Refs {
	return &Refs{
		basePath:    basePath,
		handler:     handler,
		objectStore: store,
	}
}

// Tags returns a Tags manager
func (r *Refs) Tags() *Tags {
	return &Tags{refs: r}
}

// Branches returns a Branches manager
func (r *Refs) Branches() *Branches {
	return &Branches{refs: r}
}

// TagContents represents the contents of a tag file
type TagContents struct {
	Branch       *string `json:"branch,omitempty"`
	Version      uint64  `json:"version"`
	ManifestSize int     `json:"manifestSize"`
}

// BranchContents represents the contents of a branch file
type BranchContents struct {
	ParentBranch  *string `json:"parentBranch,omitempty"`
	ParentVersion uint64  `json:"parentVersion"`
	CreatedAt     uint64  `json:"createAt"`
	ManifestSize  int     `json:"manifestSize"`
}

// Tags manages tag operations
type Tags struct {
	refs *Refs
}

// List returns all tags and their contents
func (t *Tags) List(ctx context.Context) (map[string]*TagContents, error) {
	tagsPath := t.baseTagsPath()
	entries, err := t.refs.objectStore.List(tagsPath)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*TagContents)
	for _, entry := range entries {
		if !strings.HasSuffix(entry, ".json") {
			continue
		}
		tagName := strings.TrimSuffix(entry, ".json")
		contents, err := t.Get(ctx, tagName)
		if err != nil {
			continue // Skip corrupted tags
		}
		result[tagName] = contents
	}
	return result, nil
}

// Get returns the contents of a specific tag
func (t *Tags) Get(ctx context.Context, tag string) (*TagContents, error) {
	if err := CheckValidTag(tag); err != nil {
		return nil, err
	}

	tagPath := t.tagPath(tag)
	data, err := t.refs.objectStore.Read(tagPath)
	if err != nil {
		return nil, fmt.Errorf("tag %s does not exist", tag)
	}

	var contents TagContents
	if err := json.Unmarshal(data, &contents); err != nil {
		return nil, fmt.Errorf("failed to parse tag contents: %w", err)
	}

	return &contents, nil
}

// Create creates a new tag pointing to a version
func (t *Tags) Create(ctx context.Context, tag string, version uint64) error {
	if err := CheckValidTag(tag); err != nil {
		return err
	}

	tagPath := t.tagPath(tag)
	data, err := t.refs.objectStore.Read(tagPath)
	if err == nil && len(data) > 0 {
		return fmt.Errorf("tag %s already exists", tag)
	}

	// Verify the version exists
	manifest, err := LoadManifest(ctx, t.refs.basePath, t.refs.handler, version)
	if err != nil {
		return fmt.Errorf("version %d does not exist", version)
	}

	contents := &TagContents{
		Branch:       manifest.Branch,
		Version:      version,
		ManifestSize: 0, // Would need to calculate actual size
	}

	jsonData, err := json.MarshalIndent(contents, "", "  ")
	if err != nil {
		return err
	}

	return t.refs.objectStore.Write(tagPath, jsonData)
}

// Delete deletes a tag
func (t *Tags) Delete(ctx context.Context, tag string) error {
	if err := CheckValidTag(tag); err != nil {
		return err
	}

	tagPath := t.tagPath(tag)
	_, err := t.refs.objectStore.Read(tagPath)
	if err != nil {
		return fmt.Errorf("tag %s does not exist", tag)
	}

	return os.Remove(filepath.Join(t.refs.basePath, tagPath))
}

// Update updates a tag to point to a different version
func (t *Tags) Update(ctx context.Context, tag string, version uint64) error {
	if err := CheckValidTag(tag); err != nil {
		return err
	}

	tagPath := t.tagPath(tag)
	_, err := t.refs.objectStore.Read(tagPath)
	if err != nil {
		return fmt.Errorf("tag %s does not exist", tag)
	}

	// Verify the version exists
	manifest, err := LoadManifest(ctx, t.refs.basePath, t.refs.handler, version)
	if err != nil {
		return fmt.Errorf("version %d does not exist", version)
	}

	contents := &TagContents{
		Branch:       manifest.Branch,
		Version:      version,
		ManifestSize: 0,
	}

	jsonData, err := json.MarshalIndent(contents, "", "  ")
	if err != nil {
		return err
	}

	return t.refs.objectStore.Write(tagPath, jsonData)
}

func (t *Tags) baseTagsPath() string {
	return filepath.Join("_refs", "tags")
}

func (t *Tags) tagPath(tag string) string {
	return filepath.Join(t.baseTagsPath(), tag+".json")
}

// Branches manages branch operations
type Branches struct {
	refs *Refs
}

// List returns all branches and their contents
func (b *Branches) List(ctx context.Context) (map[string]*BranchContents, error) {
	branchesPath := b.baseBranchesPath()
	entries, err := b.refs.objectStore.List(branchesPath)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*BranchContents)
	for _, entry := range entries {
		if !strings.HasSuffix(entry, ".json") {
			continue
		}
		branchName := strings.TrimSuffix(entry, ".json")
		contents, err := b.Get(ctx, branchName)
		if err != nil {
			continue // Skip corrupted branches
		}
		result[branchName] = contents
	}
	return result, nil
}

// Get returns the contents of a specific branch
func (b *Branches) Get(ctx context.Context, branch string) (*BranchContents, error) {
	if err := CheckValidBranch(branch); err != nil {
		return nil, err
	}

	branchPath := b.branchPath(branch)
	data, err := b.refs.objectStore.Read(branchPath)
	if err != nil {
		return nil, fmt.Errorf("branch %s does not exist", branch)
	}

	var contents BranchContents
	if err := json.Unmarshal(data, &contents); err != nil {
		return nil, fmt.Errorf("failed to parse branch contents: %w", err)
	}

	return &contents, nil
}

// Create creates a new branch from a version
func (b *Branches) Create(ctx context.Context, branchName string, version uint64, sourceBranch *string) error {
	if err := CheckValidBranch(branchName); err != nil {
		return err
	}

	branchPath := b.branchPath(branchName)
	data, err := b.refs.objectStore.Read(branchPath)
	if err == nil && len(data) > 0 {
		return fmt.Errorf("branch %s already exists", branchName)
	}

	// Verify the source version exists
	_, err = LoadManifest(ctx, b.refs.basePath, b.refs.handler, version)
	if err != nil {
		return fmt.Errorf("version %d does not exist", version)
	}

	contents := &BranchContents{
		ParentBranch:  sourceBranch,
		ParentVersion: version,
		CreatedAt:     uint64(time.Now().Unix()),
		ManifestSize:  0,
	}

	jsonData, err := json.MarshalIndent(contents, "", "  ")
	if err != nil {
		return err
	}

	return b.refs.objectStore.Write(branchPath, jsonData)
}

// Delete deletes a branch
func (b *Branches) Delete(ctx context.Context, branch string, force bool) error {
	if err := CheckValidBranch(branch); err != nil {
		return err
	}

	branchPath := b.branchPath(branch)
	_, err := b.refs.objectStore.Read(branchPath)
	if err != nil {
		if !force {
			return fmt.Errorf("branch %s does not exist", branch)
		}
	} else {
		if err := os.Remove(filepath.Join(b.refs.basePath, branchPath)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// Clean up branch directory
	branchDir := filepath.Join(b.refs.basePath, "tree", branch)
	_ = os.RemoveAll(branchDir) // Ignore errors for cleanup

	return nil
}

func (b *Branches) baseBranchesPath() string {
	return filepath.Join("_refs", "branches")
}

func (b *Branches) branchPath(branch string) string {
	return filepath.Join(b.baseBranchesPath(), branch+".json")
}

// CheckValidTag validates a tag name
func CheckValidTag(tag string) error {
	if tag == "" {
		return fmt.Errorf("tag name cannot be empty")
	}

	// Only alphanumeric, '.', '-', '_'
	validTag := regexp.MustCompile(`^[a-zA-Z0-9.\-_]+$`)
	if !validTag.MatchString(tag) {
		return fmt.Errorf("tag name contains invalid characters")
	}

	if strings.HasPrefix(tag, ".") {
		return fmt.Errorf("tag name cannot begin with a dot")
	}

	if strings.HasSuffix(tag, ".") {
		return fmt.Errorf("tag name cannot end with a dot")
	}

	if strings.HasSuffix(tag, ".lock") {
		return fmt.Errorf("tag name cannot end with .lock")
	}

	if strings.Contains(tag, "..") {
		return fmt.Errorf("tag name cannot have two consecutive dots")
	}

	return nil
}

// CheckValidBranch validates a branch name
func CheckValidBranch(branch string) error {
	if branch == "" {
		return fmt.Errorf("branch name cannot be empty")
	}

	if strings.HasPrefix(branch, "/") || strings.HasSuffix(branch, "/") {
		return fmt.Errorf("branch name cannot start or end with a '/'")
	}

	if strings.Contains(branch, "//") {
		return fmt.Errorf("branch name cannot contain consecutive '/'")
	}

	if strings.Contains(branch, "..") || strings.Contains(branch, "\\") {
		return fmt.Errorf("branch name cannot contain '..' or '\\'")
	}

	for _, segment := range strings.Split(branch, "/") {
		if segment == "" {
			return fmt.Errorf("branch name cannot have empty segments between '/'")
		}
		validSegment := regexp.MustCompile(`^[a-zA-Z0-9.\-_]+$`)
		if !validSegment.MatchString(segment) {
			return fmt.Errorf("branch segment '%s' contains invalid characters", segment)
		}
	}

	if strings.HasSuffix(branch, ".lock") {
		return fmt.Errorf("branch name cannot end with '.lock'")
	}

	if branch == MainBranch {
		return fmt.Errorf("branch name cannot be 'main'")
	}

	return nil
}

// Restore restores a dataset to a previous version
func Restore(ctx context.Context, basePath string, handler CommitHandler, version uint64) (*Manifest, error) {
	// Load the target version's manifest
	targetManifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return nil, fmt.Errorf("failed to load version %d: %w", version, err)
	}

	// Get the latest version
	latest, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return nil, err
	}

	// Create a new version that restores to the target
	newVersion := latest + 1

	// Create a new manifest with the target's content but new version
	newManifest := &Manifest{
		Version:            newVersion,
		Fields:             targetManifest.Fields,
		Fragments:          targetManifest.Fragments,
		Config:             targetManifest.Config,
		SchemaMetadata:     targetManifest.SchemaMetadata,
		TableMetadata:      targetManifest.TableMetadata,
		ReaderFeatureFlags: targetManifest.ReaderFeatureFlags,
		WriterFeatureFlags: targetManifest.WriterFeatureFlags,
		NextRowId:          targetManifest.NextRowId,
		Branch:             targetManifest.Branch,
		Tag:                "", // Clear tag on restore
	}

	// Commit the new manifest
	if err := handler.Commit(ctx, basePath, newVersion, newManifest); err != nil {
		return nil, fmt.Errorf("failed to commit restore: %w", err)
	}

	return newManifest, nil
}

// CheckoutVersion checks out a specific version (read-only view)
func CheckoutVersion(ctx context.Context, basePath string, handler CommitHandler, version uint64) (*Manifest, error) {
	return LoadManifest(ctx, basePath, handler, version)
}

// CheckoutTag checks out a version by tag (read-only view)
func CheckoutTag(ctx context.Context, basePath string, handler CommitHandler, tag string) (*Manifest, error) {
	// First, try to read tag from refs structure
	store := NewLocalObjectStore(basePath)
	refs := NewRefs(basePath, handler, store)
	tagContents, err := refs.Tags().Get(ctx, tag)
	if err == nil {
		return LoadManifest(ctx, basePath, handler, tagContents.Version)
	}

	// Fall back to legacy tag lookup
	version, ok, err := ResolveTagVersion(ctx, basePath, handler, tag)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("tag %s does not exist", tag)
	}
	return LoadManifest(ctx, basePath, handler, version)
}

// CreateBranch creates a new branch from a version
func CreateBranch(ctx context.Context, basePath string, handler CommitHandler, branchName string, version uint64, sourceBranch *string) error {
	store := NewLocalObjectStore(basePath)
	refs := NewRefs(basePath, handler, store)
	return refs.Branches().Create(ctx, branchName, version, sourceBranch)
}

// DeleteBranch deletes a branch
func DeleteBranch(ctx context.Context, basePath string, handler CommitHandler, branch string, force bool) error {
	store := NewLocalObjectStore(basePath)
	refs := NewRefs(basePath, handler, store)
	return refs.Branches().Delete(ctx, branch, force)
}

// ListBranches lists all branches
func ListBranches(ctx context.Context, basePath string, handler CommitHandler) (map[string]*BranchContents, error) {
	store := NewLocalObjectStore(basePath)
	refs := NewRefs(basePath, handler, store)
	return refs.Branches().List(ctx)
}
