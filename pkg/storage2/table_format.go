package storage2

import (
	"context"
	"fmt"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// TableFormat represents the format version of the table
type TableFormat int

const (
	// TableFormatV1 is the original table format
	TableFormatV1 TableFormat = iota
	// TableFormatV2 is the improved table format with better object store support
	TableFormatV2
)

// TableConfig contains table-level configuration
type TableConfig struct {
	// Format is the table format version
	Format TableFormat
	// EnableStableRowIDs enables stable row ID tracking
	EnableStableRowIDs bool
	// EnableDeletionFiles enables deletion file support
	EnableDeletionFiles bool
	// EnableMoveStableRowIDs enables moving stable row IDs during compaction
	EnableMoveStableRowIDs bool
}

// DefaultTableConfig returns the default table configuration
func DefaultTableConfig() *TableConfig {
	return &TableConfig{
		Format:                 TableFormatV2,
		EnableStableRowIDs:     true,
		EnableDeletionFiles:    true,
		EnableMoveStableRowIDs: false,
	}
}

// Table represents a dataset table with all its metadata
type Table struct {
	// BasePath is the root path of the table
	BasePath string
	// Config is the table configuration
	Config *TableConfig
	// Handler is the commit handler
	Handler CommitHandler
	// Refs manages tags and branches
	Refs *Refs
	// IndexManager manages indexes
	IndexManager *IndexManager
}

// NewTable creates a new table instance
func NewTable(basePath string, handler CommitHandler) *Table {
	store := NewLocalObjectStore(basePath)
	return &Table{
		BasePath:     basePath,
		Config:       DefaultTableConfig(),
		Handler:      handler,
		Refs:         NewRefs(basePath, handler, store),
		IndexManager: NewIndexManager(basePath, handler),
	}
}

// CreateTable creates a new table
func CreateTable(ctx context.Context, basePath string, handler CommitHandler, config *TableConfig) (*Table, error) {
	if config == nil {
		config = DefaultTableConfig()
	}

	// Create initial manifest
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}

	// Set feature flags based on config
	if config.EnableStableRowIDs {
		m0.ReaderFeatureFlags |= 2 // Feature flag 2: stable row IDs
		m0.WriterFeatureFlags |= 2
	}
	if config.EnableDeletionFiles {
		m0.ReaderFeatureFlags |= 1 // Feature flag 1: deletion files
		m0.WriterFeatureFlags |= 1
	}

	if err := handler.Commit(ctx, basePath, 0, m0); err != nil {
		return nil, fmt.Errorf("failed to create initial manifest: %w", err)
	}

	return NewTable(basePath, handler), nil
}

// OpenTable opens an existing table
func OpenTable(ctx context.Context, basePath string, handler CommitHandler) (*Table, error) {
	// Verify table exists by checking for manifest
	_, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	return NewTable(basePath, handler), nil
}

// GetLatestVersion returns the latest version of the table
func (t *Table) GetLatestVersion(ctx context.Context) (uint64, error) {
	return t.Handler.ResolveLatestVersion(ctx, t.BasePath)
}

// GetManifest loads a manifest for a specific version
func (t *Table) GetManifest(ctx context.Context, version uint64) (*Manifest, error) {
	return LoadManifest(ctx, t.BasePath, t.Handler, version)
}

// GetSchema returns the schema of the table at the given version
func (t *Table) GetSchema(ctx context.Context, version uint64) ([]*storage2pb.Field, error) {
	m, err := t.GetManifest(ctx, version)
	if err != nil {
		return nil, err
	}
	return m.Fields, nil
}

// CountRows returns the total number of rows in the table
func (t *Table) CountRows(ctx context.Context, version uint64) (uint64, error) {
	m, err := t.GetManifest(ctx, version)
	if err != nil {
		return 0, err
	}

	var total uint64
	for _, f := range m.Fragments {
		if f != nil {
			total += f.PhysicalRows
		}
	}
	return total, nil
}

// TableStats contains statistics about a table
type TableStats struct {
	// NumVersions is the number of versions
	NumVersions uint64
	// NumFragments is the number of fragments
	NumFragments uint64
	// TotalRows is the total number of rows
	TotalRows uint64
	// TotalSizeBytes is the total size in bytes
	TotalSizeBytes uint64
	// IndexStats contains statistics about indexes
	IndexStats []IndexStats
}

// GetStats returns statistics about the table
func (t *Table) GetStats(ctx context.Context, version uint64) (*TableStats, error) {
	m, err := t.GetManifest(ctx, version)
	if err != nil {
		return nil, err
	}

	stats := &TableStats{
		NumFragments: uint64(len(m.Fragments)),
	}

	for _, f := range m.Fragments {
		if f != nil {
			stats.TotalRows += f.PhysicalRows
			for _, df := range f.Files {
				if df != nil {
					stats.TotalSizeBytes += uint64(df.FileSizeBytes)
				}
			}
		}
	}

	return stats, nil
}

// ValidateTable validates the integrity of the table
func (t *Table) ValidateTable(ctx context.Context) error {
	latest, err := t.GetLatestVersion(ctx)
	if err != nil {
		return err
	}

	m, err := t.GetManifest(ctx, latest)
	if err != nil {
		return err
	}

	// Validate fragments
	for _, f := range m.Fragments {
		if f == nil {
			continue
		}
		// Validate data files exist
		for _, df := range f.Files {
			if df == nil || df.Path == "" {
				continue
			}
			// TODO: Check if file exists
		}
	}

	return nil
}

// MigrationManager handles table format migrations
type MigrationManager struct {
	table *Table
}

// CountDeletedRows returns the total number of deleted rows across all fragments
func CountDeletedRows(ctx context.Context, basePath string, store ObjectStoreExt, handler CommitHandler) (uint64, error) {
	// Get the latest version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return 0, fmt.Errorf("resolve latest version: %w", err)
	}

	// Load the manifest
	manifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return 0, fmt.Errorf("load manifest: %w", err)
	}

	return CountDeletedRowsFromManifest(manifest), nil
}

// CountDeletedRowsFromManifest counts deleted rows from a given manifest
func CountDeletedRowsFromManifest(manifest *Manifest) uint64 {
	if manifest == nil {
		return 0
	}

	var totalDeleted uint64
	for _, frag := range manifest.Fragments {
		if frag == nil || frag.DeletionFile == nil {
			continue
		}
		totalDeleted += frag.DeletionFile.NumDeletedRows
	}

	return totalDeleted
}

// CountDeletedRowsForFragment returns the number of deleted rows for a specific fragment
func CountDeletedRowsForFragment(ctx context.Context, basePath string, store ObjectStoreExt,
	handler CommitHandler, fragmentID uint64) (uint64, error) {

	// Get the latest version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		return 0, fmt.Errorf("resolve latest version: %w", err)
	}

	// Load the manifest
	manifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		return 0, fmt.Errorf("load manifest: %w", err)
	}

	// Find the fragment
	for _, frag := range manifest.Fragments {
		if frag != nil && frag.Id == fragmentID {
			if frag.DeletionFile != nil {
				return frag.DeletionFile.NumDeletedRows, nil
			}
			return 0, nil
		}
	}

	return 0, fmt.Errorf("fragment %d not found", fragmentID)
}

// Table.CountDeletedRows returns the total number of deleted rows for this table
func (t *Table) CountDeletedRows(ctx context.Context) (uint64, error) {
	store := NewLocalObjectStoreExt(t.BasePath, nil)
	return CountDeletedRows(ctx, t.BasePath, store, t.Handler)
}

// NewMigrationManager creates a new migration manager
func NewMigrationManager(table *Table) *MigrationManager {
	return &MigrationManager{table: table}
}

// NeedsMigration checks if migration is needed
func (m *MigrationManager) NeedsMigration(ctx context.Context) (bool, error) {
	latest, err := m.table.GetLatestVersion(ctx)
	if err != nil {
		return false, err
	}

	manifest, err := m.table.GetManifest(ctx, latest)
	if err != nil {
		return false, err
	}

	// Check if using V1 naming scheme by examining existing manifest files
	store := NewLocalObjectStore(m.table.BasePath)
	versionsDir := m.table.BasePath + "/" + VersionsDir

	files, err := store.List(versionsDir)
	if err != nil {
		return false, nil // No versions dir means nothing to migrate
	}

	for _, file := range files {
		_, scheme, err := ParseVersionEx(file)
		if err != nil {
			continue
		}
		if scheme == ManifestNamingV1 {
			return true, nil
		}
	}

	_ = manifest
	return false, nil
}

// MigrateToV2 migrates the table from V1 naming scheme to V2 naming scheme.
// V1 uses simple version numbers: 1.manifest, 2.manifest, ...
// V2 uses inverted version numbers: %020d.manifest where number = MaxUint64 - version
func (m *MigrationManager) MigrateToV2(ctx context.Context) error {
	store := NewLocalObjectStore(m.table.BasePath)
	versionsDir := m.table.BasePath + "/" + VersionsDir

	// List all manifest files
	files, err := store.List(versionsDir)
	if err != nil {
		return fmt.Errorf("failed to list manifest files: %w", err)
	}

	// Collect V1 files that need migration
	type migrationItem struct {
		oldPath string
		newPath string
		version uint64
	}
	var toMigrate []migrationItem

	for _, file := range files {
		version, scheme, err := ParseVersionEx(file)
		if err != nil {
			continue
		}

		if scheme == ManifestNamingV1 {
			oldPath := m.table.BasePath + "/" + ManifestPathV1(version)
			newPath := m.table.BasePath + "/" + ManifestPathV2(version)
			toMigrate = append(toMigrate, migrationItem{
				oldPath: oldPath,
				newPath: newPath,
				version: version,
			})
		}
	}

	if len(toMigrate) == 0 {
		// Nothing to migrate
		return nil
	}

	// Perform migration: copy to new name, then delete old
	// We do copy+delete instead of rename to ensure atomicity
	for _, item := range toMigrate {
		// Read the manifest data
		data, err := store.Read(item.oldPath)
		if err != nil {
			return fmt.Errorf("failed to read manifest v%d: %w", item.version, err)
		}

		// Write to new path
		if err := store.Write(item.newPath, data); err != nil {
			return fmt.Errorf("failed to write migrated manifest v%d: %w", item.version, err)
		}
	}

	// Delete old files after all new files are written
	for _, item := range toMigrate {
		if err := store.Delete(item.oldPath); err != nil {
			// Log warning but continue - the migration is complete
			// Old files can be cleaned up later
			continue
		}
	}

	return nil
}
