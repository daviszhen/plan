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

	// Check if using V1 naming scheme
	// In V2, manifests use inverted version numbers
	_ = manifest

	return false, nil
}

// MigrateToV2 migrates the table to V2 format
func (m *MigrationManager) MigrateToV2(ctx context.Context) error {
	// TODO: Implement migration from V1 to V2
	// This involves:
	// 1. Renaming manifest files to use inverted version numbers
	// 2. Updating metadata
	return nil
}
