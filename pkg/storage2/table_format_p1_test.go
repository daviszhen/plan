package storage2

import (
	"context"
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// TestCreateTable tests creating a new table.
func TestCreateTable(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	// Create table with default config
	table, err := CreateTable(ctx, dir, handler, nil)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if table == nil {
		t.Fatal("table should not be nil")
	}
	if table.BasePath != dir {
		t.Errorf("BasePath: got %q, want %q", table.BasePath, dir)
	}

	// Verify initial version exists
	version, err := table.GetLatestVersion(ctx)
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if version != 0 {
		t.Errorf("initial version: got %d, want 0", version)
	}
}

// TestCreateTableWithConfig tests creating a table with custom config.
func TestCreateTableWithConfig(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	config := &TableConfig{
		Format:              TableFormatV2,
		EnableStableRowIDs:  true,
		EnableDeletionFiles: true,
	}

	table, err := CreateTable(ctx, dir, handler, config)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify config was applied
	if table.Config.Format != TableFormatV2 {
		t.Errorf("Format: got %v, want V2", table.Config.Format)
	}
	if !table.Config.EnableStableRowIDs {
		t.Error("EnableStableRowIDs should be true")
	}
	if !table.Config.EnableDeletionFiles {
		t.Error("EnableDeletionFiles should be true")
	}
}

// TestOpenTable tests opening an existing table.
func TestOpenTable(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	// Create table first
	_, err := CreateTable(ctx, dir, handler, nil)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Open existing table
	table, err := OpenTable(ctx, dir, handler)
	if err != nil {
		t.Fatalf("OpenTable failed: %v", err)
	}
	if table == nil {
		t.Fatal("table should not be nil")
	}
}

// TestOpenTableNotFound tests opening a non-existent table.
func TestOpenTableNotFound(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	// Try to open - may succeed but GetManifest should fail
	table, err := OpenTable(ctx, dir, handler)
	if err != nil {
		// Expected: table not found
		return
	}

	// If OpenTable succeeded, GetManifest should fail for version 0
	_, err = table.GetManifest(ctx, 0)
	if err == nil {
		t.Error("GetManifest should fail for non-existent table")
	}
}

// TestTableGetLatestVersion tests getting the latest version.
func TestTableGetLatestVersion(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	version, err := table.GetLatestVersion(ctx)
	if err != nil {
		t.Fatalf("GetLatestVersion failed: %v", err)
	}
	if version != 0 {
		t.Errorf("version: got %d, want 0", version)
	}
}

// TestTableGetManifest tests getting a manifest.
func TestTableGetManifest(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	m, err := table.GetManifest(ctx, 0)
	if err != nil {
		t.Fatalf("GetManifest failed: %v", err)
	}
	if m == nil {
		t.Fatal("manifest should not be nil")
	}
	if m.Version != 0 {
		t.Errorf("manifest version: got %d, want 0", m.Version)
	}
}

// TestTableGetSchema tests getting the schema.
func TestTableGetSchema(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	fields, err := table.GetSchema(ctx, 0)
	if err != nil {
		t.Fatalf("GetSchema failed: %v", err)
	}
	// Initial table has no fields
	if fields != nil && len(fields) != 0 {
		t.Errorf("initial schema should be empty, got %d fields", len(fields))
	}
}

// TestTableCountRows tests counting rows.
func TestTableCountRows(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	count, err := table.CountRows(ctx, 0)
	if err != nil {
		t.Fatalf("CountRows failed: %v", err)
	}
	if count != 0 {
		t.Errorf("initial count: got %d, want 0", count)
	}
}

// TestTableGetStats tests getting table statistics.
func TestTableGetStats(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	stats, err := table.GetStats(ctx, 0)
	if err != nil {
		t.Fatalf("GetStats failed: %v", err)
	}
	if stats == nil {
		t.Fatal("stats should not be nil")
	}
	if stats.NumFragments != 0 {
		t.Errorf("NumFragments: got %d, want 0", stats.NumFragments)
	}
	if stats.TotalRows != 0 {
		t.Errorf("TotalRows: got %d, want 0", stats.TotalRows)
	}
}

// TestTableValidateTable tests validating a table.
func TestTableValidateTable(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	err := table.ValidateTable(ctx)
	if err != nil {
		t.Fatalf("ValidateTable failed: %v", err)
	}
}

// TestDefaultTableConfig tests the default table configuration.
func TestDefaultTableConfig(t *testing.T) {
	config := DefaultTableConfig()

	if config.Format != TableFormatV2 {
		t.Errorf("Format: got %v, want V2", config.Format)
	}
	if !config.EnableStableRowIDs {
		t.Error("EnableStableRowIDs should be true by default")
	}
	if !config.EnableDeletionFiles {
		t.Error("EnableDeletionFiles should be true by default")
	}
	if config.EnableMoveStableRowIDs {
		t.Error("EnableMoveStableRowIDs should be false by default")
	}
}

// TestNewTable tests creating a table instance directly.
func TestNewTable(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	table := NewTable(dir, handler)

	if table.BasePath != dir {
		t.Errorf("BasePath: got %q, want %q", table.BasePath, dir)
	}
	if table.Handler != handler {
		t.Error("Handler mismatch")
	}
	if table.Config == nil {
		t.Error("Config should not be nil")
	}
	if table.Refs == nil {
		t.Error("Refs should not be nil")
	}
	if table.IndexManager == nil {
		t.Error("IndexManager should not be nil")
	}
}

// TestMigrationManagerNeedsMigration tests checking if migration is needed.
func TestMigrationManagerNeedsMigration(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	mgr := NewMigrationManager(table)
	needs, err := mgr.NeedsMigration(ctx)
	if err != nil {
		t.Fatalf("NeedsMigration failed: %v", err)
	}
	// Newly created tables don't need migration
	if needs {
		t.Error("new table should not need migration")
	}
}

// TestMigrationManagerMigrateToV2 tests migrating to V2 format.
func TestMigrationManagerMigrateToV2(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, _ := CreateTable(ctx, dir, handler, nil)

	mgr := NewMigrationManager(table)
	err := mgr.MigrateToV2(ctx)
	// Currently just returns nil (not implemented)
	if err != nil {
		t.Fatalf("MigrateToV2 failed: %v", err)
	}
}

// TestCountDeletedRows tests counting deleted rows.
func TestCountDeletedRows(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()
	store := NewLocalObjectStoreExt(dir, nil)

	// Create a table
	_, err := CreateTable(ctx, dir, handler, nil)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Count deleted rows on empty table
	count, err := CountDeletedRows(ctx, dir, store, handler)
	if err != nil {
		t.Fatalf("CountDeletedRows failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 deleted rows, got %d", count)
	}
}

// TestCountDeletedRowsFromManifest tests counting deleted rows from manifest.
func TestCountDeletedRowsFromManifest(t *testing.T) {
	// Test nil manifest
	count := CountDeletedRowsFromManifest(nil)
	if count != 0 {
		t.Errorf("nil manifest: expected 0, got %d", count)
	}

	// Test manifest with no deletions
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		{Id: 0, PhysicalRows: 100},
		{Id: 1, PhysicalRows: 200},
	}
	count = CountDeletedRowsFromManifest(m)
	if count != 0 {
		t.Errorf("no deletions: expected 0, got %d", count)
	}

	// Test manifest with deletions
	m.Fragments[0].DeletionFile = &storage2pb.DeletionFile{NumDeletedRows: 10}
	m.Fragments[1].DeletionFile = &storage2pb.DeletionFile{NumDeletedRows: 25}
	count = CountDeletedRowsFromManifest(m)
	if count != 35 {
		t.Errorf("with deletions: expected 35, got %d", count)
	}
}

// TestTableCountDeletedRows tests the Table method.
func TestTableCountDeletedRows(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	table, err := CreateTable(ctx, dir, handler, nil)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	count, err := table.CountDeletedRows(ctx)
	if err != nil {
		t.Fatalf("CountDeletedRows failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 deleted rows, got %d", count)
	}
}
