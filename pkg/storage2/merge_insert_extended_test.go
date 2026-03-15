// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"sync"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// setupMergeInsertDataset creates a 2-column (id INTEGER, value BIGINT) dataset
// and returns the latest version.
func setupMergeInsertDataset(t *testing.T, basePath string, handler CommitHandler, store ObjectStoreExt, numRows int) uint64 {
	t.Helper()
	ctx := context.Background()

	// Create table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Create initial data
	types := []common.LType{common.IntegerType(), common.BigintType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		chk.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64(i * 100)})
	}
	chk.SetCard(numRows)

	// Insert initial data
	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)
	_, err = executor.Execute(ctx, []*chunk.Chunk{chk})
	if err != nil {
		t.Fatalf("insert initial data: %v", err)
	}

	// Get latest version
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		t.Fatalf("resolve version: %v", err)
	}

	return version
}

// TestMergeInsert_UpsertBasic tests insert initial data, then upsert overlapping keys,
// verifying updates and inserts
func TestMergeInsert_UpsertBasic(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset with 5 rows (id=0..4)
	_ = setupMergeInsertDataset(t, basePath, handler, store, 5)

	// Create source data with overlapping keys
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 4)

	// Rows: id=2 (update), id=3 (update), id=5 (insert), id=6 (insert)
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 2})
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 3})
	source.Data[0].SetValue(2, &chunk.Value{Typ: types[0], I64: 5})
	source.Data[0].SetValue(3, &chunk.Value{Typ: types[0], I64: 6})

	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 200}) // updated
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 300}) // updated
	source.Data[1].SetValue(2, &chunk.Value{Typ: types[1], I64: 500}) // new
	source.Data[1].SetValue(3, &chunk.Value{Typ: types[1], I64: 600}) // new
	source.SetCard(4)

	// Execute upsert (default: update matched, insert not matched)
	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Verify result counts
	if result.RowsUpdated != 2 {
		t.Errorf("expected 2 rows updated, got %d", result.RowsUpdated)
	}
	if result.RowsInserted != 2 {
		t.Errorf("expected 2 rows inserted, got %d", result.RowsInserted)
	}

	// Verify data by scanning
	// Note: ScanChunks does NOT filter deleted rows, so we get:
	// - Original fragment with 5 rows (id=0..4, with id=2,3 marked deleted)
	// - New fragment with 4 rows (id=2,3,5,6 - updated/inserted values)
	// Total: 9 rows from ScanChunks (deleted rows still counted)
	version, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		t.Fatalf("resolve version: %v", err)
	}

	manifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	// Check fragment count - should have 2 fragments now
	if len(manifest.Fragments) != 2 {
		t.Errorf("expected 2 fragments, got %d", len(manifest.Fragments))
	}

	// Verify the deletion file exists on the original fragment
	foundDeletion := false
	for _, frag := range manifest.Fragments {
		if frag.DeletionFile != nil {
			foundDeletion = true
			if frag.DeletionFile.NumDeletedRows != 2 {
				t.Errorf("expected 2 deleted rows in deletion file, got %d", frag.DeletionFile.NumDeletedRows)
			}
		}
	}
	if !foundDeletion {
		t.Error("expected deletion file to be created for updated rows")
	}
}

// TestMergeInsert_MatchedUpdateColumns tests using WhenMatchedUpdate(columns)
// to update only specific columns
func TestMergeInsert_MatchedUpdateColumns(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert initial data with 3 columns (id, value1, value2)
	types := []common.LType{common.IntegerType(), common.BigintType(), common.BigintType()}
	initial := &chunk.Chunk{}
	initial.Init(types, 2)

	initial.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	initial.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 2})
	initial.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 100})
	initial.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 200})
	initial.Data[2].SetValue(0, &chunk.Value{Typ: types[2], I64: 1000})
	initial.Data[2].SetValue(1, &chunk.Value{Typ: types[2], I64: 2000})
	initial.SetCard(2)

	initConfig := NewMergeInsertBuilder(0).Build()
	initExecutor := NewMergeInsertExecutor(basePath, store, handler, initConfig)
	_, err = initExecutor.Execute(ctx, []*chunk.Chunk{initial})
	if err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	// Create source data to update only column 1 (value1), not column 2 (value2)
	source := &chunk.Chunk{}
	source.Init(types, 1)
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1}) // matches id=1
	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 999})
	source.Data[2].SetValue(0, &chunk.Value{Typ: types[2], I64: 8888}) // should be ignored
	source.SetCard(1)

	// Configure to update only column 1
	config := NewMergeInsertBuilder(0).
		WhenMatchedUpdate(1). // only update column 1
		Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if result.RowsUpdated != 1 {
		t.Errorf("expected 1 row updated, got %d", result.RowsUpdated)
	}
}

// TestMergeInsert_MatchedDelete tests using WhenMatched(MatchedDelete)
// to delete matched rows
func TestMergeInsert_MatchedDelete(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset with 5 rows
	_ = setupMergeInsertDataset(t, basePath, handler, store, 5)

	// Create source data with keys to delete
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 2)

	// Delete id=1 and id=3
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 3})
	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 0}) // value doesn't matter
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 0})
	source.SetCard(2)

	// Configure to delete matched rows
	config := NewMergeInsertBuilder(0).
		WhenMatched(MatchedDelete).
		WhenNotMatched(NotMatchedSkip). // don't insert non-matching
		Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if result.RowsDeleted != 2 {
		t.Errorf("expected 2 rows deleted, got %d", result.RowsDeleted)
	}
	if result.RowsInserted != 0 {
		t.Errorf("expected 0 rows inserted, got %d", result.RowsInserted)
	}
}

// TestMergeInsert_NotMatchedSkip tests using WhenNotMatched(NotMatchedSkip),
// verifying unmatched rows are not inserted
func TestMergeInsert_NotMatchedSkip(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset with 3 rows (id=0,1,2)
	_ = setupMergeInsertDataset(t, basePath, handler, store, 3)

	// Create source data with mix of matching and non-matching keys
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 3)

	// id=1 (matches), id=5 (no match), id=6 (no match)
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 5})
	source.Data[0].SetValue(2, &chunk.Value{Typ: types[0], I64: 6})
	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 111})
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 555})
	source.Data[1].SetValue(2, &chunk.Value{Typ: types[1], I64: 666})
	source.SetCard(3)

	// Configure to skip non-matched rows, do nothing on match
	config := NewMergeInsertBuilder(0).
		WhenMatched(MatchedDoNothing).
		WhenNotMatched(NotMatchedSkip).
		Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// No rows should be inserted (skipped), 1 row unchanged
	if result.RowsInserted != 0 {
		t.Errorf("expected 0 rows inserted, got %d", result.RowsInserted)
	}
	if result.RowsUnchanged != 1 {
		t.Errorf("expected 1 row unchanged, got %d", result.RowsUnchanged)
	}

	// Verify total rows unchanged
	version, _ := handler.ResolveLatestVersion(ctx, basePath)
	chunks, _ := ScanChunks(ctx, basePath, handler, version)
	totalRows := 0
	for _, chk := range chunks {
		totalRows += chk.Card()
	}
	if totalRows != 3 {
		t.Errorf("expected 3 total rows (no new inserts), got %d", totalRows)
	}
}

// TestMergeInsert_NoOverlap tests when source has no matching keys,
// all rows should be inserted
func TestMergeInsert_NoOverlap(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset with 3 rows (id=0,1,2)
	_ = setupMergeInsertDataset(t, basePath, handler, store, 3)

	// Create source data with completely different keys
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 3)

	// id=10, 11, 12 (no overlap with existing 0,1,2)
	for i := 0; i < 3; i++ {
		source.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(10 + i)})
		source.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64((10 + i) * 100)})
	}
	source.SetCard(3)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// All 3 rows should be inserted, 0 updated
	if result.RowsInserted != 3 {
		t.Errorf("expected 3 rows inserted, got %d", result.RowsInserted)
	}
	if result.RowsUpdated != 0 {
		t.Errorf("expected 0 rows updated, got %d", result.RowsUpdated)
	}
}

// TestMergeInsert_AllOverlap tests when source keys all match existing,
// verify all updated (0 inserts)
func TestMergeInsert_AllOverlap(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset with 5 rows (id=0..4)
	_ = setupMergeInsertDataset(t, basePath, handler, store, 5)

	// Create source data with same keys (all overlap)
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 5)

	// id=0..4 (all match existing)
	for i := 0; i < 5; i++ {
		source.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		source.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64(i * 1000)}) // updated values
	}
	source.SetCard(5)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// All 5 rows should be updated, 0 inserted
	if result.RowsUpdated != 5 {
		t.Errorf("expected 5 rows updated, got %d", result.RowsUpdated)
	}
	if result.RowsInserted != 0 {
		t.Errorf("expected 0 rows inserted, got %d", result.RowsInserted)
	}
}

// TestMergeInsert_LargeDataset tests merging 1000 rows into existing 1000 rows
// with 50% overlap
func TestMergeInsert_LargeDataset(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	const totalRows = 1000
	const overlapStart = 500 // 50% overlap: source has id=500..1499, target has id=0..999

	// Create table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert initial data (id=0..999)
	types := []common.LType{common.IntegerType(), common.BigintType()}
	initial := &chunk.Chunk{}
	initial.Init(types, util.DefaultVectorSize)

	for i := 0; i < totalRows; i++ {
		initial.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		initial.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64(i * 100)})
	}
	initial.SetCard(totalRows)

	initConfig := NewMergeInsertBuilder(0).Build()
	initExecutor := NewMergeInsertExecutor(basePath, store, handler, initConfig)
	_, err = initExecutor.Execute(ctx, []*chunk.Chunk{initial})
	if err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	// Create source data (id=500..1499) - 500 overlap, 500 new
	source := &chunk.Chunk{}
	source.Init(types, util.DefaultVectorSize)

	for i := 0; i < totalRows; i++ {
		source.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(overlapStart + i)})
		source.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64((overlapStart + i) * 100)})
	}
	source.SetCard(totalRows)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// 500 rows should be updated (id=500..999), 500 inserted (id=1000..1499)
	if result.RowsUpdated != 500 {
		t.Errorf("expected 500 rows updated, got %d", result.RowsUpdated)
	}
	if result.RowsInserted != 500 {
		t.Errorf("expected 500 rows inserted, got %d", result.RowsInserted)
	}
}

// TestMergeInsert_MultipleKeyColumns tests using 2 key columns for matching
func TestMergeInsert_MultipleKeyColumns(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert initial data with 3 columns (key1, key2, value)
	types := []common.LType{common.IntegerType(), common.IntegerType(), common.BigintType()}
	initial := &chunk.Chunk{}
	initial.Init(types, 3)

	// (1, 10, 100), (1, 20, 200), (2, 10, 300)
	initial.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	initial.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 1})
	initial.Data[0].SetValue(2, &chunk.Value{Typ: types[0], I64: 2})

	initial.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 10})
	initial.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 20})
	initial.Data[1].SetValue(2, &chunk.Value{Typ: types[1], I64: 10})

	initial.Data[2].SetValue(0, &chunk.Value{Typ: types[2], I64: 100})
	initial.Data[2].SetValue(1, &chunk.Value{Typ: types[2], I64: 200})
	initial.Data[2].SetValue(2, &chunk.Value{Typ: types[2], I64: 300})
	initial.SetCard(3)

	// Use columns 0 and 1 as key columns
	initConfig := NewMergeInsertBuilder(0, 1).Build()
	initExecutor := NewMergeInsertExecutor(basePath, store, handler, initConfig)
	_, err = initExecutor.Execute(ctx, []*chunk.Chunk{initial})
	if err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	// Create source data
	source := &chunk.Chunk{}
	source.Init(types, 3)

	// (1, 10, 999) - matches (1, 10) -> update
	// (1, 20, 888) - matches (1, 20) -> update
	// (3, 30, 777) - no match -> insert
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 1})
	source.Data[0].SetValue(2, &chunk.Value{Typ: types[0], I64: 3})

	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 10})
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 20})
	source.Data[1].SetValue(2, &chunk.Value{Typ: types[1], I64: 30})

	source.Data[2].SetValue(0, &chunk.Value{Typ: types[2], I64: 999})
	source.Data[2].SetValue(1, &chunk.Value{Typ: types[2], I64: 888})
	source.Data[2].SetValue(2, &chunk.Value{Typ: types[2], I64: 777})
	source.SetCard(3)

	// Use same key columns (0, 1)
	config := NewMergeInsertBuilder(0, 1).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if result.RowsUpdated != 2 {
		t.Errorf("expected 2 rows updated, got %d", result.RowsUpdated)
	}
	if result.RowsInserted != 1 {
		t.Errorf("expected 1 row inserted, got %d", result.RowsInserted)
	}
}

// TestMergeInsert_EmptySource tests with empty source data, verify no changes
func TestMergeInsert_EmptySource(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset
	_ = setupMergeInsertDataset(t, basePath, handler, store, 5)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	// Execute with nil source
	result, err := executor.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if result.RowsInserted != 0 || result.RowsUpdated != 0 || result.RowsDeleted != 0 {
		t.Errorf("expected no changes with empty source, got inserted=%d updated=%d deleted=%d",
			result.RowsInserted, result.RowsUpdated, result.RowsDeleted)
	}

	// Execute with empty slice
	result, err = executor.Execute(ctx, []*chunk.Chunk{})
	if err != nil {
		t.Fatalf("execute empty slice: %v", err)
	}

	if result.RowsInserted != 0 || result.RowsUpdated != 0 || result.RowsDeleted != 0 {
		t.Errorf("expected no changes with empty slice, got inserted=%d updated=%d deleted=%d",
			result.RowsInserted, result.RowsUpdated, result.RowsDeleted)
	}
}

// TestMergeInsert_EmptyTarget tests merging into empty table, all should be inserted
func TestMergeInsert_EmptyTarget(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create empty table (no initial data)
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Create source data
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 5)

	for i := 0; i < 5; i++ {
		source.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		source.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64(i * 100)})
	}
	source.SetCard(5)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// All 5 rows should be inserted
	if result.RowsInserted != 5 {
		t.Errorf("expected 5 rows inserted, got %d", result.RowsInserted)
	}
	if result.RowsUpdated != 0 {
		t.Errorf("expected 0 rows updated, got %d", result.RowsUpdated)
	}
}

// TestMergeInsert_ConcurrentMerge tests two concurrent merge operations
// on separate tables, verifying both complete successfully
func TestMergeInsert_ConcurrentMerge(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create two separate tables for concurrent operations
	table1Path := basePath + "/table1"
	table2Path := basePath + "/table2"

	store1 := NewLocalObjectStoreExt(table1Path, nil)
	store2 := NewLocalObjectStoreExt(table2Path, nil)

	// Setup both tables with initial data
	_ = setupMergeInsertDataset(t, table1Path, handler, store1, 100)
	_ = setupMergeInsertDataset(t, table2Path, handler, store2, 100)

	// Create two source datasets with non-overlapping keys
	types := []common.LType{common.IntegerType(), common.BigintType()}

	// Source 1: id=200..249 for table1
	source1 := &chunk.Chunk{}
	source1.Init(types, 50)
	for i := 0; i < 50; i++ {
		source1.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(200 + i)})
		source1.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64((200 + i) * 100)})
	}
	source1.SetCard(50)

	// Source 2: id=200..249 for table2
	source2 := &chunk.Chunk{}
	source2.Init(types, 50)
	for i := 0; i < 50; i++ {
		source2.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(200 + i)})
		source2.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64((200 + i) * 100)})
	}
	source2.SetCard(50)

	// Run two merges concurrently on separate tables
	var wg sync.WaitGroup
	var err1, err2 error
	var result1, result2 *MergeInsertResult

	wg.Add(2)
	go func() {
		defer wg.Done()
		config := NewMergeInsertBuilder(0).Build()
		executor := NewMergeInsertExecutor(table1Path, store1, handler, config)
		result1, err1 = executor.Execute(ctx, []*chunk.Chunk{source1})
	}()
	go func() {
		defer wg.Done()
		config := NewMergeInsertBuilder(0).Build()
		executor := NewMergeInsertExecutor(table2Path, store2, handler, config)
		result2, err2 = executor.Execute(ctx, []*chunk.Chunk{source2})
	}()
	wg.Wait()

	if err1 != nil {
		t.Errorf("merge 1 failed: %v", err1)
	}
	if err2 != nil {
		t.Errorf("merge 2 failed: %v", err2)
	}

	// Both should succeed with 50 inserts each
	if result1 != nil && result1.RowsInserted != 50 {
		t.Errorf("merge 1: expected 50 inserts, got %d", result1.RowsInserted)
	}
	if result2 != nil && result2.RowsInserted != 50 {
		t.Errorf("merge 2: expected 50 inserts, got %d", result2.RowsInserted)
	}
}

// TestMergeInsert_ResultCounts verifies RowsInserted, RowsUpdated, RowsDeleted,
// RowsUnchanged are accurate
func TestMergeInsert_ResultCounts(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create table with 10 rows (id=0..9)
	_ = setupMergeInsertDataset(t, basePath, handler, store, 10)

	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 15)

	// Mix of operations:
	// id=0..9: match existing -> update (10 rows)
	// id=10..14: no match -> insert (5 rows)
	for i := 0; i < 15; i++ {
		source.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		source.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64(i * 1000)})
	}
	source.SetCard(15)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Verify counts: 10 updates (id=0..9 match), 5 inserts (id=10..14 new)
	if result.RowsUpdated != 10 {
		t.Errorf("expected 10 rows updated, got %d", result.RowsUpdated)
	}
	if result.RowsInserted != 5 {
		t.Errorf("expected 5 rows inserted, got %d", result.RowsInserted)
	}
	if result.RowsDeleted != 0 {
		t.Errorf("expected 0 rows deleted, got %d", result.RowsDeleted)
	}

	// Now test with delete operation
	source2 := &chunk.Chunk{}
	source2.Init(types, 3)

	// Delete id=0, 1, 2
	for i := 0; i < 3; i++ {
		source2.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		source2.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: 0})
	}
	source2.SetCard(3)

	deleteConfig := NewMergeInsertBuilder(0).
		WhenMatched(MatchedDelete).
		WhenNotMatched(NotMatchedSkip).
		Build()
	deleteExecutor := NewMergeInsertExecutor(basePath, store, handler, deleteConfig)

	deleteResult, err := deleteExecutor.Execute(ctx, []*chunk.Chunk{source2})
	if err != nil {
		t.Fatalf("delete execute: %v", err)
	}

	if deleteResult.RowsDeleted != 3 {
		t.Errorf("expected 3 rows deleted, got %d", deleteResult.RowsDeleted)
	}
	if deleteResult.RowsInserted != 0 {
		t.Errorf("expected 0 rows inserted, got %d", deleteResult.RowsInserted)
	}
}

// TestMergeInsert_BuilderChaining tests builder fluent API with all options
func TestMergeInsert_BuilderChaining(t *testing.T) {
	// Test basic chaining
	config := NewMergeInsertBuilder(0).
		WhenMatched(MatchedUpdateAll).
		WhenNotMatched(NotMatchedInsert).
		Build()

	if len(config.KeyColumns) != 1 || config.KeyColumns[0] != 0 {
		t.Errorf("expected key column 0, got %v", config.KeyColumns)
	}
	if config.WhenMatched != MatchedUpdateAll {
		t.Errorf("expected MatchedUpdateAll, got %v", config.WhenMatched)
	}
	if config.WhenNotMatched != NotMatchedInsert {
		t.Errorf("expected NotMatchedInsert, got %v", config.WhenNotMatched)
	}

	// Test with multiple key columns
	config2 := NewMergeInsertBuilder(0, 1, 2).
		WhenMatched(MatchedDelete).
		WhenNotMatched(NotMatchedSkip).
		Build()

	if len(config2.KeyColumns) != 3 {
		t.Errorf("expected 3 key columns, got %d", len(config2.KeyColumns))
	}
	if config2.WhenMatched != MatchedDelete {
		t.Errorf("expected MatchedDelete, got %v", config2.WhenMatched)
	}
	if config2.WhenNotMatched != NotMatchedSkip {
		t.Errorf("expected NotMatchedSkip, got %v", config2.WhenNotMatched)
	}

	// Test WhenMatchedUpdate
	config3 := NewMergeInsertBuilder(0).
		WhenMatchedUpdate(1, 2, 3).
		WhenNotMatched(NotMatchedInsert).
		Build()

	if config3.WhenMatched != MatchedUpdateColumns {
		t.Errorf("expected MatchedUpdateColumns, got %v", config3.WhenMatched)
	}
	if len(config3.UpdateColumns) != 3 {
		t.Errorf("expected 3 update columns, got %d", len(config3.UpdateColumns))
	}

	// Test all WhenMatched actions
	actions := []WhenMatchedAction{
		MatchedUpdateAll,
		MatchedUpdateColumns,
		MatchedDelete,
		MatchedDoNothing,
	}
	for i, action := range actions {
		cfg := NewMergeInsertBuilder(0).WhenMatched(action).Build()
		if cfg.WhenMatched != action {
			t.Errorf("test %d: expected %v, got %v", i, action, cfg.WhenMatched)
		}
	}

	// Test all WhenNotMatched actions
	notMatchedActions := []WhenNotMatchedAction{
		NotMatchedInsert,
		NotMatchedSkip,
	}
	for i, action := range notMatchedActions {
		cfg := NewMergeInsertBuilder(0).WhenNotMatched(action).Build()
		if cfg.WhenNotMatched != action {
			t.Errorf("test %d: expected %v, got %v", i, action, cfg.WhenNotMatched)
		}
	}
}

// TestMergeInsert_ConvenienceFunction tests MergeInsert() convenience function
func TestMergeInsert_ConvenienceFunction(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Create source data
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 3)

	for i := 0; i < 3; i++ {
		source.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		source.Data[1].SetValue(i, &chunk.Value{Typ: types[1], I64: int64(i * 100)})
	}
	source.SetCard(3)

	// Use convenience function
	result, err := MergeInsert(ctx, basePath, store, handler, []*chunk.Chunk{source}, []int{0})
	if err != nil {
		t.Fatalf("MergeInsert: %v", err)
	}

	if result.RowsInserted != 3 {
		t.Errorf("expected 3 rows inserted, got %d", result.RowsInserted)
	}

	// Insert again with overlapping keys
	source2 := &chunk.Chunk{}
	source2.Init(types, 2)
	source2.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1}) // update
	source2.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 3}) // insert
	source2.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 111})
	source2.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 333})
	source2.SetCard(2)

	result2, err := MergeInsert(ctx, basePath, store, handler, []*chunk.Chunk{source2}, []int{0})
	if err != nil {
		t.Fatalf("MergeInsert 2: %v", err)
	}

	if result2.RowsUpdated != 1 {
		t.Errorf("expected 1 row updated, got %d", result2.RowsUpdated)
	}
	if result2.RowsInserted != 1 {
		t.Errorf("expected 1 row inserted, got %d", result2.RowsInserted)
	}
}

// TestMergeInsert_DoNothing tests WhenMatched(MatchedDoNothing) +
// WhenNotMatched(NotMatchedInsert)
func TestMergeInsert_DoNothing(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Setup initial dataset with 5 rows
	_ = setupMergeInsertDataset(t, basePath, handler, store, 5)

	// Create source data
	types := []common.LType{common.IntegerType(), common.BigintType()}
	source := &chunk.Chunk{}
	source.Init(types, 4)

	// id=0,1 (exist - do nothing), id=10,11 (new - insert)
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 0})
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 1})
	source.Data[0].SetValue(2, &chunk.Value{Typ: types[0], I64: 10})
	source.Data[0].SetValue(3, &chunk.Value{Typ: types[0], I64: 11})

	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], I64: 999}) // ignored
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], I64: 888}) // ignored
	source.Data[1].SetValue(2, &chunk.Value{Typ: types[1], I64: 1000})
	source.Data[1].SetValue(3, &chunk.Value{Typ: types[1], I64: 1100})
	source.SetCard(4)

	// Configure: matched rows unchanged, non-matched inserted
	config := NewMergeInsertBuilder(0).
		WhenMatched(MatchedDoNothing).
		WhenNotMatched(NotMatchedInsert).
		Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// 2 rows unchanged (id=0,1), 2 rows inserted (id=10,11)
	if result.RowsUnchanged != 2 {
		t.Errorf("expected 2 rows unchanged, got %d", result.RowsUnchanged)
	}
	if result.RowsInserted != 2 {
		t.Errorf("expected 2 rows inserted, got %d", result.RowsInserted)
	}
	if result.RowsUpdated != 0 {
		t.Errorf("expected 0 rows updated, got %d", result.RowsUpdated)
	}

	// Verify data - original values for id=0,1 should be preserved
	version, _ := handler.ResolveLatestVersion(ctx, basePath)
	manifest, err := LoadManifest(ctx, basePath, handler, version)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}

	// Total rows should be 7 (original 5 + new 2)
	totalRows := 0
	for _, frag := range manifest.Fragments {
		totalRows += int(frag.PhysicalRows)
	}
	if totalRows != 7 {
		t.Errorf("expected 7 total rows, got %d", totalRows)
	}
}
