// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

func TestMergeInsertBuilder(t *testing.T) {
	// Test default builder
	builder := NewMergeInsertBuilder(0)
	config := builder.Build()

	if len(config.KeyColumns) != 1 || config.KeyColumns[0] != 0 {
		t.Errorf("expected KeyColumns=[0], got %v", config.KeyColumns)
	}
	if config.WhenMatched != MatchedUpdateAll {
		t.Errorf("expected WhenMatched=MatchedUpdateAll, got %v", config.WhenMatched)
	}
	if config.WhenNotMatched != NotMatchedInsert {
		t.Errorf("expected WhenNotMatched=NotMatchedInsert, got %v", config.WhenNotMatched)
	}

	// Test with options
	config2 := NewMergeInsertBuilder(0, 1).
		WhenMatched(MatchedDelete).
		WhenNotMatched(NotMatchedSkip).
		Build()

	if len(config2.KeyColumns) != 2 {
		t.Errorf("expected 2 key columns, got %d", len(config2.KeyColumns))
	}
	if config2.WhenMatched != MatchedDelete {
		t.Errorf("expected WhenMatched=MatchedDelete, got %v", config2.WhenMatched)
	}
	if config2.WhenNotMatched != NotMatchedSkip {
		t.Errorf("expected WhenNotMatched=NotMatchedSkip, got %v", config2.WhenNotMatched)
	}

	// Test WhenMatchedUpdate
	config3 := NewMergeInsertBuilder(0).
		WhenMatchedUpdate(1, 2, 3).
		Build()

	if config3.WhenMatched != MatchedUpdateColumns {
		t.Errorf("expected WhenMatched=MatchedUpdateColumns, got %v", config3.WhenMatched)
	}
	if len(config3.UpdateColumns) != 3 {
		t.Errorf("expected 3 update columns, got %d", len(config3.UpdateColumns))
	}
}

func TestComputeKeyHash(t *testing.T) {
	// Create a test chunk with some data
	types := []common.LType{common.IntegerType(), common.VarcharType()}
	chk := &chunk.Chunk{}
	chk.Init(types, 3)

	// Set values using proper Value types
	chk.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	chk.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 2})
	chk.Data[0].SetValue(2, &chunk.Value{Typ: types[0], I64: 1}) // Duplicate key

	chk.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "a"})
	chk.Data[1].SetValue(1, &chunk.Value{Typ: types[1], Str: "b"})
	chk.Data[1].SetValue(2, &chunk.Value{Typ: types[1], Str: "c"})

	chk.SetCard(3)

	executor := &MergeInsertExecutor{
		config: MergeInsertConfig{KeyColumns: []int{0}},
	}

	// Hash for same key values should be equal
	hash0 := executor.computeKeyHash(chk, 0)
	hash2 := executor.computeKeyHash(chk, 2)
	if hash0 != hash2 {
		t.Error("rows 0 and 2 have same key value (1), should have same hash")
	}

	// Hash for different key values should be different
	hash1 := executor.computeKeyHash(chk, 1)
	if hash0 == hash1 {
		t.Error("rows 0 and 1 have different key values, should have different hashes")
	}
}

func TestExtractKeys(t *testing.T) {
	types := []common.LType{common.IntegerType(), common.VarcharType()}
	chk := &chunk.Chunk{}
	chk.Init(types, 2)

	chk.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	chk.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 2})
	chk.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "a"})
	chk.Data[1].SetValue(1, &chunk.Value{Typ: types[1], Str: "b"})
	chk.SetCard(2)

	executor := &MergeInsertExecutor{
		config: MergeInsertConfig{KeyColumns: []int{0}},
	}

	keys := executor.extractKeys([]*chunk.Chunk{chk})

	if len(keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(keys))
	}

	// Each key should have the correct row info
	for _, info := range keys {
		if info.chunk != chk {
			t.Error("key info should reference original chunk")
		}
	}
}

func TestMergeInsertEmptySource(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create an empty table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	// Execute with empty source
	result, err := executor.Execute(ctx, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if result.RowsInserted != 0 || result.RowsUpdated != 0 {
		t.Errorf("expected no changes, got inserted=%d updated=%d",
			result.RowsInserted, result.RowsUpdated)
	}
}

func TestMergeInsertIntoEmptyTable(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create an empty table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Create source data
	types := []common.LType{common.IntegerType(), common.VarcharType()}
	chk := &chunk.Chunk{}
	chk.Init(types, 2)

	chk.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	chk.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 2})
	chk.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "value1"})
	chk.Data[1].SetValue(1, &chunk.Value{Typ: types[1], Str: "value2"})
	chk.SetCard(2)

	config := NewMergeInsertBuilder(0).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	// Execute
	result, err := executor.Execute(ctx, []*chunk.Chunk{chk})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// All rows should be inserted (no matches in empty table)
	if result.RowsInserted != 2 {
		t.Errorf("expected 2 rows inserted, got %d", result.RowsInserted)
	}
	if result.RowsUpdated != 0 {
		t.Errorf("expected 0 rows updated, got %d", result.RowsUpdated)
	}
}

func TestMergeInsertWhenMatchedDoNothing(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create an empty table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert initial data
	types := []common.LType{common.IntegerType(), common.VarcharType()}
	initial := &chunk.Chunk{}
	initial.Init(types, 1)
	initial.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	initial.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "initial"})
	initial.SetCard(1)

	initConfig := NewMergeInsertBuilder(0).Build()
	initExecutor := NewMergeInsertExecutor(basePath, store, handler, initConfig)
	_, err = initExecutor.Execute(ctx, []*chunk.Chunk{initial})
	if err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	// Now merge with WhenMatched=DoNothing
	source := &chunk.Chunk{}
	source.Init(types, 2)
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1}) // Matches existing
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 2}) // New
	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "updated"})
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], Str: "new"})
	source.SetCard(2)

	config := NewMergeInsertBuilder(0).
		WhenMatched(MatchedDoNothing).
		Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Row 1 should be unchanged (matched), row 2 should be inserted
	if result.RowsUnchanged != 1 {
		t.Errorf("expected 1 row unchanged, got %d", result.RowsUnchanged)
	}
	if result.RowsInserted != 1 {
		t.Errorf("expected 1 row inserted, got %d", result.RowsInserted)
	}
}

func TestMergeInsertWhenNotMatchedSkip(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create an empty table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Insert initial data
	types := []common.LType{common.IntegerType(), common.VarcharType()}
	initial := &chunk.Chunk{}
	initial.Init(types, 1)
	initial.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
	initial.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "initial"})
	initial.SetCard(1)

	initConfig := NewMergeInsertBuilder(0).Build()
	initExecutor := NewMergeInsertExecutor(basePath, store, handler, initConfig)
	_, err = initExecutor.Execute(ctx, []*chunk.Chunk{initial})
	if err != nil {
		t.Fatalf("insert initial: %v", err)
	}

	// Now merge with WhenNotMatched=Skip
	source := &chunk.Chunk{}
	source.Init(types, 2)
	source.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1}) // Matches existing
	source.Data[0].SetValue(1, &chunk.Value{Typ: types[0], I64: 2}) // New - should be skipped
	source.Data[1].SetValue(0, &chunk.Value{Typ: types[1], Str: "updated"})
	source.Data[1].SetValue(1, &chunk.Value{Typ: types[1], Str: "skipped"})
	source.SetCard(2)

	config := NewMergeInsertBuilder(0).
		WhenMatched(MatchedDoNothing).
		WhenNotMatched(NotMatchedSkip).
		Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)

	result, err := executor.Execute(ctx, []*chunk.Chunk{source})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	// Row 1 unchanged (matched), row 2 skipped (not matched)
	if result.RowsUnchanged != 1 {
		t.Errorf("expected 1 row unchanged, got %d", result.RowsUnchanged)
	}
	if result.RowsInserted != 0 {
		t.Errorf("expected 0 rows inserted, got %d", result.RowsInserted)
	}
}
