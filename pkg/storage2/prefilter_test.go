// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"testing"
	"time"
)

func TestNoFilter(t *testing.T) {
	f := &NoFilter{}

	if err := f.WaitForReady(context.Background()); err != nil {
		t.Errorf("WaitForReady: %v", err)
	}
	if !f.IsEmpty() {
		t.Error("NoFilter should be empty")
	}
	if !f.ShouldInclude(0) {
		t.Error("NoFilter should include all rows")
	}
	if !f.ShouldInclude(1000) {
		t.Error("NoFilter should include all rows")
	}

	rowIDs := []uint64{1, 2, 3, 4, 5}
	filtered := f.FilterRowIDs(rowIDs)
	if len(filtered) != len(rowIDs) {
		t.Errorf("expected %d rows, got %d", len(rowIDs), len(filtered))
	}
}

func TestBitmapPreFilter(t *testing.T) {
	f := NewBitmapPreFilter(100)

	// Initially empty (no valid rows)
	if f.ShouldInclude(0) {
		t.Error("should not include row 0 before adding")
	}

	// Add some valid rows
	f.AddValidRow(1)
	f.AddValidRow(3)
	f.AddValidRow(5)

	if !f.ShouldInclude(1) {
		t.Error("should include row 1")
	}
	if f.ShouldInclude(2) {
		t.Error("should not include row 2")
	}
	if !f.ShouldInclude(3) {
		t.Error("should include row 3")
	}

	// Test cardinality
	if f.Cardinality() != 3 {
		t.Errorf("expected cardinality 3, got %d", f.Cardinality())
	}

	// Test FilterRowIDs
	rowIDs := []uint64{0, 1, 2, 3, 4, 5, 6}
	filtered := f.FilterRowIDs(rowIDs)
	if len(filtered) != 3 {
		t.Errorf("expected 3 filtered rows, got %d", len(filtered))
	}

	// Test RemoveRow
	f.RemoveRow(3)
	if f.ShouldInclude(3) {
		t.Error("should not include row 3 after removal")
	}
	if f.Cardinality() != 2 {
		t.Errorf("expected cardinality 2, got %d", f.Cardinality())
	}
}

func TestBitmapPreFilterFromValid(t *testing.T) {
	validIDs := []uint64{10, 20, 30, 40, 50}
	f := NewBitmapPreFilterFromValid(validIDs, 100)

	if f.Cardinality() != 5 {
		t.Errorf("expected cardinality 5, got %d", f.Cardinality())
	}

	for _, id := range validIDs {
		if !f.ShouldInclude(id) {
			t.Errorf("should include row %d", id)
		}
	}

	if f.ShouldInclude(15) {
		t.Error("should not include row 15")
	}
}

func TestDeletionPreFilter(t *testing.T) {
	bitmap := NewDeletionBitmap()
	bitmap.MarkDeleted(2)
	bitmap.MarkDeleted(4)
	bitmap.MarkDeleted(6)

	f := NewDeletionPreFilter(bitmap, 10)

	// Should include non-deleted rows
	if !f.ShouldInclude(0) {
		t.Error("should include row 0")
	}
	if !f.ShouldInclude(1) {
		t.Error("should include row 1")
	}

	// Should exclude deleted rows
	if f.ShouldInclude(2) {
		t.Error("should not include row 2")
	}
	if f.ShouldInclude(4) {
		t.Error("should not include row 4")
	}

	// Cardinality should be total - deleted
	if f.Cardinality() != 7 {
		t.Errorf("expected cardinality 7, got %d", f.Cardinality())
	}

	// FilterRowIDs
	rowIDs := []uint64{0, 1, 2, 3, 4, 5, 6, 7}
	filtered := f.FilterRowIDs(rowIDs)
	if len(filtered) != 5 {
		t.Errorf("expected 5 filtered rows, got %d", len(filtered))
	}
}

func TestDeletionPreFilterNilBitmap(t *testing.T) {
	f := NewDeletionPreFilter(nil, 100)

	if !f.IsEmpty() {
		t.Error("nil bitmap should result in empty filter")
	}
	if !f.ShouldInclude(50) {
		t.Error("should include all rows with nil bitmap")
	}
	if f.Cardinality() != 100 {
		t.Errorf("expected cardinality 100, got %d", f.Cardinality())
	}
}

func TestCombinedPreFilter(t *testing.T) {
	// Create two filters
	f1 := NewBitmapPreFilterFromValid([]uint64{1, 2, 3, 4, 5}, 10)
	f2 := NewBitmapPreFilterFromValid([]uint64{2, 3, 4, 6, 7}, 10)

	combined := NewCombinedPreFilter(f1, f2)

	// Only rows 2, 3, 4 are in both filters
	if !combined.ShouldInclude(2) {
		t.Error("should include row 2")
	}
	if !combined.ShouldInclude(3) {
		t.Error("should include row 3")
	}
	if !combined.ShouldInclude(4) {
		t.Error("should include row 4")
	}

	if combined.ShouldInclude(1) {
		t.Error("should not include row 1 (not in f2)")
	}
	if combined.ShouldInclude(6) {
		t.Error("should not include row 6 (not in f1)")
	}

	// FilterRowIDs
	rowIDs := []uint64{1, 2, 3, 4, 5, 6, 7}
	filtered := combined.FilterRowIDs(rowIDs)
	if len(filtered) != 3 {
		t.Errorf("expected 3 filtered rows, got %d", len(filtered))
	}
}

func TestCombinedPreFilterEmpty(t *testing.T) {
	combined := NewCombinedPreFilter()

	if !combined.IsEmpty() {
		t.Error("empty combined filter should be empty")
	}
	if !combined.ShouldInclude(100) {
		t.Error("empty combined filter should include all")
	}
}

func TestAsyncPreFilter(t *testing.T) {
	// Create an async filter that takes a bit to load
	f := NewAsyncPreFilter(func() (PreFilter, error) {
		time.Sleep(10 * time.Millisecond)
		return NewBitmapPreFilterFromValid([]uint64{1, 2, 3}, 10), nil
	})

	// Wait for ready
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := f.WaitForReady(ctx); err != nil {
		t.Fatalf("WaitForReady: %v", err)
	}

	// Now should work correctly
	if !f.ShouldInclude(1) {
		t.Error("should include row 1")
	}
	if f.ShouldInclude(5) {
		t.Error("should not include row 5")
	}
	if f.Cardinality() != 3 {
		t.Errorf("expected cardinality 3, got %d", f.Cardinality())
	}
}

func TestAsyncPreFilterTimeout(t *testing.T) {
	// Create an async filter that takes too long
	f := NewAsyncPreFilter(func() (PreFilter, error) {
		time.Sleep(1 * time.Second)
		return NewBitmapPreFilterFromValid([]uint64{1, 2, 3}, 10), nil
	})

	// Wait with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := f.WaitForReady(ctx)
	if err == nil {
		t.Error("expected timeout error")
	}
}

func TestPreFilterBuilderDeletion(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create an empty table
	_, err := CreateTable(ctx, basePath, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	builder := NewPreFilterBuilder(basePath, store, handler)
	filter, err := builder.BuildDeletionFilter(ctx)
	if err != nil {
		t.Fatalf("BuildDeletionFilter: %v", err)
	}

	// Empty table, filter should include all
	if !filter.IsEmpty() {
		t.Error("filter for empty table should be empty")
	}
}
