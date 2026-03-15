// Copyright 2026 The Plan Authors
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"strings"
	"testing"
	"time"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestDefaultDataValidationOptions(t *testing.T) {
	opts := DefaultDataValidationOptions()

	if !opts.ValidateRowCount {
		t.Error("expected ValidateRowCount to be true")
	}

	if !opts.ValidateChecksum {
		t.Error("expected ValidateChecksum to be true")
	}

	if !opts.ValidateSchema {
		t.Error("expected ValidateSchema to be true")
	}

	expectedTimeout := 5 * time.Minute
	if opts.MaxValidationTime != expectedTimeout {
		t.Errorf("expected MaxValidationTime %v, got %v", expectedTimeout, opts.MaxValidationTime)
	}
}

func TestDefaultDataReplacementOptions(t *testing.T) {
	opts := DefaultDataReplacementOptions()

	if !opts.Atomic {
		t.Error("expected Atomic to be true")
	}

	if !opts.Validate {
		t.Error("expected Validate to be true")
	}

	if opts.ValidationOptions == nil {
		t.Error("expected ValidationOptions to be non-nil")
	}
}

func TestDataReplacementBatch(t *testing.T) {
	t.Run("New batch", func(t *testing.T) {
		batch := NewDataReplacementBatch(5)
		if batch.maxBatchSize != 5 {
			t.Errorf("expected max batch size 5, got %d", batch.maxBatchSize)
		}
		if !batch.IsEmpty() {
			t.Error("expected new batch to be empty")
		}
	})

	t.Run("Default batch size", func(t *testing.T) {
		batch := NewDataReplacementBatch(0)
		if batch.maxBatchSize != 10 {
			t.Errorf("expected default batch size 10, got %d", batch.maxBatchSize)
		}
	})

	t.Run("Add and size", func(t *testing.T) {
		batch := NewDataReplacementBatch(3)

		repl := FragmentReplacement{FragmentID: 0}
		if !batch.Add(repl) {
			t.Error("expected Add to succeed")
		}

		if batch.Size() != 1 {
			t.Errorf("expected size 1, got %d", batch.Size())
		}
	})

	t.Run("Batch full", func(t *testing.T) {
		batch := NewDataReplacementBatch(2)

		batch.Add(FragmentReplacement{FragmentID: 0})
		batch.Add(FragmentReplacement{FragmentID: 1})

		if !batch.IsFull() {
			t.Error("expected batch to be full")
		}

		if batch.Add(FragmentReplacement{FragmentID: 2}) {
			t.Error("expected Add to fail when batch is full")
		}
	})

	t.Run("Clear", func(t *testing.T) {
		batch := NewDataReplacementBatch(5)
		batch.Add(FragmentReplacement{FragmentID: 0})
		batch.Add(FragmentReplacement{FragmentID: 1})

		batch.Clear()

		if !batch.IsEmpty() {
			t.Error("expected batch to be empty after clear")
		}
	})

	t.Run("ToOperation", func(t *testing.T) {
		batch := NewDataReplacementBatch(5)

		// Empty batch should return nil
		op := batch.ToOperation()
		if op != nil {
			t.Error("expected nil operation for empty batch")
		}

		// Non-empty batch should return operation
		batch.Add(FragmentReplacement{FragmentID: 0})
		op = batch.ToOperation()
		if op == nil {
			t.Error("expected non-nil operation for non-empty batch")
		}
		if len(op.Replacements) != 1 {
			t.Errorf("expected 1 replacement, got %d", len(op.Replacements))
		}
	})
}

func TestDataReplacementProgressTracker(t *testing.T) {
	tracker := NewDataReplacementProgressTracker(10, 1000)

	t.Run("Initial progress", func(t *testing.T) {
		progress := tracker.GetProgress()
		if progress.TotalFragments != 10 {
			t.Errorf("expected total fragments 10, got %d", progress.TotalFragments)
		}
		if progress.TotalBytes != 1000 {
			t.Errorf("expected total bytes 1000, got %d", progress.TotalBytes)
		}
		if progress.PercentComplete != 0 {
			t.Errorf("expected 0%% complete, got %d%%", progress.PercentComplete)
		}
	})

	t.Run("Update progress", func(t *testing.T) {
		progress := tracker.Update(5, 500, 5)

		if progress.CompletedFragments != 5 {
			t.Errorf("expected 5 completed fragments, got %d", progress.CompletedFragments)
		}
		if progress.BytesProcessed != 500 {
			t.Errorf("expected 500 bytes processed, got %d", progress.BytesProcessed)
		}
		if progress.PercentComplete != 50 {
			t.Errorf("expected 50%% complete, got %d%%", progress.PercentComplete)
		}
		if progress.CurrentFragment != 5 {
			t.Errorf("expected current fragment 5, got %d", progress.CurrentFragment)
		}
	})
}

func TestFieldsMatch(t *testing.T) {
	tests := []struct {
		name     string
		a        []int32
		b        []int32
		expected bool
	}{
		{"equal", []int32{1, 2, 3}, []int32{1, 2, 3}, true},
		{"different order", []int32{1, 2, 3}, []int32{3, 2, 1}, false},
		{"different length", []int32{1, 2}, []int32{1, 2, 3}, false},
		{"empty", []int32{}, []int32{}, true},
		{"one empty", []int32{1}, []int32{}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := fieldsMatch(tc.a, tc.b)
			if result != tc.expected {
				t.Errorf("fieldsMatch(%v, %v): expected %v, got %v", tc.a, tc.b, tc.expected, result)
			}
		})
	}
}

func TestNewTransactionDataReplacement(t *testing.T) {
	replacements := []*storage2pb.Transaction_DataReplacementGroup{
		{
			FragmentId: 0,
			NewFile:    &DataFile{Path: "data0.bin", Fields: []int32{0, 1}},
		},
		{
			FragmentId: 1,
			NewFile:    &DataFile{Path: "data1.bin", Fields: []int32{0, 1}},
		},
	}

	txn := NewTransactionDataReplacement(1, "test-uuid", replacements)

	if txn.ReadVersion != 1 {
		t.Errorf("expected read_version 1, got %d", txn.ReadVersion)
	}

	if txn.Uuid != "test-uuid" {
		t.Errorf("expected uuid 'test-uuid', got %s", txn.Uuid)
	}

	replacementOp := txn.GetDataReplacement()
	if replacementOp == nil {
		t.Fatal("expected DataReplacement operation, got nil")
	}

	if len(replacementOp.Replacements) != 2 {
		t.Errorf("expected 2 replacements, got %d", len(replacementOp.Replacements))
	}
}

func TestNewTransactionDataReplacementNil(t *testing.T) {
	txn := NewTransactionDataReplacement(1, "test-uuid", nil)

	replacementOp := txn.GetDataReplacement()
	if replacementOp == nil {
		t.Fatal("expected DataReplacement operation, got nil")
	}

	if len(replacementOp.Replacements) != 0 {
		t.Errorf("expected 0 replacements, got %d", len(replacementOp.Replacements))
	}
}

func TestBuildManifestDataReplacement(t *testing.T) {
	// Create initial manifest
	manifest := NewManifest(1)
	manifest.Fragments = []*DataFragment{
		{
			Id: 0,
			Files: []*DataFile{
				{Path: "old0.bin", Fields: []int32{0, 1}},
			},
		},
		{
			Id: 1,
			Files: []*DataFile{
				{Path: "old1.bin", Fields: []int32{0, 1}},
			},
		},
	}

	replacementOp := &storage2pb.Transaction_DataReplacement{
		Replacements: []*storage2pb.Transaction_DataReplacementGroup{
			{
				FragmentId: 0,
				NewFile:    &DataFile{Path: "new0.bin", Fields: []int32{0, 1}},
			},
		},
	}

	newManifest, err := buildManifestDataReplacement(manifest, replacementOp)
	if err != nil {
		t.Fatalf("buildManifestDataReplacement failed: %v", err)
	}

	if newManifest.Version != 2 {
		t.Errorf("expected version 2, got %d", newManifest.Version)
	}

	// Check that fragment 0 has the new file
	found := false
	for _, frag := range newManifest.Fragments {
		if frag.Id == 0 {
			for _, file := range frag.Files {
				if file.Path == "new0.bin" {
					found = true
					break
				}
			}
		}
	}
	if !found {
		t.Error("expected fragment 0 to have new file 'new0.bin'")
	}
}

func TestCheckDataReplacementConflict(t *testing.T) {
	tests := []struct {
		name         string
		myTxn        *Transaction
		otherTxn     *Transaction
		wantConflict bool
	}{
		{
			name: "replacement vs replacement same fragment",
			myTxn: NewTransactionDataReplacement(1, "uuid1", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 0},
			}),
			otherTxn: NewTransactionDataReplacement(1, "uuid2", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 0},
			}),
			wantConflict: true,
		},
		{
			name: "replacement vs replacement different fragments",
			myTxn: NewTransactionDataReplacement(1, "uuid1", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 0},
			}),
			otherTxn: NewTransactionDataReplacement(1, "uuid2", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 1},
			}),
			wantConflict: false,
		},
		{
			name: "replacement vs update same fragment",
			myTxn: NewTransactionDataReplacement(1, "uuid1", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 0},
			}),
			otherTxn: NewTransactionUpdate(1, "uuid2",
				[]uint64{},
				[]*DataFragment{{Id: 0}},
				[]*DataFragment{},
				[]uint32{0},
				UpdateModeRewriteRows),
			wantConflict: true,
		},
		{
			name: "replacement vs delete same fragment",
			myTxn: NewTransactionDataReplacement(1, "uuid1", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 0},
			}),
			otherTxn: NewTransactionDelete(1, "uuid2",
				[]*DataFragment{}, []uint64{0}, "c0 = 1"),
			wantConflict: true,
		},
		{
			name: "replacement vs rewrite same fragment",
			myTxn: NewTransactionDataReplacement(1, "uuid1", []*storage2pb.Transaction_DataReplacementGroup{
				{FragmentId: 0},
			}),
			otherTxn: NewTransactionRewrite(1, "uuid2",
				[]*DataFragment{{Id: 0}},
				[]*DataFragment{{Id: 2}}),
			wantConflict: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CheckDataReplacementConflict(tc.myTxn, tc.otherTxn)
			if result != tc.wantConflict {
				t.Errorf("CheckDataReplacementConflict(): expected %v, got %v", tc.wantConflict, result)
			}
		})
	}
}

func TestDataReplacementPlanner(t *testing.T) {
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	scheduler := DefaultIOScheduler()
	store := NewLocalObjectStoreExt(basePath, scheduler)

	manifest := NewManifest(1)
	manifest.Fragments = []*DataFragment{
		{Id: 0, Files: []*DataFile{{Path: "data0.bin", FileSizeBytes: 1000}}},
		{Id: 1, Files: []*DataFile{{Path: "data1.bin", FileSizeBytes: 2000}}},
	}

	planner := NewDataReplacementPlanner(basePath, handler, store)

	tests := []struct {
		name    string
		op      *DataReplacementOperation
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil operation",
			op:      nil,
			wantErr: true,
			errMsg:  "data replacement operation is nil",
		},
		{
			name: "no replacements",
			op: &DataReplacementOperation{
				Replacements: []FragmentReplacement{},
			},
			wantErr: true,
			errMsg:  "no replacements specified",
		},
		{
			name: "fragment not found",
			op: &DataReplacementOperation{
				Replacements: []FragmentReplacement{
					{FragmentID: 99},
				},
			},
			wantErr: true,
			errMsg:  "fragment 99 not found",
		},
		{
			name: "valid replacement",
			op: &DataReplacementOperation{
				Replacements: []FragmentReplacement{
					{
						FragmentID: 0,
						NewFiles:   []*DataFile{{Path: "new0.bin", FileSizeBytes: 1500}},
					},
				},
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planner.PlanReplacement(context.Background(), manifest, tc.op)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
					return
				}
				if !strings.Contains(err.Error(), tc.errMsg) {
					t.Errorf("expected error containing %q, got %q", tc.errMsg, err.Error())
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if plan == nil {
				t.Error("expected plan, got nil")
			}
		})
	}
}

func TestDataReplacementValidator(t *testing.T) {
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	scheduler := DefaultIOScheduler()
	store := NewLocalObjectStoreExt(basePath, scheduler)

	manifest := NewManifest(1)
	manifest.Fragments = []*DataFragment{
		{
			Id: 0,
			Files: []*DataFile{
				{Path: "data0.bin"},
				{Path: "data1.bin"},
			},
		},
	}

	validator := NewDataReplacementValidator(basePath, handler, store)

	tests := []struct {
		name    string
		op      *DataReplacementOperation
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid replacement",
			op: &DataReplacementOperation{
				Replacements: []FragmentReplacement{
					{FragmentID: 0},
				},
			},
			wantErr: false,
		},
		{
			name: "fragment not found",
			op: &DataReplacementOperation{
				Replacements: []FragmentReplacement{
					{FragmentID: 99},
				},
			},
			wantErr: true,
			errMsg:  "fragment 99 not found",
		},
		{
			name: "old file not found",
			op: &DataReplacementOperation{
				Replacements: []FragmentReplacement{
					{
						FragmentID: 0,
						OldFileIDs: []string{"nonexistent.bin"},
					},
				},
			},
			wantErr: true,
			errMsg:  "not found in fragment",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.ValidatePreconditions(context.Background(), manifest, tc.op)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
					return
				}
				if !strings.Contains(err.Error(), tc.errMsg) {
					t.Errorf("expected error containing %q, got %q", tc.errMsg, err.Error())
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestDataReplacementRollback(t *testing.T) {
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	scheduler := DefaultIOScheduler()
	store := NewLocalObjectStoreExt(basePath, scheduler)
	rollback := NewDataReplacementRollback(basePath, handler, store)

	ctx := context.Background()

	t.Run("Rollback nil replacement", func(t *testing.T) {
		err := rollback.RollbackReplacement(ctx, nil)
		if err != nil {
			t.Errorf("RollbackReplacement with nil failed: %v", err)
		}
	})

	t.Run("Rollback with replacements", func(t *testing.T) {
		replacement := &storage2pb.Transaction_DataReplacement{
			Replacements: []*storage2pb.Transaction_DataReplacementGroup{
				{
					FragmentId: 0,
					NewFile:    &DataFile{Path: "new0.bin"},
				},
			},
		}
		err := rollback.RollbackReplacement(ctx, replacement)
		if err != nil {
			t.Errorf("RollbackReplacement failed: %v", err)
		}
	})
}

func TestDataReplacementManager(t *testing.T) {
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	scheduler := DefaultIOScheduler()
	store := NewLocalObjectStoreExt(basePath, scheduler)

	// Create initial manifest
	ctx := context.Background()
	manifest := NewManifest(1)
	manifest.Fragments = []*DataFragment{
		{Id: 0, Files: []*DataFile{{Path: "data0.bin", FileSizeBytes: 1000}}},
	}
	if err := handler.Commit(ctx, basePath, 1, manifest); err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}

	manager := NewDataReplacementManager(basePath, handler, store)

	t.Run("Replace data", func(t *testing.T) {
		op := &DataReplacementOperation{
			Replacements: []FragmentReplacement{
				{
					FragmentID:       0,
					NewFiles:         []*DataFile{{Path: "new0.bin", FileSizeBytes: 1500}},
					ExpectedRowCount: 100,
				},
			},
			ValidationOptions: &DataValidationOptions{
				ValidateRowCount: false, // Skip row count validation for simplicity
				ValidateChecksum: false,
				ValidateSchema:   false,
			},
		}

		result, err := manager.ReplaceData(ctx, 1, op)
		if err != nil {
			t.Errorf("ReplaceData failed: %v", err)
			return
		}

		if !result.Success {
			t.Error("expected replacement to succeed")
		}

		if len(result.ReplacedFragments) != 1 {
			t.Errorf("expected 1 replaced fragment, got %d", len(result.ReplacedFragments))
		}
	})
}

func TestDataReplacementPlannerEstimateIO(t *testing.T) {
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	scheduler := DefaultIOScheduler()
	store := NewLocalObjectStoreExt(basePath, scheduler)
	planner := NewDataReplacementPlanner(basePath, handler, store)

	op := &DataReplacementOperation{
		Replacements: []FragmentReplacement{
			{
				FragmentID: 0,
				NewFiles: []*DataFile{
					{FileSizeBytes: 1000},
					{FileSizeBytes: 2000},
				},
			},
			{
				FragmentID: 1,
				NewFiles: []*DataFile{
					{FileSizeBytes: 3000},
				},
			},
		},
	}

	// Expected: (1000 + 2000 + 3000) * 2 = 12000
	expectedIO := uint64(12000)
	actualIO := planner.estimateIO(op)

	if actualIO != expectedIO {
		t.Errorf("expected IO estimate %d, got %d", expectedIO, actualIO)
	}
}
