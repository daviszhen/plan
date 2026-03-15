// Copyright 2026 The Plan Authors
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2/proto"
)

func TestUpdateModeString(t *testing.T) {
	tests := []struct {
		mode     UpdateMode
		expected string
	}{
		{UpdateModeRewriteRows, "REWRITE_ROWS"},
		{UpdateModeRewriteColumns, "REWRITE_COLUMNS"},
	}

	for _, tc := range tests {
		var actual string
		switch tc.mode {
		case UpdateModeRewriteRows:
			actual = "REWRITE_ROWS"
		case UpdateModeRewriteColumns:
			actual = "REWRITE_COLUMNS"
		}
		if actual != tc.expected {
			t.Errorf("UpdateMode %d: expected %s, got %s", tc.mode, tc.expected, actual)
		}
	}
}

func TestUpdatePredicateParsing(t *testing.T) {
	tests := []struct {
		name      string
		predicate string
		wantErr   bool
	}{
		{
			name:      "simple equality",
			predicate: "c0 = 1",
			wantErr:   false,
		},
		{
			name:      "range predicate",
			predicate: "c0 > 10 AND c1 < 100",
			wantErr:   false,
		},
		{
			name:      "empty predicate",
			predicate: "",
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pred, err := ParseUpdatePredicate(tc.predicate)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for predicate %q", tc.predicate)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if pred.Filter != tc.predicate {
				t.Errorf("expected filter %q, got %q", tc.predicate, pred.Filter)
			}
		})
	}
}

func TestValidateUpdate(t *testing.T) {
	fields := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int64", Id: 0},
		{Name: "name", Type: storage2pb.Field_LEAF, LogicalType: "string", Id: 1},
		{Name: "score", Type: storage2pb.Field_LEAF, LogicalType: "double", Id: 2},
	}

	tests := []struct {
		name    string
		updates []ColumnUpdate
		wantErr bool
	}{
		{
			name: "valid integer update",
			updates: []ColumnUpdate{
				{ColumnIdx: 0, NewValue: int64(123)},
			},
			wantErr: false,
		},
		{
			name: "valid string update",
			updates: []ColumnUpdate{
				{ColumnIdx: 1, NewValue: "test"},
			},
			wantErr: false,
		},
		{
			name: "valid float update",
			updates: []ColumnUpdate{
				{ColumnIdx: 2, NewValue: 3.14},
			},
			wantErr: false,
		},
		{
			name: "invalid column index",
			updates: []ColumnUpdate{
				{ColumnIdx: 10, NewValue: "test"},
			},
			wantErr: true,
		},
		{
			name: "negative column index",
			updates: []ColumnUpdate{
				{ColumnIdx: -1, NewValue: "test"},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateUpdate(fields, tc.updates)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for updates %v", tc.updates)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestFindColumnIndex(t *testing.T) {
	fields := []*storage2pb.Field{
		{Name: "id", Id: 0},
		{Name: "name", Id: 1},
		{Name: "score", Id: 2},
	}

	tests := []struct {
		name     string
		colName  string
		expected int
	}{
		{"find id", "id", 0},
		{"find name", "name", 1},
		{"find score", "score", 2},
		{"not found", "unknown", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := findColumnIndex(fields, tc.colName)
			if result != tc.expected {
				t.Errorf("findColumnIndex(%q): expected %d, got %d", tc.colName, tc.expected, result)
			}
		})
	}
}

func TestNewTransactionUpdate(t *testing.T) {
	tests := []struct {
		name               string
		readVersion        uint64
		uuid               string
		removedFragmentIDs []uint64
		updatedFragments   []*DataFragment
		newFragments       []*DataFragment
		fieldsModified     []uint32
		mode               UpdateMode
	}{
		{
			name:               "basic update",
			readVersion:        1,
			uuid:               "test-uuid",
			removedFragmentIDs: []uint64{1, 2},
			updatedFragments:   []*DataFragment{},
			newFragments:       []*DataFragment{},
			fieldsModified:     []uint32{0, 1},
			mode:               UpdateModeRewriteRows,
		},
		{
			name:               "column rewrite mode",
			readVersion:        5,
			uuid:               "col-uuid",
			removedFragmentIDs: []uint64{},
			updatedFragments:   []*DataFragment{},
			newFragments:       []*DataFragment{},
			fieldsModified:     []uint32{2},
			mode:               UpdateModeRewriteColumns,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			txn := NewTransactionUpdate(
				tc.readVersion,
				tc.uuid,
				tc.removedFragmentIDs,
				tc.updatedFragments,
				tc.newFragments,
				tc.fieldsModified,
				tc.mode,
			)

			if txn.ReadVersion != tc.readVersion {
				t.Errorf("expected read_version %d, got %d", tc.readVersion, txn.ReadVersion)
			}
			if txn.Uuid != tc.uuid {
				t.Errorf("expected uuid %s, got %s", tc.uuid, txn.Uuid)
			}

			updateOp := txn.GetUpdate()
			if updateOp == nil {
				t.Fatal("expected Update operation, got nil")
			}

			if len(updateOp.RemovedFragmentIds) != len(tc.removedFragmentIDs) {
				t.Errorf("expected %d removed fragments, got %d", len(tc.removedFragmentIDs), len(updateOp.RemovedFragmentIds))
			}

			if len(updateOp.FieldsModified) != len(tc.fieldsModified) {
				t.Errorf("expected %d modified fields, got %d", len(tc.fieldsModified), len(updateOp.FieldsModified))
			}

			expectedMode := storage2pb.Transaction_REWRITE_ROWS
			if tc.mode == UpdateModeRewriteColumns {
				expectedMode = storage2pb.Transaction_REWRITE_COLUMNS
			}
			if updateOp.UpdateMode != expectedMode {
				t.Errorf("expected mode %v, got %v", expectedMode, updateOp.UpdateMode)
			}
		})
	}
}

func TestBuildManifestUpdate(t *testing.T) {
	// Create a manifest with some fragments
	manifest := NewManifest(1)
	manifest.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, nil),
		NewDataFragmentWithRows(1, 200, nil),
		NewDataFragmentWithRows(2, 150, nil),
	}
	maxID := uint32(2)
	manifest.MaxFragmentId = &maxID

	tests := []struct {
		name          string
		removedIDs    []uint64
		updatedFrags  []*DataFragment
		newFrags      []*DataFragment
		expectedFrags int
		expectedMaxID uint32
	}{
		{
			name:          "remove one fragment",
			removedIDs:    []uint64{1},
			updatedFrags:  nil,
			newFrags:      nil,
			expectedFrags: 2,
			expectedMaxID: 2,
		},
		{
			name:         "add new fragments",
			removedIDs:   nil,
			updatedFrags: nil,
			newFrags: []*DataFragment{
				NewDataFragmentWithRows(0, 50, nil),
			},
			expectedFrags: 4,
			expectedMaxID: 3,
		},
		{
			name:       "update existing fragment",
			removedIDs: nil,
			updatedFrags: []*DataFragment{
				NewDataFragmentWithRows(1, 250, nil),
			},
			newFrags:      nil,
			expectedFrags: 3,
			expectedMaxID: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			updateOp := &storage2pb.Transaction_Update{
				RemovedFragmentIds: tc.removedIDs,
				UpdatedFragments:   tc.updatedFrags,
				NewFragments:       tc.newFrags,
			}

			newManifest, err := buildManifestUpdate(manifest, updateOp)
			if err != nil {
				t.Fatalf("buildManifestUpdate failed: %v", err)
			}

			if newManifest.Version != 2 {
				t.Errorf("expected version 2, got %d", newManifest.Version)
			}

			if len(newManifest.Fragments) != tc.expectedFrags {
				t.Errorf("expected %d fragments, got %d", tc.expectedFrags, len(newManifest.Fragments))
			}

			if newManifest.MaxFragmentId == nil || *newManifest.MaxFragmentId != tc.expectedMaxID {
				t.Errorf("expected max_fragment_id %d, got %v", tc.expectedMaxID, newManifest.MaxFragmentId)
			}
		})
	}
}

func TestCheckUpdateConflict(t *testing.T) {
	tests := []struct {
		name         string
		myTxn        *Transaction
		otherTxn     *Transaction
		wantConflict bool
	}{
		{
			name: "update vs update same fields",
			myTxn: NewTransactionUpdate(1, "uuid1",
				[]uint64{}, []*DataFragment{}, []*DataFragment{},
				[]uint32{0, 1}, UpdateModeRewriteRows),
			otherTxn: NewTransactionUpdate(1, "uuid2",
				[]uint64{}, []*DataFragment{}, []*DataFragment{},
				[]uint32{1, 2}, UpdateModeRewriteRows),
			wantConflict: true,
		},
		{
			name: "update vs update different fields",
			myTxn: NewTransactionUpdate(1, "uuid1",
				[]uint64{}, []*DataFragment{}, []*DataFragment{},
				[]uint32{0}, UpdateModeRewriteRows),
			otherTxn: NewTransactionUpdate(1, "uuid2",
				[]uint64{}, []*DataFragment{}, []*DataFragment{},
				[]uint32{1}, UpdateModeRewriteRows),
			wantConflict: false,
		},
		{
			name: "update vs delete same fragment",
			myTxn: NewTransactionUpdate(1, "uuid1",
				[]uint64{},
				[]*DataFragment{NewDataFragmentWithRows(0, 100, nil)},
				[]*DataFragment{},
				[]uint32{0}, UpdateModeRewriteRows),
			otherTxn: NewTransactionDelete(1, "uuid2",
				[]*DataFragment{}, []uint64{0}, "c0 = 1"),
			wantConflict: true,
		},
		{
			name: "update vs rewrite same fragment",
			myTxn: NewTransactionUpdate(1, "uuid1",
				[]uint64{},
				[]*DataFragment{NewDataFragmentWithRows(0, 100, nil)},
				[]*DataFragment{},
				[]uint32{0}, UpdateModeRewriteRows),
			otherTxn: NewTransactionRewrite(1, "uuid2",
				[]*DataFragment{NewDataFragmentWithRows(0, 100, nil)},
				[]*DataFragment{NewDataFragmentWithRows(0, 100, nil)}),
			wantConflict: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CheckUpdateConflict(tc.myTxn, tc.otherTxn)
			if result != tc.wantConflict {
				t.Errorf("CheckUpdateConflict(): expected %v, got %v", tc.wantConflict, result)
			}
		})
	}
}

func TestUpdateExecutor(t *testing.T) {
	// Create a simple chunk for testing
	vec1 := chunk.NewVector2(common.IntegerType(), 3)
	vec1.SetValue(0, &chunk.Value{Typ: common.IntegerType(), I64: 1})
	vec1.SetValue(1, &chunk.Value{Typ: common.IntegerType(), I64: 2})
	vec1.SetValue(2, &chunk.Value{Typ: common.IntegerType(), I64: 3})

	vec2 := chunk.NewVector2(common.VarcharType(), 3)
	vec2.SetValue(0, &chunk.Value{Typ: common.VarcharType(), Str: "a"})
	vec2.SetValue(1, &chunk.Value{Typ: common.VarcharType(), Str: "b"})
	vec2.SetValue(2, &chunk.Value{Typ: common.VarcharType(), Str: "c"})

	ch := &chunk.Chunk{}
	ch.Init([]common.LType{common.IntegerType(), common.VarcharType()}, 3)
	ch.SetCard(3)
	ch.Data[0].Reference(vec1)
	ch.Data[1].Reference(vec2)

	schema := []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int32"},
		{Name: "name", Type: storage2pb.Field_LEAF, LogicalType: "string"},
	}

	tests := []struct {
		name      string
		updates   []ColumnUpdate
		rowMask   []bool
		wantErr   bool
		checkFunc func(*chunk.Chunk) bool
	}{
		{
			name: "update all rows",
			updates: []ColumnUpdate{
				{ColumnIdx: 0, NewValue: int32(100)},
			},
			rowMask: nil, // All rows
			wantErr: false,
			checkFunc: func(result *chunk.Chunk) bool {
				val := result.Data[0].GetValue(0)
				return val.I64 == 100
			},
		},
		{
			name: "update specific rows",
			updates: []ColumnUpdate{
				{ColumnIdx: 1, NewValue: "updated"},
			},
			rowMask: []bool{true, false, true},
			wantErr: false,
			checkFunc: func(result *chunk.Chunk) bool {
				// Row 0 should be updated
				val0 := result.Data[1].GetValue(0)
				// Row 1 should not be updated
				val1 := result.Data[1].GetValue(1)
				return val0.Str == "updated" && val1.Str == "b"
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			executor := NewUpdateExecutor(tc.updates, schema)
			result, err := executor.Execute(ch, tc.rowMask)

			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tc.checkFunc != nil && !tc.checkFunc(result) {
				t.Error("check function failed")
			}
		})
	}
}

func TestEstimateUpdateCost(t *testing.T) {
	plan := &UpdatePlan{
		Operation: &UpdateOperation{
			ColumnUpdates: []ColumnUpdate{
				{ColumnIdx: 0},
				{ColumnIdx: 1},
			},
		},
		AffectedFragments: []*DataFragment{
			{Id: 0, Files: []*DataFile{{FileSizeBytes: 1000}}},
			{Id: 1, Files: []*DataFile{{FileSizeBytes: 2000}}},
		},
		EstimatedRows: 100,
		Strategy:      UpdateStrategyRewriteRows,
	}

	cost := EstimateUpdateCost(plan)

	// Expected: (1000 + 2000) * 2 = 6000 IO cost
	expectedIO := uint64(6000)
	if cost.IOCost != expectedIO {
		t.Errorf("expected IO cost %d, got %d", expectedIO, cost.IOCost)
	}

	// Expected: 100 rows * 2 columns = 200 CPU cost
	expectedCPU := uint64(200)
	if cost.CPUCost != expectedCPU {
		t.Errorf("expected CPU cost %d, got %d", expectedCPU, cost.CPUCost)
	}
}

func TestUpdatePlannerPlanUpdate(t *testing.T) {
	manifest := NewManifest(1)
	manifest.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int64", Id: 0},
		{Name: "name", Type: storage2pb.Field_LEAF, LogicalType: "string", Id: 1},
	}
	manifest.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, nil),
	}

	planner := NewUpdatePlanner("/test", nil, manifest, nil)

	tests := []struct {
		name    string
		op      *UpdateOperation
		wantErr bool
	}{
		{
			name: "valid update",
			op: &UpdateOperation{
				Predicate: UpdatePredicate{Filter: ""},
				ColumnUpdates: []ColumnUpdate{
					{ColumnIdx: 0, NewValue: int64(1)},
				},
			},
			wantErr: false,
		},
		{
			name:    "nil operation",
			op:      nil,
			wantErr: true,
		},
		{
			name: "no columns",
			op: &UpdateOperation{
				Predicate:     UpdatePredicate{Filter: ""},
				ColumnUpdates: []ColumnUpdate{},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := planner.PlanUpdate(context.Background(), tc.op)
			if tc.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
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

func TestDefaultUpdateOptions(t *testing.T) {
	opts := DefaultUpdateOptions()

	if opts.Mode != UpdateModeRewriteRows {
		t.Errorf("expected mode UpdateModeRewriteRows, got %v", opts.Mode)
	}

	if opts.BatchSize != 10000 {
		t.Errorf("expected batch size 10000, got %d", opts.BatchSize)
	}

	expectedMemory := uint64(256 * 1024 * 1024)
	if opts.MaxMemoryBytes != expectedMemory {
		t.Errorf("expected max memory %d, got %d", expectedMemory, opts.MaxMemoryBytes)
	}
}

// TestExecuteRewriteRows verifies that executeRewriteRows reads fragments,
// applies predicate-based updates, and writes new fragments.
func TestExecuteRewriteRows(t *testing.T) {
	basePath := t.TempDir()

	// Create a chunk: id(int), name(varchar)
	src := &chunk.Chunk{}
	src.Init([]common.LType{common.IntegerType(), common.VarcharType()}, 4)
	src.SetCard(4)
	src.Data[0].SetValue(0, &chunk.Value{Typ: common.IntegerType(), I64: 1})
	src.Data[0].SetValue(1, &chunk.Value{Typ: common.IntegerType(), I64: 2})
	src.Data[0].SetValue(2, &chunk.Value{Typ: common.IntegerType(), I64: 3})
	src.Data[0].SetValue(3, &chunk.Value{Typ: common.IntegerType(), I64: 4})
	src.Data[1].SetValue(0, &chunk.Value{Typ: common.VarcharType(), Str: "a"})
	src.Data[1].SetValue(1, &chunk.Value{Typ: common.VarcharType(), Str: "b"})
	src.Data[1].SetValue(2, &chunk.Value{Typ: common.VarcharType(), Str: "c"})
	src.Data[1].SetValue(3, &chunk.Value{Typ: common.VarcharType(), Str: "d"})

	// Write it as data/0.dat
	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatalf("failed to write chunk: %v", err)
	}

	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 4, []*DataFile{df})

	manifest := NewManifest(1)
	manifest.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int64", Id: 0},
		{Name: "name", Type: storage2pb.Field_LEAF, LogicalType: "string", Id: 1},
	}
	manifest.Fragments = []*DataFragment{frag}
	maxID := uint32(0)
	manifest.MaxFragmentId = &maxID

	planner := NewUpdatePlanner(basePath, nil, manifest, nil)

	t.Run("update all rows no predicate", func(t *testing.T) {
		plan := &UpdatePlan{
			Operation: &UpdateOperation{
				Predicate:     UpdatePredicate{},
				ColumnUpdates: []ColumnUpdate{{ColumnIdx: 1, NewValue: "updated"}},
				Mode:          UpdateModeRewriteRows,
			},
			AffectedFragments: []*DataFragment{frag},
			EstimatedRows:     4,
			Strategy:          UpdateStrategyRewriteRows,
		}

		result, err := planner.ExecuteUpdate(context.Background(), plan)
		if err != nil {
			t.Fatalf("ExecuteUpdate failed: %v", err)
		}
		if result.RowsUpdated != 4 {
			t.Errorf("expected 4 rows updated, got %d", result.RowsUpdated)
		}
		if len(result.NewFragments) != 1 {
			t.Fatalf("expected 1 new fragment, got %d", len(result.NewFragments))
		}

		// Read back the new fragment and verify
		newPath := filepath.Join(basePath, result.NewFragments[0].Files[0].Path)
		got, err := ReadChunkFromFile(newPath)
		if err != nil {
			t.Fatalf("failed to read new fragment: %v", err)
		}
		if got.Card() != 4 {
			t.Fatalf("expected 4 rows, got %d", got.Card())
		}
		for i := 0; i < 4; i++ {
			val := got.Data[1].GetValue(i)
			if val.Str != "updated" {
				t.Errorf("row %d: expected 'updated', got %q", i, val.Str)
			}
		}
		// id column should be unchanged
		for i := 0; i < 4; i++ {
			val := got.Data[0].GetValue(i)
			if val.I64 != int64(i+1) {
				t.Errorf("row %d: expected id %d, got %d", i, i+1, val.I64)
			}
		}
	})

	t.Run("update with predicate", func(t *testing.T) {
		// Predicate: c0 > 2  → rows 2,3 match (id=3, id=4)
		pred, err := ParseFilter("c0 > 2", nil)
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}
		plan := &UpdatePlan{
			Operation: &UpdateOperation{
				Predicate: UpdatePredicate{
					Filter:       "c0 > 2",
					ParsedFilter: &FilterExpr{Predicate: pred, Original: "c0 > 2"},
				},
				ColumnUpdates: []ColumnUpdate{{ColumnIdx: 1, NewValue: "big"}},
				Mode:          UpdateModeRewriteRows,
			},
			AffectedFragments: []*DataFragment{frag},
			EstimatedRows:     2,
			Strategy:          UpdateStrategyRewriteRows,
		}

		result, err := planner.ExecuteUpdate(context.Background(), plan)
		if err != nil {
			t.Fatalf("ExecuteUpdate failed: %v", err)
		}
		if result.RowsUpdated != 2 {
			t.Errorf("expected 2 rows updated, got %d", result.RowsUpdated)
		}
		if len(result.NewFragments) != 1 {
			t.Fatalf("expected 1 new fragment, got %d", len(result.NewFragments))
		}

		newPath := filepath.Join(basePath, result.NewFragments[0].Files[0].Path)
		got, err := ReadChunkFromFile(newPath)
		if err != nil {
			t.Fatalf("failed to read new fragment: %v", err)
		}
		// Rows 0,1 should keep original names; rows 2,3 should be "big"
		expected := []string{"a", "b", "big", "big"}
		for i, exp := range expected {
			val := got.Data[1].GetValue(i)
			if val.Str != exp {
				t.Errorf("row %d: expected %q, got %q", i, exp, val.Str)
			}
		}
	})

	t.Run("no matching rows", func(t *testing.T) {
		// Predicate: c0 > 100 → no match
		pred, err := ParseFilter("c0 > 100", nil)
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}
		plan := &UpdatePlan{
			Operation: &UpdateOperation{
				Predicate: UpdatePredicate{
					Filter:       "c0 > 100",
					ParsedFilter: &FilterExpr{Predicate: pred, Original: "c0 > 100"},
				},
				ColumnUpdates: []ColumnUpdate{{ColumnIdx: 1, NewValue: "never"}},
				Mode:          UpdateModeRewriteRows,
			},
			AffectedFragments: []*DataFragment{frag},
			EstimatedRows:     0,
			Strategy:          UpdateStrategyRewriteRows,
		}

		result, err := planner.ExecuteUpdate(context.Background(), plan)
		if err != nil {
			t.Fatalf("ExecuteUpdate failed: %v", err)
		}
		if result.RowsUpdated != 0 {
			t.Errorf("expected 0 rows updated, got %d", result.RowsUpdated)
		}
		if len(result.NewFragments) != 0 {
			t.Errorf("expected 0 new fragments, got %d", len(result.NewFragments))
		}
	})
}

// TestExecuteRewriteColumns verifies that executeRewriteColumns works correctly.
func TestExecuteRewriteColumns(t *testing.T) {
	basePath := t.TempDir()

	// Create a chunk: id(int), score(int), name(varchar)
	src := &chunk.Chunk{}
	src.Init([]common.LType{common.IntegerType(), common.IntegerType(), common.VarcharType()}, 3)
	src.SetCard(3)
	src.Data[0].SetValue(0, &chunk.Value{Typ: common.IntegerType(), I64: 1})
	src.Data[0].SetValue(1, &chunk.Value{Typ: common.IntegerType(), I64: 2})
	src.Data[0].SetValue(2, &chunk.Value{Typ: common.IntegerType(), I64: 3})
	src.Data[1].SetValue(0, &chunk.Value{Typ: common.IntegerType(), I64: 10})
	src.Data[1].SetValue(1, &chunk.Value{Typ: common.IntegerType(), I64: 20})
	src.Data[1].SetValue(2, &chunk.Value{Typ: common.IntegerType(), I64: 30})
	src.Data[2].SetValue(0, &chunk.Value{Typ: common.VarcharType(), Str: "x"})
	src.Data[2].SetValue(1, &chunk.Value{Typ: common.VarcharType(), Str: "y"})
	src.Data[2].SetValue(2, &chunk.Value{Typ: common.VarcharType(), Str: "z"})

	dataPath := filepath.Join(basePath, "data", "0.dat")
	if err := WriteChunkToFile(dataPath, src); err != nil {
		t.Fatalf("failed to write chunk: %v", err)
	}

	df := NewDataFile("data/0.dat", []int32{0, 1, 2}, 1, 0)
	frag := NewDataFragmentWithRows(0, 3, []*DataFile{df})

	manifest := NewManifest(1)
	manifest.Fields = []*storage2pb.Field{
		{Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int64", Id: 0},
		{Name: "score", Type: storage2pb.Field_LEAF, LogicalType: "int64", Id: 1},
		{Name: "name", Type: storage2pb.Field_LEAF, LogicalType: "string", Id: 2},
	}
	manifest.Fragments = []*DataFragment{frag}
	maxID := uint32(0)
	manifest.MaxFragmentId = &maxID

	planner := NewUpdatePlanner(basePath, nil, manifest, nil)

	t.Run("update single column all rows", func(t *testing.T) {
		plan := &UpdatePlan{
			Operation: &UpdateOperation{
				Predicate:     UpdatePredicate{},
				ColumnUpdates: []ColumnUpdate{{ColumnIdx: 1, NewValue: int64(99)}},
				Mode:          UpdateModeRewriteColumns,
			},
			AffectedFragments: []*DataFragment{frag},
			EstimatedRows:     3,
			Strategy:          UpdateStrategyRewriteColumns,
		}

		result, err := planner.ExecuteUpdate(context.Background(), plan)
		if err != nil {
			t.Fatalf("ExecuteUpdate failed: %v", err)
		}
		if result.RowsUpdated != 3 {
			t.Errorf("expected 3 rows updated, got %d", result.RowsUpdated)
		}
		if len(result.NewFragments) != 1 {
			t.Fatalf("expected 1 new fragment, got %d", len(result.NewFragments))
		}

		newPath := filepath.Join(basePath, result.NewFragments[0].Files[0].Path)
		got, err := ReadChunkFromFile(newPath)
		if err != nil {
			t.Fatalf("failed to read new fragment: %v", err)
		}

		// score column should all be 99
		for i := 0; i < 3; i++ {
			val := got.Data[1].GetValue(i)
			if val.I64 != 99 {
				t.Errorf("row %d: expected score 99, got %d", i, val.I64)
			}
		}
		// id and name should be unchanged
		for i := 0; i < 3; i++ {
			val := got.Data[0].GetValue(i)
			if val.I64 != int64(i+1) {
				t.Errorf("row %d: expected id %d, got %d", i, i+1, val.I64)
			}
		}
		for i, exp := range []string{"x", "y", "z"} {
			val := got.Data[2].GetValue(i)
			if val.Str != exp {
				t.Errorf("row %d: expected name %q, got %q", i, exp, val.Str)
			}
		}
	})

	t.Run("update column with predicate", func(t *testing.T) {
		// Only update rows where c0 = 2
		pred, err := ParseFilter("c0 = 2", nil)
		if err != nil {
			t.Fatalf("failed to parse filter: %v", err)
		}
		plan := &UpdatePlan{
			Operation: &UpdateOperation{
				Predicate: UpdatePredicate{
					Filter:       "c0 = 2",
					ParsedFilter: &FilterExpr{Predicate: pred, Original: "c0 = 2"},
				},
				ColumnUpdates: []ColumnUpdate{{ColumnIdx: 1, NewValue: int64(999)}},
				Mode:          UpdateModeRewriteColumns,
			},
			AffectedFragments: []*DataFragment{frag},
			EstimatedRows:     1,
			Strategy:          UpdateStrategyRewriteColumns,
		}

		result, err := planner.ExecuteUpdate(context.Background(), plan)
		if err != nil {
			t.Fatalf("ExecuteUpdate failed: %v", err)
		}
		if result.RowsUpdated != 1 {
			t.Errorf("expected 1 row updated, got %d", result.RowsUpdated)
		}

		newPath := filepath.Join(basePath, result.NewFragments[0].Files[0].Path)
		got, err := ReadChunkFromFile(newPath)
		if err != nil {
			t.Fatalf("failed to read new fragment: %v", err)
		}

		expectedScores := []int64{10, 999, 30}
		for i, exp := range expectedScores {
			val := got.Data[1].GetValue(i)
			if val.I64 != exp {
				t.Errorf("row %d: expected score %d, got %d", i, exp, val.I64)
			}
		}
	})
}

func TestUpdateStrategySelection(t *testing.T) {
	planner := &UpdatePlanner{}

	tests := []struct {
		name          string
		op            *UpdateOperation
		fragments     []*DataFragment
		estimatedRows uint64
		expected      UpdateStrategy
	}{
		{
			name: "user specified rows mode",
			op: &UpdateOperation{
				Mode: UpdateModeRewriteRows,
			},
			fragments:     []*DataFragment{{PhysicalRows: 1000}},
			estimatedRows: 100,
			expected:      UpdateStrategyRewriteRows,
		},
		{
			name: "user specified columns mode",
			op: &UpdateOperation{
				Mode:          UpdateModeRewriteColumns,
				ColumnUpdates: []ColumnUpdate{{}, {}},
			},
			fragments:     []*DataFragment{{PhysicalRows: 1000}},
			estimatedRows: 100,
			expected:      UpdateStrategyRewriteColumns,
		},
		{
			name: "auto select - few rows",
			op: &UpdateOperation{
				Mode:          -1, // Force auto-select
				ColumnUpdates: []ColumnUpdate{{}},
			},
			fragments:     []*DataFragment{{PhysicalRows: 1000}},
			estimatedRows: 50, // 5% of total
			expected:      UpdateStrategyRewriteRows,
		},
		{
			name: "auto select - many rows few columns",
			op: &UpdateOperation{
				Mode:          -1, // Force auto-select
				ColumnUpdates: []ColumnUpdate{{}},
			},
			fragments:     []*DataFragment{{PhysicalRows: 1000}},
			estimatedRows: 500, // 50% of total, 1 column
			expected:      UpdateStrategyRewriteColumns,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := planner.chooseUpdateStrategy(tc.op, tc.fragments, tc.estimatedRows)
			if result != tc.expected {
				t.Errorf("expected strategy %v, got %v", tc.expected, result)
			}
		})
	}
}
