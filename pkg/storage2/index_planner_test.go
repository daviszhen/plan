// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

func TestIndexPlannerV2FullScan(t *testing.T) {
	mgr := NewIndexManager("", nil)
	planner := NewIndexPlannerV2(mgr)

	// Nil predicate -> full scan
	plan, err := planner.PlanQuery(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.UseFullScan {
		t.Fatal("expected full scan for nil predicate")
	}

	// Predicate with no matching index -> full scan
	pred := NewColumnPredicate(0, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 42})
	plan, err = planner.PlanQuery(context.Background(), pred)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.UseFullScan {
		t.Fatal("expected full scan when no index available")
	}
}

func TestIndexPlannerV2WithBTreeIndex(t *testing.T) {
	mgr := NewIndexManager("", nil)

	// Create a B-tree index on column 0
	if err := mgr.CreateScalarIndex(context.Background(), "idx_c0", 0); err != nil {
		t.Fatal(err)
	}

	planner := NewIndexPlannerV2(mgr)
	planner.SetTableStats(&TableStatistics{
		TotalRows: 100000,
		ColumnStats: map[int]*ColumnStatisticsInfo{
			0: {DistinctCount: 1000},
		},
	})

	// Equality predicate on column 0
	pred := NewColumnPredicate(0, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 42})
	plan, err := planner.PlanQuery(context.Background(), pred)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Plan: %s", plan)

	// With 100k rows and 1000 distinct values, selectivity ~0.001 = 100 rows
	// Index cost should be much lower than full scan cost (100000 * 0.5 = 50000)
	if plan.UseFullScan {
		t.Fatal("expected index scan for equality predicate with high selectivity")
	}
	if plan.IndexName != "idx_c0" {
		t.Fatalf("expected index idx_c0, got %s", plan.IndexName)
	}
	if plan.Selectivity > 0.01 {
		t.Fatalf("expected selectivity <= 0.01, got %f", plan.Selectivity)
	}
}

func TestIndexPlannerV2SelectivityEstimation(t *testing.T) {
	mgr := NewIndexManager("", nil)
	planner := NewIndexPlannerV2(mgr)
	planner.SetTableStats(&TableStatistics{
		TotalRows: 10000,
		ColumnStats: map[int]*ColumnStatisticsInfo{
			0: {DistinctCount: 100},
			1: {DistinctCount: 5000},
		},
	})

	// Eq on column with 100 distinct -> 1/100 = 0.01
	pred1 := NewColumnPredicate(0, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 1})
	sel1 := planner.estimateSelectivity(pred1)
	if sel1 != 0.01 {
		t.Fatalf("expected selectivity 0.01, got %f", sel1)
	}

	// Eq on column with 5000 distinct -> 1/5000 = 0.0002
	pred2 := NewColumnPredicate(1, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 1})
	sel2 := planner.estimateSelectivity(pred2)
	if sel2 != 0.0002 {
		t.Fatalf("expected selectivity 0.0002, got %f", sel2)
	}

	// AND -> multiply
	andPred := NewAndPredicate(pred1, pred2)
	selAnd := planner.estimateSelectivity(andPred)
	expected := sel1 * sel2
	if selAnd != expected {
		t.Fatalf("AND selectivity: expected %f, got %f", expected, selAnd)
	}

	// OR -> s1 + s2 - s1*s2
	orPred := NewOrPredicate(pred1, pred2)
	selOr := planner.estimateSelectivity(orPred)
	expectedOr := sel1 + sel2 - sel1*sel2
	if selOr != expectedOr {
		t.Fatalf("OR selectivity: expected %f, got %f", expectedOr, selOr)
	}
}

func TestIndexPlannerV2RangeSelectivity(t *testing.T) {
	mgr := NewIndexManager("", nil)
	planner := NewIndexPlannerV2(mgr)
	planner.SetTableStats(&TableStatistics{
		TotalRows: 10000,
		ColumnStats: map[int]*ColumnStatisticsInfo{
			0: {
				DistinctCount: 1000,
				MinValue:      int64(0),
				MaxValue:      int64(100),
			},
		},
	})

	// Lt 50 on [0, 100] -> selectivity ~0.5
	pred := NewColumnPredicate(0, Lt, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 50})
	sel := planner.estimateSelectivity(pred)
	if sel < 0.49 || sel > 0.51 {
		t.Fatalf("expected selectivity ~0.5 for Lt 50 on [0,100], got %f", sel)
	}

	// Gt 100 -> selectivity 0
	pred2 := NewColumnPredicate(0, Gt, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 100})
	sel2 := planner.estimateSelectivity(pred2)
	if sel2 != 0.0 {
		t.Fatalf("expected selectivity 0.0 for Gt 100 on [0,100], got %f", sel2)
	}
}

func TestIndexPlannerV2CompoundPredicate(t *testing.T) {
	mgr := NewIndexManager("", nil)

	// Indexes on columns 0 and 1
	mgr.CreateScalarIndex(context.Background(), "idx_c0", 0)
	mgr.CreateScalarIndex(context.Background(), "idx_c1", 1)

	planner := NewIndexPlannerV2(mgr)
	planner.SetTableStats(&TableStatistics{
		TotalRows: 100000,
		ColumnStats: map[int]*ColumnStatisticsInfo{
			0: {DistinctCount: 10},
			1: {DistinctCount: 50000},
		},
	})

	// AND(c0 = 5, c1 = 100) -> should pick index with better selectivity (c1 = 1/50000)
	pred := NewAndPredicate(
		NewColumnPredicate(0, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 5}),
		NewColumnPredicate(1, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 100}),
	)

	plan, err := planner.PlanQuery(context.Background(), pred)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Plan for AND: %s", plan)

	// Should use an index (not full scan) since combined selectivity is very low
	if plan.UseFullScan {
		t.Fatal("expected index scan for highly selective AND predicate")
	}
}

func TestIndexPlannerV2LegacyPlanQuery(t *testing.T) {
	mgr := NewIndexManager("", nil)
	planner := NewIndexPlanner(mgr)

	// Without indexes, returns nil, false
	pred := NewColumnPredicate(0, Eq, &chunk.Value{Typ: common.LType{Id: common.LTID_INTEGER}, I64: 1})
	rowIDs, used := planner.PlanQuery(pred)
	if used {
		t.Fatal("should not use index when none exists")
	}
	if rowIDs != nil {
		t.Fatal("should return nil rowIDs")
	}
}

func TestExtractPredicateColumns(t *testing.T) {
	pred := NewAndPredicate(
		NewColumnPredicate(3, Eq, nil),
		NewOrPredicate(
			NewColumnPredicate(1, Lt, nil),
			NewColumnPredicate(5, Gt, nil),
		),
	)

	cols := extractPredicateColumns(pred)
	expected := []int{1, 3, 5}
	if len(cols) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, cols)
	}
	for i, c := range cols {
		if c != expected[i] {
			t.Fatalf("expected col %d at pos %d, got %d", expected[i], i, c)
		}
	}
}
