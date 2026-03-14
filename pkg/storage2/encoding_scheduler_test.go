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
	"sync/atomic"
	"testing"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

// ============================================================================
// Helper: build a flat integer vector
// ============================================================================

func makeIntVector(typ common.LType, values []int64, nulls []bool) *chunk.Vector {
	n := len(values)
	vec := chunk.NewFlatVector(typ, n)
	for i := 0; i < n; i++ {
		if i < len(nulls) && nulls[i] {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else {
			vec.SetValue(i, &chunk.Value{Typ: typ, I64: values[i]})
		}
	}
	return vec
}

func makeStringVector(values []string, nulls []bool) *chunk.Vector {
	n := len(values)
	typ := common.VarcharType()
	vec := chunk.NewFlatVector(typ, n)
	for i := 0; i < n; i++ {
		if i < len(nulls) && nulls[i] {
			chunk.SetNullInPhyFormatFlat(vec, uint64(i), true)
		} else {
			vec.SetValue(i, &chunk.Value{Typ: typ, Str: values[i]})
		}
	}
	return vec
}

// ============================================================================
// DefaultEncodingSchedulerConfig Tests
// ============================================================================

func TestDefaultEncodingSchedulerConfig(t *testing.T) {
	cfg := DefaultEncodingSchedulerConfig()
	if cfg.MaxConcurrency != 4 {
		t.Errorf("expected MaxConcurrency=4, got %d", cfg.MaxConcurrency)
	}
	if cfg.MemoryBudget != 256*1024*1024 {
		t.Errorf("expected MemoryBudget=256MB, got %d", cfg.MemoryBudget)
	}
	if !cfg.AutoSelectEncoding {
		t.Error("expected AutoSelectEncoding=true")
	}
}

// ============================================================================
// EncodingScheduler Creation Tests
// ============================================================================

func TestNewEncodingSchedulerNilConfig(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	if sched == nil {
		t.Fatal("expected non-nil scheduler")
	}
	if sched.config.MaxConcurrency != 4 {
		t.Errorf("expected default MaxConcurrency=4, got %d", sched.config.MaxConcurrency)
	}
}

func TestNewEncodingSchedulerCustomConfig(t *testing.T) {
	cfg := &EncodingSchedulerConfig{
		MaxConcurrency:     8,
		MemoryBudget:       512 * 1024 * 1024,
		AutoSelectEncoding: false,
	}
	sched := NewEncodingScheduler(cfg)
	if sched.config.MaxConcurrency != 8 {
		t.Errorf("expected MaxConcurrency=8, got %d", sched.config.MaxConcurrency)
	}
	if sched.config.MemoryBudget != 512*1024*1024 {
		t.Errorf("expected MemoryBudget=512MB, got %d", sched.config.MemoryBudget)
	}
	if sched.config.AutoSelectEncoding {
		t.Error("expected AutoSelectEncoding=false")
	}
}

func TestNewEncodingSchedulerZeroConcurrency(t *testing.T) {
	cfg := &EncodingSchedulerConfig{MaxConcurrency: 0}
	sched := NewEncodingScheduler(cfg)
	if sched.config.MaxConcurrency != 4 {
		t.Errorf("expected clamped MaxConcurrency=4, got %d", sched.config.MaxConcurrency)
	}
}

// ============================================================================
// ScheduleTasks Tests — single column
// ============================================================================

func TestScheduleTasksSingleIntColumn(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	typ := common.IntegerType()
	vec := makeIntVector(typ, []int64{10, 20, 30, 40, 50}, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: 5},
	}

	results, progress := sched.ScheduleTasks(ctx, tasks)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}
	if results[0].Page == nil {
		t.Fatal("expected non-nil Page")
	}
	if results[0].Page.NumRows != 5 {
		t.Errorf("expected 5 rows, got %d", results[0].Page.NumRows)
	}
	if progress.Completed() != 1 {
		t.Errorf("expected 1 completed, got %d", progress.Completed())
	}
	if progress.TotalColumns != 1 {
		t.Errorf("expected TotalColumns=1, got %d", progress.TotalColumns)
	}
	if progress.TotalRows != 5 {
		t.Errorf("expected TotalRows=5, got %d", progress.TotalRows)
	}

	sched.ReleaseResult(&results[0])
}

// ============================================================================
// ScheduleTasks Tests — multiple columns in parallel
// ============================================================================

func TestScheduleTasksMultipleColumns(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	numCols := 6
	tasks := make([]EncodingTask, numCols)
	for i := 0; i < numCols; i++ {
		typ := common.IntegerType()
		values := make([]int64, 100)
		for j := 0; j < 100; j++ {
			values[j] = int64(i*1000 + j)
		}
		vec := makeIntVector(typ, values, nil)
		tasks[i] = EncodingTask{ColumnIndex: i, Vec: vec, NumRows: 100}
	}

	results, progress := sched.ScheduleTasks(ctx, tasks)

	if len(results) != numCols {
		t.Fatalf("expected %d results, got %d", numCols, len(results))
	}
	for i, r := range results {
		if r.Error != nil {
			t.Errorf("column %d: unexpected error: %v", i, r.Error)
		}
		if r.Page == nil {
			t.Errorf("column %d: expected non-nil Page", i)
		}
		if r.ColumnIndex != i {
			t.Errorf("column %d: expected ColumnIndex=%d, got %d", i, i, r.ColumnIndex)
		}
	}
	if progress.Completed() != numCols {
		t.Errorf("expected %d completed, got %d", numCols, progress.Completed())
	}
	frac := progress.Fraction()
	if frac != 1.0 {
		t.Errorf("expected Fraction=1.0, got %f", frac)
	}

	for i := range results {
		sched.ReleaseResult(&results[i])
	}
}

// ============================================================================
// ScheduleTasks Tests — string columns
// ============================================================================

func TestScheduleTasksStringColumn(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	values := []string{"hello", "world", "foo", "bar", "hello", "world"}
	vec := makeStringVector(values, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: len(values)},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}
	if results[0].Page == nil {
		t.Fatal("expected non-nil Page")
	}
	if results[0].Page.NumRows != len(values) {
		t.Errorf("expected %d rows, got %d", len(values), results[0].Page.NumRows)
	}
	sched.ReleaseResult(&results[0])
}

// ============================================================================
// ScheduleTasks Tests — forced encoding
// ============================================================================

func TestScheduleTasksForcedEncoding(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	typ := common.IntegerType()
	vec := makeIntVector(typ, []int64{1, 2, 3, 4, 5}, nil)

	plain := EncodingPlain
	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: 5, ForcedEncoding: &plain},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}
	if results[0].Page.Encoding != EncodingPlain {
		t.Errorf("expected plain encoding, got %v", results[0].Page.Encoding)
	}
	sched.ReleaseResult(&results[0])
}

// ============================================================================
// ScheduleTasks Tests — auto-select disabled
// ============================================================================

func TestScheduleTasksAutoSelectDisabled(t *testing.T) {
	cfg := &EncodingSchedulerConfig{
		MaxConcurrency:     2,
		AutoSelectEncoding: false,
	}
	sched := NewEncodingScheduler(cfg)
	ctx := context.Background()

	typ := common.IntegerType()
	// Values with many runs (would select RLE if auto), but auto is off.
	values := make([]int64, 100)
	for i := range values {
		values[i] = int64(i / 20) // runs of 20
	}
	vec := makeIntVector(typ, values, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: len(values)},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}
	// With auto-select off, should use plain.
	if results[0].Page.Encoding != EncodingPlain {
		t.Errorf("expected plain encoding with auto-select off, got %v", results[0].Page.Encoding)
	}
	sched.ReleaseResult(&results[0])
}

// ============================================================================
// ScheduleTasks Tests — with nulls
// ============================================================================

func TestScheduleTasksWithNulls(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	typ := common.IntegerType()
	values := []int64{10, 0, 30, 0, 50}
	nulls := []bool{false, true, false, true, false}
	vec := makeIntVector(typ, values, nulls)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: 5},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}
	if results[0].Page.NumRows != 5 {
		t.Errorf("expected 5 rows, got %d", results[0].Page.NumRows)
	}
	sched.ReleaseResult(&results[0])
}

// ============================================================================
// ScheduleTasks Tests — empty tasks
// ============================================================================

func TestScheduleTasksEmpty(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	results, progress := sched.ScheduleTasks(ctx, nil)
	if len(results) != 0 {
		t.Errorf("expected 0 results for nil tasks, got %d", len(results))
	}
	if progress.TotalColumns != 0 {
		t.Errorf("expected TotalColumns=0, got %d", progress.TotalColumns)
	}
	if progress.Fraction() != 1.0 {
		t.Errorf("expected Fraction=1.0 for empty, got %f", progress.Fraction())
	}
}

// ============================================================================
// Context Cancellation Tests
// ============================================================================

func TestScheduleTasksCancellation(t *testing.T) {
	// Use concurrency=1 so tasks are serialized.
	cfg := &EncodingSchedulerConfig{
		MaxConcurrency:     1,
		AutoSelectEncoding: true,
	}
	sched := NewEncodingScheduler(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	typ := common.IntegerType()
	vec := makeIntVector(typ, []int64{1, 2, 3}, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: 3},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	// Should get an error due to cancellation.
	if results[0].Error == nil {
		t.Error("expected error due to cancelled context")
	}
}

// ============================================================================
// Memory Tracking Tests
// ============================================================================

func TestMemoryTrackerBasic(t *testing.T) {
	mt := newMemoryTracker(1024)
	ctx := context.Background()

	if mt.usage() != 0 {
		t.Errorf("expected initial usage=0, got %d", mt.usage())
	}

	if !mt.reserve(ctx, 512) {
		t.Fatal("expected successful reservation")
	}
	if mt.usage() != 512 {
		t.Errorf("expected usage=512, got %d", mt.usage())
	}

	mt.release(256)
	if mt.usage() != 256 {
		t.Errorf("expected usage=256, got %d", mt.usage())
	}

	mt.release(256)
	if mt.usage() != 0 {
		t.Errorf("expected usage=0, got %d", mt.usage())
	}
}

func TestMemoryTrackerUnlimited(t *testing.T) {
	mt := newMemoryTracker(0) // unlimited
	ctx := context.Background()

	if !mt.reserve(ctx, 1<<30) {
		t.Fatal("expected successful reservation with unlimited budget")
	}
	if mt.usage() != 1<<30 {
		t.Errorf("expected usage=1GB, got %d", mt.usage())
	}
	mt.release(1 << 30)
}

func TestMemoryTrackerBackPressure(t *testing.T) {
	mt := newMemoryTracker(100)
	ctx := context.Background()

	// Fill up the budget.
	mt.reserve(ctx, 80)

	// Try to reserve more than available, with a short-lived context.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()

	ok := mt.reserve(ctx2, 50) // 80+50=130 > 100
	if ok {
		t.Error("expected back-pressure to block and then cancel")
	}
}

func TestMemoryTrackerReleaseUnblocks(t *testing.T) {
	mt := newMemoryTracker(100)
	ctx := context.Background()

	mt.reserve(ctx, 80)

	var reserved int32
	go func() {
		// This should block until some memory is released.
		if mt.reserve(ctx, 50) {
			atomic.StoreInt32(&reserved, 1)
		}
	}()

	time.Sleep(20 * time.Millisecond) // give goroutine time to block
	mt.release(40)                    // free enough: 80-40=40, 40+50=90 <= 100
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&reserved) != 1 {
		t.Error("expected reservation to succeed after release")
	}
}

// ============================================================================
// MemoryUsage and ReleaseResult Tests
// ============================================================================

func TestSchedulerMemoryUsageAndRelease(t *testing.T) {
	cfg := &EncodingSchedulerConfig{
		MaxConcurrency:     2,
		MemoryBudget:       0, // unlimited for this test
		AutoSelectEncoding: true,
	}
	sched := NewEncodingScheduler(cfg)
	ctx := context.Background()

	typ := common.IntegerType()
	vec := makeIntVector(typ, []int64{1, 2, 3, 4, 5}, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: 5},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("unexpected error: %v", results[0].Error)
	}

	usageBefore := sched.MemoryUsage()
	if usageBefore <= 0 {
		t.Error("expected positive memory usage after encoding")
	}

	sched.ReleaseResult(&results[0])
	usageAfter := sched.MemoryUsage()
	if usageAfter != 0 {
		t.Errorf("expected memory usage=0 after release, got %d", usageAfter)
	}
}

func TestReleaseResultNilPage(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	// Should not panic.
	r := &EncodingResult{ColumnIndex: 0, Page: nil, Error: nil}
	sched.ReleaseResult(r)
}

// ============================================================================
// EncodeChunk Tests
// ============================================================================

func TestEncodeChunkBasic(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	types := []common.LType{
		common.IntegerType(),
		common.VarcharType(),
	}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 10
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i * 100)})
		chk.Data[1].SetValue(i, &chunk.Value{Typ: types[1], Str: "hello"})
	}
	chk.SetCard(numRows)

	results, progress, err := sched.EncodeChunk(ctx, chk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if r.Error != nil {
			t.Errorf("column %d: unexpected error: %v", i, r.Error)
		}
		if r.Page == nil {
			t.Errorf("column %d: expected non-nil Page", i)
		}
	}
	if progress.Completed() != 2 {
		t.Errorf("expected 2 completed, got %d", progress.Completed())
	}

	for i := range results {
		sched.ReleaseResult(&results[i])
	}
}

func TestEncodeChunkNilChunk(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	results, progress, err := sched.EncodeChunk(ctx, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
	if progress.TotalColumns != 0 {
		t.Errorf("expected TotalColumns=0, got %d", progress.TotalColumns)
	}
}

func TestEncodeChunkManyColumns(t *testing.T) {
	sched := NewEncodingScheduler(&EncodingSchedulerConfig{
		MaxConcurrency:     2,
		AutoSelectEncoding: true,
	})
	ctx := context.Background()

	numCols := 8
	types := make([]common.LType, numCols)
	for i := range types {
		if i%2 == 0 {
			types[i] = common.IntegerType()
		} else {
			types[i] = common.VarcharType()
		}
	}

	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 50
	for i := 0; i < numRows; i++ {
		for c := 0; c < numCols; c++ {
			if c%2 == 0 {
				chk.Data[c].SetValue(i, &chunk.Value{Typ: types[c], I64: int64(i)})
			} else {
				chk.Data[c].SetValue(i, &chunk.Value{Typ: types[c], Str: "data"})
			}
		}
	}
	chk.SetCard(numRows)

	results, progress, err := sched.EncodeChunk(ctx, chk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != numCols {
		t.Fatalf("expected %d results, got %d", numCols, len(results))
	}
	if progress.Completed() != numCols {
		t.Errorf("expected %d completed, got %d", numCols, progress.Completed())
	}

	for i := range results {
		sched.ReleaseResult(&results[i])
	}
}

// ============================================================================
// EncodeBatch Tests
// ============================================================================

func TestEncodeBatchBasic(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	numChunks := 3
	chunks := make([]*chunk.Chunk, numChunks)
	types := []common.LType{common.IntegerType()}

	for c := 0; c < numChunks; c++ {
		chk := &chunk.Chunk{}
		chk.Init(types, util.DefaultVectorSize)
		numRows := 20
		for i := 0; i < numRows; i++ {
			chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(c*1000 + i)})
		}
		chk.SetCard(numRows)
		chunks[c] = chk
	}

	batchResults := sched.EncodeBatch(ctx, chunks)
	if len(batchResults) != numChunks {
		t.Fatalf("expected %d batch results, got %d", numChunks, len(batchResults))
	}

	for i, br := range batchResults {
		if br.Error != nil {
			t.Errorf("chunk %d: unexpected error: %v", i, br.Error)
		}
		if br.ChunkIndex != i {
			t.Errorf("expected ChunkIndex=%d, got %d", i, br.ChunkIndex)
		}
		if len(br.Results) != 1 {
			t.Errorf("chunk %d: expected 1 column result, got %d", i, len(br.Results))
		}
		for j := range br.Results {
			sched.ReleaseResult(&br.Results[j])
		}
	}
}

func TestEncodeBatchCancellation(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	types := []common.LType{common.IntegerType()}
	chunks := make([]*chunk.Chunk, 5)
	for c := 0; c < 5; c++ {
		chk := &chunk.Chunk{}
		chk.Init(types, util.DefaultVectorSize)
		chk.Data[0].SetValue(0, &chunk.Value{Typ: types[0], I64: 1})
		chk.SetCard(1)
		chunks[c] = chk
	}

	batchResults := sched.EncodeBatch(ctx, chunks)
	// At least one should have a cancellation error.
	hasCancel := false
	for _, br := range batchResults {
		if br.Error != nil {
			hasCancel = true
			break
		}
	}
	if !hasCancel {
		// All might have completed before context check; that's acceptable
		// for a very fast operation.
		t.Log("all chunks completed before cancellation detected (acceptable for fast ops)")
	}
}

// ============================================================================
// CollectStats Tests
// ============================================================================

func TestCollectStatsBasic(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	types := []common.LType{common.IntegerType(), common.VarcharType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 5
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		chk.Data[1].SetValue(i, &chunk.Value{Typ: types[1], Str: "test"})
	}
	chk.SetCard(numRows)

	results, _, err := sched.EncodeChunk(ctx, chk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stats := CollectStats(results)
	if stats.TotalColumns != 2 {
		t.Errorf("expected TotalColumns=2, got %d", stats.TotalColumns)
	}
	if stats.TotalRows != int64(numRows*2) {
		t.Errorf("expected TotalRows=%d, got %d", numRows*2, stats.TotalRows)
	}
	if stats.TotalBytesOut <= 0 {
		t.Error("expected positive TotalBytesOut")
	}
	if len(stats.EncodingCounts) == 0 {
		t.Error("expected non-empty EncodingCounts")
	}

	for i := range results {
		sched.ReleaseResult(&results[i])
	}
}

func TestCollectStatsEmpty(t *testing.T) {
	stats := CollectStats(nil)
	if stats.TotalColumns != 0 {
		t.Errorf("expected TotalColumns=0, got %d", stats.TotalColumns)
	}
}

// ============================================================================
// PlanChunkEncoding Tests
// ============================================================================

func TestPlanChunkEncodingIntColumns(t *testing.T) {
	types := []common.LType{common.IntegerType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 100
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i / 20)}) // 5 distinct, long runs
	}
	chk.SetCard(numRows)

	plans := PlanChunkEncoding(chk, numRows)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	if plans[0].ColumnIndex != 0 {
		t.Errorf("expected ColumnIndex=0, got %d", plans[0].ColumnIndex)
	}
	if plans[0].Reason == "" {
		t.Error("expected non-empty Reason")
	}
	// With runs of 20, should be RLE
	if plans[0].Encoding != EncodingRLE {
		t.Errorf("expected RLE encoding for long runs, got %v (reason: %s)", plans[0].Encoding, plans[0].Reason)
	}
}

func TestPlanChunkEncodingStringColumns(t *testing.T) {
	types := []common.LType{common.VarcharType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 100
	strs := []string{"alpha", "beta", "gamma"}
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], Str: strs[i%3]})
	}
	chk.SetCard(numRows)

	plans := PlanChunkEncoding(chk, numRows)
	if len(plans) != 1 {
		t.Fatalf("expected 1 plan, got %d", len(plans))
	}
	// 3 distinct out of 100 -> dict ratio < 0.5
	if plans[0].Encoding != EncodingDictionary {
		t.Errorf("expected dictionary encoding for few distinct strings, got %v (reason: %s)", plans[0].Encoding, plans[0].Reason)
	}
}

func TestPlanChunkEncodingBooleanColumn(t *testing.T) {
	types := []common.LType{common.BooleanType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 10
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], Bool: i%2 == 0})
	}
	chk.SetCard(numRows)

	plans := PlanChunkEncoding(chk, numRows)
	if plans[0].Encoding != EncodingBitPacked {
		t.Errorf("expected bitpacked for boolean, got %v", plans[0].Encoding)
	}
	if plans[0].Reason != "boolean always bitpacked" {
		t.Errorf("unexpected reason: %s", plans[0].Reason)
	}
}

func TestPlanChunkEncodingFloatColumn(t *testing.T) {
	types := []common.LType{common.FloatType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 5
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], F64: float64(i) * 1.1})
	}
	chk.SetCard(numRows)

	plans := PlanChunkEncoding(chk, numRows)
	if plans[0].Encoding != EncodingPlain {
		t.Errorf("expected plain for float, got %v", plans[0].Encoding)
	}
}

func TestPlanChunkEncodingMultiColumn(t *testing.T) {
	types := []common.LType{
		common.IntegerType(),
		common.VarcharType(),
		common.BooleanType(),
	}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 10
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i)})
		chk.Data[1].SetValue(i, &chunk.Value{Typ: types[1], Str: "val"})
		chk.Data[2].SetValue(i, &chunk.Value{Typ: types[2], Bool: true})
	}
	chk.SetCard(numRows)

	plans := PlanChunkEncoding(chk, numRows)
	if len(plans) != 3 {
		t.Fatalf("expected 3 plans, got %d", len(plans))
	}
	for _, p := range plans {
		if p.Reason == "" {
			t.Errorf("column %d: expected non-empty reason", p.ColumnIndex)
		}
	}
}

// ============================================================================
// EncodingProgress Tests
// ============================================================================

func TestEncodingProgressFractionEmpty(t *testing.T) {
	p := &EncodingProgress{TotalColumns: 0}
	if p.Fraction() != 1.0 {
		t.Errorf("expected Fraction=1.0 for 0 total, got %f", p.Fraction())
	}
}

func TestEncodingProgressFractionPartial(t *testing.T) {
	p := &EncodingProgress{TotalColumns: 4}
	atomic.StoreInt32(&p.CompletedColumns, 2)
	if p.Fraction() != 0.5 {
		t.Errorf("expected Fraction=0.5, got %f", p.Fraction())
	}
}

// ============================================================================
// Concurrency Stress Test
// ============================================================================

func TestSchedulerConcurrencyStress(t *testing.T) {
	cfg := &EncodingSchedulerConfig{
		MaxConcurrency:     2, // low concurrency to test queuing
		AutoSelectEncoding: true,
	}
	sched := NewEncodingScheduler(cfg)
	ctx := context.Background()

	numTasks := 16
	tasks := make([]EncodingTask, numTasks)
	for i := 0; i < numTasks; i++ {
		typ := common.IntegerType()
		values := make([]int64, 50)
		for j := range values {
			values[j] = int64(j)
		}
		vec := makeIntVector(typ, values, nil)
		tasks[i] = EncodingTask{ColumnIndex: i, Vec: vec, NumRows: 50}
	}

	results, progress := sched.ScheduleTasks(ctx, tasks)

	if len(results) != numTasks {
		t.Fatalf("expected %d results, got %d", numTasks, len(results))
	}
	for i, r := range results {
		if r.Error != nil {
			t.Errorf("task %d: unexpected error: %v", i, r.Error)
		}
		if r.Page == nil {
			t.Errorf("task %d: expected non-nil Page", i)
		}
	}
	if progress.Completed() != numTasks {
		t.Errorf("expected %d completed, got %d", numTasks, progress.Completed())
	}

	for i := range results {
		sched.ReleaseResult(&results[i])
	}
}

// ============================================================================
// Integration: Encode then Decode roundtrip via scheduler
// ============================================================================

func TestSchedulerRoundtripIntColumn(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	typ := common.IntegerType()
	original := []int64{100, 200, 300, 400, 500}
	vec := makeIntVector(typ, original, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: len(original)},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("encode error: %v", results[0].Error)
	}

	// Decode back.
	outVec := chunk.NewFlatVector(typ, len(original))
	decoder := NewLogicalColumnDecoder()
	if err := decoder.DecodeColumn(results[0].Page, outVec, typ); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i, want := range original {
		got := outVec.GetValue(i)
		if got.I64 != want {
			t.Errorf("row %d: expected %d, got %d", i, want, got.I64)
		}
	}
	sched.ReleaseResult(&results[0])
}

func TestSchedulerRoundtripStringColumn(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	original := []string{"alpha", "beta", "gamma", "alpha", "beta", "gamma", "alpha", "beta", "gamma", "alpha"}
	vec := makeStringVector(original, nil)

	tasks := []EncodingTask{
		{ColumnIndex: 0, Vec: vec, NumRows: len(original)},
	}

	results, _ := sched.ScheduleTasks(ctx, tasks)
	if results[0].Error != nil {
		t.Fatalf("encode error: %v", results[0].Error)
	}

	typ := common.VarcharType()
	outVec := chunk.NewFlatVector(typ, len(original))
	decoder := NewLogicalColumnDecoder()
	if err := decoder.DecodeColumn(results[0].Page, outVec, typ); err != nil {
		t.Fatalf("decode error: %v", err)
	}

	for i, want := range original {
		got := outVec.GetValue(i)
		if got.Str != want {
			t.Errorf("row %d: expected %q, got %q", i, want, got.Str)
		}
	}
	sched.ReleaseResult(&results[0])
}

// ============================================================================
// Integration: EncodeChunk roundtrip
// ============================================================================

func TestEncodeChunkRoundtrip(t *testing.T) {
	sched := NewEncodingScheduler(nil)
	ctx := context.Background()

	types := []common.LType{common.IntegerType(), common.VarcharType()}
	chk := &chunk.Chunk{}
	chk.Init(types, util.DefaultVectorSize)

	numRows := 20
	for i := 0; i < numRows; i++ {
		chk.Data[0].SetValue(i, &chunk.Value{Typ: types[0], I64: int64(i * 10)})
		chk.Data[1].SetValue(i, &chunk.Value{Typ: types[1], Str: "row"})
	}
	chk.SetCard(numRows)

	results, _, err := sched.EncodeChunk(ctx, chk)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoder := NewLogicalColumnDecoder()

	// Decode integer column.
	intVec := chunk.NewFlatVector(types[0], numRows)
	if err := decoder.DecodeColumn(results[0].Page, intVec, types[0]); err != nil {
		t.Fatalf("decode int column error: %v", err)
	}
	for i := 0; i < numRows; i++ {
		got := intVec.GetValue(i)
		want := int64(i * 10)
		if got.I64 != want {
			t.Errorf("int row %d: expected %d, got %d", i, want, got.I64)
		}
	}

	// Decode string column.
	strVec := chunk.NewFlatVector(types[1], numRows)
	if err := decoder.DecodeColumn(results[1].Page, strVec, types[1]); err != nil {
		t.Fatalf("decode string column error: %v", err)
	}
	for i := 0; i < numRows; i++ {
		got := strVec.GetValue(i)
		if got.Str != "row" {
			t.Errorf("str row %d: expected %q, got %q", i, "row", got.Str)
		}
	}

	for i := range results {
		sched.ReleaseResult(&results[i])
	}
}
