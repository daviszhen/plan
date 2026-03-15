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
)

// TestCompactionPlannerBasic tests basic compaction planning.
func TestCompactionPlannerBasic(t *testing.T) {
	// Create test fragments
	fragments := make([]*DataFragment, 10)
	for i := 0; i < 10; i++ {
		fragments[i] = &DataFragment{
			Id:           uint64(i),
			PhysicalRows: 100,
			Files: []*DataFile{
				{Path: "test.bin", FileSizeBytes: 1024 * 1024}, // 1MB each
			},
		}
	}

	planner := NewCompactionPlanner(DefaultCompactionPlannerConfig())
	plan := planner.Plan(fragments)

	if plan == nil {
		t.Fatal("expected non-nil plan")
	}

	if len(plan.Tasks) == 0 {
		t.Fatal("expected at least one task")
	}

	t.Logf("Created %d tasks from %d fragments", len(plan.Tasks), len(fragments))
}

// TestCompactionPlannerEmpty tests planning with insufficient fragments.
func TestCompactionPlannerEmpty(t *testing.T) {
	planner := NewCompactionPlanner(DefaultCompactionPlannerConfig())

	// Empty fragments
	plan := planner.Plan(nil)
	if plan == nil || len(plan.Tasks) != 0 {
		t.Error("expected empty plan for nil fragments")
	}

	// Single fragment
	plan = planner.Plan([]*DataFragment{{Id: 0}})
	if plan == nil || len(plan.Tasks) != 0 {
		t.Error("expected empty plan for single fragment")
	}
}

// TestCompactionPlannerStrategies tests different planning strategies.
func TestCompactionPlannerStrategies(t *testing.T) {
	fragments := make([]*DataFragment, 20)
	for i := 0; i < 20; i++ {
		fragments[i] = &DataFragment{
			Id:           uint64(i),
			PhysicalRows: 1000,
			Files: []*DataFile{
				{Path: "test.bin", FileSizeBytes: 10 * 1024 * 1024}, // 10MB each
			},
		}
	}

	strategies := []CompactionStrategy{
		StrategySize,
		StrategyCount,
		StrategyHybrid,
		StrategyBinPacking,
	}

	for _, strategy := range strategies {
		t.Run(string(strategy), func(t *testing.T) {
			cfg := DefaultCompactionPlannerConfig()
			cfg.Strategy = strategy
			planner := NewCompactionPlanner(cfg)
			plan := planner.Plan(fragments)

			if plan == nil {
				t.Fatal("expected non-nil plan")
			}

			t.Logf("Strategy %s: %d tasks, %d total fragments",
				strategy, len(plan.Tasks), plan.TotalOldFragments)
		})
	}
}

// TestCompactionPlannerWithDeletions tests planning with deletion files.
func TestCompactionPlannerWithDeletions(t *testing.T) {
	fragments := make([]*DataFragment, 10)
	for i := 0; i < 10; i++ {
		fragments[i] = &DataFragment{
			Id:           uint64(i),
			PhysicalRows: 100,
			Files: []*DataFile{
				{Path: "test.bin", FileSizeBytes: 1024 * 1024},
			},
		}
		// Add deletion file to half the fragments
		if i%2 == 0 {
			fragments[i].DeletionFile = &DeletionFile{
				FileType:       0,
				NumDeletedRows: 10,
			}
		}
	}

	// Test with deletions included
	cfg := DefaultCompactionPlannerConfig()
	cfg.IncludeDeletions = true
	planner := NewCompactionPlanner(cfg)
	planWith := planner.Plan(fragments)

	// Test with deletions excluded
	cfg.IncludeDeletions = false
	planner = NewCompactionPlanner(cfg)
	planWithout := planner.Plan(fragments)

	t.Logf("With deletions: %d tasks", len(planWith.Tasks))
	t.Logf("Without deletions: %d tasks", len(planWithout.Tasks))

	// Should have fewer or equal fragments when excluding deletions
	if planWithout.TotalOldFragments > planWith.TotalOldFragments {
		t.Error("excluding deletions should not increase fragment count")
	}
}

// TestCompactionPlannerMaxTasks tests task limit enforcement.
func TestCompactionPlannerMaxTasks(t *testing.T) {
	fragments := make([]*DataFragment, 100)
	for i := 0; i < 100; i++ {
		fragments[i] = &DataFragment{
			Id:           uint64(i),
			PhysicalRows: 100,
			Files: []*DataFile{
				{Path: "test.bin", FileSizeBytes: 1024}, // 1KB each
			},
		}
	}

	cfg := DefaultCompactionPlannerConfig()
	cfg.MaxTasks = 5
	cfg.MinFragmentsPerTask = 2

	planner := NewCompactionPlanner(cfg)
	plan := planner.Plan(fragments)

	if len(plan.Tasks) > 5 {
		t.Errorf("expected at most 5 tasks, got %d", len(plan.Tasks))
	}
}

// TestWorkerPoolBasic tests worker pool operations.
func TestWorkerPoolBasic(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)
	pool := NewWorkerPool(4, store)

	ctx := context.Background()

	// Acquire and release workers
	for i := 0; i < 4; i++ {
		worker, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("failed to acquire worker: %v", err)
		}
		if worker == nil {
			t.Fatal("expected non-nil worker")
		}
		pool.Release()
	}

	if pool.ActiveCount() != 0 {
		t.Errorf("expected 0 active workers, got %d", pool.ActiveCount())
	}
}

// TestProgressTracker tests progress tracking.
func TestProgressTracker(t *testing.T) {
	tracker := NewProgressTracker(10, 1000)

	// Initial state
	if tracker.OverallProgress() != 0 {
		t.Error("expected 0 initial progress")
	}

	// Complete some tasks
	for i := 0; i < 5; i++ {
		tracker.MarkTaskComplete(i, 100)
	}

	progress := tracker.OverallProgress()
	if progress != 0.5 {
		t.Errorf("expected 0.5 progress, got %f", progress)
	}

	if tracker.CompletedTasks() != 5 {
		t.Errorf("expected 5 completed tasks, got %d", tracker.CompletedTasks())
	}

	if tracker.RemainingTasks() != 5 {
		t.Errorf("expected 5 remaining tasks, got %d", tracker.RemainingTasks())
	}
}

// TestCompactionCoordinatorConfig tests coordinator configuration.
func TestCompactionCoordinatorConfig(t *testing.T) {
	cfg := DefaultCompactionCoordinatorConfig()

	if cfg.MaxConcurrency <= 0 {
		t.Error("expected positive max concurrency")
	}

	if cfg.MaxRetries < 0 {
		t.Error("expected non-negative max retries")
	}
}

// TestMetricsCollector tests metrics collection.
func TestMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector()

	collector.RecordTasksPlanned(10)
	collector.AddBytesRead(1000)
	collector.AddBytesWritten(500)
	collector.IncrementConflictCount()

	metrics := collector.GetMetrics()

	if metrics.TasksPlanned != 10 {
		t.Errorf("expected 10 tasks planned, got %d", metrics.TasksPlanned)
	}

	if metrics.TotalBytesRead != 1000 {
		t.Errorf("expected 1000 bytes read, got %d", metrics.TotalBytesRead)
	}

	if metrics.TotalBytesWritten != 500 {
		t.Errorf("expected 500 bytes written, got %d", metrics.TotalBytesWritten)
	}

	if metrics.ConflictCount != 1 {
		t.Errorf("expected 1 conflict, got %d", metrics.ConflictCount)
	}

	// Check compression ratio
	expectedRatio := 0.5 // 500/1000
	if metrics.CompressionRatio != expectedRatio {
		t.Errorf("expected compression ratio %f, got %f", expectedRatio, metrics.CompressionRatio)
	}
}

// TestEstimateTaskDuration tests task duration estimation.
func TestEstimateTaskDuration(t *testing.T) {
	planner := NewCompactionPlanner(DefaultCompactionPlannerConfig())

	task := &CompactionTask{
		ID:           0,
		TotalBytes:   100 * 1024 * 1024, // 100MB
		TotalRows:    100000,
		HasDeletions: false,
	}

	duration := planner.EstimateTaskDuration(task)
	if duration <= 0 {
		t.Error("expected positive duration estimate")
	}

	// Task with deletions should take longer
	taskWithDeletions := &CompactionTask{
		ID:           1,
		TotalBytes:   100 * 1024 * 1024,
		TotalRows:    100000,
		HasDeletions: true,
	}

	durationWithDeletions := planner.EstimateTaskDuration(taskWithDeletions)
	if durationWithDeletions <= duration {
		t.Error("task with deletions should have longer duration estimate")
	}
}

// TestFragmentSize tests fragment size calculation.
func TestFragmentSize(t *testing.T) {
	frag := &DataFragment{
		Id: 0,
		Files: []*DataFile{
			{Path: "a.bin", FileSizeBytes: 100},
			{Path: "b.bin", FileSizeBytes: 200},
		},
	}

	size := fragmentSize(frag)
	if size != 300 {
		t.Errorf("expected size 300, got %d", size)
	}

	// Empty fragment
	emptyFrag := &DataFragment{Id: 1}
	if fragmentSize(emptyFrag) != 0 {
		t.Error("expected 0 size for empty fragment")
	}
}

// TestCloneDataFragment tests fragment cloning.
func TestCloneDataFragment(t *testing.T) {
	original := &DataFragment{
		Id:           42,
		PhysicalRows: 1000,
		Files: []*DataFile{
			{Path: "test.bin", FileSizeBytes: 1024},
		},
	}

	cloned := CloneDataFragment(original)

	if cloned.Id != original.Id {
		t.Error("clone should have same ID")
	}

	if cloned.PhysicalRows != original.PhysicalRows {
		t.Error("clone should have same row count")
	}

	// Modify clone shouldn't affect original
	cloned.Id = 100
	if original.Id == 100 {
		t.Error("modifying clone should not affect original")
	}

	// Nil handling
	if CloneDataFragment(nil) != nil {
		t.Error("cloning nil should return nil")
	}
}
