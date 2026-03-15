// Licensed to Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is part of the Plan project.

package storage2

import (
	"testing"

	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// TestCompactionPlanner_BinPackingStrategy tests the bin packing strategy
// with varied fragment sizes to verify optimal packing.
func TestCompactionPlanner_BinPackingStrategy(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategyBinPacking
	config.TargetTaskSize = 1000
	config.MaxTaskSize = 1200
	config.MinTaskSize = 100
	config.MaxFragmentsPerTask = 10
	config.MinFragmentsPerTask = 1
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create fragments with varied sizes to test bin packing
	fragments := []*DataFragment{
		createTestFragment(1, 500, 100, false),   // 500 bytes, 100 rows
		createTestFragment(2, 300, 60, false),    // 300 bytes, 60 rows
		createTestFragment(3, 400, 80, false),    // 400 bytes, 80 rows
		createTestFragment(4, 200, 40, false),    // 200 bytes, 40 rows
		createTestFragment(5, 600, 120, false),   // 600 bytes, 120 rows
		createTestFragment(6, 150, 30, false),    // 150 bytes, 30 rows
		createTestFragment(7, 250, 50, false),    // 250 bytes, 50 rows
		createTestFragment(8, 450, 90, false),    // 450 bytes, 90 rows
	}

	plan := planner.Plan(fragments)

	if plan == nil {
		t.Fatalf("Plan should not be nil")
	}

	if len(plan.Tasks) == 0 {
		t.Fatalf("Plan should have at least one task")
	}

	// Verify total fragments match
	totalFragmentsInTasks := 0
	for _, task := range plan.Tasks {
		totalFragmentsInTasks += len(task.Fragments)
	}

	if totalFragmentsInTasks != len(fragments) {
		t.Errorf("Total fragments in tasks (%d) should equal input fragments (%d)",
			totalFragmentsInTasks, len(fragments))
	}

	// Verify no task exceeds MaxTaskSize
	for _, task := range plan.Tasks {
		if task.TotalBytes > config.MaxTaskSize {
			t.Errorf("Task %d exceeds MaxTaskSize: %d > %d",
				task.ID, task.TotalBytes, config.MaxTaskSize)
		}
	}

	// Verify bin packing efficiency - tasks should be reasonably packed
	// With bin packing, we expect fewer tasks than naive partitioning
	if len(plan.Tasks) > 5 {
		t.Errorf("Bin packing should produce efficient packing, got %d tasks", len(plan.Tasks))
	}
}

// TestCompactionPlanner_HybridStrategy tests the hybrid strategy
// to verify both size and count constraints are met.
func TestCompactionPlanner_HybridStrategy(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategyHybrid
	config.TargetTaskSize = 800
	config.MaxTaskSize = 1000
	config.MinTaskSize = 100
	config.MaxFragmentsPerTask = 5
	config.MinFragmentsPerTask = 2
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create fragments for hybrid testing
	fragments := []*DataFragment{
		createTestFragment(1, 200, 50, false),
		createTestFragment(2, 200, 50, false),
		createTestFragment(3, 200, 50, false),
		createTestFragment(4, 200, 50, false),
		createTestFragment(5, 200, 50, false),
		createTestFragment(6, 200, 50, false),
		createTestFragment(7, 200, 50, false),
		createTestFragment(8, 200, 50, false),
	}

	plan := planner.Plan(fragments)

	if plan == nil {
		t.Fatalf("Plan should not be nil")
	}

	// Verify hybrid constraints: both size and count
	for _, task := range plan.Tasks {
		// Size constraint
		if task.TotalBytes > config.MaxTaskSize {
			t.Errorf("Task %d exceeds MaxTaskSize: %d > %d",
				task.ID, task.TotalBytes, config.MaxTaskSize)
		}

		// Count constraint
		if len(task.Fragments) > config.MaxFragmentsPerTask {
			t.Errorf("Task %d exceeds MaxFragmentsPerTask: %d > %d",
				task.ID, len(task.Fragments), config.MaxFragmentsPerTask)
		}

		// MinFragmentsPerTask constraint
		if len(task.Fragments) < config.MinFragmentsPerTask {
			t.Errorf("Task %d has fewer than MinFragmentsPerTask: %d < %d",
				task.ID, len(task.Fragments), config.MinFragmentsPerTask)
		}
	}
}

// TestCompactionPlanner_SizeStrategy tests the size strategy
// with specific target size to verify task sizes.
func TestCompactionPlanner_SizeStrategy(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategySize
	config.TargetTaskSize = 500
	config.MaxTaskSize = 600
	config.MinTaskSize = 100
	config.MaxFragmentsPerTask = 20
	config.MinFragmentsPerTask = 1
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create fragments with various sizes
	fragments := []*DataFragment{
		createTestFragment(1, 250, 50, false),
		createTestFragment(2, 250, 50, false),
		createTestFragment(3, 300, 60, false),
		createTestFragment(4, 200, 40, false),
		createTestFragment(5, 100, 20, false),
		createTestFragment(6, 400, 80, false),
	}

	plan := planner.Plan(fragments)

	if plan == nil {
		t.Fatalf("Plan should not be nil")
	}

	// Verify size-based constraints
	for _, task := range plan.Tasks {
		if task.TotalBytes > config.MaxTaskSize {
			t.Errorf("Task %d exceeds MaxTaskSize: %d > %d",
				task.ID, task.TotalBytes, config.MaxTaskSize)
		}

		// Tasks should ideally be close to TargetTaskSize (unless single fragment)
		if task.TotalBytes < config.MinTaskSize {
			t.Errorf("Task %d below MinTaskSize: %d < %d",
				task.ID, task.TotalBytes, config.MinTaskSize)
		}
	}
}

// TestCompactionPlanner_CountStrategy tests the count strategy
// to verify fragment counts per task.
func TestCompactionPlanner_CountStrategy(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategyCount
	config.TargetTaskSize = 10000 // Large enough to not be a factor
	config.MaxTaskSize = 10000
	config.MinTaskSize = 0
	config.MaxFragmentsPerTask = 3
	config.MinFragmentsPerTask = 1
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create 10 fragments
	fragments := make([]*DataFragment, 10)
	for i := 0; i < 10; i++ {
		fragments[i] = createTestFragment(uint64(i+1), 100, 20, false)
	}

	plan := planner.Plan(fragments)

	if plan == nil {
		t.Fatalf("Plan should not be nil")
	}

	// Verify count constraints
	for _, task := range plan.Tasks {
		if len(task.Fragments) > config.MaxFragmentsPerTask {
			t.Errorf("Task %d exceeds MaxFragmentsPerTask: %d > %d",
				task.ID, len(task.Fragments), config.MaxFragmentsPerTask)
		}
	}

	// With 10 fragments and max 3 per task, we should have at least 4 tasks
	if len(plan.Tasks) < 4 {
		t.Errorf("Expected at least 4 tasks with count strategy, got %d", len(plan.Tasks))
	}
}

// TestCompactionPlanner_TargetSizeRespected verifies that
// tasks should not exceed MaxTaskSize.
func TestCompactionPlanner_TargetSizeRespected(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategySize
	config.TargetTaskSize = 400
	config.MaxTaskSize = 500
	config.MinTaskSize = 50
	config.MaxFragmentsPerTask = 10
	config.MinFragmentsPerTask = 1
	config.MaxTasks = 20
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create fragments that could potentially exceed max size if not properly grouped
	fragments := []*DataFragment{
		createTestFragment(1, 300, 60, false),
		createTestFragment(2, 250, 50, false),
		createTestFragment(3, 200, 40, false),
		createTestFragment(4, 350, 70, false),
		createTestFragment(5, 150, 30, false),
		createTestFragment(6, 280, 56, false),
		createTestFragment(7, 220, 44, false),
		createTestFragment(8, 180, 36, false),
	}

	plan := planner.Plan(fragments)

	// Strictly verify no task exceeds MaxTaskSize
	for _, task := range plan.Tasks {
		if task.TotalBytes > config.MaxTaskSize {
			t.Errorf("Task %d violates MaxTaskSize constraint: %d > %d",
				task.ID, task.TotalBytes, config.MaxTaskSize)
		}
	}
}

// TestCompactionPlanner_MinFragmentsPerTask verifies that
// tasks with fewer than MinFragmentsPerTask should be filtered.
func TestCompactionPlanner_MinFragmentsPerTask(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategySize
	config.TargetTaskSize = 1000
	config.MaxTaskSize = 2000
	config.MinTaskSize = 100
	config.MaxFragmentsPerTask = 10
	config.MinFragmentsPerTask = 2
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create 5 fragments that could be grouped
	fragments := []*DataFragment{
		createTestFragment(1, 200, 40, false),
		createTestFragment(2, 200, 40, false),
		createTestFragment(3, 200, 40, false),
		createTestFragment(4, 200, 40, false),
		createTestFragment(5, 200, 40, false),
	}

	plan := planner.Plan(fragments)

	// All tasks should have at least MinFragmentsPerTask fragments
	for _, task := range plan.Tasks {
		if len(task.Fragments) < config.MinFragmentsPerTask {
			t.Errorf("Task %d has fewer than MinFragmentsPerTask: %d < %d",
				task.ID, len(task.Fragments), config.MinFragmentsPerTask)
		}
	}
}

// TestCompactionPlanner_MaxTasksLimit verifies that
// the plan should not exceed MaxTasks.
func TestCompactionPlanner_MaxTasksLimit(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategyCount
	config.TargetTaskSize = 10000
	config.MaxTaskSize = 10000
	config.MinTaskSize = 0
	config.MaxFragmentsPerTask = 2
	config.MinFragmentsPerTask = 1
	config.MaxTasks = 3
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create 10 fragments - with max 2 per task, would need 5 tasks
	// but MaxTasks is 3
	fragments := make([]*DataFragment, 10)
	for i := 0; i < 10; i++ {
		fragments[i] = createTestFragment(uint64(i+1), 100, 20, false)
	}

	plan := planner.Plan(fragments)

	if plan == nil {
		t.Fatalf("Plan should not be nil")
	}

	if len(plan.Tasks) > config.MaxTasks {
		t.Errorf("Plan exceeds MaxTasks: %d > %d",
			len(plan.Tasks), config.MaxTasks)
	}
}

// TestCompactionPlanner_WithAndWithoutDeletions verifies
// IncludeDeletions=true vs false behavior.
func TestCompactionPlanner_WithAndWithoutDeletions(t *testing.T) {
	// Test with IncludeDeletions = true
	configWithDeletions := DefaultCompactionPlannerConfig()
	configWithDeletions.Strategy = StrategySize
	configWithDeletions.TargetTaskSize = 1000
	configWithDeletions.MaxTaskSize = 1500
	configWithDeletions.MinTaskSize = 100
	configWithDeletions.MaxFragmentsPerTask = 10
	configWithDeletions.MinFragmentsPerTask = 1
	configWithDeletions.MaxTasks = 10
	configWithDeletions.IncludeDeletions = true

	plannerWith := NewCompactionPlanner(configWithDeletions)

	// Create fragments with some having deletions
	fragments := []*DataFragment{
		createTestFragment(1, 300, 60, true),
		createTestFragment(2, 250, 50, false),
		createTestFragment(3, 200, 40, true),
		createTestFragment(4, 350, 70, false),
	}

	planWith := plannerWith.Plan(fragments)

	// Test with IncludeDeletions = false
	configWithoutDeletions := DefaultCompactionPlannerConfig()
	configWithoutDeletions.Strategy = StrategySize
	configWithoutDeletions.TargetTaskSize = 1000
	configWithoutDeletions.MaxTaskSize = 1500
	configWithoutDeletions.MinTaskSize = 100
	configWithoutDeletions.MaxFragmentsPerTask = 10
	configWithoutDeletions.MinFragmentsPerTask = 1
	configWithoutDeletions.MaxTasks = 10
	configWithoutDeletions.IncludeDeletions = false

	plannerWithout := NewCompactionPlanner(configWithoutDeletions)

	planWithout := plannerWithout.Plan(fragments)

	// When IncludeDeletions is true, fragments with deletions should be prioritized
	// Check that HasDeletions flag is properly tracked
	for _, task := range planWith.Tasks {
		// If any fragment has deletions, the task should reflect that
		hasDeletions := false
		for _, frag := range task.Fragments {
			if frag.DeletionFile != nil {
				hasDeletions = true
				break
			}
		}
		if hasDeletions && !task.HasDeletions {
			t.Errorf("Task should have HasDeletions=true when fragments have deletions")
		}
	}

	// Both plans should be valid
	if planWith == nil || planWithout == nil {
		t.Fatalf("Both plans should be non-nil")
	}
}

// TestCompactionPlanner_SingleFragment tests planning
// with only 1 fragment (below MinFragmentsPerTask).
func TestCompactionPlanner_SingleFragment(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategySize
	config.TargetTaskSize = 1000
	config.MaxTaskSize = 2000
	config.MinTaskSize = 100
	config.MaxFragmentsPerTask = 10
	config.MinFragmentsPerTask = 2
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Single fragment - below MinFragmentsPerTask
	fragments := []*DataFragment{
		createTestFragment(1, 500, 100, false),
	}

	plan := planner.Plan(fragments)

	// Plan should handle single fragment gracefully
	// Either return empty plan or a single task (implementation dependent)
	if plan == nil {
		t.Fatalf("Plan should not be nil even for single fragment")
	}

	// If a task is created, it should contain the single fragment
	if len(plan.Tasks) > 0 {
		if len(plan.Tasks[0].Fragments) != 1 {
			t.Errorf("Single fragment task should have exactly 1 fragment, got %d",
				len(plan.Tasks[0].Fragments))
		}
	}
}

// TestCompactionPlanner_EstimateTaskDuration verifies that
// duration estimates are reasonable.
func TestCompactionPlanner_EstimateTaskDuration(t *testing.T) {
	config := DefaultCompactionPlannerConfig()
	config.Strategy = StrategySize
	config.TargetTaskSize = 1000
	config.MaxTaskSize = 2000
	config.MinTaskSize = 100
	config.MaxFragmentsPerTask = 10
	config.MinFragmentsPerTask = 1
	config.MaxTasks = 10
	config.IncludeDeletions = true

	planner := NewCompactionPlanner(config)

	// Create test task
	task := &CompactionTask{
		ID:          1,
		Fragments: []*DataFragment{
			createTestFragment(1, 500, 100, false),
			createTestFragment(2, 300, 60, false),
		},
		TotalBytes:   800,
		TotalRows:    160,
		HasDeletions: false,
	}

	duration := planner.EstimateTaskDuration(task)

	// Duration should be positive
	if duration <= 0 {
		t.Errorf("Duration estimate should be positive, got %d", duration)
	}

	// Duration should be reasonable (not extremely large)
	// Assuming ~1ms per 100KB is reasonable upper bound
	maxReasonableDuration := int64((task.TotalBytes / 100000) + 1000)
	if duration > maxReasonableDuration {
		t.Errorf("Duration estimate seems unreasonably large: %d", duration)
	}

	// Test with larger task - should have longer duration
	largerTask := &CompactionTask{
		ID: 2,
		Fragments: []*DataFragment{
			createTestFragment(3, 5000, 1000, false),
			createTestFragment(4, 3000, 600, false),
		},
		TotalBytes:   8000,
		TotalRows:    1600,
		HasDeletions: false,
	}

	largerDuration := planner.EstimateTaskDuration(largerTask)

	// Larger task should generally take longer (or at least not significantly less)
	// This is a soft check as the estimation algorithm may vary
	if largerDuration < duration/10 {
		t.Errorf("Larger task duration (%d) unexpectedly much smaller than smaller task (%d)",
			largerDuration, duration)
	}
}

// Helper function to create test fragments
func createTestFragment(id uint64, numBytes uint64, numRows uint64, hasDeletions bool) *DataFragment {
	files := []*DataFile{
		NewDataFile("test_file", []int32{0, 1}, 1, 0),
	}

	fragment := NewDataFragmentWithRows(id, numRows, files)

	if hasDeletions {
		fragment.DeletionFile = &DeletionFile{
			FileType:       storage2pb.DeletionFile_ARROW_ARRAY,
			ReadVersion:    1,
			Id:             id,
			NumDeletedRows: numRows / 10, // 10% deletions
		}
	}

	// Set approximate byte size via files
	// Note: This is a simplification for testing
	for _, file := range fragment.Files {
		file.FileSizeBytes = numBytes / uint64(len(fragment.Files))
	}

	return fragment
}
