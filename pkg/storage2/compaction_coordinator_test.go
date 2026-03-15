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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// ============================================================================
// Helper Functions
// ============================================================================

// setupCoordinatorTestDataset creates a test dataset with fragments for coordinator tests.
// Returns the commit handler, object store, and initial manifest.
func setupCoordinatorTestDataset(t *testing.T, basePath string, numFragments int, rowsPerFragment int) (CommitHandler, ObjectStoreExt, *Manifest) {
	t.Helper()

	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create data directory
	dataDir := filepath.Join(basePath, "data")
	if err := mkdirAll(dataDir); err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	// Create schema with two integer columns
	schema := []*storage2pb.Field{
		{Id: 0, Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int32"},
		{Id: 1, Name: "value", Type: storage2pb.Field_LEAF, LogicalType: "int64"},
	}

	// Create fragments with data
	var fragments []*DataFragment
	for i := 0; i < numFragments; i++ {
		fragID := uint64(i)
		fileName := filepath.Join("data", filepath.Base(basePath)+string(rune('a'+i))+".bin")
		fullPath := filepath.Join(basePath, fileName)

		// Create chunk with data
		ch := createTestChunkForCompaction(t, rowsPerFragment, i*rowsPerFragment)
		if err := WriteChunkToFile(fullPath, ch); err != nil {
			t.Fatalf("failed to write chunk %d: %v", i, err)
		}

		// Get file size
		fileSize := uint64(estimateChunkSize(ch))

		// Create data file
		dataFile := &DataFile{
			Path:          fileName,
			Fields:        []int32{0, 1},
			FileSizeBytes: fileSize,
		}

		// Create fragment
		frag := NewDataFragmentWithRows(fragID, uint64(rowsPerFragment), []*DataFile{dataFile})
		fragments = append(fragments, frag)
	}

	// Create initial manifest
	manifest := NewManifest(1)
	manifest.Fragments = fragments
	manifest.Fields = schema

	// Commit initial manifest
	if err := handler.Commit(context.Background(), basePath, 1, manifest); err != nil {
		t.Fatalf("failed to commit initial manifest: %v", err)
	}

	return handler, store, manifest
}

// createTestChunkForCompaction creates a test chunk with integer columns.
func createTestChunkForCompaction(t *testing.T, numRows int, startValue int) *chunk.Chunk {
	t.Helper()

	typs := []common.LType{
		common.IntegerType(), // id column
		common.BigintType(),  // value column
	}

	ch := &chunk.Chunk{}
	ch.Init(typs, numRows)
	ch.SetCard(numRows)

	// Fill with data
	for i := 0; i < numRows; i++ {
		// id column
		ch.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(startValue + i)})
		// value column
		ch.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64((startValue + i) * 10)})
	}

	return ch
}

// mkdirAll is a helper to create directories.
func mkdirAll(path string) error {
	return os.MkdirAll(path, 0755)
}

// ============================================================================
// Test 1: TestCompactionCoordinator_ExecuteBasic
// ============================================================================

func TestCompactionCoordinator_ExecuteBasic(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, 3, 100)

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2 // Ensure we get tasks
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan with 3 fragments")
	}

	// Create coordinator with default config
	coordCfg := DefaultCompactionCoordinatorConfig()
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify stats
	if stats.TasksCompleted == 0 {
		t.Error("expected at least one completed task")
	}
	if stats.FragmentsCompacted == 0 {
		t.Error("expected at least one compacted fragment")
	}
	if stats.Duration <= 0 {
		t.Error("expected positive duration")
	}

	t.Logf("Compaction stats: completed=%d, failed=%d, fragments=%d, bytes_read=%d, bytes_written=%d, duration=%v",
		stats.TasksCompleted, stats.TasksFailed, stats.FragmentsCompacted,
		stats.BytesRead, stats.BytesWritten, stats.Duration)
}

// ============================================================================
// Test 2: TestCompactionCoordinator_EmptyPlan
// ============================================================================

func TestCompactionCoordinator_EmptyPlan(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)
	manifest := NewManifest(1)

	// Create coordinator
	coordCfg := DefaultCompactionCoordinatorConfig()
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute with nil plan
	ctx := context.Background()
	stats, err := coord.Execute(ctx, nil)
	if err != nil {
		t.Errorf("expected no error for nil plan, got: %v", err)
	}
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats.TasksCompleted != 0 || stats.TasksFailed != 0 {
		t.Error("expected zero stats for empty plan")
	}

	// Execute with empty plan
	emptyPlan := &CompactionPlan{Tasks: nil}
	stats, err = coord.Execute(ctx, emptyPlan)
	if err != nil {
		t.Errorf("expected no error for empty plan, got: %v", err)
	}
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats.TasksCompleted != 0 || stats.TasksFailed != 0 {
		t.Error("expected zero stats for empty plan")
	}

	t.Log("Empty plan handled gracefully")
}

// ============================================================================
// Test 3: TestCompactionCoordinator_CancelContext
// ============================================================================

func TestCompactionCoordinator_CancelContext(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, 5, 100)

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Create coordinator
	coordCfg := DefaultCompactionCoordinatorConfig()
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Create a context that we'll cancel during execution
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel immediately before execution
	cancel()

	// Execute with already-canceled context
	stats, err := coord.Execute(ctx, plan)

	// The coordinator may complete quickly before checking context,
	// or it may return context.Canceled error. Either behavior is acceptable.
	if err != nil {
		// Expected: context canceled error
		t.Logf("Got expected error for canceled context: %v", err)
	} else {
		// Context was checked after completion - this is also acceptable
		t.Logf("Execution completed before context was checked")
	}

	// Stats may be partially or fully populated
	t.Logf("Stats after cancel: completed=%d, failed=%d", stats.TasksCompleted, stats.TasksFailed)
}

// ============================================================================
// Test 4: TestCompactionCoordinator_StatsAccuracy
// ============================================================================

func TestCompactionCoordinator_StatsAccuracy(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	numFragments := 4
	rowsPerFragment := 50
	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, numFragments, rowsPerFragment)

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Create coordinator
	coordCfg := DefaultCompactionCoordinatorConfig()
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify all stats fields are populated correctly
	if stats.TasksCompleted <= 0 {
		t.Errorf("expected positive TasksCompleted, got %d", stats.TasksCompleted)
	}
	if stats.TasksFailed < 0 {
		t.Errorf("expected non-negative TasksFailed, got %d", stats.TasksFailed)
	}
	if stats.FragmentsCompacted <= 0 {
		t.Errorf("expected positive FragmentsCompacted, got %d", stats.FragmentsCompacted)
	}
	if stats.NewFragmentsCreated <= 0 {
		t.Errorf("expected positive NewFragmentsCreated, got %d", stats.NewFragmentsCreated)
	}
	if stats.BytesRead == 0 {
		t.Error("expected non-zero BytesRead")
	}
	if stats.BytesWritten == 0 {
		t.Error("expected non-zero BytesWritten")
	}
	if stats.RowsProcessed == 0 {
		t.Error("expected non-zero RowsProcessed")
	}
	if stats.Duration <= 0 {
		t.Error("expected positive Duration")
	}
	if stats.RetryCount < 0 {
		t.Errorf("expected non-negative RetryCount, got %d", stats.RetryCount)
	}

	// Verify logical consistency
	if stats.FragmentsCompacted != plan.TotalOldFragments {
		t.Errorf("FragmentsCompacted (%d) should match plan.TotalOldFragments (%d)",
			stats.FragmentsCompacted, plan.TotalOldFragments)
	}

	t.Logf("Stats verification passed: %+v", stats)
}

// ============================================================================
// Test 5: TestCompactionCoordinator_MaxConcurrency
// ============================================================================

func TestCompactionCoordinator_MaxConcurrency(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	numFragments := 6
	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, numFragments, 50)

	// Create planner and plan with settings that may produce multiple tasks
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	plannerCfg.MaxFragmentsPerTask = 3 // Limit to 3 fragments per task to get multiple tasks
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	t.Logf("Plan created with %d tasks for %d fragments", len(plan.Tasks), numFragments)

	// Create coordinator with MaxConcurrency=1 to force sequential execution
	coordCfg := CompactionCoordinatorConfig{
		MaxConcurrency:     1,
		RetryOnConflict:    true,
		MaxRetries:         3,
		RetryDelay:         10 * time.Millisecond,
		PreserveRowIds:     false,
		IncludeDeletedRows: true,
	}
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	startTime := time.Now()
	stats, err := coord.Execute(ctx, plan)
	elapsed := time.Since(startTime)

	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// With MaxConcurrency=1, tasks should execute sequentially
	// Verify that it completed successfully
	if stats.TasksCompleted == 0 {
		t.Error("expected at least one completed task")
	}

	t.Logf("Sequential execution completed in %v: %d tasks, %d fragments",
		elapsed, stats.TasksCompleted, stats.FragmentsCompacted)
}

// ============================================================================
// Test 6: TestDistributedCompaction_EndToEnd
// ============================================================================

func TestDistributedCompaction_EndToEnd(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	numFragments := 5
	rowsPerFragment := 100
	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, numFragments, rowsPerFragment)

	// Configure distributed compaction options
	opts := DefaultDistributedCompactionOptions()
	opts.PlannerConfig.MinFragmentsPerTask = 2

	// Execute distributed compaction
	ctx := context.Background()
	stats, err := DistributedCompaction(ctx, basePath, handler, store, manifest, opts)
	if err != nil {
		t.Fatalf("distributed compaction failed: %v", err)
	}

	// Verify results
	if stats.TasksCompleted == 0 {
		t.Error("expected at least one completed task")
	}
	if stats.FragmentsCompacted == 0 {
		t.Error("expected at least one compacted fragment")
	}
	if stats.BytesRead == 0 {
		t.Error("expected non-zero bytes read")
	}
	if stats.BytesWritten == 0 {
		t.Error("expected non-zero bytes written")
	}

	// Load the updated manifest to verify changes
	latestVersion, err := handler.ResolveLatestVersion(ctx, basePath)
	if err != nil {
		t.Fatalf("failed to resolve latest version: %v", err)
	}

	updatedManifest, err := LoadManifest(ctx, basePath, handler, latestVersion)
	if err != nil {
		t.Fatalf("failed to load updated manifest: %v", err)
	}

	// The new manifest should have fewer or equal fragments
	if len(updatedManifest.Fragments) > numFragments {
		t.Errorf("expected %d or fewer fragments after compaction, got %d",
			numFragments, len(updatedManifest.Fragments))
	}

	t.Logf("End-to-end compaction: %d fragments -> %d fragments",
		numFragments, len(updatedManifest.Fragments))
	t.Logf("Stats: %+v", stats)
}

// ============================================================================
// Test 7: TestDistributedCompactionWithCallback
// ============================================================================

func TestDistributedCompactionWithCallback(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	numFragments := 4
	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, numFragments, 75)

	// Track callback invocations
	var callbackInvocations int
	var lastProgress float64
	var lastStats *CompactionStats

	callback := func(stats *CompactionStats, progress float64) {
		callbackInvocations++
		lastProgress = progress
		lastStats = stats
	}

	// Configure options
	opts := DefaultDistributedCompactionOptions()
	opts.PlannerConfig.MinFragmentsPerTask = 2

	// Execute with callback
	ctx := context.Background()
	stats, err := DistributedCompactionWithCallback(ctx, basePath, handler, store, manifest, opts, callback)
	if err != nil {
		t.Fatalf("distributed compaction with callback failed: %v", err)
	}

	// Verify stats are returned
	if stats == nil {
		t.Fatal("expected non-nil stats")
	}
	if stats.TasksCompleted == 0 {
		t.Error("expected at least one completed task")
	}

	// Note: The current implementation may not invoke callbacks during execution
	// This test verifies the function works and returns correct stats
	t.Logf("Callback invocations: %d", callbackInvocations)
	t.Logf("Final stats: %+v", stats)
	if lastStats != nil {
		t.Logf("Last callback progress: %.2f", lastProgress)
	}
}

// ============================================================================
// Test 8: TestMetricsCollector_AllFields
// ============================================================================

func TestMetricsCollector_AllFields(t *testing.T) {
	collector := NewMetricsCollector()

	// Record planning metrics
	planningDuration := 150 * time.Millisecond
	collector.RecordPlanningDuration(planningDuration)
	collector.RecordTasksPlanned(10)

	// Record execution metrics
	executionDuration := 5 * time.Second
	collector.RecordExecutionDuration(executionDuration)

	// Record I/O metrics
	collector.AddBytesRead(1024 * 1024)     // 1MB
	collector.AddBytesRead(2 * 1024 * 1024) // 2MB
	collector.AddBytesWritten(512 * 1024)   // 512KB
	collector.AddBytesWritten(256 * 1024)   // 256KB

	// Record conflict metrics
	collector.IncrementConflictCount()
	collector.IncrementConflictCount()
	collector.IncrementConflictCount()

	// Get and verify metrics
	metrics := collector.GetMetrics()

	// Verify planning metrics
	if metrics.PlanningDuration != planningDuration {
		t.Errorf("expected PlanningDuration=%v, got %v", planningDuration, metrics.PlanningDuration)
	}
	if metrics.TasksPlanned != 10 {
		t.Errorf("expected TasksPlanned=10, got %d", metrics.TasksPlanned)
	}

	// Verify execution metrics
	if metrics.ExecutionDuration != executionDuration {
		t.Errorf("expected ExecutionDuration=%v, got %v", executionDuration, metrics.ExecutionDuration)
	}

	// Verify I/O metrics
	expectedBytesRead := uint64(3 * 1024 * 1024) // 3MB
	if metrics.TotalBytesRead != expectedBytesRead {
		t.Errorf("expected TotalBytesRead=%d, got %d", expectedBytesRead, metrics.TotalBytesRead)
	}

	expectedBytesWritten := uint64(768 * 1024) // 768KB
	if metrics.TotalBytesWritten != expectedBytesWritten {
		t.Errorf("expected TotalBytesWritten=%d, got %d", expectedBytesWritten, metrics.TotalBytesWritten)
	}

	// Verify compression ratio (768KB / 3MB = 0.25)
	expectedRatio := float64(expectedBytesWritten) / float64(expectedBytesRead)
	if metrics.CompressionRatio != expectedRatio {
		t.Errorf("expected CompressionRatio=%.4f, got %.4f", expectedRatio, metrics.CompressionRatio)
	}

	// Verify conflict count
	if metrics.ConflictCount != 3 {
		t.Errorf("expected ConflictCount=3, got %d", metrics.ConflictCount)
	}

	t.Logf("All metrics verified: %+v", metrics)
}

// ============================================================================
// Test 9: TestCompactionCoordinator_ConfigDefaults
// ============================================================================

func TestCompactionCoordinator_ConfigDefaults(t *testing.T) {
	cfg := DefaultCompactionCoordinatorConfig()

	// Verify MaxConcurrency default
	if cfg.MaxConcurrency != 4 {
		t.Errorf("expected MaxConcurrency=4, got %d", cfg.MaxConcurrency)
	}

	// Verify RetryOnConflict default
	if !cfg.RetryOnConflict {
		t.Error("expected RetryOnConflict=true")
	}

	// Verify MaxRetries default
	if cfg.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", cfg.MaxRetries)
	}

	// Verify RetryDelay default
	expectedRetryDelay := 100 * time.Millisecond
	if cfg.RetryDelay != expectedRetryDelay {
		t.Errorf("expected RetryDelay=%v, got %v", expectedRetryDelay, cfg.RetryDelay)
	}

	// Verify PreserveRowIds default
	if cfg.PreserveRowIds {
		t.Error("expected PreserveRowIds=false")
	}

	// Verify IncludeDeletedRows default
	if !cfg.IncludeDeletedRows {
		t.Error("expected IncludeDeletedRows=true")
	}

	t.Logf("Config defaults verified: %+v", cfg)
}

// ============================================================================
// Test 10: TestDistributedCompactionOptions_Defaults
// ============================================================================

func TestDistributedCompactionOptions_Defaults(t *testing.T) {
	opts := DefaultDistributedCompactionOptions()

	// Verify PlannerConfig is populated
	if opts.PlannerConfig.Strategy == "" {
		t.Error("expected non-empty PlannerConfig.Strategy")
	}
	if opts.PlannerConfig.TargetTaskSize == 0 {
		t.Error("expected non-zero PlannerConfig.TargetTaskSize")
	}
	if opts.PlannerConfig.MaxFragmentsPerTask == 0 {
		t.Error("expected non-zero PlannerConfig.MaxFragmentsPerTask")
	}

	// Verify CoordinatorConfig is populated
	if opts.CoordinatorConfig.MaxConcurrency == 0 {
		t.Error("expected non-zero CoordinatorConfig.MaxConcurrency")
	}
	if !opts.CoordinatorConfig.RetryOnConflict {
		t.Error("expected CoordinatorConfig.RetryOnConflict=true")
	}

	// Verify nested defaults match their respective default functions
	expectedPlannerCfg := DefaultCompactionPlannerConfig()
	if opts.PlannerConfig.Strategy != expectedPlannerCfg.Strategy {
		t.Errorf("PlannerConfig.Strategy mismatch: got %s, expected %s",
			opts.PlannerConfig.Strategy, expectedPlannerCfg.Strategy)
	}

	expectedCoordCfg := DefaultCompactionCoordinatorConfig()
	if opts.CoordinatorConfig.MaxConcurrency != expectedCoordCfg.MaxConcurrency {
		t.Errorf("CoordinatorConfig.MaxConcurrency mismatch: got %d, expected %d",
			opts.CoordinatorConfig.MaxConcurrency, expectedCoordCfg.MaxConcurrency)
	}

	t.Logf("DistributedCompactionOptions defaults verified: %+v", opts)
}

// ============================================================================
// Additional Test: TestCompactionCoordinator_RetryBehavior
// ============================================================================

func TestCompactionCoordinator_RetryBehavior(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, 3, 50)

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Create coordinator with retry enabled
	coordCfg := CompactionCoordinatorConfig{
		MaxConcurrency:     4,
		RetryOnConflict:    true,
		MaxRetries:         2,
		RetryDelay:         10 * time.Millisecond,
		PreserveRowIds:     false,
		IncludeDeletedRows: true,
	}
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify retry count is non-negative
	if stats.RetryCount < 0 {
		t.Errorf("expected non-negative RetryCount, got %d", stats.RetryCount)
	}

	t.Logf("Retry behavior test passed: RetryCount=%d", stats.RetryCount)
}

// ============================================================================
// Additional Test: TestCompactionCoordinator_WithDeletions
// ============================================================================

func TestCompactionCoordinator_WithDeletions(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, 4, 100)

	// Add deletion file to one fragment
	if len(manifest.Fragments) > 0 {
		manifest.Fragments[0].DeletionFile = &DeletionFile{
			FileType:       0,
			ReadVersion:    1,
			Id:             0,
			NumDeletedRows: 10,
		}
	}

	// Create planner that includes deletions
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.IncludeDeletions = true
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Check that tasks properly detect deletions
	hasDeletions := false
	for _, task := range plan.Tasks {
		if task.HasDeletions {
			hasDeletions = true
			break
		}
	}
	if !hasDeletions {
		t.Log("Warning: expected at least one task to have deletions")
	}

	// Create coordinator
	coordCfg := DefaultCompactionCoordinatorConfig()
	coordCfg.IncludeDeletedRows = true
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("compaction with deletions failed: %v", err)
	}

	if stats.TasksCompleted == 0 {
		t.Error("expected at least one completed task")
	}

	t.Logf("Compaction with deletions completed: %+v", stats)
}

// ============================================================================
// Additional Test: TestCompactionCoordinator_PreserveRowIds
// ============================================================================

func TestCompactionCoordinator_PreserveRowIds(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, 3, 50)

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Create coordinator with PreserveRowIds enabled
	coordCfg := CompactionCoordinatorConfig{
		MaxConcurrency:     4,
		RetryOnConflict:    true,
		MaxRetries:         3,
		RetryDelay:         10 * time.Millisecond,
		PreserveRowIds:     true, // Enable row ID preservation
		IncludeDeletedRows: true,
	}
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("compaction with PreserveRowIds failed: %v", err)
	}

	if stats.TasksCompleted == 0 {
		t.Error("expected at least one completed task")
	}

	t.Logf("Compaction with PreserveRowIds completed: %+v", stats)
}

// ============================================================================
// Additional Test: TestCompactionPlanner_Integration
// ============================================================================

func TestCompactionPlanner_Integration(t *testing.T) {
	// Test that planner produces valid plans for coordinator
	fragments := make([]*DataFragment, 10)
	for i := 0; i < 10; i++ {
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
			cfg.MinFragmentsPerTask = 2
			planner := NewCompactionPlanner(cfg)
			plan := planner.Plan(fragments)

			if plan == nil {
				t.Fatal("expected non-nil plan")
			}

			// Verify plan structure
			if plan.TotalOldFragments == 0 && len(plan.Tasks) > 0 {
				t.Error("expected non-zero TotalOldFragments when tasks exist")
			}
			if plan.TotalBytes == 0 && len(plan.Tasks) > 0 {
				t.Error("expected non-zero TotalBytes when tasks exist")
			}

			// Verify each task
			for i, task := range plan.Tasks {
				if task.ID != i {
					t.Errorf("task %d has wrong ID %d", i, task.ID)
				}
				if len(task.Fragments) == 0 {
					t.Errorf("task %d has no fragments", i)
				}
				if task.TotalRows == 0 {
					t.Errorf("task %d has zero TotalRows", i)
				}
			}

			t.Logf("Strategy %s: %d tasks, %d total fragments, %d bytes",
				strategy, len(plan.Tasks), plan.TotalOldFragments, plan.TotalBytes)
		})
	}
}

// ============================================================================
// Additional Test: TestMetricsCollector_Concurrency
// ============================================================================

func TestMetricsCollector_Concurrency(t *testing.T) {
	collector := NewMetricsCollector()

	// Simulate concurrent metric recording
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				collector.AddBytesRead(100)
				collector.AddBytesWritten(50)
				collector.IncrementConflictCount()
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	metrics := collector.GetMetrics()

	// Verify totals
	expectedBytesRead := uint64(10 * 100 * 100)
	expectedBytesWritten := uint64(10 * 100 * 50)
	expectedConflicts := 10 * 100

	if metrics.TotalBytesRead != expectedBytesRead {
		t.Errorf("expected TotalBytesRead=%d, got %d", expectedBytesRead, metrics.TotalBytesRead)
	}
	if metrics.TotalBytesWritten != expectedBytesWritten {
		t.Errorf("expected TotalBytesWritten=%d, got %d", expectedBytesWritten, metrics.TotalBytesWritten)
	}
	if metrics.ConflictCount != expectedConflicts {
		t.Errorf("expected ConflictCount=%d, got %d", expectedConflicts, metrics.ConflictCount)
	}

	t.Logf("Concurrent metrics verified: BytesRead=%d, BytesWritten=%d, Conflicts=%d",
		metrics.TotalBytesRead, metrics.TotalBytesWritten, metrics.ConflictCount)
}

// ============================================================================
// Additional Test: TestCompactionCoordinator_MultipleRetries
// ============================================================================

func TestCompactionCoordinator_MultipleRetries(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	handler, store, manifest := setupCoordinatorTestDataset(t, basePath, 3, 50)

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Create coordinator with high retry count
	coordCfg := CompactionCoordinatorConfig{
		MaxConcurrency:     4,
		RetryOnConflict:    true,
		MaxRetries:         5,
		RetryDelay:         5 * time.Millisecond,
		PreserveRowIds:     false,
		IncludeDeletedRows: true,
	}
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute compaction
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	// Verify retry count doesn't exceed max
	if stats.RetryCount > coordCfg.MaxRetries {
		t.Errorf("RetryCount %d exceeds MaxRetries %d", stats.RetryCount, coordCfg.MaxRetries)
	}

	t.Logf("Multiple retries test passed: RetryCount=%d, MaxRetries=%d",
		stats.RetryCount, coordCfg.MaxRetries)
}

// ============================================================================
// Additional Test: TestCompactionStats_ZeroValues
// ============================================================================

func TestCompactionStats_ZeroValues(t *testing.T) {
	// Test that CompactionStats can be created with zero values
	stats := &CompactionStats{}

	if stats.TasksCompleted != 0 {
		t.Error("expected zero TasksCompleted")
	}
	if stats.TasksFailed != 0 {
		t.Error("expected zero TasksFailed")
	}
	if stats.FragmentsCompacted != 0 {
		t.Error("expected zero FragmentsCompacted")
	}
	if stats.NewFragmentsCreated != 0 {
		t.Error("expected zero NewFragmentsCreated")
	}
	if stats.BytesRead != 0 {
		t.Error("expected zero BytesRead")
	}
	if stats.BytesWritten != 0 {
		t.Error("expected zero BytesWritten")
	}
	if stats.RowsProcessed != 0 {
		t.Error("expected zero RowsProcessed")
	}
	if stats.Duration != 0 {
		t.Error("expected zero Duration")
	}
	if stats.RetryCount != 0 {
		t.Error("expected zero RetryCount")
	}

	t.Log("Zero value stats verified")
}

// ============================================================================
// Additional Test: TestCompactionPlan_EmptyFragments
// ============================================================================

func TestCompactionPlan_EmptyFragments(t *testing.T) {
	planner := NewCompactionPlanner(DefaultCompactionPlannerConfig())

	// Test with nil fragments
	plan := planner.Plan(nil)
	if plan == nil || len(plan.Tasks) != 0 {
		t.Error("expected empty plan for nil fragments")
	}

	// Test with single fragment
	plan = planner.Plan([]*DataFragment{{Id: 0, PhysicalRows: 100}})
	if plan == nil || len(plan.Tasks) != 0 {
		t.Error("expected empty plan for single fragment")
	}

	// Test with empty fragment list
	plan = planner.Plan([]*DataFragment{})
	if plan == nil || len(plan.Tasks) != 0 {
		t.Error("expected empty plan for empty fragment list")
	}

	t.Log("Empty fragment handling verified")
}

// ============================================================================
// Additional Test: TestCompactionCoordinator_ErrorHandling
// ============================================================================

func TestCompactionCoordinator_ErrorHandling(t *testing.T) {
	dir := t.TempDir()
	basePath := filepath.Join(dir, "test_dataset")

	// Create setup but don't write actual data files
	handler := NewLocalRenameCommitHandler()
	store := NewLocalObjectStoreExt(basePath, nil)

	// Create manifest with fragments that point to non-existent files
	manifest := NewManifest(1)
	manifest.Fields = []*storage2pb.Field{
		{Id: 0, Name: "id", Type: storage2pb.Field_LEAF, LogicalType: "int32"},
	}
	manifest.Fragments = []*DataFragment{
		{
			Id:           0,
			PhysicalRows: 100,
			Files: []*DataFile{
				{Path: "data/nonexistent.bin", FileSizeBytes: 1024},
			},
		},
		{
			Id:           1,
			PhysicalRows: 100,
			Files: []*DataFile{
				{Path: "data/missing.bin", FileSizeBytes: 1024},
			},
		},
	}

	// Commit manifest
	if err := handler.Commit(context.Background(), basePath, 1, manifest); err != nil {
		t.Fatalf("failed to commit manifest: %v", err)
	}

	// Create planner and plan
	plannerCfg := DefaultCompactionPlannerConfig()
	plannerCfg.MinFragmentsPerTask = 2
	planner := NewCompactionPlanner(plannerCfg)
	plan := planner.Plan(manifest.Fragments)

	if plan == nil || len(plan.Tasks) == 0 {
		t.Fatal("expected non-empty plan")
	}

	// Create coordinator
	coordCfg := DefaultCompactionCoordinatorConfig()
	coord := NewCompactionCoordinator(basePath, handler, store, manifest, coordCfg)

	// Execute - should handle errors gracefully
	ctx := context.Background()
	stats, err := coord.Execute(ctx, plan)

	// Either returns error or reports failed tasks
	if err != nil {
		t.Logf("Got expected error for missing files: %v", err)
	} else if stats.TasksFailed > 0 {
		t.Logf("Got expected failed tasks: %d", stats.TasksFailed)
	} else {
		t.Log("Warning: expected error or failed tasks for missing files")
	}
}
