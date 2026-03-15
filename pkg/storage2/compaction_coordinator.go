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
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// ErrCompactionConflict indicates a conflict during compaction commit.
var ErrCompactionConflict = errors.New("compaction conflict: fragments modified during compaction")

// CompactionCoordinatorConfig configures the compaction coordinator.
type CompactionCoordinatorConfig struct {
	// MaxConcurrency is the maximum number of concurrent compaction tasks.
	MaxConcurrency int
	// RetryOnConflict whether to retry on commit conflict.
	RetryOnConflict bool
	// MaxRetries is the maximum number of retries on conflict.
	MaxRetries int
	// RetryDelay is the delay between retries.
	RetryDelay time.Duration
	// PreserveRowIds whether to preserve row IDs during compaction.
	PreserveRowIds bool
	// IncludeDeletedRows whether to include deleted rows.
	IncludeDeletedRows bool
}

// DefaultCompactionCoordinatorConfig returns a default configuration.
func DefaultCompactionCoordinatorConfig() CompactionCoordinatorConfig {
	return CompactionCoordinatorConfig{
		MaxConcurrency:     4,
		RetryOnConflict:    true,
		MaxRetries:         3,
		RetryDelay:         100 * time.Millisecond,
		PreserveRowIds:     false,
		IncludeDeletedRows: true,
	}
}

// CompactionCoordinator coordinates distributed compaction.
type CompactionCoordinator struct {
	config   CompactionCoordinatorConfig
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
	manifest *Manifest
	mu       sync.RWMutex
}

// NewCompactionCoordinator creates a new compaction coordinator.
func NewCompactionCoordinator(
	basePath string,
	handler CommitHandler,
	store ObjectStoreExt,
	manifest *Manifest,
	config CompactionCoordinatorConfig,
) *CompactionCoordinator {
	return &CompactionCoordinator{
		config:   config,
		basePath: basePath,
		handler:  handler,
		store:    store,
		manifest: manifest,
	}
}

// CompactionStats contains statistics about a compaction operation.
type CompactionStats struct {
	// TasksCompleted is the number of tasks that completed successfully.
	TasksCompleted int
	// TasksFailed is the number of tasks that failed.
	TasksFailed int
	// FragmentsCompacted is the number of fragments that were compacted.
	FragmentsCompacted int
	// NewFragmentsCreated is the number of new fragments created.
	NewFragmentsCreated int
	// BytesRead is the total bytes read during compaction.
	BytesRead uint64
	// BytesWritten is the total bytes written during compaction.
	BytesWritten uint64
	// RowsProcessed is the total rows processed.
	RowsProcessed uint64
	// Duration is the total duration of the compaction operation.
	Duration time.Duration
	// RetryCount is the number of retries due to conflicts.
	RetryCount int
}

// Execute executes the compaction plan.
func (c *CompactionCoordinator) Execute(ctx context.Context, plan *CompactionPlan) (*CompactionStats, error) {
	if plan == nil || len(plan.Tasks) == 0 {
		return &CompactionStats{}, nil
	}

	startTime := time.Now()
	stats := &CompactionStats{}

	for retry := 0; retry <= c.config.MaxRetries; retry++ {
		if retry > 0 {
			stats.RetryCount++
			select {
			case <-ctx.Done():
				return stats, ctx.Err()
			case <-time.After(c.config.RetryDelay):
			}

			// Refresh manifest for retry
			if err := c.refreshManifest(ctx); err != nil {
				return stats, fmt.Errorf("failed to refresh manifest: %w", err)
			}
		}

		// Execute compaction
		results, err := c.executeParallel(ctx, plan)
		if err != nil {
			return stats, err
		}

		// Collect results
		var successResults []*CompactionResult
		for _, result := range results {
			if result.Error != nil {
				stats.TasksFailed++
			} else {
				stats.TasksCompleted++
				stats.BytesRead += result.BytesRead
				stats.BytesWritten += result.BytesWritten
				stats.RowsProcessed += result.RowsProcessed
				successResults = append(successResults, result)
			}
		}

		if len(successResults) == 0 {
			stats.Duration = time.Since(startTime)
			return stats, fmt.Errorf("all compaction tasks failed")
		}

		// Commit results
		err = c.commit(ctx, successResults)
		if err == nil {
			// Success
			for _, result := range successResults {
				stats.FragmentsCompacted += len(result.OldFragments)
				stats.NewFragmentsCreated += len(result.NewFragments)
			}
			stats.Duration = time.Since(startTime)
			return stats, nil
		}

		if errors.Is(err, ErrConflict) && c.config.RetryOnConflict {
			// Retry
			continue
		}

		stats.Duration = time.Since(startTime)
		return stats, err
	}

	stats.Duration = time.Since(startTime)
	return stats, fmt.Errorf("compaction failed after %d retries", c.config.MaxRetries+1)
}

// executeParallel executes compaction tasks in parallel.
func (c *CompactionCoordinator) executeParallel(ctx context.Context, plan *CompactionPlan) ([]*CompactionResult, error) {
	pool := NewWorkerPool(c.config.MaxConcurrency, c.store)
	tracker := NewProgressTracker(len(plan.Tasks), plan.TotalBytes)

	workerConfig := &WorkerConfig{
		BasePath:           c.basePath,
		Handler:            c.handler,
		Schema:             c.manifest.Fields,
		PreserveRowIds:     c.config.PreserveRowIds,
		IncludeDeletedRows: c.config.IncludeDeletedRows,
	}

	results := make([]*CompactionResult, len(plan.Tasks))
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i, task := range plan.Tasks {
		wg.Add(1)
		go func(idx int, t *CompactionTask) {
			defer wg.Done()

			// Acquire worker
			worker, err := pool.Acquire(ctx)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				results[idx] = &CompactionResult{TaskID: t.ID, Error: err}
				return
			}
			defer pool.Release()

			// Execute task
			result, err := worker.Execute(ctx, t, workerConfig)
			if err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				results[idx] = &CompactionResult{TaskID: t.ID, Error: err}
				tracker.MarkTaskComplete(t.ID, t.TotalBytes)
				return
			}

			results[idx] = result
			tracker.MarkTaskComplete(t.ID, result.BytesRead)
		}(i, task)
	}

	wg.Wait()

	return results, nil // Return all results, including failures
}

// commit commits the compaction results as a Rewrite transaction.
func (c *CompactionCoordinator) commit(ctx context.Context, results []*CompactionResult) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(results) == 0 {
		return nil
	}

	// Build rewrite groups
	var groups []*storage2pb.Transaction_Rewrite_RewriteGroup
	for _, result := range results {
		if result.Error != nil || len(result.OldFragments) == 0 {
			continue
		}

		group := &storage2pb.Transaction_Rewrite_RewriteGroup{
			OldFragments: result.OldFragments,
			NewFragments: result.NewFragments,
		}
		groups = append(groups, group)
	}

	if len(groups) == 0 {
		return nil
	}

	// Create transaction
	uuid := generateCompactionUUID()
	txn := &Transaction{
		ReadVersion: c.manifest.Version,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_Rewrite_{
			Rewrite: &storage2pb.Transaction_Rewrite{
				Groups: groups,
			},
		},
	}

	// Commit
	if err := CommitTransaction(ctx, c.basePath, c.handler, txn); err != nil {
		return err
	}

	// Update manifest
	return c.refreshManifest(ctx)
}

// refreshManifest refreshes the current manifest.
func (c *CompactionCoordinator) refreshManifest(ctx context.Context) error {
	latest, err := c.handler.ResolveLatestVersion(ctx, c.basePath)
	if err != nil {
		return err
	}

	manifest, err := LoadManifest(ctx, c.basePath, c.handler, latest)
	if err != nil {
		return err
	}

	c.manifest = manifest
	return nil
}

// generateCompactionUUID generates a UUID for compaction transactions.
func generateCompactionUUID() string {
	bytes := make([]byte, 16)
	_, _ = rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// DistributedCompactionOptions are options for distributed compaction.
type DistributedCompactionOptions struct {
	// PlannerConfig configures the compaction planner.
	PlannerConfig CompactionPlannerConfig
	// CoordinatorConfig configures the coordinator.
	CoordinatorConfig CompactionCoordinatorConfig
}

// DefaultDistributedCompactionOptions returns default options.
func DefaultDistributedCompactionOptions() DistributedCompactionOptions {
	return DistributedCompactionOptions{
		PlannerConfig:     DefaultCompactionPlannerConfig(),
		CoordinatorConfig: DefaultCompactionCoordinatorConfig(),
	}
}

// DistributedCompaction performs distributed compaction on a dataset.
func DistributedCompaction(
	ctx context.Context,
	basePath string,
	handler CommitHandler,
	store ObjectStoreExt,
	manifest *Manifest,
	opts DistributedCompactionOptions,
) (*CompactionStats, error) {
	// Create planner
	planner := NewCompactionPlanner(opts.PlannerConfig)

	// Plan compaction
	plan := planner.Plan(manifest.Fragments)
	if plan == nil || len(plan.Tasks) == 0 {
		return &CompactionStats{}, nil
	}

	// Create coordinator
	coordinator := NewCompactionCoordinator(
		basePath,
		handler,
		store,
		manifest,
		opts.CoordinatorConfig,
	)

	// Execute
	return coordinator.Execute(ctx, plan)
}

// CompactionCallback is called during compaction with progress updates.
type CompactionCallback func(stats *CompactionStats, progress float64)

// DistributedCompactionWithCallback performs distributed compaction with progress callbacks.
func DistributedCompactionWithCallback(
	ctx context.Context,
	basePath string,
	handler CommitHandler,
	store ObjectStoreExt,
	manifest *Manifest,
	opts DistributedCompactionOptions,
	callback CompactionCallback,
) (*CompactionStats, error) {
	// Create planner
	planner := NewCompactionPlanner(opts.PlannerConfig)

	// Plan compaction
	plan := planner.Plan(manifest.Fragments)
	if plan == nil || len(plan.Tasks) == 0 {
		return &CompactionStats{}, nil
	}

	// Create coordinator
	coordinator := NewCompactionCoordinator(
		basePath,
		handler,
		store,
		manifest,
		opts.CoordinatorConfig,
	)

	// Execute with progress tracking
	// For now, just execute normally
	// In production, would integrate callback into executeParallel
	return coordinator.Execute(ctx, plan)
}

// CompactionMetrics contains detailed metrics for compaction operations.
type CompactionMetrics struct {
	// Planning metrics
	PlanningDuration  time.Duration
	TasksPlanned      int
	FragmentsAnalyzed int

	// Execution metrics
	ExecutionDuration time.Duration
	WorkersUsed       int
	PeakConcurrency   int

	// I/O metrics
	TotalBytesRead    uint64
	TotalBytesWritten uint64
	CompressionRatio  float64

	// Transaction metrics
	CommitDuration time.Duration
	ConflictCount  int
	RetryCount     int
}

// CollectCompactionMetrics collects metrics during a compaction operation.
type MetricsCollector struct {
	metrics CompactionMetrics
	mu      sync.Mutex
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// RecordPlanningDuration records the planning duration.
func (m *MetricsCollector) RecordPlanningDuration(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics.PlanningDuration = d
}

// RecordTasksPlanned records the number of tasks planned.
func (m *MetricsCollector) RecordTasksPlanned(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics.TasksPlanned = count
}

// RecordExecutionDuration records the execution duration.
func (m *MetricsCollector) RecordExecutionDuration(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics.ExecutionDuration = d
}

// AddBytesRead adds to the bytes read counter.
func (m *MetricsCollector) AddBytesRead(bytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics.TotalBytesRead += bytes
}

// AddBytesWritten adds to the bytes written counter.
func (m *MetricsCollector) AddBytesWritten(bytes uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics.TotalBytesWritten += bytes
}

// IncrementConflictCount increments the conflict counter.
func (m *MetricsCollector) IncrementConflictCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics.ConflictCount++
}

// GetMetrics returns a copy of the current metrics.
func (m *MetricsCollector) GetMetrics() CompactionMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Calculate compression ratio
	if m.metrics.TotalBytesRead > 0 {
		m.metrics.CompressionRatio = float64(m.metrics.TotalBytesWritten) / float64(m.metrics.TotalBytesRead)
	}

	return m.metrics
}
