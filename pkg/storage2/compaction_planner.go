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
	"sort"
)

// CompactionStrategy defines how fragments should be grouped for compaction.
type CompactionStrategy string

const (
	// StrategySize groups fragments by target size.
	StrategySize CompactionStrategy = "size"
	// StrategyCount groups fragments by count.
	StrategyCount CompactionStrategy = "count"
	// StrategyHybrid uses both size and count constraints.
	StrategyHybrid CompactionStrategy = "hybrid"
	// StrategyBinPacking uses bin-packing for optimal grouping.
	StrategyBinPacking CompactionStrategy = "bin_packing"
)

// CompactionPlan represents a plan for distributed compaction.
type CompactionPlan struct {
	// Tasks are the individual compaction tasks that can be executed in parallel.
	Tasks []*CompactionTask
	// TotalOldFragments is the total number of fragments to be compacted.
	TotalOldFragments int
	// EstimatedNewFragments is the estimated number of new fragments.
	EstimatedNewFragments int
	// TotalBytes is the total size of all fragments to be compacted.
	TotalBytes uint64
}

// CompactionTask represents a single compaction task that can be executed by a worker.
type CompactionTask struct {
	// ID is the unique identifier for this task.
	ID int
	// Fragments are the fragments to be compacted in this task.
	Fragments []*DataFragment
	// TotalBytes is the total size of fragments in this task.
	TotalBytes uint64
	// TotalRows is the total number of rows in this task.
	TotalRows uint64
	// HasDeletions indicates if any fragment has deletion files.
	HasDeletions bool
}

// CompactionPlannerConfig configures the compaction planner.
type CompactionPlannerConfig struct {
	// Strategy determines how fragments are grouped.
	Strategy CompactionStrategy
	// TargetTaskSize is the target size in bytes for each task.
	TargetTaskSize uint64
	// MaxTaskSize is the maximum size in bytes for each task.
	MaxTaskSize uint64
	// MinTaskSize is the minimum size in bytes for each task.
	MinTaskSize uint64
	// MaxFragmentsPerTask limits the number of fragments per task.
	MaxFragmentsPerTask int
	// MinFragmentsPerTask is the minimum number of fragments per task.
	MinFragmentsPerTask int
	// MaxTasks limits the total number of tasks (for parallelism control).
	MaxTasks int
	// IncludeDeletions whether to include fragments with deletions.
	IncludeDeletions bool
}

// DefaultCompactionPlannerConfig returns a default configuration.
func DefaultCompactionPlannerConfig() CompactionPlannerConfig {
	return CompactionPlannerConfig{
		Strategy:            StrategyHybrid,
		TargetTaskSize:      256 * 1024 * 1024,      // 256MB
		MaxTaskSize:         1 * 1024 * 1024 * 1024, // 1GB
		MinTaskSize:         32 * 1024 * 1024,       // 32MB
		MaxFragmentsPerTask: 50,
		MinFragmentsPerTask: 2,
		MaxTasks:            0, // unlimited
		IncludeDeletions:    true,
	}
}

// CompactionPlanner plans how to partition fragments for distributed compaction.
type CompactionPlanner struct {
	config CompactionPlannerConfig
}

// NewCompactionPlanner creates a new compaction planner with the given config.
func NewCompactionPlanner(config CompactionPlannerConfig) *CompactionPlanner {
	return &CompactionPlanner{config: config}
}

// Plan creates a compaction plan for the given fragments.
func (p *CompactionPlanner) Plan(fragments []*DataFragment) *CompactionPlan {
	if len(fragments) < 2 {
		return &CompactionPlan{Tasks: nil}
	}

	// Filter fragments based on config
	filtered := p.filterFragments(fragments)
	if len(filtered) < 2 {
		return &CompactionPlan{Tasks: nil}
	}

	var tasks []*CompactionTask
	switch p.config.Strategy {
	case StrategySize:
		tasks = p.planBySize(filtered)
	case StrategyCount:
		tasks = p.planByCount(filtered)
	case StrategyBinPacking:
		tasks = p.planByBinPacking(filtered)
	default: // StrategyHybrid
		tasks = p.planHybrid(filtered)
	}

	// Apply max tasks limit
	if p.config.MaxTasks > 0 && len(tasks) > p.config.MaxTasks {
		tasks = p.mergeTasks(tasks, p.config.MaxTasks)
	}

	plan := &CompactionPlan{
		Tasks:             tasks,
		TotalOldFragments: 0,
		TotalBytes:        0,
	}

	for _, task := range tasks {
		plan.TotalOldFragments += len(task.Fragments)
		plan.TotalBytes += task.TotalBytes
	}
	plan.EstimatedNewFragments = len(tasks)

	return plan
}

// filterFragments filters fragments based on configuration.
func (p *CompactionPlanner) filterFragments(fragments []*DataFragment) []*DataFragment {
	var result []*DataFragment
	for _, frag := range fragments {
		hasDeletions := frag.DeletionFile != nil
		if hasDeletions && !p.config.IncludeDeletions {
			continue
		}
		result = append(result, frag)
	}
	return result
}

// fragmentSize returns the total size of a fragment.
func fragmentSize(frag *DataFragment) uint64 {
	var size uint64
	for _, file := range frag.Files {
		size += file.FileSizeBytes
	}
	return size
}

// planBySize groups fragments by target size.
func (p *CompactionPlanner) planBySize(fragments []*DataFragment) []*CompactionTask {
	// Sort fragments by size (largest first) for better bin packing
	sorted := make([]*DataFragment, len(fragments))
	copy(sorted, fragments)
	sort.Slice(sorted, func(i, j int) bool {
		return fragmentSize(sorted[i]) > fragmentSize(sorted[j])
	})

	var tasks []*CompactionTask
	var currentFrags []*DataFragment
	var currentSize, currentRows uint64
	var hasDeletions bool
	taskID := 0

	for _, frag := range sorted {
		fragSize := fragmentSize(frag)

		// Check if adding this fragment would exceed limits
		wouldExceedSize := currentSize+fragSize > p.config.MaxTaskSize
		wouldExceedCount := len(currentFrags) >= p.config.MaxFragmentsPerTask

		// Flush current task if limits exceeded
		if len(currentFrags) > 0 && (wouldExceedSize || wouldExceedCount) {
			// Only create task if we have minimum fragments and size
			if len(currentFrags) >= p.config.MinFragmentsPerTask || currentSize >= p.config.MinTaskSize {
				tasks = append(tasks, &CompactionTask{
					ID:           taskID,
					Fragments:    currentFrags,
					TotalBytes:   currentSize,
					TotalRows:    currentRows,
					HasDeletions: hasDeletions,
				})
				taskID++
			}
			currentFrags = nil
			currentSize = 0
			currentRows = 0
			hasDeletions = false
		}

		currentFrags = append(currentFrags, frag)
		currentSize += fragSize
		currentRows += frag.PhysicalRows
		if frag.DeletionFile != nil {
			hasDeletions = true
		}

		// Check if we reached target size
		if currentSize >= p.config.TargetTaskSize && len(currentFrags) >= p.config.MinFragmentsPerTask {
			tasks = append(tasks, &CompactionTask{
				ID:           taskID,
				Fragments:    currentFrags,
				TotalBytes:   currentSize,
				TotalRows:    currentRows,
				HasDeletions: hasDeletions,
			})
			taskID++
			currentFrags = nil
			currentSize = 0
			currentRows = 0
			hasDeletions = false
		}
	}

	// Handle remaining fragments
	if len(currentFrags) >= p.config.MinFragmentsPerTask {
		tasks = append(tasks, &CompactionTask{
			ID:           taskID,
			Fragments:    currentFrags,
			TotalBytes:   currentSize,
			TotalRows:    currentRows,
			HasDeletions: hasDeletions,
		})
	}

	return tasks
}

// planByCount groups fragments by count.
func (p *CompactionPlanner) planByCount(fragments []*DataFragment) []*CompactionTask {
	maxPerTask := p.config.MaxFragmentsPerTask
	if maxPerTask <= 0 {
		maxPerTask = 10
	}
	minPerTask := p.config.MinFragmentsPerTask
	if minPerTask <= 0 {
		minPerTask = 2
	}

	var tasks []*CompactionTask
	taskID := 0

	for i := 0; i < len(fragments); i += maxPerTask {
		end := i + maxPerTask
		if end > len(fragments) {
			end = len(fragments)
		}
		batch := fragments[i:end]

		if len(batch) < minPerTask {
			continue
		}

		var totalBytes, totalRows uint64
		var hasDeletions bool
		for _, frag := range batch {
			totalBytes += fragmentSize(frag)
			totalRows += frag.PhysicalRows
			if frag.DeletionFile != nil {
				hasDeletions = true
			}
		}

		tasks = append(tasks, &CompactionTask{
			ID:           taskID,
			Fragments:    batch,
			TotalBytes:   totalBytes,
			TotalRows:    totalRows,
			HasDeletions: hasDeletions,
		})
		taskID++
	}

	return tasks
}

// planHybrid combines size and count constraints.
func (p *CompactionPlanner) planHybrid(fragments []*DataFragment) []*CompactionTask {
	// Sort by size ascending to group small fragments together
	sorted := make([]*DataFragment, len(fragments))
	copy(sorted, fragments)
	sort.Slice(sorted, func(i, j int) bool {
		return fragmentSize(sorted[i]) < fragmentSize(sorted[j])
	})

	var tasks []*CompactionTask
	var currentFrags []*DataFragment
	var currentSize, currentRows uint64
	var hasDeletions bool
	taskID := 0

	for _, frag := range sorted {
		fragSize := fragmentSize(frag)

		// Skip oversized fragments
		if fragSize > p.config.MaxTaskSize {
			continue
		}

		// Check constraints
		wouldExceedSize := currentSize+fragSize > p.config.MaxTaskSize
		wouldExceedCount := len(currentFrags) >= p.config.MaxFragmentsPerTask
		meetsMinimum := len(currentFrags) >= p.config.MinFragmentsPerTask ||
			currentSize >= p.config.MinTaskSize

		// Flush if we exceed limits and meet minimums
		if len(currentFrags) > 0 && (wouldExceedSize || wouldExceedCount) && meetsMinimum {
			tasks = append(tasks, &CompactionTask{
				ID:           taskID,
				Fragments:    currentFrags,
				TotalBytes:   currentSize,
				TotalRows:    currentRows,
				HasDeletions: hasDeletions,
			})
			taskID++
			currentFrags = nil
			currentSize = 0
			currentRows = 0
			hasDeletions = false
		}

		currentFrags = append(currentFrags, frag)
		currentSize += fragSize
		currentRows += frag.PhysicalRows
		if frag.DeletionFile != nil {
			hasDeletions = true
		}

		// Flush if we hit target and meet minimums
		if currentSize >= p.config.TargetTaskSize && len(currentFrags) >= p.config.MinFragmentsPerTask {
			tasks = append(tasks, &CompactionTask{
				ID:           taskID,
				Fragments:    currentFrags,
				TotalBytes:   currentSize,
				TotalRows:    currentRows,
				HasDeletions: hasDeletions,
			})
			taskID++
			currentFrags = nil
			currentSize = 0
			currentRows = 0
			hasDeletions = false
		}
	}

	// Handle remaining
	if len(currentFrags) >= p.config.MinFragmentsPerTask {
		tasks = append(tasks, &CompactionTask{
			ID:           taskID,
			Fragments:    currentFrags,
			TotalBytes:   currentSize,
			TotalRows:    currentRows,
			HasDeletions: hasDeletions,
		})
	}

	return tasks
}

// planByBinPacking uses first-fit decreasing bin packing.
func (p *CompactionPlanner) planByBinPacking(fragments []*DataFragment) []*CompactionTask {
	// Sort fragments by size descending (FFD algorithm)
	sorted := make([]*DataFragment, len(fragments))
	copy(sorted, fragments)
	sort.Slice(sorted, func(i, j int) bool {
		return fragmentSize(sorted[i]) > fragmentSize(sorted[j])
	})

	type bin struct {
		fragments    []*DataFragment
		totalBytes   uint64
		totalRows    uint64
		hasDeletions bool
	}

	var bins []*bin

	for _, frag := range sorted {
		fragSize := fragmentSize(frag)

		// Skip if fragment is too large
		if fragSize > p.config.MaxTaskSize {
			continue
		}

		// Try to fit in existing bin
		placed := false
		for _, b := range bins {
			wouldExceedSize := b.totalBytes+fragSize > p.config.TargetTaskSize
			wouldExceedCount := len(b.fragments) >= p.config.MaxFragmentsPerTask

			if !wouldExceedSize && !wouldExceedCount {
				b.fragments = append(b.fragments, frag)
				b.totalBytes += fragSize
				b.totalRows += frag.PhysicalRows
				if frag.DeletionFile != nil {
					b.hasDeletions = true
				}
				placed = true
				break
			}
		}

		// Create new bin if not placed
		if !placed {
			hasDeletions := frag.DeletionFile != nil
			bins = append(bins, &bin{
				fragments:    []*DataFragment{frag},
				totalBytes:   fragSize,
				totalRows:    frag.PhysicalRows,
				hasDeletions: hasDeletions,
			})
		}
	}

	// Convert bins to tasks, filtering by minimum constraints
	var tasks []*CompactionTask
	for i, b := range bins {
		if len(b.fragments) >= p.config.MinFragmentsPerTask {
			tasks = append(tasks, &CompactionTask{
				ID:           i,
				Fragments:    b.fragments,
				TotalBytes:   b.totalBytes,
				TotalRows:    b.totalRows,
				HasDeletions: b.hasDeletions,
			})
		}
	}

	// Reassign IDs
	for i := range tasks {
		tasks[i].ID = i
	}

	return tasks
}

// mergeTasks merges tasks to reduce total count.
func (p *CompactionPlanner) mergeTasks(tasks []*CompactionTask, maxTasks int) []*CompactionTask {
	if len(tasks) <= maxTasks {
		return tasks
	}

	// Sort tasks by size ascending
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].TotalBytes < tasks[j].TotalBytes
	})

	// Merge smallest tasks first
	for len(tasks) > maxTasks {
		// Merge the two smallest tasks
		t1 := tasks[0]
		t2 := tasks[1]

		merged := &CompactionTask{
			ID:           t1.ID,
			Fragments:    append(t1.Fragments, t2.Fragments...),
			TotalBytes:   t1.TotalBytes + t2.TotalBytes,
			TotalRows:    t1.TotalRows + t2.TotalRows,
			HasDeletions: t1.HasDeletions || t2.HasDeletions,
		}

		// Remove first two, add merged
		tasks = append([]*CompactionTask{merged}, tasks[2:]...)

		// Re-sort
		sort.Slice(tasks, func(i, j int) bool {
			return tasks[i].TotalBytes < tasks[j].TotalBytes
		})
	}

	// Reassign IDs
	for i := range tasks {
		tasks[i].ID = i
	}

	return tasks
}

// EstimateTaskDuration estimates the duration of a task based on size and rows.
// Returns estimated milliseconds.
func (p *CompactionPlanner) EstimateTaskDuration(task *CompactionTask) int64 {
	// Simple estimation: ~100MB/s throughput
	bytesPerMs := uint64(100 * 1024) // 100KB/ms = 100MB/s
	durationByBytes := int64(task.TotalBytes / bytesPerMs)

	// Row processing overhead: ~1ms per 10000 rows
	durationByRows := int64(task.TotalRows / 10000)

	// Additional overhead for deletions
	var deletionOverhead int64
	if task.HasDeletions {
		deletionOverhead = 500 // 500ms overhead for deletion handling
	}

	return durationByBytes + durationByRows + deletionOverhead + 100 // +100ms base overhead
}
