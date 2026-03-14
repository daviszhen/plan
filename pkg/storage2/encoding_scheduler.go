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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// ============================================================================
// Encoding Scheduler Configuration
// ============================================================================

// EncodingSchedulerConfig holds configuration for the encoding scheduler.
type EncodingSchedulerConfig struct {
	// MaxConcurrency is the maximum number of columns encoded in parallel.
	// Defaults to 4 if zero.
	MaxConcurrency int

	// MemoryBudget is the maximum bytes of encoded output held in memory
	// before back-pressure is applied. Zero means unlimited.
	MemoryBudget int64

	// AutoSelectEncoding enables automatic encoding selection per column.
	// When false, PlainEncoder is used for all columns.
	AutoSelectEncoding bool
}

// DefaultEncodingSchedulerConfig returns a sensible default configuration.
func DefaultEncodingSchedulerConfig() *EncodingSchedulerConfig {
	return &EncodingSchedulerConfig{
		MaxConcurrency:     4,
		MemoryBudget:       256 * 1024 * 1024, // 256 MB
		AutoSelectEncoding: true,
	}
}

// ============================================================================
// Encoding Task and Result
// ============================================================================

// EncodingTask describes a single column encoding job.
type EncodingTask struct {
	// ColumnIndex is the position of the column in the chunk.
	ColumnIndex int
	// Vec is the column vector to encode.
	Vec *chunk.Vector
	// NumRows is the number of valid rows in Vec.
	NumRows int
	// ForcedEncoding, if non-nil, overrides auto-selection.
	ForcedEncoding *EncodingType
}

// EncodingResult holds the output of encoding a single column.
type EncodingResult struct {
	// ColumnIndex is the position of the column that was encoded.
	ColumnIndex int
	// Page is the encoded page, nil when Error is set.
	Page *EncodedPage
	// Error is non-nil if encoding failed.
	Error error
}

// ============================================================================
// Encoding Progress
// ============================================================================

// EncodingProgress reports the progress of an encoding batch.
type EncodingProgress struct {
	TotalColumns     int
	CompletedColumns int32 // accessed atomically
	TotalRows        int64
	BytesEncoded     int64 // accessed atomically
}

// Completed returns the number of columns that have finished encoding.
func (p *EncodingProgress) Completed() int {
	return int(atomic.LoadInt32(&p.CompletedColumns))
}

// Fraction returns a value between 0.0 and 1.0 representing completion.
func (p *EncodingProgress) Fraction() float64 {
	total := p.TotalColumns
	if total == 0 {
		return 1.0
	}
	return float64(p.Completed()) / float64(total)
}

// EncodedBytes returns the total number of bytes produced so far.
func (p *EncodingProgress) EncodedBytes() int64 {
	return atomic.LoadInt64(&p.BytesEncoded)
}

// ============================================================================
// Memory Tracker
// ============================================================================

// memoryTracker tracks in-flight memory usage with optional back-pressure.
type memoryTracker struct {
	mu      sync.Mutex
	cond    *sync.Cond
	current int64
	budget  int64 // 0 means unlimited
}

func newMemoryTracker(budget int64) *memoryTracker {
	mt := &memoryTracker{budget: budget}
	mt.cond = sync.NewCond(&mt.mu)
	return mt
}

// reserve blocks until there is room for 'size' bytes, or ctx is cancelled.
// Returns false if the context was cancelled before reservation succeeded.
func (m *memoryTracker) reserve(ctx context.Context, size int64) bool {
	if m.budget <= 0 {
		// Unlimited: just track.
		m.mu.Lock()
		m.current += size
		m.mu.Unlock()
		return true
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for m.current+size > m.budget {
		// Check context before waiting.
		select {
		case <-ctx.Done():
			return false
		default:
		}
		// Wait for release to free up space; use a polling approach with cond.
		// We release the lock inside Wait.
		waitDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				m.cond.Broadcast()
			case <-waitDone:
			}
		}()
		m.cond.Wait()
		close(waitDone)
	}

	m.current += size
	return true
}

// release frees 'size' bytes and wakes any blocked reserve calls.
func (m *memoryTracker) release(size int64) {
	m.mu.Lock()
	m.current -= size
	if m.current < 0 {
		m.current = 0
	}
	m.mu.Unlock()
	m.cond.Broadcast()
}

// usage returns current tracked memory.
func (m *memoryTracker) usage() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.current
}

// ============================================================================
// Encoding Scheduler
// ============================================================================

// EncodingScheduler provides parallel, memory-bounded column encoding with
// progress tracking and cancellation support.
type EncodingScheduler struct {
	config  EncodingSchedulerConfig
	sem     *Semaphore
	memTrak *memoryTracker
}

// NewEncodingScheduler creates a scheduler with the given configuration.
// If config is nil, DefaultEncodingSchedulerConfig() is used.
func NewEncodingScheduler(config *EncodingSchedulerConfig) *EncodingScheduler {
	if config == nil {
		config = DefaultEncodingSchedulerConfig()
	}
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 4
	}
	return &EncodingScheduler{
		config:  *config,
		sem:     NewSemaphore(config.MaxConcurrency),
		memTrak: newMemoryTracker(config.MemoryBudget),
	}
}

// ScheduleTasks encodes multiple columns in parallel, respecting concurrency
// and memory limits. It returns results ordered by ColumnIndex.
// The caller may observe progress through the returned *EncodingProgress.
//
// Cancelling ctx will stop pending tasks; tasks already running will complete
// but subsequent tasks will not start.
func (s *EncodingScheduler) ScheduleTasks(ctx context.Context, tasks []EncodingTask) ([]EncodingResult, *EncodingProgress) {
	progress := &EncodingProgress{
		TotalColumns: len(tasks),
	}
	for _, t := range tasks {
		progress.TotalRows += int64(t.NumRows)
	}

	results := make([]EncodingResult, len(tasks))
	var wg sync.WaitGroup

	for i := range tasks {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			task := &tasks[idx]

			// Acquire concurrency slot.
			if err := s.sem.Acquire(ctx); err != nil {
				results[idx] = EncodingResult{
					ColumnIndex: task.ColumnIndex,
					Error:       fmt.Errorf("encoding scheduler: %w", err),
				}
				atomic.AddInt32(&progress.CompletedColumns, 1)
				return
			}
			defer s.sem.Release()

			// Check cancellation before encoding.
			select {
			case <-ctx.Done():
				results[idx] = EncodingResult{
					ColumnIndex: task.ColumnIndex,
					Error:       ctx.Err(),
				}
				atomic.AddInt32(&progress.CompletedColumns, 1)
				return
			default:
			}

			// Encode.
			page, err := s.encodeTask(task)
			if err != nil {
				results[idx] = EncodingResult{
					ColumnIndex: task.ColumnIndex,
					Error:       err,
				}
				atomic.AddInt32(&progress.CompletedColumns, 1)
				return
			}

			// Track memory for encoded page.
			pageSize := int64(len(page.Data) + len(page.DictData) + len(page.Validity))
			if !s.memTrak.reserve(ctx, pageSize) {
				results[idx] = EncodingResult{
					ColumnIndex: task.ColumnIndex,
					Error:       fmt.Errorf("encoding scheduler: memory reservation cancelled: %w", ctx.Err()),
				}
				atomic.AddInt32(&progress.CompletedColumns, 1)
				return
			}

			atomic.AddInt64(&progress.BytesEncoded, pageSize)
			results[idx] = EncodingResult{
				ColumnIndex: task.ColumnIndex,
				Page:        page,
			}
			atomic.AddInt32(&progress.CompletedColumns, 1)
		}(i)
	}

	wg.Wait()
	return results, progress
}

// encodeTask encodes a single column vector.
func (s *EncodingScheduler) encodeTask(task *EncodingTask) (*EncodedPage, error) {
	var encoder *LogicalColumnEncoder
	if task.ForcedEncoding != nil {
		encoder = NewLogicalColumnEncoderWithEncoding(*task.ForcedEncoding)
	} else if s.config.AutoSelectEncoding {
		encoder = NewLogicalColumnEncoder()
	} else {
		encoder = NewLogicalColumnEncoderWithEncoding(EncodingPlain)
	}
	return encoder.EncodeColumn(task.Vec, task.NumRows)
}

// ReleaseResult releases the memory tracked for a result's encoded page.
// The caller must call this for each result after consuming the page data.
func (s *EncodingScheduler) ReleaseResult(result *EncodingResult) {
	if result.Page == nil {
		return
	}
	pageSize := int64(len(result.Page.Data) + len(result.Page.DictData) + len(result.Page.Validity))
	s.memTrak.release(pageSize)
}

// MemoryUsage returns the current tracked memory usage in bytes.
func (s *EncodingScheduler) MemoryUsage() int64 {
	return s.memTrak.usage()
}

// ============================================================================
// Convenience: EncodeChunk — encode all columns of a Chunk in parallel
// ============================================================================

// EncodeChunk encodes every column of the given chunk in parallel.
// It returns the encoded pages indexed by column position.
func (s *EncodingScheduler) EncodeChunk(ctx context.Context, chk *chunk.Chunk) ([]EncodingResult, *EncodingProgress, error) {
	if chk == nil || chk.ColumnCount() == 0 {
		return nil, &EncodingProgress{}, nil
	}

	numCols := chk.ColumnCount()
	numRows := chk.Card()
	tasks := make([]EncodingTask, numCols)
	for i := 0; i < numCols; i++ {
		tasks[i] = EncodingTask{
			ColumnIndex: i,
			Vec:         chk.Data[i],
			NumRows:     numRows,
		}
	}

	results, progress := s.ScheduleTasks(ctx, tasks)

	// Check for any encoding errors.
	for i := range results {
		if results[i].Error != nil {
			return results, progress, fmt.Errorf("encoding column %d: %w", results[i].ColumnIndex, results[i].Error)
		}
	}
	return results, progress, nil
}

// ============================================================================
// Encoding Statistics
// ============================================================================

// EncodingStats summarizes the encoding work done by the scheduler.
type EncodingStats struct {
	TotalColumns   int
	TotalRows      int64
	TotalBytesIn   int64
	TotalBytesOut  int64
	EncodingCounts map[EncodingType]int
}

// CollectStats computes aggregate encoding statistics from results.
func CollectStats(results []EncodingResult) EncodingStats {
	stats := EncodingStats{
		EncodingCounts: make(map[EncodingType]int),
	}
	for _, r := range results {
		if r.Page == nil {
			continue
		}
		stats.TotalColumns++
		stats.TotalRows += int64(r.Page.NumRows)
		stats.TotalBytesOut += int64(len(r.Page.Data) + len(r.Page.DictData) + len(r.Page.Validity))
		stats.EncodingCounts[r.Page.Encoding]++
	}
	return stats
}

// ============================================================================
// Batch Encoder — high-level API for encoding multiple chunks
// ============================================================================

// BatchEncodingResult holds the result of encoding one chunk.
type BatchEncodingResult struct {
	ChunkIndex int
	Results    []EncodingResult
	Progress   *EncodingProgress
	Error      error
}

// EncodeBatch encodes multiple chunks sequentially, with each chunk's columns
// encoded in parallel. Context cancellation stops processing of remaining
// chunks.
func (s *EncodingScheduler) EncodeBatch(ctx context.Context, chunks []*chunk.Chunk) []BatchEncodingResult {
	batchResults := make([]BatchEncodingResult, len(chunks))

	for i, chk := range chunks {
		select {
		case <-ctx.Done():
			// Fill remaining with cancellation error.
			for j := i; j < len(chunks); j++ {
				batchResults[j] = BatchEncodingResult{
					ChunkIndex: j,
					Error:      ctx.Err(),
				}
			}
			return batchResults
		default:
		}

		results, progress, err := s.EncodeChunk(ctx, chk)
		batchResults[i] = BatchEncodingResult{
			ChunkIndex: i,
			Results:    results,
			Progress:   progress,
			Error:      err,
		}
	}
	return batchResults
}

// ============================================================================
// Encoding Plan — determines encoding strategy before execution
// ============================================================================

// ColumnEncodingPlan describes the planned encoding for a single column.
type ColumnEncodingPlan struct {
	ColumnIndex int
	ColumnType  common.LType
	Encoding    EncodingType
	Reason      string
}

// PlanChunkEncoding analyzes a chunk and returns the planned encoding for
// each column without actually encoding. Useful for previewing encoding
// decisions and estimated compression.
func PlanChunkEncoding(chk *chunk.Chunk, numRows int) []ColumnEncodingPlan {
	plans := make([]ColumnEncodingPlan, chk.ColumnCount())
	for i := 0; i < chk.ColumnCount(); i++ {
		vec := chk.Data[i]
		typ := vec.Typ()
		plans[i] = ColumnEncodingPlan{
			ColumnIndex: i,
			ColumnType:  typ,
		}

		switch typ.Id {
		case common.LTID_BOOLEAN:
			plans[i].Encoding = EncodingBitPacked
			plans[i].Reason = "boolean always bitpacked"

		case common.LTID_TINYINT, common.LTID_UTINYINT:
			stats := AnalyzeIntColumn(vec, numRows)
			plans[i].Encoding = SelectIntEncoding(stats, 1)
			plans[i].Reason = intEncodingReason(stats, 1)

		case common.LTID_SMALLINT, common.LTID_USMALLINT:
			stats := AnalyzeIntColumn(vec, numRows)
			plans[i].Encoding = SelectIntEncoding(stats, 2)
			plans[i].Reason = intEncodingReason(stats, 2)

		case common.LTID_INTEGER, common.LTID_UINTEGER, common.LTID_DATE:
			stats := AnalyzeIntColumn(vec, numRows)
			plans[i].Encoding = SelectIntEncoding(stats, 4)
			plans[i].Reason = intEncodingReason(stats, 4)

		case common.LTID_BIGINT, common.LTID_UBIGINT, common.LTID_TIMESTAMP:
			stats := AnalyzeIntColumn(vec, numRows)
			plans[i].Encoding = SelectIntEncoding(stats, 8)
			plans[i].Reason = intEncodingReason(stats, 8)

		case common.LTID_FLOAT, common.LTID_DOUBLE:
			plans[i].Encoding = EncodingPlain
			plans[i].Reason = "floating point always plain"

		case common.LTID_VARCHAR, common.LTID_CHAR, common.LTID_BLOB:
			stats := AnalyzeStringColumn(vec, numRows)
			plans[i].Encoding = SelectStringEncoding(stats)
			plans[i].Reason = stringEncodingReason(stats)

		default:
			plans[i].Encoding = EncodingPlain
			plans[i].Reason = "unknown type fallback"
		}
	}
	return plans
}

func intEncodingReason(stats ColumnEncodingStats, byteWidth int) string {
	validRows := stats.NumRows - stats.NumNulls
	if validRows == 0 {
		return "all nulls, plain fallback"
	}
	if stats.NumRuns > 0 {
		avgRunLen := float64(validRows) / float64(stats.NumRuns)
		if avgRunLen > 4.0 {
			return fmt.Sprintf("rle: avg run length %.1f", avgRunLen)
		}
	}
	if stats.NumDistinct > 0 && stats.NumDistinct < 256 {
		dictRatio := float64(stats.NumDistinct) / float64(validRows)
		if dictRatio < 0.1 {
			return fmt.Sprintf("dictionary: %d distinct / %d rows (%.1f%%)", stats.NumDistinct, validRows, dictRatio*100)
		}
	}
	return "plain: high cardinality or wide range"
}

func stringEncodingReason(stats ColumnEncodingStats) string {
	validRows := stats.NumRows - stats.NumNulls
	if validRows == 0 {
		return "all nulls, varbinary fallback"
	}
	if stats.NumDistinct > 0 && stats.NumDistinct < 4096 {
		dictRatio := float64(stats.NumDistinct) / float64(validRows)
		if dictRatio < 0.5 {
			return fmt.Sprintf("dictionary: %d distinct / %d rows (%.1f%%)", stats.NumDistinct, validRows, dictRatio*100)
		}
	}
	return "varbinary: high cardinality"
}
