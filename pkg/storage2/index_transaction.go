// Copyright 2026 The Plan Authors
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"fmt"
	"sync"
	"time"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/storage2/proto"
)

// CreateIndexOperation represents an index creation operation.
type CreateIndexOperation struct {
	// IndexName is the unique name for the index
	IndexName string

	// IndexType is the type of index to create
	IndexType IndexType

	// ColumnIndices are the column indices to index
	ColumnIndices []int

	// IndexParams are index-specific parameters
	IndexParams map[string]string

	// ReplaceExisting if true, drops existing index with same name
	ReplaceExisting bool

	// Async if true, creates index in background
	Async bool
}

// CreateIndexResult contains the result of a create index operation.
type CreateIndexResult struct {
	// IndexID is the unique identifier for the created index
	IndexID string

	// IndexPath is the storage path for the index
	IndexPath string

	// BuildTime is the time taken to build the index
	BuildTime time.Duration

	// NumEntries is the number of entries indexed
	NumEntries uint64

	// IndexSizeBytes is the size of the index in bytes
	IndexSizeBytes uint64

	// Success indicates if the index was created successfully
	Success bool

	// Error message if creation failed
	Error string
}

// IndexBuildState represents the state of an index build operation.
type IndexBuildState int

const (
	// IndexBuildStatePending means the build is queued.
	IndexBuildStatePending IndexBuildState = iota

	// IndexBuildStateRunning means the build is in progress.
	IndexBuildStateRunning

	// IndexBuildStateCompleted means the build finished successfully.
	IndexBuildStateCompleted

	// IndexBuildStateFailed means the build failed.
	IndexBuildStateFailed

	// IndexBuildStateCancelled means the build was cancelled.
	IndexBuildStateCancelled
)

func (s IndexBuildState) String() string {
	switch s {
	case IndexBuildStatePending:
		return "pending"
	case IndexBuildStateRunning:
		return "running"
	case IndexBuildStateCompleted:
		return "completed"
	case IndexBuildStateFailed:
		return "failed"
	case IndexBuildStateCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// IndexBuildJob represents an ongoing index build operation.
type IndexBuildJob struct {
	// JobID is the unique identifier for this job
	JobID string

	// Operation is the create index operation
	Operation *CreateIndexOperation

	// State is the current build state
	State IndexBuildState

	// Progress is the build progress (0-100)
	Progress int

	// Result is populated when the build completes
	Result *CreateIndexResult

	// Error is set if the build failed
	Error error

	// StartTime is when the build started
	StartTime time.Time

	// EndTime is when the build completed
	EndTime time.Time

	// CancelFunc can be called to cancel the build
	CancelFunc context.CancelFunc

	// ctx is the build context
	ctx context.Context

	// mu protects concurrent access
	mu sync.RWMutex
}

// IsActive returns true if the job is still active (pending or running).
func (j *IndexBuildJob) IsActive() bool {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.State == IndexBuildStatePending || j.State == IndexBuildStateRunning
}

// SetState updates the job state.
func (j *IndexBuildJob) SetState(state IndexBuildState) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.State = state
	if state == IndexBuildStateCompleted || state == IndexBuildStateFailed || state == IndexBuildStateCancelled {
		j.EndTime = time.Now()
	}
}

// SetProgress updates the build progress.
func (j *IndexBuildJob) SetProgress(progress int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if progress < 0 {
		progress = 0
	}
	if progress > 100 {
		progress = 100
	}
	j.Progress = progress
}

// GetProgress returns the current progress.
func (j *IndexBuildJob) GetProgress() int {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Progress
}

// IndexBuilder builds indexes transactionally.
type IndexBuilder struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt

	// activeJobs tracks ongoing index builds
	activeJobs map[string]*IndexBuildJob
	jobsMu     sync.RWMutex

	// jobCounter for generating unique job IDs
	jobCounter uint64
}

// NewIndexBuilder creates a new transactional index builder.
func NewIndexBuilder(basePath string, handler CommitHandler, store ObjectStoreExt) *IndexBuilder {
	return &IndexBuilder{
		basePath:   basePath,
		handler:    handler,
		store:      store,
		activeJobs: make(map[string]*IndexBuildJob),
	}
}

// CreateIndex creates an index transactionally.
// If async is true, returns immediately and builds in background.
func (b *IndexBuilder) CreateIndex(
	ctx context.Context,
	readVersion uint64,
	op *CreateIndexOperation,
) (*CreateIndexResult, error) {
	if op == nil {
		return nil, fmt.Errorf("create index operation is nil")
	}

	if op.IndexName == "" {
		return nil, fmt.Errorf("index name is required")
	}

	if len(op.ColumnIndices) == 0 {
		return nil, fmt.Errorf("at least one column must be specified")
	}

	// Load manifest to validate columns
	manifest, err := LoadManifest(ctx, b.basePath, b.handler, readVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Validate column indices
	for _, colIdx := range op.ColumnIndices {
		if colIdx < 0 || colIdx >= len(manifest.Fields) {
			return nil, fmt.Errorf("invalid column index: %d", colIdx)
		}
	}

	if op.Async {
		// Start async build
		job := b.startAsyncBuild(ctx, readVersion, op)
		return &CreateIndexResult{
			Success: true,
		}, job.Error
	}

	// Synchronous build
	return b.buildIndex(ctx, readVersion, manifest, op)
}

// startAsyncBuild starts an asynchronous index build.
func (b *IndexBuilder) startAsyncBuild(ctx context.Context, readVersion uint64, op *CreateIndexOperation) *IndexBuildJob {
	jobCtx, cancel := context.WithCancel(ctx)

	b.jobsMu.Lock()
	b.jobCounter++
	jobID := fmt.Sprintf("index_build_%d_%d", time.Now().Unix(), b.jobCounter)
	job := &IndexBuildJob{
		JobID:     jobID,
		Operation: op,
		State:     IndexBuildStatePending,
		Progress:  0,
		StartTime: time.Now(),
		CancelFunc: cancel,
		ctx:       jobCtx,
	}
	b.activeJobs[jobID] = job
	b.jobsMu.Unlock()

	// Start background build
	go b.runAsyncBuild(jobCtx, job, readVersion)

	return job
}

// runAsyncBuild runs the index build in background.
func (b *IndexBuilder) runAsyncBuild(ctx context.Context, job *IndexBuildJob, readVersion uint64) {
	job.SetState(IndexBuildStateRunning)

	// Load manifest
	manifest, err := LoadManifest(ctx, b.basePath, b.handler, readVersion)
	if err != nil {
		job.Error = fmt.Errorf("failed to load manifest: %w", err)
		job.SetState(IndexBuildStateFailed)
		return
	}

	// Build index
	result, err := b.buildIndex(ctx, readVersion, manifest, job.Operation)
	if err != nil {
		job.Error = err
		job.SetState(IndexBuildStateFailed)
		return
	}

	job.Result = result
	job.SetState(IndexBuildStateCompleted)
}

// buildIndex performs the actual index building.
func (b *IndexBuilder) buildIndex(
	ctx context.Context,
	readVersion uint64,
	manifest *Manifest,
	op *CreateIndexOperation,
) (*CreateIndexResult, error) {
	startTime := time.Now()

	// Create index path
	indexPath := fmt.Sprintf("%s/indexes/%s", b.basePath, op.IndexName)

	// Build index based on type
	var indexMetadata *storage2pb.IndexMetadata
	switch op.IndexType {
	case ScalarIndex:
		metadata, err := b.buildScalarIndex(ctx, manifest, op, indexPath)
		if err != nil {
			return nil, err
		}
		indexMetadata = metadata

	case VectorIndex:
		metadata, err := b.buildVectorIndex(ctx, manifest, op, indexPath)
		if err != nil {
			return nil, err
		}
		indexMetadata = metadata

	default:
		return nil, fmt.Errorf("unsupported index type: %v", op.IndexType)
	}

	buildTime := time.Since(startTime)

	// Calculate total rows
	var numEntries uint64
	for _, frag := range manifest.Fragments {
		numEntries += frag.PhysicalRows
	}

	return &CreateIndexResult{
		IndexID:        indexMetadata.GetName(),
		IndexPath:      indexPath,
		BuildTime:      buildTime,
		NumEntries:     numEntries,
		IndexSizeBytes: 0, // Would be populated from actual index size
		Success:        true,
	}, nil
}

// buildScalarIndex builds a scalar index.
func (b *IndexBuilder) buildScalarIndex(
	ctx context.Context,
	manifest *Manifest,
	op *CreateIndexOperation,
	indexPath string,
) (*storage2pb.IndexMetadata, error) {
	// Create index metadata
	metadata := &storage2pb.IndexMetadata{
		Name:   op.IndexName,
		Fields: make([]int32, len(op.ColumnIndices)),
	}

	for i, idx := range op.ColumnIndices {
		metadata.Fields[i] = int32(idx)
	}

	// TODO: Actually build and persist the index data

	return metadata, nil
}

// buildVectorIndex builds a vector index.
func (b *IndexBuilder) buildVectorIndex(
	ctx context.Context,
	manifest *Manifest,
	op *CreateIndexOperation,
	indexPath string,
) (*storage2pb.IndexMetadata, error) {
	// Create index metadata
	metadata := &storage2pb.IndexMetadata{
		Name:   op.IndexName,
		Fields: make([]int32, len(op.ColumnIndices)),
	}

	for i, idx := range op.ColumnIndices {
		metadata.Fields[i] = int32(idx)
	}

	// TODO: Actually build and persist the index data

	return metadata, nil
}

// GetJob returns an active job by ID.
func (b *IndexBuilder) GetJob(jobID string) (*IndexBuildJob, bool) {
	b.jobsMu.RLock()
	defer b.jobsMu.RUnlock()
	job, ok := b.activeJobs[jobID]
	return job, ok
}

// ListActiveJobs returns all active (pending or running) jobs.
func (b *IndexBuilder) ListActiveJobs() []*IndexBuildJob {
	b.jobsMu.RLock()
	defer b.jobsMu.RUnlock()

	var jobs []*IndexBuildJob
	for _, job := range b.activeJobs {
		if job.IsActive() {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

// CancelJob cancels an active job.
func (b *IndexBuilder) CancelJob(jobID string) error {
	b.jobsMu.Lock()
	job, ok := b.activeJobs[jobID]
	if !ok {
		b.jobsMu.Unlock()
		return fmt.Errorf("job %s not found", jobID)
	}
	b.jobsMu.Unlock()

	if !job.IsActive() {
		return fmt.Errorf("job %s is not active", jobID)
	}

	if job.CancelFunc != nil {
		job.CancelFunc()
	}
	job.SetState(IndexBuildStateCancelled)

	return nil
}

// CleanupCompletedJobs removes completed jobs from tracking.
func (b *IndexBuilder) CleanupCompletedJobs() int {
	b.jobsMu.Lock()
	defer b.jobsMu.Unlock()

	count := 0
	for id, job := range b.activeJobs {
		if !job.IsActive() {
			delete(b.activeJobs, id)
			count++
		}
	}
	return count
}

// IndexTransactionBuilder builds CreateIndex transactions.
type IndexTransactionBuilder struct {
	basePath string
	handler  CommitHandler
}

// NewIndexTransactionBuilder creates a new index transaction builder.
func NewIndexTransactionBuilder(basePath string, handler CommitHandler) *IndexTransactionBuilder {
	return &IndexTransactionBuilder{
		basePath: basePath,
		handler:  handler,
	}
}

// BuildCreateIndexTransaction builds a transaction for creating an index.
func (b *IndexTransactionBuilder) BuildCreateIndexTransaction(
	readVersion uint64,
	uuid string,
	newIndices []*storage2pb.IndexMetadata,
	removedIndices []*storage2pb.IndexMetadata,
) *Transaction {
	if newIndices == nil {
		newIndices = []*storage2pb.IndexMetadata{}
	}
	if removedIndices == nil {
		removedIndices = []*storage2pb.IndexMetadata{}
	}

	return &Transaction{
		ReadVersion: readVersion,
		Uuid:        uuid,
		Operation: &storage2pb.Transaction_CreateIndex_{
			CreateIndex: &storage2pb.Transaction_CreateIndex{
				NewIndices:     newIndices,
				RemovedIndices: removedIndices,
			},
		},
	}
}

// buildManifestCreateIndex applies CreateIndex operation to manifest.
// Note: In Lance, index metadata is stored in a separate IndexSection.
// This function currently just increments the version. The actual index
// metadata management would need to be implemented based on the storage
// layout of the specific Lance format version.
func buildManifestCreateIndex(current *Manifest, createIndexOp *storage2pb.Transaction_CreateIndex) (*Manifest, error) {
	next := protobuf.Clone(current).(*Manifest)
	next.Version = current.Version + 1

	// Index metadata in Lance is stored separately via IndexSection
	// The actual index tracking would be handled by the index store

	return next, nil
}

// CheckCreateIndexConflict checks if a CreateIndex transaction conflicts with another.
func CheckCreateIndexConflict(myTxn, otherTxn *Transaction) bool {
	myCreateIndex := myTxn.GetCreateIndex()
	if myCreateIndex == nil {
		return true
	}

	// Get indices being created by my transaction
	myNewIndices := make(map[string]bool)
	for _, idx := range myCreateIndex.GetNewIndices() {
		myNewIndices[idx.Name] = true
	}

	// Check based on other transaction type
	switch otherOp := otherTxn.Operation.(type) {
	case *storage2pb.Transaction_CreateIndex_:
		otherCreateIndex := otherOp.CreateIndex
		// Conflict if creating index with same name
		for _, idx := range otherCreateIndex.GetNewIndices() {
			if myNewIndices[idx.Name] {
				return true
			}
		}
		// Also conflict if other is removing an index we're creating
		for _, idx := range otherCreateIndex.GetRemovedIndices() {
			if myNewIndices[idx.Name] {
				return true
			}
		}

	default:
		// CreateIndex doesn't conflict with data operations
		return false
	}

	return false
}

// IndexRollback handles rollback of index creation.
type IndexRollback struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
}

// NewIndexRollback creates a new index rollback handler.
func NewIndexRollback(basePath string, handler CommitHandler, store ObjectStoreExt) *IndexRollback {
	return &IndexRollback{
		basePath: basePath,
		handler:  handler,
		store:    store,
	}
}

// RollbackCreateIndex rolls back an index creation.
func (r *IndexRollback) RollbackCreateIndex(ctx context.Context, indexMetadata *storage2pb.IndexMetadata) error {
	if indexMetadata == nil {
		return nil
	}

	// Delete index files
	indexPath := fmt.Sprintf("%s/indexes/%s", r.basePath, indexMetadata.Name)

	// List and delete all files under index path
	// This is a simplified implementation
	_ = indexPath

	return nil
}

// RollbackDropIndex rolls back an index drop (restores the index).
func (r *IndexRollback) RollbackDropIndex(ctx context.Context, indexMetadata *storage2pb.IndexMetadata) error {
	// To rollback a drop, we would need to have kept the index files
	// This would require a more complex implementation with backup
	return fmt.Errorf("rollback of index drop not yet implemented")
}

// IndexRecovery handles recovery of incomplete index builds.
type IndexRecovery struct {
	basePath string
	handler  CommitHandler
	store    ObjectStoreExt
}

// NewIndexRecovery creates a new index recovery handler.
func NewIndexRecovery(basePath string, handler CommitHandler, store ObjectStoreExt) *IndexRecovery {
	return &IndexRecovery{
		basePath: basePath,
		handler:  handler,
		store:    store,
	}
}

// RecoverIncompleteBuilds recovers any incomplete index builds after a crash.
func (r *IndexRecovery) RecoverIncompleteBuilds(ctx context.Context) error {
	// List all index directories
	// Check for incomplete builds (e.g., temp files, missing metadata)
	// Clean up or resume as appropriate

	// This is a simplified implementation
	return nil
}

// IndexBuildProgress tracks progress of an index build.
type IndexBuildProgress struct {
	// TotalFragments is the total number of fragments to process
	TotalFragments int

	// ProcessedFragments is the number of fragments processed so far
	ProcessedFragments int

	// CurrentFragment is the ID of the fragment being processed
	CurrentFragment uint64

	// PercentComplete is the percentage of completion (0-100)
	PercentComplete int

	// EstimatedTimeRemaining is the estimated time to completion
	EstimatedTimeRemaining time.Duration
}

// IndexBuildProgressTracker tracks progress of index builds.
type IndexBuildProgressTracker struct {
	mu sync.RWMutex

	totalFragments     int
	processedFragments int
	startTime          time.Time
	lastUpdateTime     time.Time
}

// NewIndexBuildProgressTracker creates a new progress tracker.
func NewIndexBuildProgressTracker(totalFragments int) *IndexBuildProgressTracker {
	now := time.Now()
	return &IndexBuildProgressTracker{
		totalFragments: totalFragments,
		startTime:      now,
		lastUpdateTime: now,
	}
}

// UpdateProgress updates the progress.
func (t *IndexBuildProgressTracker) UpdateProgress(processedFragments int, currentFragment uint64) IndexBuildProgress {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.processedFragments = processedFragments
	t.lastUpdateTime = time.Now()

	percentComplete := 0
	if t.totalFragments > 0 {
		percentComplete = (processedFragments * 100) / t.totalFragments
	}

	// Estimate remaining time based on average processing time
	var estimatedRemaining time.Duration
	if processedFragments > 0 {
		elapsed := t.lastUpdateTime.Sub(t.startTime)
		avgTimePerFragment := elapsed / time.Duration(processedFragments)
		remainingFragments := t.totalFragments - processedFragments
		estimatedRemaining = avgTimePerFragment * time.Duration(remainingFragments)
	}

	return IndexBuildProgress{
		TotalFragments:         t.totalFragments,
		ProcessedFragments:     processedFragments,
		CurrentFragment:        currentFragment,
		PercentComplete:        percentComplete,
		EstimatedTimeRemaining: estimatedRemaining,
	}
}

// GetProgress returns the current progress.
func (t *IndexBuildProgressTracker) GetProgress() IndexBuildProgress {
	t.mu.RLock()
	defer t.mu.RUnlock()

	percentComplete := 0
	if t.totalFragments > 0 {
		percentComplete = (t.processedFragments * 100) / t.totalFragments
	}

	return IndexBuildProgress{
		TotalFragments:     t.totalFragments,
		ProcessedFragments: t.processedFragments,
		PercentComplete:    percentComplete,
	}
}

// ConcurrentIndexBuilder builds multiple indexes concurrently.
type ConcurrentIndexBuilder struct {
	basePath  string
	handler   CommitHandler
	store     ObjectStoreExt
	maxWorkers int
}

// NewConcurrentIndexBuilder creates a new concurrent index builder.
func NewConcurrentIndexBuilder(basePath string, handler CommitHandler, store ObjectStoreExt, maxWorkers int) *ConcurrentIndexBuilder {
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	return &ConcurrentIndexBuilder{
		basePath:   basePath,
		handler:    handler,
		store:      store,
		maxWorkers: maxWorkers,
	}
}

// BuildIndexes builds multiple indexes concurrently.
func (b *ConcurrentIndexBuilder) BuildIndexes(
	ctx context.Context,
	readVersion uint64,
	operations []*CreateIndexOperation,
) ([]*CreateIndexResult, error) {
	if len(operations) == 0 {
		return nil, nil
	}

	// Load manifest once
	manifest, err := LoadManifest(ctx, b.basePath, b.handler, readVersion)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Create worker pool
	var wg sync.WaitGroup
	results := make([]*CreateIndexResult, len(operations))
	errors := make([]error, len(operations))

	// Semaphore to limit concurrency
	semaphore := make(chan struct{}, b.maxWorkers)

	for i, op := range operations {
		wg.Add(1)
		go func(idx int, operation *CreateIndexOperation) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			builder := NewIndexBuilder(b.basePath, b.handler, b.store)
			result, err := builder.buildIndex(ctx, readVersion, manifest, operation)
			results[idx] = result
			errors[idx] = err
		}(i, op)
	}

	wg.Wait()

	// Check for errors
	for _, err := range errors {
		if err != nil {
			return results, fmt.Errorf("one or more index builds failed: %w", err)
		}
	}

	return results, nil
}

// IndexBuildMetrics contains metrics for index builds.
type IndexBuildMetrics struct {
	// TotalBuilds is the total number of index builds
	TotalBuilds uint64

	// SuccessfulBuilds is the number of successful builds
	SuccessfulBuilds uint64

	// FailedBuilds is the number of failed builds
	FailedBuilds uint64

	// CancelledBuilds is the number of cancelled builds
	CancelledBuilds uint64

	// AverageBuildTime is the average build time
	AverageBuildTime time.Duration

	// TotalBytesIndexed is the total bytes indexed
	TotalBytesIndexed uint64
}

// IndexBuildMetricsCollector collects metrics for index builds.
type IndexBuildMetricsCollector struct {
	mu sync.RWMutex

	metrics IndexBuildMetrics
	buildTimes []time.Duration
}

// NewIndexBuildMetricsCollector creates a new metrics collector.
func NewIndexBuildMetricsCollector() *IndexBuildMetricsCollector {
	return &IndexBuildMetricsCollector{
		buildTimes: make([]time.Duration, 0),
	}
}

// RecordBuild records a completed build.
func (c *IndexBuildMetricsCollector) RecordBuild(success bool, cancelled bool, buildTime time.Duration, bytesIndexed uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics.TotalBuilds++

	if cancelled {
		c.metrics.CancelledBuilds++
	} else if success {
		c.metrics.SuccessfulBuilds++
	} else {
		c.metrics.FailedBuilds++
	}

	c.buildTimes = append(c.buildTimes, buildTime)
	c.metrics.TotalBytesIndexed += bytesIndexed

	// Recalculate average
	var total time.Duration
	for _, t := range c.buildTimes {
		total += t
	}
	if len(c.buildTimes) > 0 {
		c.metrics.AverageBuildTime = total / time.Duration(len(c.buildTimes))
	}
}

// GetMetrics returns the current metrics.
func (c *IndexBuildMetricsCollector) GetMetrics() IndexBuildMetrics {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.metrics
}
