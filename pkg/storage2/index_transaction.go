// Copyright 2026 The Plan Authors
// SPDX-License-Identifier: Apache-2.0

package storage2

import (
	"context"
	"fmt"
	"sync"
	"time"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
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
		JobID:      jobID,
		Operation:  op,
		State:      IndexBuildStatePending,
		Progress:   0,
		StartTime:  time.Now(),
		CancelFunc: cancel,
		ctx:        jobCtx,
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

	// Determine index implementation type from params
	indexImplType := "btree" // default
	if op.IndexParams != nil {
		if t, ok := op.IndexParams["type"]; ok {
			indexImplType = t
		}
	}

	// Create the appropriate index type
	var idx Index
	colIdx := op.ColumnIndices[0] // Primary column for the index

	switch indexImplType {
	case "btree":
		idx = NewBTreeIndex(op.IndexName, colIdx)
	case "bitmap":
		idx = NewBitmapIndex(colIdx, WithBitmapIndexName(op.IndexName))
	case "zonemap":
		idx = NewZoneMapIndex(WithZoneMapIndexName(op.IndexName))
	case "bloomfilter":
		idx = NewBloomFilterIndex(WithBloomFilterIndexName(op.IndexName))
	default:
		idx = NewBTreeIndex(op.IndexName, colIdx)
	}

	// Scan all fragments and build index
	var globalRowID uint64 = 0
	for _, frag := range manifest.Fragments {
		if frag == nil {
			continue
		}

		for _, df := range frag.Files {
			if df == nil || df.Path == "" {
				continue
			}

			// Read chunk from file
			fullPath := fmt.Sprintf("%s/%s", b.basePath, df.Path)
			chk, err := ReadChunkFromFile(fullPath)
			if err != nil {
				return nil, fmt.Errorf("failed to read chunk: %w", err)
			}

			// Add data to index based on index type
			if err := b.addChunkToIndex(ctx, idx, chk, colIdx, globalRowID); err != nil {
				return nil, fmt.Errorf("failed to add chunk to index: %w", err)
			}

			globalRowID += uint64(chk.Card())
		}
	}

	// Persist the index using LocalIndexStore
	indexStore := NewLocalIndexStore(b.basePath)

	// Save index data
	if serializable, ok := idx.(Serializable); ok {
		data, err := serializable.Marshal()
		if err != nil {
			return nil, fmt.Errorf("failed to serialize index: %w", err)
		}
		if err := indexStore.WriteIndex(ctx, op.IndexName, data); err != nil {
			return nil, fmt.Errorf("failed to write index: %w", err)
		}
	}

	// Save index metadata
	idxMeta := &IndexMetadata{
		Name:          op.IndexName,
		Type:          ScalarIndex,
		ColumnIndices: op.ColumnIndices,
		Version:       manifest.Version,
		Path:          fmt.Sprintf("_indices/%s", op.IndexName),
		Params:        op.IndexParams,
	}
	if err := indexStore.WriteIndexMetadata(ctx, op.IndexName, idxMeta); err != nil {
		return nil, fmt.Errorf("failed to write index metadata: %w", err)
	}

	// Save index stats
	stats := idx.Statistics()
	if err := indexStore.WriteIndexStats(ctx, op.IndexName, &stats); err != nil {
		return nil, fmt.Errorf("failed to write index stats: %w", err)
	}

	return metadata, nil
}

// addChunkToIndex adds chunk data to the index based on index type.
func (b *IndexBuilder) addChunkToIndex(ctx context.Context, idx Index, chk *chunk.Chunk, colIdx int, startRowID uint64) error {
	if chk == nil || colIdx >= chk.ColumnCount() {
		return nil
	}

	vec := chk.Data[colIdx]
	if vec == nil {
		return nil
	}

	numRows := uint64(chk.Card())

	switch typedIdx := idx.(type) {
	case *BTreeIndex:
		// Add each row to BTree
		for i := uint64(0); i < numRows; i++ {
			val := vec.GetValue(int(i))
			if val != nil && !val.IsNull {
				value := convertValueToInterface(val)
				typedIdx.Insert(value, startRowID+i)
			}
		}

	case *BitmapIndex:
		// Add each row to Bitmap
		for i := uint64(0); i < numRows; i++ {
			val := vec.GetValue(int(i))
			value := convertValueToInterface(val)
			typedIdx.Insert(value, startRowID+i)
		}

	case *ZoneMapIndex:
		// Collect values for batch update
		var values []interface{}
		var nullCount uint64
		for i := uint64(0); i < numRows; i++ {
			val := vec.GetValue(int(i))
			if val == nil || val.IsNull {
				nullCount++
			} else {
				values = append(values, convertValueToInterface(val))
			}
		}
		if err := typedIdx.UpdateZoneMapBatch(colIdx, values, nullCount); err != nil {
			return err
		}

	case *BloomFilterIndex:
		// Add each row to Bloom filter
		for i := uint64(0); i < numRows; i++ {
			val := vec.GetValue(int(i))
			if val != nil && !val.IsNull {
				value := convertValueToInterface(val)
				if err := typedIdx.AddToFilter(colIdx, value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// convertValueToInterface converts a chunk.Value to interface{} for index storage.
func convertValueToInterface(val *chunk.Value) interface{} {
	if val == nil || val.IsNull {
		return nil
	}

	switch val.Typ.Id {
	case common.LTID_BOOLEAN:
		return val.Bool
	case common.LTID_TINYINT, common.LTID_SMALLINT, common.LTID_INTEGER, common.LTID_BIGINT:
		return val.I64
	case common.LTID_UBIGINT:
		return val.U64
	case common.LTID_FLOAT, common.LTID_DOUBLE:
		return val.F64
	case common.LTID_VARCHAR:
		return val.Str
	default:
		// Fall back to string representation
		return val.String()
	}
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

// objectDeleter is an optional interface for stores that support deleting objects.
type objectDeleter interface {
	Delete(ctx context.Context, path string) error
}

// RollbackCreateIndex rolls back an index creation by deleting all index files.
func (r *IndexRollback) RollbackCreateIndex(ctx context.Context, indexMetadata *storage2pb.IndexMetadata) error {
	if indexMetadata == nil {
		return nil
	}

	if r.store == nil {
		return nil
	}

	deleter, ok := r.store.(objectDeleter)
	if !ok {
		// Store does not support deletion; nothing we can do.
		return nil
	}

	// Index files are stored under indexes/<name>/
	indexDir := fmt.Sprintf("indexes/%s", indexMetadata.Name)

	// List all files under the index directory
	entries, err := r.store.List(indexDir)
	if err != nil {
		// Directory may not exist if build failed early — not an error.
		return nil
	}

	// Delete each file
	for _, entry := range entries {
		filePath := fmt.Sprintf("%s/%s", indexDir, entry)
		if err := deleter.Delete(ctx, filePath); err != nil {
			return fmt.Errorf("failed to delete index file %s: %w", filePath, err)
		}
	}

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
// It scans the indexes directory for orphaned subdirectories that have no
// corresponding entry in the current manifest and cleans them up.
func (r *IndexRecovery) RecoverIncompleteBuilds(ctx context.Context) error {
	if r.store == nil {
		return nil // No store — nothing to recover.
	}

	// Resolve the latest manifest to know which indexes are valid.
	latestVersion, err := r.handler.ResolveLatestVersion(ctx, r.basePath)
	if err != nil || latestVersion == 0 {
		// No manifest at all — any index directories are orphaned.
		return r.cleanAllIndexDirs(ctx)
	}

	manifest, err := LoadManifest(ctx, r.basePath, r.handler, latestVersion)
	if err != nil {
		// Cannot load manifest — skip recovery rather than risk deleting valid data.
		return fmt.Errorf("index recovery: failed to load manifest v%d: %w", latestVersion, err)
	}

	// Build a set of known index names from the manifest's transaction file.
	// If a transaction references a CreateIndex, that name is considered valid.
	knownIndexes := r.collectKnownIndexNames(manifest)

	// List all subdirectories under indexes/
	indexDirs, err := r.store.List("indexes")
	if err != nil {
		return nil // No indexes directory — nothing to recover.
	}

	for _, dirName := range indexDirs {
		if _, ok := knownIndexes[dirName]; ok {
			continue // This index is referenced in the manifest — keep it.
		}

		// Orphaned index directory — clean it up.
		if err := r.deleteIndexDir(ctx, dirName); err != nil {
			return fmt.Errorf("index recovery: failed to clean up orphaned index %s: %w", dirName, err)
		}
	}

	return nil
}

// cleanAllIndexDirs removes all index subdirectories (used when no manifest exists).
func (r *IndexRecovery) cleanAllIndexDirs(ctx context.Context) error {
	dirs, err := r.store.List("indexes")
	if err != nil {
		return nil
	}
	for _, dir := range dirs {
		if err := r.deleteIndexDir(ctx, dir); err != nil {
			return err
		}
	}
	return nil
}

// deleteIndexDir deletes all files under indexes/<name>/.
func (r *IndexRecovery) deleteIndexDir(ctx context.Context, name string) error {
	deleter, ok := r.store.(objectDeleter)
	if !ok {
		return nil
	}
	dirPath := fmt.Sprintf("indexes/%s", name)
	entries, err := r.store.List(dirPath)
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		filePath := fmt.Sprintf("%s/%s", dirPath, entry)
		if err := deleter.Delete(ctx, filePath); err != nil {
			return err
		}
	}
	return nil
}

// collectKnownIndexNames extracts index names that are referenced in the manifest.
func (r *IndexRecovery) collectKnownIndexNames(manifest *Manifest) map[string]struct{} {
	names := make(map[string]struct{})

	// Check the transaction file for CreateIndex operations.
	if manifest != nil && manifest.TransactionFile != "" {
		// The index name is typically the directory name under indexes/.
		// We also consider any indexes whose section offset is set as valid.
		if manifest.IndexSection != nil {
			// If there's an index section, the manifest has at least one index.
			// Without parsing the full index section, we rely on directory existence
			// matching the manifest — conservative approach: keep all directories
			// that have non-empty content.
		}
	}

	// If manifest has fragments that reference index data, those are valid.
	// For safety, also keep any indexes that have a metadata file.
	entries, err := r.store.List("indexes")
	if err != nil {
		return names
	}
	for _, dirName := range entries {
		metadataPath := fmt.Sprintf("indexes/%s/metadata", dirName)
		data, err := r.store.Read(metadataPath)
		if err == nil && len(data) > 0 {
			names[dirName] = struct{}{}
		}
	}

	return names
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
	basePath   string
	handler    CommitHandler
	store      ObjectStoreExt
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

	metrics    IndexBuildMetrics
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
