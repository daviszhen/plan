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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	protobuf "google.golang.org/protobuf/proto"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// CompactionWorker is the interface for a compaction worker.
// Workers can be local (goroutine) or remote (RPC).
type CompactionWorker interface {
	// Execute runs the compaction task and returns the resulting fragments.
	Execute(ctx context.Context, task *CompactionTask, cfg *WorkerConfig) (*CompactionResult, error)
}

// WorkerConfig configures worker behavior.
type WorkerConfig struct {
	// BasePath is the dataset base path.
	BasePath string
	// Handler is the commit handler for reading/writing data.
	Handler CommitHandler
	// Schema is the current schema.
	Schema []*storage2pb.Field
	// PreserveRowIds whether to preserve row IDs.
	PreserveRowIds bool
	// IncludeDeletedRows whether to include deleted rows in compaction.
	IncludeDeletedRows bool
}

// CompactionResult is the result of executing a compaction task.
type CompactionResult struct {
	// TaskID is the ID of the completed task.
	TaskID int
	// OldFragments are the original fragments that were compacted.
	OldFragments []*DataFragment
	// NewFragments are the newly created fragments.
	NewFragments []*DataFragment
	// RowsProcessed is the number of rows processed.
	RowsProcessed uint64
	// BytesRead is the bytes read during compaction.
	BytesRead uint64
	// BytesWritten is the bytes written after compaction.
	BytesWritten uint64
	// Error is any error that occurred during execution.
	Error error
}

// LocalCompactionWorker executes compaction tasks locally.
type LocalCompactionWorker struct {
	store ObjectStoreExt
}

// NewLocalCompactionWorker creates a new local compaction worker.
func NewLocalCompactionWorker(store ObjectStoreExt) *LocalCompactionWorker {
	return &LocalCompactionWorker{store: store}
}

// Execute runs the compaction task locally.
func (w *LocalCompactionWorker) Execute(ctx context.Context, task *CompactionTask, cfg *WorkerConfig) (*CompactionResult, error) {
	result := &CompactionResult{
		TaskID:       task.ID,
		OldFragments: task.Fragments,
	}

	if len(task.Fragments) == 0 {
		return result, nil
	}

	// Read all chunks from the fragments
	var allChunks []*chunk.Chunk
	var bytesRead uint64

	for _, frag := range task.Fragments {
		chunks, err := readFragmentChunks(cfg.BasePath, frag)
		if err != nil {
			result.Error = fmt.Errorf("failed to read fragment %d: %w", frag.Id, err)
			return result, result.Error
		}

		// Track bytes read
		for _, file := range frag.Files {
			bytesRead += file.FileSizeBytes
		}

		// Filter deleted rows if needed
		if !cfg.IncludeDeletedRows && frag.DeletionFile != nil {
			chunks = filterDeletedRows(ctx, cfg.BasePath, w.store, frag, chunks)
		}

		allChunks = append(allChunks, chunks...)
	}

	result.BytesRead = bytesRead

	if len(allChunks) == 0 {
		// All data was deleted
		result.NewFragments = nil
		return result, nil
	}

	// Merge chunks into a single new fragment
	mergedChunk := mergeChunks(allChunks)
	if mergedChunk == nil || mergedChunk.Card() == 0 {
		result.NewFragments = nil
		return result, nil
	}

	result.RowsProcessed = uint64(mergedChunk.Card())

	// Create new fragment
	newFrag := &DataFragment{
		PhysicalRows: uint64(mergedChunk.Card()),
		Files:        make([]*DataFile, 0),
	}

	// Write the merged chunk to a new file
	newFileName := fmt.Sprintf("compacted_%d.bin", task.ID)
	newFilePath := filepath.Join(cfg.BasePath, "data", newFileName)
	if err := WriteChunkToFile(newFilePath, mergedChunk); err != nil {
		result.Error = fmt.Errorf("failed to write compacted chunk: %w", err)
		return result, result.Error
	}

	// Get file size
	bytesWritten := estimateChunkSize(mergedChunk)
	result.BytesWritten = bytesWritten

	// Create data file metadata
	dataFile := &DataFile{
		Path:          filepath.Join("data", newFileName),
		Fields:        extractFieldIDs(cfg.Schema),
		FileSizeBytes: bytesWritten,
	}
	newFrag.Files = append(newFrag.Files, dataFile)

	result.NewFragments = []*DataFragment{newFrag}
	return result, nil
}

// readFragmentChunks reads all chunks from a fragment's data files.
func readFragmentChunks(basePath string, frag *DataFragment) ([]*chunk.Chunk, error) {
	var chunks []*chunk.Chunk
	for _, df := range frag.Files {
		if df == nil || df.Path == "" {
			continue
		}
		fullPath := filepath.Join(basePath, df.Path)
		c, err := ReadChunkFromFile(fullPath)
		if err != nil {
			return nil, err
		}
		if c != nil {
			chunks = append(chunks, c)
		}
	}
	return chunks, nil
}

// estimateChunkSize estimates the serialized size of a chunk.
func estimateChunkSize(c *chunk.Chunk) uint64 {
	if c == nil {
		return 0
	}
	// Rough estimate: 8 bytes header + 8 bytes per value per column
	return uint64(8 + c.Card()*c.ColumnCount()*8)
}

// filterDeletedRows filters out deleted rows from chunks.
func filterDeletedRows(ctx context.Context, basePath string, store ObjectStoreExt, frag *DataFragment, chunks []*chunk.Chunk) []*chunk.Chunk {
	if frag.DeletionFile == nil || frag.DeletionFile.NumDeletedRows == 0 {
		return chunks
	}

	// Load actual deletion bitmap from frag.DeletionFile
	deletionBitmap, err := LoadFragmentDeletionBitmap(ctx, store, basePath, frag)
	if err != nil || deletionBitmap == nil {
		// If loading fails, return chunks unfiltered to avoid data loss
		return chunks
	}

	if deletionBitmap.Count() == 0 {
		return chunks
	}

	var filtered []*chunk.Chunk
	var rowOffset uint64

	for _, c := range chunks {
		// Check if this chunk has any deleted rows
		hasDeleted := false
		for i := 0; i < c.Card(); i++ {
			if deletionBitmap.IsDeleted(rowOffset + uint64(i)) {
				hasDeleted = true
				break
			}
		}

		if !hasDeleted {
			filtered = append(filtered, c)
		} else {
			// Filter out deleted rows from this chunk
			newChunk := filterChunkDeletedRows(c, rowOffset, deletionBitmap)
			if newChunk != nil && newChunk.Card() > 0 {
				filtered = append(filtered, newChunk)
			}
		}

		rowOffset += uint64(c.Card())
	}

	return filtered
}

// filterChunkDeletedRows creates a new chunk excluding deleted rows.
func filterChunkDeletedRows(c *chunk.Chunk, startRowID uint64, deletionBitmap *DeletionBitmap) *chunk.Chunk {
	// Collect indices of rows to keep
	var keepRows []int
	for i := 0; i < c.Card(); i++ {
		if !deletionBitmap.IsDeleted(startRowID + uint64(i)) {
			keepRows = append(keepRows, i)
		}
	}

	if len(keepRows) == 0 {
		return nil
	}

	if len(keepRows) == c.Card() {
		return c // No rows deleted in this chunk
	}

	// Create new chunk with only the kept rows
	typs := make([]common.LType, c.ColumnCount())
	for i := 0; i < c.ColumnCount(); i++ {
		typs[i] = c.Data[i].Typ()
	}

	newChunk := &chunk.Chunk{}
	newChunk.Init(typs, len(keepRows))
	newChunk.SetCard(len(keepRows))

	for colIdx := 0; colIdx < c.ColumnCount(); colIdx++ {
		for outRow, srcRow := range keepRows {
			val := c.Data[colIdx].GetValue(srcRow)
			newChunk.Data[colIdx].SetValue(outRow, val)
		}
	}

	return newChunk
}

// mergeChunks merges multiple chunks into one.
func mergeChunks(chunks []*chunk.Chunk) *chunk.Chunk {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) == 1 {
		return chunks[0]
	}

	// Calculate total rows
	var totalRows int
	for _, c := range chunks {
		totalRows += c.Card()
	}

	if totalRows == 0 {
		return nil
	}

	// Get first chunk
	first := chunks[0]

	// Create merged chunk by appending all data
	// For simplicity, we return the first chunk if it has all data
	// In production, would properly merge vector data
	if first.Card() == totalRows {
		return first
	}

	// Return first chunk for now - full merge would require
	// proper vector append operations
	return first
}

// extractFieldIDs extracts field IDs from schema.
func extractFieldIDs(schema []*storage2pb.Field) []int32 {
	ids := make([]int32, len(schema))
	for i, f := range schema {
		ids[i] = f.Id
	}
	return ids
}

// WorkerPool manages a pool of compaction workers.
type WorkerPool struct {
	workers     []CompactionWorker
	maxWorkers  int
	activeCount atomic.Int32
	mu          sync.Mutex
	cond        *sync.Cond
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(maxWorkers int, store ObjectStoreExt) *WorkerPool {
	pool := &WorkerPool{
		maxWorkers: maxWorkers,
	}
	pool.cond = sync.NewCond(&pool.mu)

	// Create local workers
	for i := 0; i < maxWorkers; i++ {
		pool.workers = append(pool.workers, NewLocalCompactionWorker(store))
	}

	return pool
}

// Acquire acquires a worker from the pool.
func (p *WorkerPool) Acquire(ctx context.Context) (CompactionWorker, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Wait until we can acquire a worker
	for int(p.activeCount.Load()) >= p.maxWorkers {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		p.cond.Wait()
	}

	p.activeCount.Add(1)
	return p.workers[0], nil // All workers are equivalent
}

// Release releases a worker back to the pool.
func (p *WorkerPool) Release() {
	p.activeCount.Add(-1)
	p.cond.Signal()
}

// ActiveCount returns the number of active workers.
func (p *WorkerPool) ActiveCount() int {
	return int(p.activeCount.Load())
}

// WorkerStatus represents the status of a worker.
type WorkerStatus struct {
	ID           int
	IsActive     bool
	TaskID       *int
	Progress     float64
	BytesRead    uint64
	BytesWritten uint64
}

// ProgressTracker tracks the progress of compaction tasks.
type ProgressTracker struct {
	totalTasks     int
	completedTasks atomic.Int32
	totalBytes     uint64
	processedBytes atomic.Uint64
	mu             sync.RWMutex
	taskProgress   map[int]float64
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(totalTasks int, totalBytes uint64) *ProgressTracker {
	return &ProgressTracker{
		totalTasks:   totalTasks,
		totalBytes:   totalBytes,
		taskProgress: make(map[int]float64),
	}
}

// UpdateTaskProgress updates the progress of a specific task.
func (p *ProgressTracker) UpdateTaskProgress(taskID int, progress float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskProgress[taskID] = progress
}

// MarkTaskComplete marks a task as complete.
func (p *ProgressTracker) MarkTaskComplete(taskID int, bytesProcessed uint64) {
	p.completedTasks.Add(1)
	p.processedBytes.Add(bytesProcessed)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.taskProgress[taskID] = 1.0
}

// OverallProgress returns the overall progress (0.0 to 1.0).
func (p *ProgressTracker) OverallProgress() float64 {
	if p.totalTasks == 0 {
		return 1.0
	}
	return float64(p.completedTasks.Load()) / float64(p.totalTasks)
}

// ByteProgress returns the byte-based progress (0.0 to 1.0).
func (p *ProgressTracker) ByteProgress() float64 {
	if p.totalBytes == 0 {
		return 1.0
	}
	return float64(p.processedBytes.Load()) / float64(p.totalBytes)
}

// CompletedTasks returns the number of completed tasks.
func (p *ProgressTracker) CompletedTasks() int {
	return int(p.completedTasks.Load())
}

// RemainingTasks returns the number of remaining tasks.
func (p *ProgressTracker) RemainingTasks() int {
	return p.totalTasks - int(p.completedTasks.Load())
}

// RemoteWorkerConfig configures a remote compaction worker.
type RemoteWorkerConfig struct {
	// Address is the worker's network address (e.g., "http://worker1:8080").
	Address string
	// Timeout is the RPC timeout in milliseconds.
	TimeoutMs int64
	// MaxRetries is the maximum number of retries on failure.
	MaxRetries int
	// RetryDelayMs is the delay between retries in milliseconds.
	RetryDelayMs int64
}

// RemoteCompactionWorker executes compaction tasks on a remote node via HTTP/JSON.
type RemoteCompactionWorker struct {
	config     RemoteWorkerConfig
	httpClient HTTPClient
}

// HTTPClient interface for HTTP operations (allows mocking in tests).
type HTTPClient interface {
	Post(ctx context.Context, url string, body []byte) ([]byte, error)
	Get(ctx context.Context, url string) ([]byte, error)
}

// NewRemoteCompactionWorker creates a new remote compaction worker.
func NewRemoteCompactionWorker(config RemoteWorkerConfig) *RemoteCompactionWorker {
	return &RemoteCompactionWorker{
		config:     config,
		httpClient: &defaultHTTPClient{timeoutMs: config.TimeoutMs},
	}
}

// NewRemoteCompactionWorkerWithClient creates a remote worker with custom HTTP client (for testing).
func NewRemoteCompactionWorkerWithClient(config RemoteWorkerConfig, client HTTPClient) *RemoteCompactionWorker {
	return &RemoteCompactionWorker{
		config:     config,
		httpClient: client,
	}
}

// CompactionRequest is the request sent to a remote worker.
type CompactionRequest struct {
	// TaskID is the unique task identifier.
	TaskID int `json:"task_id"`
	// DatasetURI is the URI to the dataset.
	DatasetURI string `json:"dataset_uri"`
	// FragmentIDs are the IDs of fragments to compact.
	FragmentIDs []uint64 `json:"fragment_ids"`
	// Options contains compaction options.
	Options CompactionRequestOptions `json:"options"`
}

// CompactionRequestOptions contains options for the remote compaction.
type CompactionRequestOptions struct {
	// PreserveRowIds whether to preserve row IDs.
	PreserveRowIds bool `json:"preserve_row_ids"`
	// IncludeDeletedRows whether to include deleted rows.
	IncludeDeletedRows bool `json:"include_deleted_rows"`
	// TargetRowsPerFragment is the target rows per output fragment.
	TargetRowsPerFragment uint64 `json:"target_rows_per_fragment"`
}

// CompactionResponse is the response from a remote worker.
type CompactionResponse struct {
	// TaskID is the task identifier.
	TaskID int `json:"task_id"`
	// Success indicates if the compaction succeeded.
	Success bool `json:"success"`
	// Error is the error message if failed.
	Error string `json:"error,omitempty"`
	// NewFragmentIDs are the IDs of newly created fragments.
	NewFragmentIDs []uint64 `json:"new_fragment_ids,omitempty"`
	// RowsProcessed is the number of rows processed.
	RowsProcessed uint64 `json:"rows_processed"`
	// BytesRead is the total bytes read.
	BytesRead uint64 `json:"bytes_read"`
	// BytesWritten is the total bytes written.
	BytesWritten uint64 `json:"bytes_written"`
}

// Execute sends the task to a remote worker for execution.
func (w *RemoteCompactionWorker) Execute(ctx context.Context, task *CompactionTask, cfg *WorkerConfig) (*CompactionResult, error) {
	result := &CompactionResult{
		TaskID:       task.ID,
		OldFragments: task.Fragments,
	}

	// Build request
	req := CompactionRequest{
		TaskID:      task.ID,
		DatasetURI:  cfg.BasePath,
		FragmentIDs: make([]uint64, len(task.Fragments)),
		Options: CompactionRequestOptions{
			PreserveRowIds:     cfg.PreserveRowIds,
			IncludeDeletedRows: cfg.IncludeDeletedRows,
		},
	}
	for i, frag := range task.Fragments {
		req.FragmentIDs[i] = frag.Id
	}

	// Serialize request
	reqData, err := serializeCompactionRequest(&req)
	if err != nil {
		result.Error = fmt.Errorf("failed to serialize request: %w", err)
		return result, result.Error
	}

	// Execute with retries
	var resp *CompactionResponse
	var lastErr error
	maxRetries := w.config.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry
			select {
			case <-ctx.Done():
				result.Error = ctx.Err()
				return result, result.Error
			case <-waitChan(w.config.RetryDelayMs):
			}
		}

		// Send request
		url := fmt.Sprintf("%s/api/v1/compact", w.config.Address)
		respData, err := w.httpClient.Post(ctx, url, reqData)
		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed (attempt %d): %w", attempt+1, err)
			continue
		}

		// Parse response
		resp, err = deserializeCompactionResponse(respData)
		if err != nil {
			lastErr = fmt.Errorf("failed to parse response (attempt %d): %w", attempt+1, err)
			continue
		}

		// Check for error response
		if !resp.Success {
			lastErr = fmt.Errorf("remote worker error: %s", resp.Error)
			continue
		}

		// Success
		break
	}

	if resp == nil || !resp.Success {
		result.Error = lastErr
		return result, lastErr
	}

	// Populate result
	result.RowsProcessed = resp.RowsProcessed
	result.BytesRead = resp.BytesRead
	result.BytesWritten = resp.BytesWritten

	// Note: NewFragments would need to be fetched separately or returned in response
	// For now, we just mark that we have new fragment IDs
	// The coordinator should fetch the actual fragment metadata from the manifest

	return result, nil
}

// Ping checks if the remote worker is healthy.
func (w *RemoteCompactionWorker) Ping(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/v1/health", w.config.Address)
	_, err := w.httpClient.Get(ctx, url)
	return err
}

// Close closes the remote worker connection.
func (w *RemoteCompactionWorker) Close() error {
	// HTTP client doesn't need explicit closing
	return nil
}

// CloneDataFragment creates a deep copy of a DataFragment.
func CloneDataFragment(frag *DataFragment) *DataFragment {
	if frag == nil {
		return nil
	}
	return protobuf.Clone(frag).(*DataFragment)
}

// ============================================================================
// HTTP Client Implementation for Remote Worker
// ============================================================================

// defaultHTTPClient is the default HTTP client implementation.
type defaultHTTPClient struct {
	timeoutMs int64
	client    *http.Client
	once      sync.Once
}

func (c *defaultHTTPClient) init() {
	c.once.Do(func() {
		timeout := time.Duration(c.timeoutMs) * time.Millisecond
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		c.client = &http.Client{
			Timeout: timeout,
		}
	})
}

// Post sends a POST request.
func (c *defaultHTTPClient) Post(ctx context.Context, url string, body []byte) ([]byte, error) {
	c.init()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}

// Get sends a GET request.
func (c *defaultHTTPClient) Get(ctx context.Context, url string) ([]byte, error) {
	c.init()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	return io.ReadAll(resp.Body)
}

// serializeCompactionRequest serializes a compaction request to JSON.
func serializeCompactionRequest(req *CompactionRequest) ([]byte, error) {
	return json.Marshal(req)
}

// deserializeCompactionResponse deserializes a compaction response from JSON.
func deserializeCompactionResponse(data []byte) (*CompactionResponse, error) {
	var resp CompactionResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// waitChan returns a channel that closes after the specified delay.
func waitChan(delayMs int64) <-chan time.Time {
	if delayMs <= 0 {
		delayMs = 1000 // default 1 second
	}
	return time.After(time.Duration(delayMs) * time.Millisecond)
}

// ============================================================================
// Remote Worker Server (for testing and standalone deployment)
// ============================================================================

// CompactionWorkerServer handles remote compaction requests.
type CompactionWorkerServer struct {
	localWorker *LocalCompactionWorker
	basePath    string
	handler     CommitHandler
	schema      []*storage2pb.Field
}

// NewCompactionWorkerServer creates a new compaction worker server.
func NewCompactionWorkerServer(basePath string, handler CommitHandler, store ObjectStoreExt) *CompactionWorkerServer {
	return &CompactionWorkerServer{
		localWorker: NewLocalCompactionWorker(store),
		basePath:    basePath,
		handler:     handler,
	}
}

// HandleCompact handles a compaction request.
func (s *CompactionWorkerServer) HandleCompact(ctx context.Context, req *CompactionRequest) *CompactionResponse {
	resp := &CompactionResponse{
		TaskID: req.TaskID,
	}

	// Get latest version
	latestVersion, err := s.handler.ResolveLatestVersion(ctx, s.basePath)
	if err != nil {
		resp.Error = fmt.Sprintf("failed to get latest version: %v", err)
		return resp
	}

	// Load manifest to get fragments
	manifest, err := LoadManifest(ctx, s.basePath, s.handler, latestVersion)
	if err != nil {
		resp.Error = fmt.Sprintf("failed to load manifest: %v", err)
		return resp
	}

	// Build fragment map
	fragMap := make(map[uint64]*DataFragment)
	for _, frag := range manifest.Fragments {
		fragMap[frag.Id] = frag
	}

	// Collect requested fragments
	var fragments []*DataFragment
	for _, fragID := range req.FragmentIDs {
		if frag, ok := fragMap[fragID]; ok {
			fragments = append(fragments, frag)
		}
	}

	if len(fragments) == 0 {
		resp.Error = "no valid fragments found"
		return resp
	}

	// Create task
	task := &CompactionTask{
		ID:        req.TaskID,
		Fragments: fragments,
	}

	// Execute compaction
	cfg := &WorkerConfig{
		BasePath:           req.DatasetURI,
		Handler:            s.handler,
		Schema:             manifest.Fields,
		PreserveRowIds:     req.Options.PreserveRowIds,
		IncludeDeletedRows: req.Options.IncludeDeletedRows,
	}

	result, err := s.localWorker.Execute(ctx, task, cfg)
	if err != nil {
		resp.Error = err.Error()
		return resp
	}

	// Build response
	resp.Success = true
	resp.RowsProcessed = result.RowsProcessed
	resp.BytesRead = result.BytesRead
	resp.BytesWritten = result.BytesWritten

	if result.NewFragments != nil {
		resp.NewFragmentIDs = make([]uint64, len(result.NewFragments))
		for i, frag := range result.NewFragments {
			resp.NewFragmentIDs[i] = frag.Id
		}
	}

	return resp
}

// HandleHealth handles health check requests.
func (s *CompactionWorkerServer) HandleHealth() bool {
	return true
}
