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
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================
// LT-1: V2 Manifest Naming (Inverted Version Numbers)
// ============================================================================
//
// Lance V2 uses inverted version numbers in manifest filenames so that the
// latest version sorts first lexicographically. This is critical for S3 where
// ListObjectsV2 returns results in lexicographic order -- fetching the first
// result gives the latest manifest without scanning all versions.
//
// Format: %020d.manifest where number = ManifestNamingV2Max - version
// Example: version 1 -> 18446744073709551614.manifest
//          version 2 -> 18446744073709551613.manifest

const (
	// ManifestNamingV2Max is the maximum value for V2 naming inversion.
	// This is math.MaxUint64.
	ManifestNamingV2Max uint64 = math.MaxUint64

	// ManifestV2Width is the zero-padded width for V2 filenames.
	ManifestV2Width = 20
)

// ManifestNamingScheme defines how manifest files are named on disk.
type ManifestNamingScheme int

const (
	// ManifestNamingV1 uses simple version numbers: {version}.manifest
	ManifestNamingV1 ManifestNamingScheme = iota
	// ManifestNamingV2 uses inverted version numbers: %020d.manifest
	// where the number is ManifestNamingV2Max - version.
	ManifestNamingV2
)

// ManifestPathV2 returns the V2 manifest filename for the given version.
// The file sorts lexicographically so that the latest version comes first.
func ManifestPathV2(version uint64) string {
	inverted := ManifestNamingV2Max - version
	name := fmt.Sprintf("%020d.%s", inverted, ManifestExtension)
	return filepath.Join(VersionsDir, name)
}

// ParseVersionV2 parses the version from a V2 manifest filename.
func ParseVersionV2(filename string) (uint64, error) {
	ext := "." + ManifestExtension
	if !strings.HasSuffix(filename, ext) {
		return 0, fmt.Errorf("invalid manifest filename: %s", filename)
	}
	numStr := strings.TrimSuffix(filename, ext)
	inverted, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse V2 manifest version: %w", err)
	}
	return ManifestNamingV2Max - inverted, nil
}

// DetectNamingScheme inspects the _versions directory to determine
// which naming scheme is in use. Returns ManifestNamingV2 if any file
// uses the 20-digit format; ManifestNamingV1 otherwise.
func DetectNamingScheme(basePath string) ManifestNamingScheme {
	dir := filepath.Join(basePath, VersionsDir)
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		return ManifestNamingV1
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		ext := "." + ManifestExtension
		if !strings.HasSuffix(name, ext) {
			continue
		}
		numStr := strings.TrimSuffix(name, ext)
		if len(numStr) == ManifestV2Width {
			return ManifestNamingV2
		}
	}
	return ManifestNamingV1
}

// V2CommitHandler extends LocalRenameCommitHandler with V2 naming support.
type V2CommitHandler struct {
	Scheme ManifestNamingScheme
}

// NewV2CommitHandler creates a commit handler with the specified naming scheme.
func NewV2CommitHandler(scheme ManifestNamingScheme) *V2CommitHandler {
	return &V2CommitHandler{Scheme: scheme}
}

// manifestPath returns the correct path based on the naming scheme.
func (h *V2CommitHandler) manifestPath(version uint64) string {
	if h.Scheme == ManifestNamingV2 {
		return ManifestPathV2(version)
	}
	return ManifestPath(version)
}

// parseVersion parses a manifest filename using the current scheme.
func (h *V2CommitHandler) parseVersion(filename string) (uint64, error) {
	ext := "." + ManifestExtension
	if !strings.HasSuffix(filename, ext) {
		return 0, fmt.Errorf("invalid manifest filename: %s", filename)
	}
	numStr := strings.TrimSuffix(filename, ext)
	if h.Scheme == ManifestNamingV2 && len(numStr) == ManifestV2Width {
		return ParseVersionV2(filename)
	}
	return ParseVersion(filename)
}

// ResolveLatestVersion lists manifest files and returns the highest version.
func (h *V2CommitHandler) ResolveLatestVersion(ctx context.Context, basePath string) (uint64, error) {
	dir := filepath.Join(basePath, VersionsDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	if h.Scheme == ManifestNamingV2 {
		// V2: the first (lexicographically smallest) file is the latest version.
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			v, err := h.parseVersion(e.Name())
			if err != nil {
				continue
			}
			return v, nil
		}
		return 0, nil
	}

	// V1: scan all files for the maximum version.
	var maxVersion uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		v, err := ParseVersion(e.Name())
		if err != nil {
			continue
		}
		if v > maxVersion {
			maxVersion = v
		}
	}
	return maxVersion, nil
}

// ResolveVersion returns the relative path to the manifest for the given version.
func (h *V2CommitHandler) ResolveVersion(_ context.Context, _ string, version uint64) (string, error) {
	return h.manifestPath(version), nil
}

// Commit writes the manifest file using the V2 naming scheme with atomic rename.
func (h *V2CommitHandler) Commit(ctx context.Context, basePath string, version uint64, manifest *Manifest) error {
	data, err := MarshalManifest(manifest)
	if err != nil {
		return err
	}
	versionsDir := filepath.Join(basePath, VersionsDir)
	if err := os.MkdirAll(versionsDir, 0755); err != nil {
		return err
	}

	relPath := h.manifestPath(version)
	finalPath := filepath.Join(basePath, relPath)
	tmpPath := finalPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// ============================================================================
// LT-2: Table Validation (file existence, fragment consistency)
// ============================================================================

// ValidationError represents a specific validation failure.
type ValidationError struct {
	FragmentID uint64
	FilePath   string
	Message    string
}

func (e *ValidationError) Error() string {
	if e.FilePath != "" {
		return fmt.Sprintf("fragment %d, file %s: %s", e.FragmentID, e.FilePath, e.Message)
	}
	return fmt.Sprintf("fragment %d: %s", e.FragmentID, e.Message)
}

// ValidationResult contains the results of a table validation.
type ValidationResult struct {
	Valid  bool
	Errors []*ValidationError
}

// ValidateManifest performs comprehensive validation of a manifest.
// Checks: fragment ID uniqueness, max_fragment_id consistency,
// file references, deletion file references.
func ValidateManifest(m *Manifest) *ValidationResult {
	result := &ValidationResult{Valid: true}
	if m == nil {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{
			Message: "manifest is nil",
		})
		return result
	}

	// Check fragment ID uniqueness
	fragIDs := make(map[uint64]int)
	var maxFragID uint64
	for _, f := range m.Fragments {
		if f == nil {
			continue
		}
		if prev, exists := fragIDs[f.Id]; exists {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				FragmentID: f.Id,
				Message:    fmt.Sprintf("duplicate fragment ID (also at index %d)", prev),
			})
		}
		fragIDs[f.Id] = len(fragIDs)
		if f.Id > maxFragID {
			maxFragID = f.Id
		}
	}

	// Check max_fragment_id consistency
	if m.MaxFragmentId != nil && len(m.Fragments) > 0 {
		declared := uint64(*m.MaxFragmentId)
		if declared < maxFragID {
			result.Valid = false
			result.Errors = append(result.Errors, &ValidationError{
				Message: fmt.Sprintf("max_fragment_id (%d) < actual max fragment ID (%d)", declared, maxFragID),
			})
		}
	}

	// Check data files have non-empty paths
	for _, f := range m.Fragments {
		if f == nil {
			continue
		}
		for _, df := range f.Files {
			if df == nil {
				result.Valid = false
				result.Errors = append(result.Errors, &ValidationError{
					FragmentID: f.Id,
					Message:    "nil data file entry",
				})
				continue
			}
			if df.Path == "" {
				result.Valid = false
				result.Errors = append(result.Errors, &ValidationError{
					FragmentID: f.Id,
					Message:    "empty data file path",
				})
			}
		}

		// Check deletion file consistency
		if f.DeletionFile != nil {
			if f.DeletionFile.NumDeletedRows > f.PhysicalRows {
				result.Valid = false
				result.Errors = append(result.Errors, &ValidationError{
					FragmentID: f.Id,
					Message: fmt.Sprintf("deletion count (%d) exceeds physical rows (%d)",
						f.DeletionFile.NumDeletedRows, f.PhysicalRows),
				})
			}
		}
	}

	return result
}

// ValidateTableFiles checks that all data files referenced in the manifest
// exist on disk at the given basePath.
func ValidateTableFiles(basePath string, m *Manifest) *ValidationResult {
	result := &ValidationResult{Valid: true}
	if m == nil {
		result.Valid = false
		result.Errors = append(result.Errors, &ValidationError{Message: "manifest is nil"})
		return result
	}

	for _, f := range m.Fragments {
		if f == nil {
			continue
		}
		for _, df := range f.Files {
			if df == nil || df.Path == "" {
				continue
			}
			fullPath := filepath.Join(basePath, df.Path)
			if _, err := os.Stat(fullPath); os.IsNotExist(err) {
				result.Valid = false
				result.Errors = append(result.Errors, &ValidationError{
					FragmentID: f.Id,
					FilePath:   df.Path,
					Message:    "file not found on disk",
				})
			}
		}
	}
	return result
}

// ============================================================================
// LI-1: IO Scheduler with Semaphore-based Concurrency Control
// ============================================================================

// Semaphore provides a counting semaphore for concurrency control.
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore creates a semaphore with the given concurrency limit.
func NewSemaphore(n int) *Semaphore {
	if n <= 0 {
		n = 1
	}
	return &Semaphore{ch: make(chan struct{}, n)}
}

// Acquire blocks until a slot is available or ctx is cancelled.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.ch <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release frees a slot.
func (s *Semaphore) Release() {
	<-s.ch
}

// ConcurrentIOScheduler manages concurrent IO operations with semaphores.
type ConcurrentIOScheduler struct {
	readSem  *Semaphore
	writeSem *Semaphore
	config   IOScheduler
	stats    *IOStatsCollector
}

// NewConcurrentIOScheduler creates a scheduler with the given config.
func NewConcurrentIOScheduler(config *IOScheduler) *ConcurrentIOScheduler {
	if config == nil {
		config = DefaultIOScheduler()
	}
	return &ConcurrentIOScheduler{
		readSem:  NewSemaphore(config.MaxConcurrentReads),
		writeSem: NewSemaphore(config.MaxConcurrentWrites),
		config:   *config,
		stats:    NewIOStatsCollector(),
	}
}

// ScheduleRead executes a read operation respecting concurrency limits.
func (s *ConcurrentIOScheduler) ScheduleRead(ctx context.Context, fn func() ([]byte, error)) ([]byte, error) {
	if err := s.readSem.Acquire(ctx); err != nil {
		return nil, fmt.Errorf("acquire read semaphore: %w", err)
	}
	defer s.readSem.Release()

	start := time.Now()
	data, err := fn()
	elapsed := time.Since(start).Seconds() * 1000
	if err == nil {
		s.stats.RecordRead(uint64(len(data)), elapsed)
	}
	return data, err
}

// ScheduleWrite executes a write operation respecting concurrency limits.
func (s *ConcurrentIOScheduler) ScheduleWrite(ctx context.Context, fn func() error, bytes uint64) error {
	if err := s.writeSem.Acquire(ctx); err != nil {
		return fmt.Errorf("acquire write semaphore: %w", err)
	}
	defer s.writeSem.Release()

	start := time.Now()
	err := fn()
	elapsed := time.Since(start).Seconds() * 1000
	if err == nil {
		s.stats.RecordWrite(bytes, elapsed)
	}
	return err
}

// Stats returns the current IO statistics.
func (s *ConcurrentIOScheduler) Stats() IOStats {
	return s.stats.GetStats()
}

// ============================================================================
// LI-1 (cont.): Parallel Multi-file Reader
// ============================================================================

// FileReadResult holds the result of reading a single file.
type FileReadResult struct {
	Path  string
	Data  []byte
	Error error
	Index int // Original request index for ordering
}

// ParallelMultiFileReader reads multiple files concurrently using the scheduler.
type ParallelMultiFileReader struct {
	store     ObjectStore
	scheduler *ConcurrentIOScheduler
}

// NewParallelMultiFileReader creates a new parallel multi-file reader.
func NewParallelMultiFileReader(store ObjectStore, scheduler *ConcurrentIOScheduler) *ParallelMultiFileReader {
	return &ParallelMultiFileReader{store: store, scheduler: scheduler}
}

// ReadAll reads all specified paths concurrently and returns results in order.
func (r *ParallelMultiFileReader) ReadAll(ctx context.Context, paths []string) []FileReadResult {
	results := make([]FileReadResult, len(paths))
	var wg sync.WaitGroup

	for i, path := range paths {
		wg.Add(1)
		go func(idx int, p string) {
			defer wg.Done()
			data, err := r.scheduler.ScheduleRead(ctx, func() ([]byte, error) {
				return r.store.Read(p)
			})
			results[idx] = FileReadResult{
				Path:  p,
				Data:  data,
				Error: err,
				Index: idx,
			}
		}(i, path)
	}

	wg.Wait()
	return results
}

// ============================================================================
// LI-1 (cont.): Chunked Parallel File Reader
// ============================================================================

const (
	// DefaultChunkSize is the default read chunk size (4MB).
	DefaultChunkSize int64 = 4 * 1024 * 1024
)

// ChunkedParallelReader reads a large file in parallel chunks using ReadRange.
type ChunkedParallelReader struct {
	store     ObjectStoreExt
	scheduler *ConcurrentIOScheduler
	chunkSize int64
}

// NewChunkedParallelReader creates a new chunked parallel reader.
func NewChunkedParallelReader(store ObjectStoreExt, scheduler *ConcurrentIOScheduler, chunkSize int64) *ChunkedParallelReader {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	return &ChunkedParallelReader{
		store:     store,
		scheduler: scheduler,
		chunkSize: chunkSize,
	}
}

// ReadFile reads a file by splitting it into chunks and reading them in parallel.
func (r *ChunkedParallelReader) ReadFile(ctx context.Context, path string) ([]byte, error) {
	size, err := r.store.GetSize(ctx, path)
	if err != nil {
		return nil, err
	}

	// For files smaller than chunk size, read directly.
	if size <= r.chunkSize {
		return r.scheduler.ScheduleRead(ctx, func() ([]byte, error) {
			return r.store.ReadRange(ctx, path, ReadOptions{Length: size})
		})
	}

	// Split into chunks.
	numChunks := (size + r.chunkSize - 1) / r.chunkSize
	chunks := make([][]byte, numChunks)
	var wg sync.WaitGroup
	var firstErr atomic.Value

	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)
		go func(idx int64) {
			defer wg.Done()
			offset := idx * r.chunkSize
			length := r.chunkSize
			if offset+length > size {
				length = size - offset
			}
			data, err := r.scheduler.ScheduleRead(ctx, func() ([]byte, error) {
				return r.store.ReadRange(ctx, path, ReadOptions{
					Offset: offset,
					Length: length,
				})
			})
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			chunks[idx] = data
		}(i)
	}

	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return nil, v.(error)
	}

	// Concatenate chunks.
	result := make([]byte, 0, size)
	for _, chunk := range chunks {
		result = append(result, chunk...)
	}
	return result, nil
}

// ============================================================================
// LI-2: Object Store Retry Wrapper
// ============================================================================

// RetryConfig configures retry behavior for object store operations.
type RetryConfig struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

// DefaultRetryConfig returns reasonable defaults for retrying IO operations.
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
	}
}

// RetryableObjectStore wraps an ObjectStore with retry logic.
type RetryableObjectStore struct {
	inner  ObjectStore
	config RetryConfig
}

// NewRetryableObjectStore creates a new retryable object store wrapper.
func NewRetryableObjectStore(inner ObjectStore, config *RetryConfig) *RetryableObjectStore {
	if config == nil {
		config = DefaultRetryConfig()
	}
	return &RetryableObjectStore{
		inner:  inner,
		config: *config,
	}
}

// retryOp executes fn with retry logic, returning the last error on failure.
func (s *RetryableObjectStore) retryOp(fn func() error) error {
	var lastErr error
	delay := s.config.InitialDelay

	for attempt := 0; attempt <= s.config.MaxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		// Don't retry "not found" or similar non-transient errors.
		if os.IsNotExist(err) || os.IsPermission(err) {
			return err
		}

		if attempt < s.config.MaxRetries {
			time.Sleep(delay)
			delay = time.Duration(float64(delay) * s.config.BackoffFactor)
			if delay > s.config.MaxDelay {
				delay = s.config.MaxDelay
			}
		}
	}
	return fmt.Errorf("after %d retries: %w", s.config.MaxRetries, lastErr)
}

// Read implements ObjectStore with retry.
func (s *RetryableObjectStore) Read(path string) ([]byte, error) {
	var data []byte
	err := s.retryOp(func() error {
		var e error
		data, e = s.inner.Read(path)
		return e
	})
	return data, err
}

// Write implements ObjectStore with retry.
func (s *RetryableObjectStore) Write(path string, data []byte) error {
	return s.retryOp(func() error {
		return s.inner.Write(path, data)
	})
}

// List implements ObjectStore with retry.
func (s *RetryableObjectStore) List(dir string) ([]string, error) {
	var names []string
	err := s.retryOp(func() error {
		var e error
		names, e = s.inner.List(dir)
		return e
	})
	return names, err
}

// MkdirAll implements ObjectStore with retry.
func (s *RetryableObjectStore) MkdirAll(dir string) error {
	return s.retryOp(func() error {
		return s.inner.MkdirAll(dir)
	})
}

// ============================================================================
// LI-2 (cont.): RetryableObjectStoreExt wraps ObjectStoreExt
// ============================================================================

// RetryableObjectStoreExt wraps an ObjectStoreExt with retry logic.
type RetryableObjectStoreExt struct {
	*RetryableObjectStore
	innerExt ObjectStoreExt
}

// NewRetryableObjectStoreExt creates a retryable wrapper for ObjectStoreExt.
func NewRetryableObjectStoreExt(inner ObjectStoreExt, config *RetryConfig) *RetryableObjectStoreExt {
	return &RetryableObjectStoreExt{
		RetryableObjectStore: NewRetryableObjectStore(inner, config),
		innerExt:             inner,
	}
}

// ReadRange implements ObjectStoreExt with retry.
func (s *RetryableObjectStoreExt) ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error) {
	var data []byte
	err := s.retryOp(func() error {
		var e error
		data, e = s.innerExt.ReadRange(ctx, path, opts)
		return e
	})
	return data, err
}

// GetSize implements ObjectStoreExt with retry.
func (s *RetryableObjectStoreExt) GetSize(ctx context.Context, path string) (int64, error) {
	var size int64
	err := s.retryOp(func() error {
		var e error
		size, e = s.innerExt.GetSize(ctx, path)
		return e
	})
	return size, err
}

// GetETag implements ObjectStoreExt with retry.
func (s *RetryableObjectStoreExt) GetETag(ctx context.Context, path string) (string, error) {
	var etag string
	err := s.retryOp(func() error {
		var e error
		etag, e = s.innerExt.GetETag(ctx, path)
		return e
	})
	return etag, err
}

// Copy implements ObjectStoreExt with retry.
func (s *RetryableObjectStoreExt) Copy(ctx context.Context, src, dst string) error {
	return s.retryOp(func() error {
		return s.innerExt.Copy(ctx, src, dst)
	})
}

// Rename implements ObjectStoreExt with retry.
func (s *RetryableObjectStoreExt) Rename(ctx context.Context, src, dst string) error {
	return s.retryOp(func() error {
		return s.innerExt.Rename(ctx, src, dst)
	})
}
