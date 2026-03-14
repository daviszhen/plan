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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ============================================================================
// LT-1 Tests: V2 Manifest Naming
// ============================================================================

func TestManifestPathV2(t *testing.T) {
	// version 0 -> inverted = MaxUint64
	p0 := ManifestPathV2(0)
	expected0 := filepath.Join(VersionsDir, fmt.Sprintf("%020d.manifest", uint64(math.MaxUint64)))
	if p0 != expected0 {
		t.Fatalf("ManifestPathV2(0) = %q, want %q", p0, expected0)
	}

	// version 1 -> inverted = MaxUint64 - 1
	p1 := ManifestPathV2(1)
	expected1 := filepath.Join(VersionsDir, fmt.Sprintf("%020d.manifest", uint64(math.MaxUint64)-1))
	if p1 != expected1 {
		t.Fatalf("ManifestPathV2(1) = %q, want %q", p1, expected1)
	}

	// Higher version -> smaller inverted number -> sorts first
	p10 := ManifestPathV2(10)
	p5 := ManifestPathV2(5)
	// p10 should be lexicographically less than p5 (newer sorts first)
	if p10 >= p5 {
		t.Fatalf("V2: version 10 path %q should sort before version 5 path %q", p10, p5)
	}
}

func TestParseVersionV2(t *testing.T) {
	// Round-trip for several versions
	for _, v := range []uint64{0, 1, 5, 100, 1000, math.MaxUint64 / 2} {
		path := ManifestPathV2(v)
		filename := filepath.Base(path)
		parsed, err := ParseVersionV2(filename)
		if err != nil {
			t.Fatalf("ParseVersionV2(%q) error: %v", filename, err)
		}
		if parsed != v {
			t.Fatalf("ParseVersionV2(%q) = %d, want %d", filename, parsed, v)
		}
	}

	// Invalid filenames
	_, err := ParseVersionV2("invalid.manifest")
	if err == nil {
		t.Fatal("expected error for non-numeric V2 filename")
	}
	_, err = ParseVersionV2("1.txt")
	if err == nil {
		t.Fatal("expected error for wrong extension")
	}
}

func TestV2NamingLexicographicOrder(t *testing.T) {
	// Verify that higher versions produce lexicographically smaller filenames
	paths := make([]string, 100)
	for i := 0; i < 100; i++ {
		paths[i] = filepath.Base(ManifestPathV2(uint64(i)))
	}

	for i := 1; i < len(paths); i++ {
		if paths[i] >= paths[i-1] {
			t.Fatalf("V2 naming not lexicographically sorted: version %d (%q) >= version %d (%q)",
				i, paths[i], i-1, paths[i-1])
		}
	}
}

func TestDetectNamingScheme(t *testing.T) {
	// Empty directory -> V1
	dir := t.TempDir()
	scheme := DetectNamingScheme(dir)
	if scheme != ManifestNamingV1 {
		t.Fatalf("empty dir: expected V1, got %d", scheme)
	}

	// Create V1 manifest
	versDir := filepath.Join(dir, VersionsDir)
	os.MkdirAll(versDir, 0755)
	os.WriteFile(filepath.Join(versDir, "1.manifest"), []byte("v1"), 0644)
	scheme = DetectNamingScheme(dir)
	if scheme != ManifestNamingV1 {
		t.Fatalf("V1 file: expected V1, got %d", scheme)
	}

	// Add V2 manifest
	v2Name := filepath.Base(ManifestPathV2(1))
	os.WriteFile(filepath.Join(versDir, v2Name), []byte("v2"), 0644)
	scheme = DetectNamingScheme(dir)
	if scheme != ManifestNamingV2 {
		t.Fatalf("V2 file present: expected V2, got %d", scheme)
	}
}

func TestV2CommitHandler_Basic(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV2)

	// Commit version 0
	m0 := NewManifest(0)
	if err := handler.Commit(ctx, dir, 0, m0); err != nil {
		t.Fatalf("commit v0: %v", err)
	}

	// Resolve latest
	latest, err := handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if latest != 0 {
		t.Fatalf("latest = %d, want 0", latest)
	}

	// Commit version 1
	m1 := NewManifest(1)
	if err := handler.Commit(ctx, dir, 1, m1); err != nil {
		t.Fatalf("commit v1: %v", err)
	}

	latest, err = handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if latest != 1 {
		t.Fatalf("latest = %d, want 1", latest)
	}

	// Commit version 5
	m5 := NewManifest(5)
	if err := handler.Commit(ctx, dir, 5, m5); err != nil {
		t.Fatalf("commit v5: %v", err)
	}

	latest, err = handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if latest != 5 {
		t.Fatalf("latest = %d, want 5", latest)
	}

	// ResolveVersion
	relPath, err := handler.ResolveVersion(ctx, dir, 1)
	if err != nil {
		t.Fatalf("resolve version 1: %v", err)
	}
	expectedPath := ManifestPathV2(1)
	if relPath != expectedPath {
		t.Fatalf("resolve version 1 path = %q, want %q", relPath, expectedPath)
	}
}

func TestV2CommitHandler_V1Mode(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV1)

	// Should behave like original commit handler
	m0 := NewManifest(0)
	if err := handler.Commit(ctx, dir, 0, m0); err != nil {
		t.Fatalf("commit v0: %v", err)
	}

	m1 := NewManifest(1)
	if err := handler.Commit(ctx, dir, 1, m1); err != nil {
		t.Fatalf("commit v1: %v", err)
	}

	latest, err := handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if latest != 1 {
		t.Fatalf("latest = %d, want 1", latest)
	}

	// Check V1 path format
	relPath, err := handler.ResolveVersion(ctx, dir, 1)
	if err != nil {
		t.Fatalf("resolve version: %v", err)
	}
	if relPath != ManifestPath(1) {
		t.Fatalf("V1 path = %q, want %q", relPath, ManifestPath(1))
	}
}

func TestV2CommitHandler_LoadManifest(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV2)

	// Create a manifest with fragments
	m := NewManifest(1)
	frag := NewDataFragmentWithRows(0, 100, []*DataFile{
		NewDataFile("data/0.chunk", []int32{0, 1}, 0, 1),
	})
	m.Fragments = []*DataFragment{frag}
	m.MaxFragmentId = ptrUint32(0)

	if err := handler.Commit(ctx, dir, 1, m); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Load it back
	loaded, err := LoadManifest(ctx, dir, handler, 1)
	if err != nil {
		t.Fatalf("load manifest: %v", err)
	}
	if loaded.Version != 1 {
		t.Fatalf("version = %d, want 1", loaded.Version)
	}
	if len(loaded.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(loaded.Fragments))
	}
	if loaded.Fragments[0].PhysicalRows != 100 {
		t.Fatalf("physical rows = %d, want 100", loaded.Fragments[0].PhysicalRows)
	}
}

// ============================================================================
// LT-2 Tests: Table Validation
// ============================================================================

func TestValidateManifest_Valid(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile("data/0.chunk", []int32{0}, 0, 1),
		}),
		NewDataFragmentWithRows(1, 200, []*DataFile{
			NewDataFile("data/1.chunk", []int32{0}, 0, 1),
		}),
	}
	m.MaxFragmentId = ptrUint32(1)

	result := ValidateManifest(m)
	if !result.Valid {
		t.Fatalf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidateManifest_Nil(t *testing.T) {
	result := ValidateManifest(nil)
	if result.Valid {
		t.Fatal("expected invalid for nil manifest")
	}
	if len(result.Errors) != 1 {
		t.Fatalf("expected 1 error, got %d", len(result.Errors))
	}
}

func TestValidateManifest_DuplicateFragmentID(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile("data/0.chunk", []int32{0}, 0, 1),
		}),
		NewDataFragmentWithRows(0, 200, []*DataFile{ // duplicate ID
			NewDataFile("data/1.chunk", []int32{0}, 0, 1),
		}),
	}

	result := ValidateManifest(m)
	if result.Valid {
		t.Fatal("expected invalid for duplicate fragment IDs")
	}

	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "duplicate fragment ID") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected 'duplicate fragment ID' error")
	}
}

func TestValidateManifest_MaxFragmentIdInconsistent(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(5, 100, []*DataFile{
			NewDataFile("data/0.chunk", []int32{0}, 0, 1),
		}),
	}
	m.MaxFragmentId = ptrUint32(3) // less than actual max (5)

	result := ValidateManifest(m)
	if result.Valid {
		t.Fatal("expected invalid for inconsistent max_fragment_id")
	}

	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "max_fragment_id") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected max_fragment_id error")
	}
}

func TestValidateManifest_EmptyDataFilePath(t *testing.T) {
	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile("", []int32{0}, 0, 1), // empty path
		}),
	}

	result := ValidateManifest(m)
	if result.Valid {
		t.Fatal("expected invalid for empty data file path")
	}
}

func TestValidateManifest_DeletionExceedsPhysicalRows(t *testing.T) {
	m := NewManifest(1)
	frag := NewDataFragmentWithRows(0, 100, []*DataFile{
		NewDataFile("data/0.chunk", []int32{0}, 0, 1),
	})
	frag.DeletionFile = NewDeletionFile(0, 1, 0, 200) // 200 > 100
	m.Fragments = []*DataFragment{frag}

	result := ValidateManifest(m)
	if result.Valid {
		t.Fatal("expected invalid for deletion count exceeding physical rows")
	}

	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "deletion count") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected deletion count error")
	}
}

func TestValidateTableFiles_AllExist(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	os.MkdirAll(dataDir, 0755)
	os.WriteFile(filepath.Join(dataDir, "0.chunk"), []byte("data"), 0644)

	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile("data/0.chunk", []int32{0}, 0, 1),
		}),
	}

	result := ValidateTableFiles(dir, m)
	if !result.Valid {
		t.Fatalf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidateTableFiles_MissingFile(t *testing.T) {
	dir := t.TempDir()

	m := NewManifest(1)
	m.Fragments = []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile("data/missing.chunk", []int32{0}, 0, 1),
		}),
	}

	result := ValidateTableFiles(dir, m)
	if result.Valid {
		t.Fatal("expected invalid for missing file")
	}

	found := false
	for _, e := range result.Errors {
		if strings.Contains(e.Message, "file not found") {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected 'file not found' error")
	}
}

// ============================================================================
// LI-1 Tests: IO Scheduler & Parallel Reader
// ============================================================================

func TestSemaphore_Basic(t *testing.T) {
	sem := NewSemaphore(2)
	ctx := context.Background()

	// Acquire twice should work
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if err := sem.Acquire(ctx); err != nil {
		t.Fatalf("second acquire: %v", err)
	}

	// Third acquire should block, so test with timeout
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err := sem.Acquire(ctx2)
	if err == nil {
		t.Fatal("expected context timeout on third acquire")
	}

	// Release one, then third should succeed
	sem.Release()
	ctx3, cancel3 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel3()
	if err := sem.Acquire(ctx3); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
}

func TestSemaphore_CancelledContext(t *testing.T) {
	sem := NewSemaphore(1)
	ctx := context.Background()
	sem.Acquire(ctx) // fill the semaphore

	ctx2, cancel := context.WithCancel(ctx)
	cancel() // cancel immediately

	err := sem.Acquire(ctx2)
	if err == nil {
		t.Fatal("expected error for cancelled context")
	}
}

func TestConcurrentIOScheduler_ReadWrite(t *testing.T) {
	config := &IOScheduler{
		MaxConcurrentReads:  2,
		MaxConcurrentWrites: 1,
		IOBufferSize:        1024,
	}
	scheduler := NewConcurrentIOScheduler(config)
	ctx := context.Background()

	// Schedule reads
	data, err := scheduler.ScheduleRead(ctx, func() ([]byte, error) {
		return []byte("hello"), nil
	})
	if err != nil {
		t.Fatalf("schedule read: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q, want %q", string(data), "hello")
	}

	// Schedule write
	err = scheduler.ScheduleWrite(ctx, func() error {
		return nil
	}, 100)
	if err != nil {
		t.Fatalf("schedule write: %v", err)
	}

	// Check stats
	stats := scheduler.Stats()
	if stats.ReadOps != 1 {
		t.Fatalf("read ops = %d, want 1", stats.ReadOps)
	}
	if stats.WriteOps != 1 {
		t.Fatalf("write ops = %d, want 1", stats.WriteOps)
	}
	if stats.BytesRead != 5 {
		t.Fatalf("bytes read = %d, want 5", stats.BytesRead)
	}
	if stats.BytesWritten != 100 {
		t.Fatalf("bytes written = %d, want 100", stats.BytesWritten)
	}
}

func TestConcurrentIOScheduler_ConcurrencyLimit(t *testing.T) {
	config := &IOScheduler{
		MaxConcurrentReads:  2,
		MaxConcurrentWrites: 1,
		IOBufferSize:        1024,
	}
	scheduler := NewConcurrentIOScheduler(config)
	ctx := context.Background()

	// Track concurrent reads
	var maxConcurrent int64
	var current int64
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scheduler.ScheduleRead(ctx, func() ([]byte, error) {
				c := atomic.AddInt64(&current, 1)
				// Update max
				for {
					old := atomic.LoadInt64(&maxConcurrent)
					if c <= old {
						break
					}
					if atomic.CompareAndSwapInt64(&maxConcurrent, old, c) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&current, -1)
				return []byte{}, nil
			})
		}()
	}

	wg.Wait()

	mc := atomic.LoadInt64(&maxConcurrent)
	if mc > 2 {
		t.Fatalf("max concurrent reads = %d, expected <= 2", mc)
	}
}

func TestParallelMultiFileReader_Basic(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)

	// Create test files
	for i := 0; i < 5; i++ {
		path := fmt.Sprintf("file_%d.dat", i)
		data := fmt.Sprintf("content_%d", i)
		if err := store.Write(path, []byte(data)); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	scheduler := NewConcurrentIOScheduler(nil)
	reader := NewParallelMultiFileReader(store, scheduler)
	ctx := context.Background()

	paths := make([]string, 5)
	for i := 0; i < 5; i++ {
		paths[i] = fmt.Sprintf("file_%d.dat", i)
	}

	results := reader.ReadAll(ctx, paths)
	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	for i, r := range results {
		if r.Error != nil {
			t.Fatalf("result %d error: %v", i, r.Error)
		}
		expected := fmt.Sprintf("content_%d", i)
		if string(r.Data) != expected {
			t.Fatalf("result %d: %q, want %q", i, string(r.Data), expected)
		}
		if r.Index != i {
			t.Fatalf("result %d index = %d, want %d", i, r.Index, i)
		}
	}
}

func TestParallelMultiFileReader_MissingFile(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)

	// Create one file, leave one missing
	store.Write("exists.dat", []byte("data"))

	scheduler := NewConcurrentIOScheduler(nil)
	reader := NewParallelMultiFileReader(store, scheduler)
	ctx := context.Background()

	results := reader.ReadAll(ctx, []string{"exists.dat", "missing.dat"})
	if results[0].Error != nil {
		t.Fatalf("existing file error: %v", results[0].Error)
	}
	if results[1].Error == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestChunkedParallelReader_SmallFile(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	// Write a small file
	data := []byte("small file content")
	store.Write("small.dat", data)

	scheduler := NewConcurrentIOScheduler(nil)
	reader := NewChunkedParallelReader(store, scheduler, 1024) // 1KB chunks
	ctx := context.Background()

	result, err := reader.ReadFile(ctx, "small.dat")
	if err != nil {
		t.Fatalf("read small file: %v", err)
	}
	if string(result) != string(data) {
		t.Fatalf("data mismatch: got %q, want %q", string(result), string(data))
	}
}

func TestChunkedParallelReader_LargeFile(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	// Write a file larger than chunk size
	size := 1024 * 10 // 10KB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	store.Write("large.dat", data)

	scheduler := NewConcurrentIOScheduler(nil)
	reader := NewChunkedParallelReader(store, scheduler, 1024) // 1KB chunks
	ctx := context.Background()

	result, err := reader.ReadFile(ctx, "large.dat")
	if err != nil {
		t.Fatalf("read large file: %v", err)
	}
	if len(result) != size {
		t.Fatalf("size mismatch: got %d, want %d", len(result), size)
	}
	for i := range data {
		if result[i] != data[i] {
			t.Fatalf("byte %d: got %d, want %d", i, result[i], data[i])
		}
	}
}

// ============================================================================
// LI-2 Tests: Retryable Object Store
// ============================================================================

// failingStore wraps a store and fails the first N operations.
type failingStore struct {
	inner     ObjectStore
	failCount int64
	callCount int64
}

func newFailingStore(inner ObjectStore, failCount int) *failingStore {
	return &failingStore{inner: inner, failCount: int64(failCount)}
}

func (s *failingStore) Read(path string) ([]byte, error) {
	count := atomic.AddInt64(&s.callCount, 1)
	if count <= s.failCount {
		return nil, fmt.Errorf("transient read error (attempt %d)", count)
	}
	return s.inner.Read(path)
}

func (s *failingStore) Write(path string, data []byte) error {
	count := atomic.AddInt64(&s.callCount, 1)
	if count <= s.failCount {
		return fmt.Errorf("transient write error (attempt %d)", count)
	}
	return s.inner.Write(path, data)
}

func (s *failingStore) List(dir string) ([]string, error) {
	count := atomic.AddInt64(&s.callCount, 1)
	if count <= s.failCount {
		return nil, fmt.Errorf("transient list error (attempt %d)", count)
	}
	return s.inner.List(dir)
}

func (s *failingStore) MkdirAll(dir string) error {
	count := atomic.AddInt64(&s.callCount, 1)
	if count <= s.failCount {
		return fmt.Errorf("transient mkdir error (attempt %d)", count)
	}
	return s.inner.MkdirAll(dir)
}

func TestRetryableObjectStore_ReadSuccess(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStore(dir)
	inner.Write("test.dat", []byte("hello"))

	// Fails twice, succeeds on third try
	failing := newFailingStore(inner, 2)
	retry := NewRetryableObjectStore(failing, &RetryConfig{
		MaxRetries:    3,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	data, err := retry.Read("test.dat")
	if err != nil {
		t.Fatalf("read with retry: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("data = %q, want %q", string(data), "hello")
	}
}

func TestRetryableObjectStore_ReadExhausted(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStore(dir)
	inner.Write("test.dat", []byte("hello"))

	// Always fails
	failing := newFailingStore(inner, 100)
	retry := NewRetryableObjectStore(failing, &RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	_, err := retry.Read("test.dat")
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "after 2 retries") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRetryableObjectStore_NoRetryOnNotExist(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStore(dir)

	retry := NewRetryableObjectStore(inner, &RetryConfig{
		MaxRetries:    5,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	// File doesn't exist -> should NOT retry
	_, err := retry.Read("nonexistent.dat")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
	// Error should be the original "not exist" error, not wrapped
	if !os.IsNotExist(err) {
		t.Fatalf("expected IsNotExist error, got: %v", err)
	}
}

func TestRetryableObjectStore_WriteSuccess(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStore(dir)

	// Fails once, succeeds on second try
	failing := newFailingStore(inner, 1)
	retry := NewRetryableObjectStore(failing, &RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	err := retry.Write("output.dat", []byte("data"))
	if err != nil {
		t.Fatalf("write with retry: %v", err)
	}

	// Verify through inner store
	data, err := inner.Read("output.dat")
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(data) != "data" {
		t.Fatalf("data = %q, want %q", string(data), "data")
	}
}

func TestRetryableObjectStore_ListSuccess(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStore(dir)
	inner.Write("a.txt", []byte("a"))
	inner.Write("b.txt", []byte("b"))

	failing := newFailingStore(inner, 1)
	retry := NewRetryableObjectStore(failing, &RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	names, err := retry.List(".")
	if err != nil {
		t.Fatalf("list with retry: %v", err)
	}
	if len(names) != 2 {
		t.Fatalf("list count = %d, want 2", len(names))
	}
}

func TestRetryableObjectStore_MkdirAllSuccess(t *testing.T) {
	dir := t.TempDir()
	inner := NewLocalObjectStore(dir)

	failing := newFailingStore(inner, 1)
	retry := NewRetryableObjectStore(failing, &RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	err := retry.MkdirAll("subdir/nested")
	if err != nil {
		t.Fatalf("mkdir with retry: %v", err)
	}
}

// ============================================================================
// Integration Test: V2 Commit with Transaction
// ============================================================================

func TestV2CommitHandler_WithTransaction(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV2)

	// Create initial empty manifest
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	if err := handler.Commit(ctx, dir, 0, m0); err != nil {
		t.Fatalf("commit v0: %v", err)
	}

	// Build and commit an Append transaction
	appendFrags := []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			NewDataFile("data/0.chunk", []int32{0, 1}, 0, 1),
		}),
	}
	txn := NewTransactionAppend(0, "test-uuid-1", appendFrags)
	next, err := BuildManifest(m0, txn)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}

	if err := handler.Commit(ctx, dir, next.Version, next); err != nil {
		t.Fatalf("commit v1: %v", err)
	}

	// Verify
	latest, err := handler.ResolveLatestVersion(ctx, dir)
	if err != nil {
		t.Fatalf("resolve latest: %v", err)
	}
	if latest != 1 {
		t.Fatalf("latest = %d, want 1", latest)
	}

	loaded, err := LoadManifest(ctx, dir, handler, 1)
	if err != nil {
		t.Fatalf("load manifest v1: %v", err)
	}
	if len(loaded.Fragments) != 1 {
		t.Fatalf("fragments = %d, want 1", len(loaded.Fragments))
	}
	if loaded.Fragments[0].PhysicalRows != 100 {
		t.Fatalf("rows = %d, want 100", loaded.Fragments[0].PhysicalRows)
	}

	// Validate manifest structure
	result := ValidateManifest(loaded)
	if !result.Valid {
		t.Fatalf("manifest validation failed: %v", result.Errors)
	}
}

// ============================================================================
// Integration Test: Table Create/Open with V2 + Validation
// ============================================================================

func TestTable_V2_CreateAndValidate(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV2)

	// Create table
	table, err := CreateTable(ctx, dir, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Get initial manifest and validate
	latest, err := table.GetLatestVersion(ctx)
	if err != nil {
		t.Fatalf("get latest version: %v", err)
	}
	m, err := table.GetManifest(ctx, latest)
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	result := ValidateManifest(m)
	if !result.Valid {
		t.Fatalf("initial manifest invalid: %v", result.Errors)
	}

	// Count rows should be 0
	rows, err := table.CountRows(ctx, latest)
	if err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("rows = %d, want 0", rows)
	}
}

func TestTable_V2_OpenExisting(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV2)

	// Create table
	_, err := CreateTable(ctx, dir, handler, nil)
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	// Open existing table
	table2, err := OpenTable(ctx, dir, handler)
	if err != nil {
		t.Fatalf("open table: %v", err)
	}

	latest, err := table2.GetLatestVersion(ctx)
	if err != nil {
		t.Fatalf("get latest: %v", err)
	}
	if latest != 0 {
		t.Fatalf("latest = %d, want 0", latest)
	}
}

func TestTable_V2_Stats(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	handler := NewV2CommitHandler(ManifestNamingV2)

	// Create table with some data
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	handler.Commit(ctx, dir, 0, m0)

	// Add fragments via Append
	appendFrags := []*DataFragment{
		NewDataFragmentWithRows(0, 100, []*DataFile{
			{Path: "data/0.chunk", Fields: []int32{0}, FileSizeBytes: 1024},
		}),
		NewDataFragmentWithRows(1, 200, []*DataFile{
			{Path: "data/1.chunk", Fields: []int32{0}, FileSizeBytes: 2048},
		}),
	}
	txn := NewTransactionAppend(0, "uuid-stats", appendFrags)
	next, _ := BuildManifest(m0, txn)
	handler.Commit(ctx, dir, next.Version, next)

	table := NewTable(dir, handler)
	stats, err := table.GetStats(ctx, 1)
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	if stats.NumFragments != 2 {
		t.Fatalf("fragments = %d, want 2", stats.NumFragments)
	}
	if stats.TotalRows != 300 {
		t.Fatalf("total rows = %d, want 300", stats.TotalRows)
	}
	if stats.TotalSizeBytes != 3072 {
		t.Fatalf("total size = %d, want 3072", stats.TotalSizeBytes)
	}
}

// ============================================================================
// Integration Test: RetryableObjectStore with V2 CommitHandler
// ============================================================================

func TestRetryable_WithV2Handler(t *testing.T) {
	dir := t.TempDir()
	_ = context.Background()

	// Create store and handler
	inner := NewLocalObjectStore(dir)
	retryable := NewRetryableObjectStore(inner, &RetryConfig{
		MaxRetries:    2,
		InitialDelay:  1 * time.Millisecond,
		MaxDelay:      10 * time.Millisecond,
		BackoffFactor: 2.0,
	})

	// Write test data through retryable store
	err := retryable.Write("test.dat", []byte("content"))
	if err != nil {
		t.Fatalf("write: %v", err)
	}

	data, err := retryable.Read("test.dat")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(data) != "content" {
		t.Fatalf("data = %q, want %q", string(data), "content")
	}

	// List
	names, err := retryable.List(".")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(names) != 1 || names[0] != "test.dat" {
		t.Fatalf("list = %v, want [test.dat]", names)
	}
}
