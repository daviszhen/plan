package storage2

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// IOScheduler handles IO scheduling for optimal performance
type IOScheduler struct {
	// MaxConcurrentReads is the maximum number of concurrent reads
	MaxConcurrentReads int
	// MaxConcurrentWrites is the maximum number of concurrent writes
	MaxConcurrentWrites int
	// IOBufferSize is the buffer size for IO operations (threshold for parallel IO)
	IOBufferSize int64
	// ChunkSize is the size of each chunk for parallel operations
	ChunkSize int64
}

// DefaultIOScheduler returns a default IO scheduler
func DefaultIOScheduler() *IOScheduler {
	return &IOScheduler{
		MaxConcurrentReads:  256,
		MaxConcurrentWrites: 64,
		IOBufferSize:        8 * 1024 * 1024, // 8MB threshold for parallel IO
		ChunkSize:           4 * 1024 * 1024, // 4MB chunk size
	}
}

// ReadOptions contains options for read operations
type ReadOptions struct {
	// Offset is the byte offset to start reading from
	Offset int64
	// Length is the number of bytes to read
	Length int64
	// Prefetch indicates whether to prefetch subsequent data
	Prefetch bool
}

// WriteOptions contains options for write operations
type WriteOptions struct {
	// Create indicates whether to create the file if it doesn't exist
	Create bool
	// Append indicates whether to append to the file
	Append bool
	// Sync indicates whether to sync after write
	Sync bool
}

// ObjectStoreExt extends ObjectStore with advanced IO capabilities
type ObjectStoreExt interface {
	ObjectStore
	// ReadRange reads a range of bytes from the object
	ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error)
	// ReadStream returns a stream for reading
	ReadStream(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error)
	// WriteStream returns a stream for writing
	WriteStream(ctx context.Context, path string, opts WriteOptions) (io.WriteCloser, error)
	// Copy copies an object
	Copy(ctx context.Context, src, dst string) error
	// Rename renames an object
	Rename(ctx context.Context, src, dst string) error
	// GetSize returns the size of an object
	GetSize(ctx context.Context, path string) (int64, error)
	// GetETag returns the ETag of an object
	GetETag(ctx context.Context, path string) (string, error)
}

// LocalObjectStoreExt is an extended local filesystem implementation
type LocalObjectStoreExt struct {
	*LocalObjectStore
	scheduler *IOScheduler
}

// NewLocalObjectStoreExt creates a new LocalObjectStoreExt
func NewLocalObjectStoreExt(root string, scheduler *IOScheduler) *LocalObjectStoreExt {
	if scheduler == nil {
		scheduler = DefaultIOScheduler()
	}
	return &LocalObjectStoreExt{
		LocalObjectStore: NewLocalObjectStore(root),
		scheduler:        scheduler,
	}
}

// ReadRange implements ObjectStoreExt
func (s *LocalObjectStoreExt) ReadRange(ctx context.Context, path string, opts ReadOptions) ([]byte, error) {
	fullPath := filepath.Join(s.Root, path)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if opts.Offset > 0 {
		_, err = file.Seek(opts.Offset, 0)
		if err != nil {
			return nil, err
		}
	}

	length := opts.Length
	if length <= 0 {
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		length = info.Size() - opts.Offset
	}

	buf := make([]byte, length)
	_, err = io.ReadFull(file, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}

// ReadStream implements ObjectStoreExt
func (s *LocalObjectStoreExt) ReadStream(ctx context.Context, path string, opts ReadOptions) (io.ReadCloser, error) {
	fullPath := filepath.Join(s.Root, path)
	file, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}

	if opts.Offset > 0 {
		_, err = file.Seek(opts.Offset, 0)
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	if opts.Length > 0 {
		return &limitedReader{file, opts.Length}, nil
	}

	return file, nil
}

// WriteStream implements ObjectStoreExt
func (s *LocalObjectStoreExt) WriteStream(ctx context.Context, path string, opts WriteOptions) (io.WriteCloser, error) {
	fullPath := filepath.Join(s.Root, path)

	flag := os.O_WRONLY
	if opts.Create {
		flag |= os.O_CREATE
	}
	if opts.Append {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	file, err := os.OpenFile(fullPath, flag, 0644)
	if err != nil {
		return nil, err
	}

	return &syncWriter{file, opts.Sync}, nil
}

// Copy implements ObjectStoreExt
func (s *LocalObjectStoreExt) Copy(ctx context.Context, src, dst string) error {
	srcPath := filepath.Join(s.Root, src)
	dstPath := filepath.Join(s.Root, dst)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// Rename implements ObjectStoreExt
func (s *LocalObjectStoreExt) Rename(ctx context.Context, src, dst string) error {
	srcPath := filepath.Join(s.Root, src)
	dstPath := filepath.Join(s.Root, dst)

	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}

	return os.Rename(srcPath, dstPath)
}

// GetSize implements ObjectStoreExt
func (s *LocalObjectStoreExt) GetSize(ctx context.Context, path string) (int64, error) {
	fullPath := filepath.Join(s.Root, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// GetETag implements ObjectStoreExt
func (s *LocalObjectStoreExt) GetETag(ctx context.Context, path string) (string, error) {
	fullPath := filepath.Join(s.Root, path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return "", err
	}

	// Generate ETag from size and mod time
	return fmt.Sprintf("%d-%d", info.Size(), info.ModTime().UnixNano()), nil
}

// limitedReader wraps a file with a read limit
type limitedReader struct {
	file   *os.File
	remain int64
}

func (r *limitedReader) Read(p []byte) (int, error) {
	if r.remain <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remain {
		p = p[:r.remain]
	}
	n, err := r.file.Read(p)
	r.remain -= int64(n)
	return n, err
}

func (r *limitedReader) Close() error {
	return r.file.Close()
}

// syncWriter optionally syncs after write
type syncWriter struct {
	file *os.File
	sync bool
}

func (w *syncWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	if err != nil {
		return n, err
	}
	if w.sync {
		err = w.file.Sync()
	}
	return n, err
}

func (w *syncWriter) Close() error {
	if w.sync {
		w.file.Sync()
	}
	return w.file.Close()
}

// ParallelReader performs parallel reads for large files
type ParallelReader struct {
	store     ObjectStoreExt
	scheduler *IOScheduler
}

// NewParallelReader creates a new ParallelReader
func NewParallelReader(store ObjectStoreExt, scheduler *IOScheduler) *ParallelReader {
	return &ParallelReader{store: store, scheduler: scheduler}
}

// Read reads a file in parallel chunks
func (r *ParallelReader) Read(ctx context.Context, path string) ([]byte, error) {
	size, err := r.store.GetSize(ctx, path)
	if err != nil {
		return nil, err
	}

	// For small files, just read directly
	if size < r.scheduler.IOBufferSize {
		return r.store.ReadRange(ctx, path, ReadOptions{})
	}

	// Calculate chunk parameters
	chunkSize := r.scheduler.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024 // default 4MB
	}
	numChunks := (size + chunkSize - 1) / chunkSize

	// Allocate result buffer
	result := make([]byte, size)

	// Create semaphore for concurrency control
	maxConcurrent := r.scheduler.MaxConcurrentReads
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}
	sem := make(chan struct{}, maxConcurrent)

	// Error channel for collecting errors
	errChan := make(chan error, numChunks)
	var wg sync.WaitGroup

	// Read chunks in parallel
	for i := int64(0); i < numChunks; i++ {
		wg.Add(1)
		go func(chunkIdx int64) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}

			// Calculate chunk boundaries
			offset := chunkIdx * chunkSize
			length := chunkSize
			if offset+length > size {
				length = size - offset
			}

			// Read chunk
			data, err := r.store.ReadRange(ctx, path, ReadOptions{
				Offset: offset,
				Length: length,
			})
			if err != nil {
				errChan <- fmt.Errorf("read chunk %d: %w", chunkIdx, err)
				return
			}

			// Copy to result buffer
			copy(result[offset:], data)
		}(i)
	}

	// Wait for all goroutines
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// ParallelWriter performs parallel writes for large files
type ParallelWriter struct {
	store     ObjectStoreExt
	scheduler *IOScheduler
}

// NewParallelWriter creates a new ParallelWriter
func NewParallelWriter(store ObjectStoreExt, scheduler *IOScheduler) *ParallelWriter {
	return &ParallelWriter{store: store, scheduler: scheduler}
}

// Write writes a file in parallel chunks
func (w *ParallelWriter) Write(ctx context.Context, path string, data []byte) error {
	// For small files, just write directly
	if int64(len(data)) < w.scheduler.IOBufferSize {
		return w.store.Write(path, data)
	}

	// Check if store supports multipart upload (for cloud storage)
	if mp, ok := w.store.(MultipartUploader); ok {
		return w.writeMultipart(ctx, path, data, mp)
	}

	// For local filesystem, use sequential write with large buffer
	// Parallel writes to local files can cause fragmentation
	return w.store.Write(path, data)
}

// MultipartUploader interface for stores that support multipart uploads
type MultipartUploader interface {
	CreateMultipartUpload(ctx context.Context, path string) (uploadID string, err error)
	UploadPart(ctx context.Context, path, uploadID string, partNumber int, data []byte) (etag string, err error)
	CompleteMultipartUpload(ctx context.Context, path, uploadID string, parts []CompletedPart) error
	AbortMultipartUpload(ctx context.Context, path, uploadID string) error
}

// CompletedPart represents a completed upload part
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// writeMultipart writes data using multipart upload
func (w *ParallelWriter) writeMultipart(ctx context.Context, path string, data []byte, mp MultipartUploader) error {
	// Start multipart upload
	uploadID, err := mp.CreateMultipartUpload(ctx, path)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}

	// Calculate chunk parameters
	chunkSize := w.scheduler.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 8 * 1024 * 1024 // default 8MB for multipart
	}
	// Minimum part size for S3 is 5MB (except last part)
	if chunkSize < 5*1024*1024 {
		chunkSize = 5 * 1024 * 1024
	}

	dataLen := int64(len(data))
	numParts := (dataLen + chunkSize - 1) / chunkSize

	// Pre-allocate parts slice
	parts := make([]CompletedPart, numParts)
	var partsLock sync.Mutex

	// Create semaphore for concurrency control
	maxConcurrent := w.scheduler.MaxConcurrentWrites
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}
	sem := make(chan struct{}, maxConcurrent)

	// Error channel
	errChan := make(chan error, numParts)
	var wg sync.WaitGroup

	// Upload parts in parallel
	for i := int64(0); i < numParts; i++ {
		wg.Add(1)
		go func(partIdx int64) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}

			// Calculate part boundaries
			offset := partIdx * chunkSize
			end := offset + chunkSize
			if end > dataLen {
				end = dataLen
			}

			// Upload part (part numbers are 1-based)
			partNumber := int(partIdx + 1)
			etag, err := mp.UploadPart(ctx, path, uploadID, partNumber, data[offset:end])
			if err != nil {
				errChan <- fmt.Errorf("upload part %d: %w", partNumber, err)
				return
			}

			// Record completed part
			partsLock.Lock()
			parts[partIdx] = CompletedPart{
				PartNumber: partNumber,
				ETag:       etag,
			}
			partsLock.Unlock()
		}(i)
	}

	// Wait for all uploads
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		if err != nil {
			// Abort the multipart upload on error
			_ = mp.AbortMultipartUpload(ctx, path, uploadID)
			return err
		}
	}

	// Complete multipart upload
	if err := mp.CompleteMultipartUpload(ctx, path, uploadID, parts); err != nil {
		_ = mp.AbortMultipartUpload(ctx, path, uploadID)
		return fmt.Errorf("complete multipart upload: %w", err)
	}

	return nil
}

// IOStats contains IO statistics
type IOStats struct {
	// BytesRead is the total bytes read
	BytesRead uint64
	// BytesWritten is the total bytes written
	BytesWritten uint64
	// ReadOps is the number of read operations
	ReadOps uint64
	// WriteOps is the number of write operations
	WriteOps uint64
	// ReadLatency is the average read latency in ms
	ReadLatency float64
	// WriteLatency is the average write latency in ms
	WriteLatency float64
}

// IOStatsCollector collects IO statistics
type IOStatsCollector struct {
	stats IOStats
}

// NewIOStatsCollector creates a new IOStatsCollector
func NewIOStatsCollector() *IOStatsCollector {
	return &IOStatsCollector{}
}

// RecordRead records a read operation
func (c *IOStatsCollector) RecordRead(bytes uint64, latencyMs float64) {
	c.stats.BytesRead += bytes
	c.stats.ReadOps++
	// Update running average
	c.stats.ReadLatency = (c.stats.ReadLatency*float64(c.stats.ReadOps-1) + latencyMs) / float64(c.stats.ReadOps)
}

// RecordWrite records a write operation
func (c *IOStatsCollector) RecordWrite(bytes uint64, latencyMs float64) {
	c.stats.BytesWritten += bytes
	c.stats.WriteOps++
	// Update running average
	c.stats.WriteLatency = (c.stats.WriteLatency*float64(c.stats.WriteOps-1) + latencyMs) / float64(c.stats.WriteOps)
}

// GetStats returns the current statistics
func (c *IOStatsCollector) GetStats() IOStats {
	return c.stats
}
