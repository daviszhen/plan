package storage2

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLocalObjectStore(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)

	if err := store.MkdirAll("a/b"); err != nil {
		t.Fatal(err)
	}
	if err := store.Write("a/b/f.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, err := store.Read("a/b/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Errorf("got %q", data)
	}
	names, err := store.List("a/b")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "f.txt" {
		t.Errorf("List: got %v", names)
	}
	if _, err := store.List("nonexistent"); err != nil {
		t.Errorf("List nonexistent should return nil,nil: %v", err)
	}
}

func TestLocalObjectStoreListMissingDir(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)
	names, err := store.List("missing")
	if err != nil {
		t.Fatal(err)
	}
	if names != nil {
		t.Errorf("expected nil names for missing dir, got %v", names)
	}
}

func TestLocalObjectStoreWriteCreatesDir(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)
	if err := store.Write("x/y/z.txt", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "x/y/z.txt")); err != nil {
		t.Fatal(err)
	}
}

// TestObjectStoreRoundtrip verifies Write -> Read -> List -> Overwrite -> Read non-existent
// full roundtrip of the ObjectStore interface.
func TestObjectStoreRoundtrip(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)

	// Write
	data := []byte("hello, storage2")
	if err := store.Write("test/data.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read back
	got, err := store.Read("test/data.bin")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Read data mismatch: got %q want %q", got, data)
	}

	// List
	names, err := store.List("test")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(names) != 1 || names[0] != "data.bin" {
		t.Errorf("List unexpected: %v", names)
	}

	// Overwrite
	data2 := []byte("updated content")
	if err := store.Write("test/data.bin", data2); err != nil {
		t.Fatalf("Overwrite failed: %v", err)
	}
	got2, _ := store.Read("test/data.bin")
	if string(got2) != string(data2) {
		t.Errorf("Overwrite read mismatch: got %q want %q", got2, data2)
	}

	// Read non-existent
	_, err = store.Read("test/no-such-file")
	if err == nil {
		t.Error("Read non-existent should fail")
	}

	// List empty dir
	names, err = store.List("empty-dir")
	if err != nil {
		t.Fatalf("List empty dir failed: %v", err)
	}
	if len(names) != 0 {
		t.Errorf("List empty dir should be empty: %v", names)
	}
}

// TestStreamReadWrite verifies large-file chunked streaming read/write via ObjectStoreExt.
func TestStreamReadWrite(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStore(dir)

	// Generate large data (exceeding a single buffer size)
	size := 1024 * 1024 // 1MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Chunked write
	chunkSize := 64 * 1024 // 64KB
	path := "stream/large.bin"
	writer, err := store.OpenWriter(path)
	if err != nil {
		t.Fatalf("OpenWriter failed: %v", err)
	}
	for offset := 0; offset < size; offset += chunkSize {
		end := offset + chunkSize
		if end > size {
			end = size
		}
		if _, err := writer.Write(data[offset:end]); err != nil {
			t.Fatalf("Write chunk at %d failed: %v", offset, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Chunked read
	reader, err := store.OpenReader(path)
	if err != nil {
		t.Fatalf("OpenReader failed: %v", err)
	}
	buf := make([]byte, chunkSize)
	var readData []byte
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			readData = append(readData, buf[:n]...)
		}
		if err != nil {
			break
		}
	}
	reader.Close()

	if len(readData) != size {
		t.Fatalf("Read size mismatch: got %d want %d", len(readData), size)
	}
	for i := 0; i < size; i++ {
		if readData[i] != data[i] {
			t.Fatalf("Data mismatch at byte %d: got %d want %d", i, readData[i], data[i])
		}
	}

	// Test GetSize
	fileSize, err := store.GetSize(path)
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if fileSize != int64(size) {
		t.Errorf("GetSize: got %d want %d", fileSize, size)
	}

	// Test Exists
	exists, err := store.Exists(path)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("Exists should be true")
	}
	exists, _ = store.Exists("nonexistent")
	if exists {
		t.Error("Exists should be false for nonexistent")
	}

	// Test Copy
	copyPath := "stream/copy.bin"
	if err := store.Copy(path, copyPath); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	copyData, _ := store.Read(copyPath)
	if len(copyData) != size {
		t.Errorf("Copy size mismatch: got %d", len(copyData))
	}

	// Test Rename
	renamePath := "stream/renamed.bin"
	if err := store.Rename(copyPath, renamePath); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if e, _ := store.Exists(copyPath); e {
		t.Error("Old path should not exist after rename")
	}
	if e, _ := store.Exists(renamePath); !e {
		t.Error("New path should exist after rename")
	}

	// Test Delete
	if err := store.Delete(renamePath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if e, _ := store.Exists(renamePath); e {
		t.Error("File should not exist after delete")
	}
}

// TestLocalObjectStoreExtReadRange tests ReadRange with various offset/length combinations.
func TestLocalObjectStoreExtReadRange(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	// Write test data
	data := []byte("0123456789abcdefghij") // 20 bytes
	if err := store.Write("range.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	tests := []struct {
		name   string
		offset int64
		length int64
		want   string
	}{
		{"Full", 0, 0, "0123456789abcdefghij"},
		{"FromStart", 0, 5, "01234"},
		{"FromMiddle", 5, 5, "56789"},
		{"ToEnd", 15, 0, "fghij"},
		{"Partial", 10, 3, "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := store.ReadRange(nil, "range.bin", ReadOptions{
				Offset: tt.offset,
				Length: tt.length,
			})
			if err != nil {
				t.Fatalf("ReadRange failed: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestLocalObjectStoreExtReadStream tests ReadStream with limited reader.
func TestLocalObjectStoreExtReadStream(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	data := []byte("hello world!")
	if err := store.Write("stream.bin", data); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read with length limit
	reader, err := store.ReadStream(nil, "stream.bin", ReadOptions{
		Offset: 6,
		Length: 5,
	})
	if err != nil {
		t.Fatalf("ReadStream failed: %v", err)
	}
	defer reader.Close()

	buf := make([]byte, 20)
	n, _ := reader.Read(buf)
	if string(buf[:n]) != "world" {
		t.Errorf("got %q, want %q", buf[:n], "world")
	}

	// Read beyond limit should return EOF
	n2, _ := reader.Read(buf)
	if n2 != 0 {
		t.Errorf("expected 0 bytes after limit, got %d", n2)
	}
}

// TestLocalObjectStoreExtWriteStream tests WriteStream with various options.
func TestLocalObjectStoreExtWriteStream(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	// Test Create option
	writer, err := store.WriteStream(nil, "new.bin", WriteOptions{Create: true})
	if err != nil {
		t.Fatalf("WriteStream Create failed: %v", err)
	}
	writer.Write([]byte("hello"))
	writer.Close()

	got, _ := store.Read("new.bin")
	if string(got) != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}

	// Test Append option
	writer2, _ := store.WriteStream(nil, "new.bin", WriteOptions{Append: true})
	writer2.Write([]byte(" world"))
	writer2.Close()

	got2, _ := store.Read("new.bin")
	if string(got2) != "hello world" {
		t.Errorf("append: got %q, want %q", got2, "hello world")
	}

	// Test Sync option (just verify it doesn't error)
	writer3, err := store.WriteStream(nil, "sync.bin", WriteOptions{Create: true, Sync: true})
	if err != nil {
		t.Fatalf("WriteStream Sync failed: %v", err)
	}
	writer3.Write([]byte("synced"))
	writer3.Close()

	got3, _ := store.Read("sync.bin")
	if string(got3) != "synced" {
		t.Errorf("sync: got %q", got3)
	}
}

// TestLocalObjectStoreExtCopyRename tests Copy and Rename operations.
func TestLocalObjectStoreExtCopyRename(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	data := []byte("original data")
	store.Write("original.bin", data)

	// Test Copy with context
	if err := store.Copy(nil, "original.bin", "copied.bin"); err != nil {
		t.Fatalf("Copy failed: %v", err)
	}
	got, _ := store.Read("copied.bin")
	if string(got) != string(data) {
		t.Errorf("copy data mismatch")
	}

	// Test Rename with context
	if err := store.Rename(nil, "copied.bin", "renamed.bin"); err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if _, err := store.Read("copied.bin"); err == nil {
		t.Error("old file should not exist after rename")
	}
	got2, _ := store.Read("renamed.bin")
	if string(got2) != string(data) {
		t.Errorf("renamed data mismatch")
	}
}

// TestLocalObjectStoreExtGetSizeETag tests GetSize and GetETag.
func TestLocalObjectStoreExtGetSizeETag(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)

	data := []byte("test data for size")
	store.Write("sized.bin", data)

	// Test GetSize
	size, err := store.GetSize(nil, "sized.bin")
	if err != nil {
		t.Fatalf("GetSize failed: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("size: got %d, want %d", size, len(data))
	}

	// Test GetETag
	etag, err := store.GetETag(nil, "sized.bin")
	if err != nil {
		t.Fatalf("GetETag failed: %v", err)
	}
	if etag == "" {
		t.Error("etag should not be empty")
	}

	// ETag should change after modification
	store.Write("sized.bin", []byte("modified"))
	etag2, _ := store.GetETag(nil, "sized.bin")
	if etag == etag2 {
		t.Error("etag should change after modification")
	}
}

// TestParallelReaderWriter tests ParallelReader and ParallelWriter.
func TestParallelReaderWriter(t *testing.T) {
	dir := t.TempDir()
	store := NewLocalObjectStoreExt(dir, nil)
	scheduler := DefaultIOScheduler()

	// Create parallel reader/writer
	reader := NewParallelReader(store, scheduler)
	writer := NewParallelWriter(store, scheduler)

	// Test small file write (below threshold)
	smallData := []byte("small file content")
	if err := writer.Write(nil, "small.bin", smallData); err != nil {
		t.Fatalf("ParallelWriter.Write small failed: %v", err)
	}

	// Test small file read
	gotSmall, err := reader.Read(nil, "small.bin")
	if err != nil {
		t.Fatalf("ParallelReader.Read small failed: %v", err)
	}
	if string(gotSmall) != string(smallData) {
		t.Errorf("small: got %q", gotSmall)
	}

	// Test larger file (still below parallel threshold, but larger)
	largeData := make([]byte, 100*1024) // 100KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	if err := writer.Write(nil, "large.bin", largeData); err != nil {
		t.Fatalf("ParallelWriter.Write large failed: %v", err)
	}

	gotLarge, err := reader.Read(nil, "large.bin")
	if err != nil {
		t.Fatalf("ParallelReader.Read large failed: %v", err)
	}
	if len(gotLarge) != len(largeData) {
		t.Errorf("large size: got %d, want %d", len(gotLarge), len(largeData))
	}
}

// TestIOStatsCollector tests IOStatsCollector statistics tracking.
func TestIOStatsCollector(t *testing.T) {
	collector := NewIOStatsCollector()

	// Record some operations
	collector.RecordRead(1000, 5.0)
	collector.RecordRead(2000, 10.0)
	collector.RecordWrite(500, 3.0)

	stats := collector.GetStats()

	if stats.BytesRead != 3000 {
		t.Errorf("BytesRead: got %d, want 3000", stats.BytesRead)
	}
	if stats.BytesWritten != 500 {
		t.Errorf("BytesWritten: got %d, want 500", stats.BytesWritten)
	}
	if stats.ReadOps != 2 {
		t.Errorf("ReadOps: got %d, want 2", stats.ReadOps)
	}
	if stats.WriteOps != 1 {
		t.Errorf("WriteOps: got %d, want 1", stats.WriteOps)
	}
	// Average latency: (5 + 10) / 2 = 7.5
	if stats.ReadLatency < 7.4 || stats.ReadLatency > 7.6 {
		t.Errorf("ReadLatency: got %f, want ~7.5", stats.ReadLatency)
	}
	if stats.WriteLatency != 3.0 {
		t.Errorf("WriteLatency: got %f, want 3.0", stats.WriteLatency)
	}
}

// TestDefaultIOScheduler tests DefaultIOScheduler values.
func TestDefaultIOScheduler(t *testing.T) {
	scheduler := DefaultIOScheduler()

	if scheduler.MaxConcurrentReads != 256 {
		t.Errorf("MaxConcurrentReads: got %d, want 256", scheduler.MaxConcurrentReads)
	}
	if scheduler.MaxConcurrentWrites != 64 {
		t.Errorf("MaxConcurrentWrites: got %d, want 64", scheduler.MaxConcurrentWrites)
	}
	if scheduler.IOBufferSize != 8*1024*1024 {
		t.Errorf("IOBufferSize: got %d, want 8MB", scheduler.IOBufferSize)
	}
	if scheduler.ChunkSize != 4*1024*1024 {
		t.Errorf("ChunkSize: got %d, want 4MB", scheduler.ChunkSize)
	}
}
