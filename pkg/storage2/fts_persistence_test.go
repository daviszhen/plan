// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFTSIndexPersistence(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Create and populate an FTS index
	idx := NewFTSIndex("test_fts", 0)

	docs := []struct {
		id   uint64
		text string
	}{
		{1, "the quick brown fox jumps over the lazy dog"},
		{2, "a quick brown dog runs in the park"},
		{3, "the fox is clever and quick"},
		{4, "lazy dogs sleep all day"},
	}

	for _, doc := range docs {
		if err := idx.IndexDocument(doc.id, doc.text); err != nil {
			t.Fatalf("failed to index document %d: %v", doc.id, err)
		}
	}

	// Verify index works before persistence
	results, _, err := idx.TextSearch(ctx, "quick brown", 10)
	if err != nil {
		t.Fatalf("search failed: %v", err)
	}
	if len(results) == 0 {
		t.Error("expected search results before persistence")
	}

	// Save the index
	store := NewFTSIndexStore(dir, nil)
	uuid := "test-fts-uuid"

	if err := store.SaveIndex(ctx, idx, uuid); err != nil {
		t.Fatalf("failed to save index: %v", err)
	}

	// Verify files were created
	indexPath := filepath.Join(dir, "_indices", uuid)
	if _, err := os.Stat(filepath.Join(indexPath, "meta.json")); err != nil {
		t.Error("meta.json not created")
	}
	if _, err := os.Stat(filepath.Join(indexPath, "terms.json")); err != nil {
		t.Error("terms.json not created")
	}
	if _, err := os.Stat(filepath.Join(indexPath, "doc_lengths.bin")); err != nil {
		t.Error("doc_lengths.bin not created")
	}

	// Load the index
	loadedIdx, err := store.LoadIndex(ctx, uuid)
	if err != nil {
		t.Fatalf("failed to load index: %v", err)
	}

	// Verify loaded index metadata
	if loadedIdx.name != idx.name {
		t.Errorf("name mismatch: got %s, want %s", loadedIdx.name, idx.name)
	}
	if loadedIdx.totalDocs != idx.totalDocs {
		t.Errorf("totalDocs mismatch: got %d, want %d", loadedIdx.totalDocs, idx.totalDocs)
	}
	if loadedIdx.totalTerms != idx.totalTerms {
		t.Errorf("totalTerms mismatch: got %d, want %d", loadedIdx.totalTerms, idx.totalTerms)
	}

	// Verify search works on loaded index
	loadedResults, _, err := loadedIdx.TextSearch(ctx, "quick brown", 10)
	if err != nil {
		t.Fatalf("search on loaded index failed: %v", err)
	}
	if len(loadedResults) != len(results) {
		t.Errorf("result count mismatch: got %d, want %d", len(loadedResults), len(results))
	}

	// Verify phrase search works
	phraseResults, err := loadedIdx.PhraseSearch(ctx, "quick brown", 10)
	if err != nil {
		t.Fatalf("phrase search on loaded index failed: %v", err)
	}
	if len(phraseResults) == 0 {
		t.Error("expected phrase search results")
	}
}

func TestFTSIndexPersistenceDocLengths(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	idx := NewFTSIndex("test_lengths", 0)

	// Index documents with known lengths
	idx.IndexDocument(10, "one two three") // 3 tokens
	idx.IndexDocument(20, "a b c d e")     // 5 tokens
	idx.IndexDocument(30, "single")        // 1 token

	store := NewFTSIndexStore(dir, nil)
	uuid := "test-lengths-uuid"

	if err := store.SaveIndex(ctx, idx, uuid); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	loadedIdx, err := store.LoadIndex(ctx, uuid)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	// Verify doc lengths
	expected := map[uint64]int{10: 3, 20: 5, 30: 1}
	for docID, expectedLen := range expected {
		if gotLen := loadedIdx.docLengths[docID]; gotLen != expectedLen {
			t.Errorf("doc %d length: got %d, want %d", docID, gotLen, expectedLen)
		}
	}
}

func TestFTSIndexPersistenceEmpty(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Empty index
	idx := NewFTSIndex("empty_fts", 0)

	store := NewFTSIndexStore(dir, nil)
	uuid := "empty-uuid"

	if err := store.SaveIndex(ctx, idx, uuid); err != nil {
		t.Fatalf("failed to save empty index: %v", err)
	}

	loadedIdx, err := store.LoadIndex(ctx, uuid)
	if err != nil {
		t.Fatalf("failed to load empty index: %v", err)
	}

	if loadedIdx.totalDocs != 0 {
		t.Errorf("expected 0 docs, got %d", loadedIdx.totalDocs)
	}
	if len(loadedIdx.invertedIndex) != 0 {
		t.Errorf("expected empty inverted index, got %d terms", len(loadedIdx.invertedIndex))
	}
}

func TestFTSIndexPersistenceDeleteIndex(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	idx := NewFTSIndex("delete_test", 0)
	idx.IndexDocument(1, "test document")

	store := NewFTSIndexStore(dir, nil)
	uuid := "delete-uuid"

	if err := store.SaveIndex(ctx, idx, uuid); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	// Verify index exists
	indexPath := filepath.Join(dir, "_indices", uuid)
	if _, err := os.Stat(indexPath); err != nil {
		t.Error("index directory should exist before delete")
	}

	// Delete index
	if err := store.DeleteIndex(ctx, uuid); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify index is gone
	if _, err := os.Stat(indexPath); !os.IsNotExist(err) {
		t.Error("index directory should not exist after delete")
	}
}

func TestFTSIndexPersistenceListIndexes(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	store := NewFTSIndexStore(dir, nil)

	// No indexes initially
	uuids, err := store.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(uuids) != 0 {
		t.Errorf("expected 0 indexes, got %d", len(uuids))
	}

	// Create multiple indexes
	for i := 1; i <= 3; i++ {
		idx := NewFTSIndex("test", 0)
		idx.IndexDocument(uint64(i), "test document")
		uuid := "test-uuid-" + string(rune('0'+i))
		if err := store.SaveIndex(ctx, idx, uuid); err != nil {
			t.Fatalf("failed to save index %d: %v", i, err)
		}
	}

	// List indexes
	uuids, err = store.ListIndexes(ctx)
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(uuids) != 3 {
		t.Errorf("expected 3 indexes, got %d", len(uuids))
	}
}

func TestPostingListEncoding(t *testing.T) {
	pl := &PostingList{
		DocFreq: 3,
		Postings: []Posting{
			{DocID: 1, TermFreq: 2, Positions: []int{0, 5}},
			{DocID: 5, TermFreq: 1, Positions: []int{3}},
			{DocID: 10, TermFreq: 3, Positions: []int{1, 7, 12}},
		},
	}

	// Encode
	data := encodePostingList(pl)
	if len(data) == 0 {
		t.Fatal("encoded data should not be empty")
	}

	// Decode
	decoded, err := decodePostingList(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	// Verify
	if decoded.DocFreq != pl.DocFreq {
		t.Errorf("DocFreq: got %d, want %d", decoded.DocFreq, pl.DocFreq)
	}
	if len(decoded.Postings) != len(pl.Postings) {
		t.Fatalf("posting count: got %d, want %d", len(decoded.Postings), len(pl.Postings))
	}

	for i, p := range decoded.Postings {
		orig := pl.Postings[i]
		if p.DocID != orig.DocID {
			t.Errorf("posting %d DocID: got %d, want %d", i, p.DocID, orig.DocID)
		}
		if p.TermFreq != orig.TermFreq {
			t.Errorf("posting %d TermFreq: got %d, want %d", i, p.TermFreq, orig.TermFreq)
		}
		if len(p.Positions) != len(orig.Positions) {
			t.Errorf("posting %d positions count: got %d, want %d", i, len(p.Positions), len(orig.Positions))
		}
		for j, pos := range p.Positions {
			if pos != orig.Positions[j] {
				t.Errorf("posting %d position %d: got %d, want %d", i, j, pos, orig.Positions[j])
			}
		}
	}
}

func TestCompressedPostingEncoder(t *testing.T) {
	encoder := &CompressedPostingEncoder{}

	// Test VByte encoding
	testCases := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1<<21 - 1, 1 << 21}
	for _, val := range testCases {
		encoded := encoder.EncodeVByte(val)
		decoded, _ := encoder.DecodeVByte(encoded, 0)
		if decoded != val {
			t.Errorf("VByte roundtrip failed for %d: got %d", val, decoded)
		}
	}

	// Test compressed posting list
	pl := &PostingList{
		DocFreq: 3,
		Postings: []Posting{
			{DocID: 1, TermFreq: 2, Positions: []int{0, 5}},
			{DocID: 5, TermFreq: 1, Positions: []int{3}},
			{DocID: 100, TermFreq: 3, Positions: []int{1, 7, 12}},
		},
	}

	compressed := encoder.EncodeCompressedPostingList(pl)
	decompressed, err := encoder.DecodeCompressedPostingList(compressed)
	if err != nil {
		t.Fatalf("decompression failed: %v", err)
	}

	if decompressed.DocFreq != pl.DocFreq {
		t.Errorf("DocFreq: got %d, want %d", decompressed.DocFreq, pl.DocFreq)
	}
	if len(decompressed.Postings) != len(pl.Postings) {
		t.Fatalf("posting count: got %d, want %d", len(decompressed.Postings), len(pl.Postings))
	}

	// Compare raw and compressed sizes
	rawData := encodePostingList(pl)
	if len(compressed) >= len(rawData) {
		t.Logf("compressed size %d >= raw size %d (may be expected for small data)", len(compressed), len(rawData))
	}
}

func TestFTSPersistenceWithPositions(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	idx := NewFTSIndex("positions_test", 0)

	// Index document where "quick" appears at positions 1 and 6
	idx.IndexDocument(1, "the quick brown fox is quick")

	store := NewFTSIndexStore(dir, nil)
	uuid := "positions-uuid"

	if err := store.SaveIndex(ctx, idx, uuid); err != nil {
		t.Fatalf("failed to save: %v", err)
	}

	loadedIdx, err := store.LoadIndex(ctx, uuid)
	if err != nil {
		t.Fatalf("failed to load: %v", err)
	}

	// Check that positions are preserved
	pl, ok := loadedIdx.invertedIndex["quick"]
	if !ok {
		t.Fatal("term 'quick' not found in loaded index")
	}

	if len(pl.Postings) != 1 {
		t.Fatalf("expected 1 posting, got %d", len(pl.Postings))
	}

	positions := pl.Postings[0].Positions
	if len(positions) != 2 {
		t.Fatalf("expected 2 positions for 'quick', got %d", len(positions))
	}

	// Positions should be 1 and 5 (0-indexed: "the"=0, "quick"=1, "brown"=2, "fox"=3, "is"=4, "quick"=5)
	expectedPositions := []int{1, 5}
	for i, pos := range positions {
		if pos != expectedPositions[i] {
			t.Errorf("position %d: got %d, want %d", i, pos, expectedPositions[i])
		}
	}
}
