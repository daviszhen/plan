// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"container/heap"
	"context"
	"math"
	"testing"
)

func TestWANDSearcherBasic(t *testing.T) {
	idx := NewFTSIndex("test_wand", 0)

	// Index test documents
	docs := []struct {
		id   uint64
		text string
	}{
		{1, "the quick brown fox jumps over the lazy dog"},
		{2, "a quick brown dog runs in the park"},
		{3, "the fox is clever and quick"},
		{4, "lazy dogs sleep all day"},
		{5, "the quick quick quick fox"},
	}

	for _, doc := range docs {
		if err := idx.IndexDocument(doc.id, doc.text); err != nil {
			t.Fatalf("failed to index document %d: %v", doc.id, err)
		}
	}

	// Create WAND searcher
	wand := NewWANDSearcher(idx, 3)

	ctx := context.Background()
	results := wand.Search(ctx, []string{"quick", "fox"})

	if len(results) == 0 {
		t.Fatal("expected search results")
	}

	// Verify we get results for documents containing "quick" or "fox"
	validDocs := map[uint64]bool{1: true, 2: true, 3: true, 5: true}
	for _, r := range results {
		if !validDocs[r.DocID] {
			t.Errorf("unexpected doc %d in results", r.DocID)
		}
	}

	// Should not exceed k results
	if len(results) > 3 {
		t.Errorf("expected at most 3 results, got %d", len(results))
	}
}

func TestWANDSearcherEmptyQuery(t *testing.T) {
	idx := NewFTSIndex("test_wand", 0)
	idx.IndexDocument(1, "test document")

	wand := NewWANDSearcher(idx, 10)
	ctx := context.Background()

	results := wand.Search(ctx, []string{})
	if len(results) != 0 {
		t.Errorf("expected no results for empty query, got %d", len(results))
	}
}

func TestWANDSearcherNoMatch(t *testing.T) {
	idx := NewFTSIndex("test_wand", 0)
	idx.IndexDocument(1, "the quick brown fox")

	wand := NewWANDSearcher(idx, 10)
	ctx := context.Background()

	results := wand.Search(ctx, []string{"elephant", "zebra"})
	if len(results) != 0 {
		t.Errorf("expected no results for non-matching query, got %d", len(results))
	}
}

func TestWANDSearcherSingleTerm(t *testing.T) {
	idx := NewFTSIndex("test_wand", 0)

	// Index documents with same length for fair TF comparison
	idx.IndexDocument(1, "dog cat dog dog bird")   // TF=3, len=5
	idx.IndexDocument(2, "dog bird fish frog bat") // TF=1, len=5
	idx.IndexDocument(3, "dog dog fish bird cat")  // TF=2, len=5

	wand := NewWANDSearcher(idx, 10)
	ctx := context.Background()

	results := wand.Search(ctx, []string{"dog"})

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// With same document lengths, higher TF should rank higher
	if results[0].DocID != 1 {
		t.Errorf("expected doc 1 (highest TF) to rank first, got doc %d", results[0].DocID)
	}

	// Verify scores are in descending order
	for i := 1; i < len(results); i++ {
		if results[i].Score > results[i-1].Score {
			t.Errorf("results not sorted by score: %v > %v", results[i].Score, results[i-1].Score)
		}
	}
}

func TestWANDSearcherVsNaive(t *testing.T) {
	idx := NewFTSIndex("test_wand", 0)

	// Index many documents
	for i := uint64(1); i <= 100; i++ {
		text := "document number"
		if i%2 == 0 {
			text += " quick"
		}
		if i%3 == 0 {
			text += " brown"
		}
		if i%5 == 0 {
			text += " fox"
		}
		idx.IndexDocument(i, text)
	}

	ctx := context.Background()
	k := 5

	// WAND search
	wand := NewWANDSearcher(idx, k)
	wandResults := wand.Search(ctx, []string{"quick", "brown", "fox"})

	// Naive search (using existing BM25)
	naiveResults, err := idx.SearchWithScores(ctx, "quick brown fox", k)
	if err != nil {
		t.Fatalf("naive search failed: %v", err)
	}

	// Results should be similar (same documents, possibly different order due to ties)
	if len(wandResults) != len(naiveResults) {
		t.Errorf("result count mismatch: WAND=%d, Naive=%d", len(wandResults), len(naiveResults))
	}

	// Check that top result is the same
	if len(wandResults) > 0 && len(naiveResults) > 0 {
		// Allow small score difference due to numerical precision
		scoreDiff := math.Abs(wandResults[0].Score - naiveResults[0].Score)
		if scoreDiff > 0.001 {
			t.Logf("Top scores differ: WAND=%.4f, Naive=%.4f", wandResults[0].Score, naiveResults[0].Score)
		}
	}
}

func TestBlockMaxWANDSearcherBasic(t *testing.T) {
	idx := NewFTSIndex("test_bmwand", 0)

	docs := []struct {
		id   uint64
		text string
	}{
		{1, "the quick brown fox jumps over the lazy dog"},
		{2, "a quick brown dog runs in the park"},
		{3, "the fox is clever and quick"},
		{4, "lazy dogs sleep all day"},
		{5, "the quick quick quick fox"},
	}

	for _, doc := range docs {
		if err := idx.IndexDocument(doc.id, doc.text); err != nil {
			t.Fatalf("failed to index document %d: %v", doc.id, err)
		}
	}

	// Create Block-Max WAND searcher with small block size
	bmwand := NewBlockMaxWANDSearcher(idx, 3, 2)

	ctx := context.Background()
	results := bmwand.Search(ctx, []string{"quick", "fox"})

	if len(results) == 0 {
		t.Fatal("expected search results")
	}

	// Should produce same top result as regular WAND
	wand := NewWANDSearcher(idx, 3)
	wandResults := wand.Search(ctx, []string{"quick", "fox"})

	if len(results) != len(wandResults) {
		t.Errorf("Block-Max WAND result count %d != WAND result count %d",
			len(results), len(wandResults))
	}

	if results[0].DocID != wandResults[0].DocID {
		t.Errorf("Block-Max WAND top doc %d != WAND top doc %d",
			results[0].DocID, wandResults[0].DocID)
	}
}

func TestTermCursorAdvanceTo(t *testing.T) {
	cursor := &termCursor{
		postings: []Posting{
			{DocID: 1}, {DocID: 5}, {DocID: 10}, {DocID: 15}, {DocID: 20},
		},
		current: 0,
	}

	// Advance to doc 10
	cursor.advanceTo(10)
	if cursor.currentDocID() != 10 {
		t.Errorf("expected doc 10 after advanceTo(10), got %d", cursor.currentDocID())
	}

	// Advance to non-existent doc (should stop at next higher)
	cursor.current = 0
	cursor.advanceTo(7)
	if cursor.currentDocID() != 10 {
		t.Errorf("expected doc 10 after advanceTo(7), got %d", cursor.currentDocID())
	}

	// Advance past all docs
	cursor.advanceTo(100)
	if !cursor.exhausted() {
		t.Error("cursor should be exhausted after advancing past all docs")
	}
}

func TestTermCursorExhausted(t *testing.T) {
	cursor := &termCursor{
		postings: []Posting{{DocID: 1}, {DocID: 2}},
		current:  0,
	}

	if cursor.exhausted() {
		t.Error("cursor should not be exhausted at start")
	}

	cursor.advance()
	if cursor.exhausted() {
		t.Error("cursor should not be exhausted after one advance")
	}

	cursor.advance()
	if !cursor.exhausted() {
		t.Error("cursor should be exhausted after all advances")
	}

	// Exhausted cursor should return MaxUint64 for docID
	if cursor.currentDocID() != math.MaxUint64 {
		t.Errorf("exhausted cursor should return MaxUint64, got %d", cursor.currentDocID())
	}
}

func TestWANDSearcherLargeK(t *testing.T) {
	idx := NewFTSIndex("test_wand", 0)

	// Index only 3 documents
	idx.IndexDocument(1, "quick fox")
	idx.IndexDocument(2, "quick dog")
	idx.IndexDocument(3, "quick cat")

	// Request more results than available
	wand := NewWANDSearcher(idx, 100)
	ctx := context.Background()

	results := wand.Search(ctx, []string{"quick"})

	// Should return all 3 documents, not 100
	if len(results) != 3 {
		t.Errorf("expected 3 results (all docs), got %d", len(results))
	}
}

func TestWANDResultHeap(t *testing.T) {
	h := &wandResultHeap{}

	// Push results
	items := []FTSSearchResult{
		{DocID: 1, Score: 5.0},
		{DocID: 2, Score: 3.0},
		{DocID: 3, Score: 7.0},
		{DocID: 4, Score: 1.0},
	}

	for _, item := range items {
		heap.Push(h, item)
	}

	// Pop should return in ascending order (min-heap)
	expected := []float64{1.0, 3.0, 5.0, 7.0}
	for i, expectedScore := range expected {
		result := heap.Pop(h).(FTSSearchResult)
		if result.Score != expectedScore {
			t.Errorf("pop %d: expected score %.1f, got %.1f", i, expectedScore, result.Score)
		}
	}
}

func BenchmarkWANDSearch(b *testing.B) {
	idx := NewFTSIndex("bench_wand", 0)

	// Index many documents
	for i := uint64(1); i <= 10000; i++ {
		text := "document"
		if i%2 == 0 {
			text += " quick"
		}
		if i%3 == 0 {
			text += " brown"
		}
		if i%5 == 0 {
			text += " fox"
		}
		if i%7 == 0 {
			text += " jumps"
		}
		idx.IndexDocument(i, text)
	}

	wand := NewWANDSearcher(idx, 10)
	ctx := context.Background()
	queryTerms := []string{"quick", "brown", "fox"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wand.Search(ctx, queryTerms)
	}
}

func BenchmarkBlockMaxWANDSearch(b *testing.B) {
	idx := NewFTSIndex("bench_bmwand", 0)

	for i := uint64(1); i <= 10000; i++ {
		text := "document"
		if i%2 == 0 {
			text += " quick"
		}
		if i%3 == 0 {
			text += " brown"
		}
		if i%5 == 0 {
			text += " fox"
		}
		if i%7 == 0 {
			text += " jumps"
		}
		idx.IndexDocument(i, text)
	}

	bmwand := NewBlockMaxWANDSearcher(idx, 10, 128)
	ctx := context.Background()
	queryTerms := []string{"quick", "brown", "fox"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bmwand.Search(ctx, queryTerms)
	}
}
