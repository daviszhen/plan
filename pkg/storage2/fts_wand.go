// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"container/heap"
	"context"
	"math"
	"sort"
)

// WANDSearcher implements the WAND (Weak AND) algorithm for efficient top-k
// document retrieval. It maintains upper-bound scores for each term to skip
// documents that cannot enter the top-k results.
type WANDSearcher struct {
	idx       *FTSIndex
	k         int
	threshold float64 // Current minimum score in top-k

	// Cursors for each query term
	cursors []*termCursor
}

// termCursor represents a cursor over a term's posting list
type termCursor struct {
	term     string
	postings []Posting
	current  int     // Current position in postings
	maxScore float64 // Maximum possible score for this term
	idf      float64 // IDF value for this term
}

// NewWANDSearcher creates a new WAND searcher
func NewWANDSearcher(idx *FTSIndex, k int) *WANDSearcher {
	return &WANDSearcher{
		idx:       idx,
		k:         k,
		threshold: 0,
		cursors:   make([]*termCursor, 0),
	}
}

// Search executes WAND search and returns top-k results
func (w *WANDSearcher) Search(ctx context.Context, queryTerms []string) []FTSSearchResult {
	// 1. Initialize cursors
	w.initCursors(queryTerms)
	if len(w.cursors) == 0 {
		return nil
	}

	// 2. Use a min-heap to maintain top-k results
	resultHeap := &wandResultHeap{}
	heap.Init(resultHeap)

	// 3. WAND main loop
	iterations := 0
	maxIterations := 1000000 // Safety limit

	for iterations < maxIterations {
		// Check context cancellation
		select {
		case <-ctx.Done():
			break
		default:
		}

		// Sort cursors by current document ID
		w.sortCursorsByDocID()

		// Find pivot
		pivotIdx := w.findPivot()
		if pivotIdx < 0 {
			break // All cursors exhausted or cannot reach threshold
		}

		pivotDocID := w.cursors[pivotIdx].currentDocID()
		if pivotDocID == math.MaxUint64 {
			break // No more documents
		}

		// Check if all cursors up to pivot point to the same document
		if w.allCursorsAtDoc(pivotDocID, pivotIdx) {
			// Compute full score for this document
			score := w.computeFullScore(pivotDocID)

			if score > w.threshold || resultHeap.Len() < w.k {
				// Add to result heap
				heap.Push(resultHeap, FTSSearchResult{DocID: pivotDocID, Score: score})

				// Maintain top-k
				if resultHeap.Len() > w.k {
					heap.Pop(resultHeap)
				}

				// Update threshold
				if resultHeap.Len() == w.k {
					w.threshold = (*resultHeap)[0].Score
				}
			}

			// Advance all cursors past this document
			w.advanceAllCursors()
		} else {
			// Skip: advance smaller cursors to pivot document
			w.advanceCursorsTo(pivotIdx, pivotDocID)
		}

		iterations++
	}

	// 4. Extract results (sorted by score descending)
	results := make([]FTSSearchResult, resultHeap.Len())
	for i := len(results) - 1; i >= 0; i-- {
		results[i] = heap.Pop(resultHeap).(FTSSearchResult)
	}

	return results
}

// initCursors initializes term cursors from query terms
func (w *WANDSearcher) initCursors(queryTerms []string) {
	w.cursors = w.cursors[:0]
	w.threshold = 0

	w.idx.mu.RLock()
	defer w.idx.mu.RUnlock()

	for _, term := range queryTerms {
		pl, ok := w.idx.invertedIndex[term]
		if !ok || pl.DocFreq == 0 {
			continue
		}

		idf := w.idx.computeIDF(pl.DocFreq)

		// Compute maximum possible score for this term
		// Assume high TF and short document for upper bound
		maxTF := 100.0
		minDocLen := w.idx.avgDocLength * 0.5
		if minDocLen < 1 {
			minDocLen = 1
		}
		maxScore := w.computeBM25Score(maxTF, idf, minDocLen)

		w.cursors = append(w.cursors, &termCursor{
			term:     term,
			postings: pl.Postings,
			current:  0,
			maxScore: maxScore,
			idf:      idf,
		})
	}

	// Sort cursors by maxScore descending for better pruning
	sort.Slice(w.cursors, func(i, j int) bool {
		return w.cursors[i].maxScore > w.cursors[j].maxScore
	})
}

// sortCursorsByDocID sorts cursors by their current document ID
func (w *WANDSearcher) sortCursorsByDocID() {
	sort.Slice(w.cursors, func(i, j int) bool {
		return w.cursors[i].currentDocID() < w.cursors[j].currentDocID()
	})
}

// findPivot finds the pivot position where cumulative maxScore >= threshold
func (w *WANDSearcher) findPivot() int {
	var sumScore float64
	for i, cursor := range w.cursors {
		if cursor.exhausted() {
			continue
		}
		sumScore += cursor.maxScore
		if sumScore >= w.threshold {
			return i
		}
	}

	// If we have results less than k, return the last non-exhausted cursor
	for i := len(w.cursors) - 1; i >= 0; i-- {
		if !w.cursors[i].exhausted() {
			return i
		}
	}

	return -1 // Cannot reach threshold
}

// allCursorsAtDoc checks if all cursors up to pivotIdx point to the same document
func (w *WANDSearcher) allCursorsAtDoc(docID uint64, pivotIdx int) bool {
	for i := 0; i <= pivotIdx; i++ {
		if w.cursors[i].exhausted() {
			continue
		}
		if w.cursors[i].currentDocID() != docID {
			return false
		}
	}
	return true
}

// computeFullScore computes the full BM25 score for a document
func (w *WANDSearcher) computeFullScore(docID uint64) float64 {
	var score float64
	docLen := float64(w.idx.docLengths[docID])
	if docLen == 0 {
		docLen = w.idx.avgDocLength
	}

	for _, cursor := range w.cursors {
		if cursor.exhausted() {
			continue
		}
		if cursor.currentDocID() == docID {
			tf := float64(cursor.currentPosting().TermFreq)
			score += w.computeBM25Score(tf, cursor.idf, docLen)
		}
	}

	return score
}

// computeBM25Score computes BM25 score for a term occurrence
func (w *WANDSearcher) computeBM25Score(tf, idf, docLen float64) float64 {
	const k1 = 1.2
	const b = 0.75

	avgDocLen := w.idx.avgDocLength
	if avgDocLen == 0 {
		avgDocLen = 1
	}

	num := tf * (k1 + 1)
	den := tf + k1*(1-b+b*docLen/avgDocLen)
	return idf * num / den
}

// advanceAllCursors advances all non-exhausted cursors by one position
func (w *WANDSearcher) advanceAllCursors() {
	for _, cursor := range w.cursors {
		if !cursor.exhausted() {
			cursor.advance()
		}
	}
}

// advanceCursorsTo advances cursors before pivotIdx to the pivot document
func (w *WANDSearcher) advanceCursorsTo(pivotIdx int, docID uint64) {
	for i := 0; i < pivotIdx; i++ {
		if !w.cursors[i].exhausted() && w.cursors[i].currentDocID() < docID {
			w.cursors[i].advanceTo(docID)
		}
	}
}

// termCursor methods

func (c *termCursor) exhausted() bool {
	return c.current >= len(c.postings)
}

func (c *termCursor) currentDocID() uint64 {
	if c.exhausted() {
		return math.MaxUint64
	}
	return c.postings[c.current].DocID
}

func (c *termCursor) currentPosting() *Posting {
	if c.exhausted() {
		return nil
	}
	return &c.postings[c.current]
}

func (c *termCursor) advance() {
	c.current++
}

func (c *termCursor) advanceTo(docID uint64) {
	// Binary search for efficiency on large posting lists
	left, right := c.current, len(c.postings)
	for left < right {
		mid := (left + right) / 2
		if c.postings[mid].DocID < docID {
			left = mid + 1
		} else {
			right = mid
		}
	}
	c.current = left
}

// wandResultHeap is a min-heap for maintaining top-k results
type wandResultHeap []FTSSearchResult

func (h wandResultHeap) Len() int           { return len(h) }
func (h wandResultHeap) Less(i, j int) bool { return h[i].Score < h[j].Score } // Min-heap
func (h wandResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *wandResultHeap) Push(x interface{}) {
	*h = append(*h, x.(FTSSearchResult))
}

func (h *wandResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// BlockMaxWANDSearcher implements Block-Max WAND for even better performance.
// It divides posting lists into blocks and maintains per-block maximum scores.
type BlockMaxWANDSearcher struct {
	idx       *FTSIndex
	k         int
	threshold float64
	blockSize int

	cursors []*blockMaxCursor
}

// blockMaxCursor extends termCursor with block-level max scores
type blockMaxCursor struct {
	termCursor
	blockSize   int
	blockMaxTFs []float64 // Maximum TF in each block
}

// NewBlockMaxWANDSearcher creates a Block-Max WAND searcher
func NewBlockMaxWANDSearcher(idx *FTSIndex, k int, blockSize int) *BlockMaxWANDSearcher {
	if blockSize <= 0 {
		blockSize = 128 // Default block size
	}
	return &BlockMaxWANDSearcher{
		idx:       idx,
		k:         k,
		threshold: 0,
		blockSize: blockSize,
		cursors:   make([]*blockMaxCursor, 0),
	}
}

// Search executes Block-Max WAND search
func (w *BlockMaxWANDSearcher) Search(ctx context.Context, queryTerms []string) []FTSSearchResult {
	// Initialize cursors with block max scores
	w.initBlockMaxCursors(queryTerms)
	if len(w.cursors) == 0 {
		return nil
	}

	resultHeap := &wandResultHeap{}
	heap.Init(resultHeap)

	iterations := 0
	maxIterations := 1000000

	for iterations < maxIterations {
		select {
		case <-ctx.Done():
			break
		default:
		}

		// Sort by current doc ID
		sort.Slice(w.cursors, func(i, j int) bool {
			return w.cursors[i].currentDocID() < w.cursors[j].currentDocID()
		})

		// Find pivot using block max scores
		pivotIdx := w.findBlockMaxPivot()
		if pivotIdx < 0 {
			break
		}

		pivotDocID := w.cursors[pivotIdx].currentDocID()
		if pivotDocID == math.MaxUint64 {
			break
		}

		// Check alignment
		if w.allCursorsAtDoc(pivotDocID, pivotIdx) {
			score := w.computeFullScore(pivotDocID)

			if score > w.threshold || resultHeap.Len() < w.k {
				heap.Push(resultHeap, FTSSearchResult{DocID: pivotDocID, Score: score})
				if resultHeap.Len() > w.k {
					heap.Pop(resultHeap)
				}
				if resultHeap.Len() == w.k {
					w.threshold = (*resultHeap)[0].Score
				}
			}

			w.advanceAllCursors()
		} else {
			w.advanceCursorsTo(pivotIdx, pivotDocID)
		}

		iterations++
	}

	results := make([]FTSSearchResult, resultHeap.Len())
	for i := len(results) - 1; i >= 0; i-- {
		results[i] = heap.Pop(resultHeap).(FTSSearchResult)
	}

	return results
}

// initBlockMaxCursors initializes cursors with block-level max TF values
func (w *BlockMaxWANDSearcher) initBlockMaxCursors(queryTerms []string) {
	w.cursors = w.cursors[:0]
	w.threshold = 0

	w.idx.mu.RLock()
	defer w.idx.mu.RUnlock()

	for _, term := range queryTerms {
		pl, ok := w.idx.invertedIndex[term]
		if !ok || pl.DocFreq == 0 {
			continue
		}

		idf := w.idx.computeIDF(pl.DocFreq)

		// Compute block max TFs
		numBlocks := (len(pl.Postings) + w.blockSize - 1) / w.blockSize
		blockMaxTFs := make([]float64, numBlocks)

		for i, posting := range pl.Postings {
			blockIdx := i / w.blockSize
			tf := float64(posting.TermFreq)
			if tf > blockMaxTFs[blockIdx] {
				blockMaxTFs[blockIdx] = tf
			}
		}

		// Compute overall max score
		var maxTF float64
		for _, tf := range blockMaxTFs {
			if tf > maxTF {
				maxTF = tf
			}
		}

		minDocLen := w.idx.avgDocLength * 0.5
		if minDocLen < 1 {
			minDocLen = 1
		}
		maxScore := w.computeBM25Score(maxTF, idf, minDocLen)

		w.cursors = append(w.cursors, &blockMaxCursor{
			termCursor: termCursor{
				term:     term,
				postings: pl.Postings,
				current:  0,
				maxScore: maxScore,
				idf:      idf,
			},
			blockSize:   w.blockSize,
			blockMaxTFs: blockMaxTFs,
		})
	}

	sort.Slice(w.cursors, func(i, j int) bool {
		return w.cursors[i].maxScore > w.cursors[j].maxScore
	})
}

// findBlockMaxPivot finds pivot using block-level max scores for tighter bounds
func (w *BlockMaxWANDSearcher) findBlockMaxPivot() int {
	var sumScore float64
	for i, cursor := range w.cursors {
		if cursor.exhausted() {
			continue
		}

		// Use block max score instead of global max score
		blockIdx := cursor.current / cursor.blockSize
		if blockIdx < len(cursor.blockMaxTFs) {
			blockMaxTF := cursor.blockMaxTFs[blockIdx]
			minDocLen := w.idx.avgDocLength * 0.5
			if minDocLen < 1 {
				minDocLen = 1
			}
			blockMaxScore := w.computeBM25Score(blockMaxTF, cursor.idf, minDocLen)
			sumScore += blockMaxScore
		} else {
			sumScore += cursor.maxScore
		}

		if sumScore >= w.threshold {
			return i
		}
	}

	for i := len(w.cursors) - 1; i >= 0; i-- {
		if !w.cursors[i].exhausted() {
			return i
		}
	}

	return -1
}

func (w *BlockMaxWANDSearcher) allCursorsAtDoc(docID uint64, pivotIdx int) bool {
	for i := 0; i <= pivotIdx; i++ {
		if w.cursors[i].exhausted() {
			continue
		}
		if w.cursors[i].currentDocID() != docID {
			return false
		}
	}
	return true
}

func (w *BlockMaxWANDSearcher) computeFullScore(docID uint64) float64 {
	var score float64
	docLen := float64(w.idx.docLengths[docID])
	if docLen == 0 {
		docLen = w.idx.avgDocLength
	}

	for _, cursor := range w.cursors {
		if cursor.exhausted() {
			continue
		}
		if cursor.currentDocID() == docID {
			tf := float64(cursor.currentPosting().TermFreq)
			score += w.computeBM25Score(tf, cursor.idf, docLen)
		}
	}

	return score
}

func (w *BlockMaxWANDSearcher) computeBM25Score(tf, idf, docLen float64) float64 {
	const k1 = 1.2
	const b = 0.75

	avgDocLen := w.idx.avgDocLength
	if avgDocLen == 0 {
		avgDocLen = 1
	}

	num := tf * (k1 + 1)
	den := tf + k1*(1-b+b*docLen/avgDocLen)
	return idf * num / den
}

func (w *BlockMaxWANDSearcher) advanceAllCursors() {
	for _, cursor := range w.cursors {
		if !cursor.exhausted() {
			cursor.advance()
		}
	}
}

func (w *BlockMaxWANDSearcher) advanceCursorsTo(pivotIdx int, docID uint64) {
	for i := 0; i < pivotIdx; i++ {
		if !w.cursors[i].exhausted() && w.cursors[i].currentDocID() < docID {
			w.cursors[i].advanceTo(docID)
		}
	}
}
