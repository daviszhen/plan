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
	"sort"
	"strings"
	"sync"
	"unicode"
)

// Tokenizer splits text into tokens for indexing and searching.
type Tokenizer interface {
	Tokenize(text string) []Token
}

// Token represents a single term with its position in the source text.
type Token struct {
	Term     string
	Position int
}

// SimpleTokenizer splits on non-alphanumeric boundaries and lowercases.
type SimpleTokenizer struct{}

// NewSimpleTokenizer creates a simple whitespace/punctuation tokenizer.
func NewSimpleTokenizer() *SimpleTokenizer {
	return &SimpleTokenizer{}
}

// Tokenize splits text into lowercase tokens.
func (t *SimpleTokenizer) Tokenize(text string) []Token {
	var tokens []Token
	var buf strings.Builder
	pos := 0

	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			buf.WriteRune(unicode.ToLower(r))
		} else {
			if buf.Len() > 0 {
				tokens = append(tokens, Token{Term: buf.String(), Position: pos})
				pos++
				buf.Reset()
			}
		}
	}
	if buf.Len() > 0 {
		tokens = append(tokens, Token{Term: buf.String(), Position: pos})
	}
	return tokens
}

// ChineseTokenizer uses forward maximum matching with a dictionary,
// falling back to single-character segmentation for unknown words.
type ChineseTokenizer struct {
	dictionary map[string]bool
	maxWordLen int
}

// NewChineseTokenizer creates a Chinese tokenizer.
func NewChineseTokenizer() *ChineseTokenizer {
	return &ChineseTokenizer{
		dictionary: make(map[string]bool),
		maxWordLen: 4,
	}
}

// AddWord adds a word to the dictionary.
func (t *ChineseTokenizer) AddWord(word string) {
	t.dictionary[word] = true
	runeLen := len([]rune(word))
	if runeLen > t.maxWordLen {
		t.maxWordLen = runeLen
	}
}

// Tokenize segments text using forward maximum matching.
func (t *ChineseTokenizer) Tokenize(text string) []Token {
	var tokens []Token
	runes := []rune(text)
	pos := 0
	i := 0

	for i < len(runes) {
		r := runes[i]

		if !isChinese(r) {
			if unicode.IsLetter(r) || unicode.IsNumber(r) {
				var word strings.Builder
				for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsNumber(runes[i])) && !isChinese(runes[i]) {
					word.WriteRune(unicode.ToLower(runes[i]))
					i++
				}
				if word.Len() > 0 {
					tokens = append(tokens, Token{Term: word.String(), Position: pos})
					pos++
				}
			} else {
				i++
			}
			continue
		}

		// Forward maximum matching
		matched := false
		for length := t.maxWordLen; length > 1; length-- {
			if i+length > len(runes) {
				continue
			}
			word := string(runes[i : i+length])
			if t.dictionary[word] {
				tokens = append(tokens, Token{Term: word, Position: pos})
				pos++
				i += length
				matched = true
				break
			}
		}
		if !matched {
			tokens = append(tokens, Token{Term: string(r), Position: pos})
			pos++
			i++
		}
	}
	return tokens
}

func isChinese(r rune) bool {
	return r >= 0x4e00 && r <= 0x9fff
}

// --- Posting list ---

// PostingList stores all occurrences of a term across documents.
type PostingList struct {
	DocFreq  int
	Postings []Posting
}

// Posting stores a term's occurrences in a single document.
type Posting struct {
	DocID     uint64
	TermFreq  int
	Positions []int // for phrase search
}

// --- FTS Index ---

// FTSIndex is an in-memory inverted index supporting BM25 scoring, phrase search,
// and boolean queries.
type FTSIndex struct {
	name      string
	columnIdx int
	indexType IndexType

	invertedIndex map[string]*PostingList
	docLengths    map[uint64]int
	totalDocs     int
	avgDocLength  float64
	totalTerms    int64

	tokenizer Tokenizer

	mu    sync.RWMutex
	stats IndexStats
}

// NewFTSIndex creates a full-text search index.
func NewFTSIndex(name string, columnIdx int) *FTSIndex {
	return &FTSIndex{
		name:          name,
		columnIdx:     columnIdx,
		indexType:     InvertedIndex,
		invertedIndex: make(map[string]*PostingList),
		docLengths:    make(map[uint64]int),
		tokenizer:     NewSimpleTokenizer(),
	}
}

// SetTokenizer replaces the default tokenizer.
func (idx *FTSIndex) SetTokenizer(tok Tokenizer) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.tokenizer = tok
}

// Name returns the index name.
func (idx *FTSIndex) Name() string { return idx.name }

// Type returns InvertedIndex.
func (idx *FTSIndex) Type() IndexType { return idx.indexType }

// Columns returns the indexed column.
func (idx *FTSIndex) Columns() []int { return []int{idx.columnIdx} }

// Statistics returns index stats.
func (idx *FTSIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return IndexStats{
		NumEntries: uint64(idx.totalDocs),
		SizeBytes:  uint64(len(idx.invertedIndex) * 64), // rough estimate
		IndexType:  "fts",
	}
}

// Search performs a BM25-ranked text search (implements Index).
func (idx *FTSIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	q, ok := query.(string)
	if !ok {
		return nil, fmt.Errorf("FTS query must be string, got %T", query)
	}
	results, err := idx.SearchWithScores(ctx, q, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(results))
	for i, r := range results {
		ids[i] = r.DocID
	}
	return ids, nil
}

// TextSearch implements InvertedIndexImpl.
func (idx *FTSIndex) TextSearch(ctx context.Context, query string, limit int) ([]uint64, []float32, error) {
	results, err := idx.SearchWithScores(ctx, query, limit)
	if err != nil {
		return nil, nil, err
	}
	ids := make([]uint64, len(results))
	scores := make([]float32, len(results))
	for i, r := range results {
		ids[i] = r.DocID
		scores[i] = float32(r.Score)
	}
	return ids, scores, nil
}

// PhraseSearch implements InvertedIndexImpl with position-aware matching.
func (idx *FTSIndex) PhraseSearch(ctx context.Context, phrase string, limit int) ([]uint64, error) {
	results, err := idx.phraseSearchInternal(ctx, phrase, limit)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(results))
	for i, r := range results {
		ids[i] = r.DocID
	}
	return ids, nil
}

// IndexDocument adds a document to the index.
func (idx *FTSIndex) IndexDocument(docID uint64, text string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tokens := idx.tokenizer.Tokenize(text)
	if len(tokens) == 0 {
		return nil
	}

	// Group by term: collect freq and positions
	type termInfo struct {
		freq      int
		positions []int
	}
	termStats := make(map[string]*termInfo)
	for _, tok := range tokens {
		ti, ok := termStats[tok.Term]
		if !ok {
			ti = &termInfo{}
			termStats[tok.Term] = ti
		}
		ti.freq++
		ti.positions = append(ti.positions, tok.Position)
	}

	for term, ti := range termStats {
		pl, ok := idx.invertedIndex[term]
		if !ok {
			pl = &PostingList{}
			idx.invertedIndex[term] = pl
		}
		pl.DocFreq++
		pl.Postings = append(pl.Postings, Posting{
			DocID:     docID,
			TermFreq:  ti.freq,
			Positions: ti.positions,
		})
	}

	idx.docLengths[docID] = len(tokens)
	idx.totalDocs++
	idx.totalTerms += int64(len(tokens))
	if idx.totalDocs > 0 {
		idx.avgDocLength = float64(idx.totalTerms) / float64(idx.totalDocs)
	}

	return nil
}

// --- BM25 search ---

// SearchResult holds a document ID and its relevance score.
type FTSSearchResult struct {
	DocID uint64
	Score float64
}

// SearchWithScores returns BM25-ranked results.
func (idx *FTSIndex) SearchWithScores(ctx context.Context, queryStr string, limit int) ([]FTSSearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	tokens := idx.tokenizer.Tokenize(queryStr)
	if len(tokens) == 0 {
		return nil, nil
	}

	docScores := make(map[uint64]float64)

	for _, tok := range tokens {
		pl, ok := idx.invertedIndex[tok.Term]
		if !ok {
			continue
		}
		idf := idx.computeIDF(pl.DocFreq)
		for _, p := range pl.Postings {
			docScores[p.DocID] += idx.computeBM25(float64(p.TermFreq), idf, float64(idx.docLengths[p.DocID]))
		}
	}

	results := make([]FTSSearchResult, 0, len(docScores))
	for id, score := range docScores {
		results = append(results, FTSSearchResult{DocID: id, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > len(results) {
		limit = len(results)
	}
	return results[:limit], nil
}

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

func (idx *FTSIndex) computeIDF(docFreq int) float64 {
	n := float64(idx.totalDocs)
	df := float64(docFreq)
	return math.Log(1 + (n-df+0.5)/(df+0.5))
}

func (idx *FTSIndex) computeBM25(tf, idf, docLen float64) float64 {
	num := tf * (bm25K1 + 1)
	den := tf + bm25K1*(1-bm25B+bm25B*docLen/idx.avgDocLength)
	return idf * num / den
}

// --- Phrase search ---

func (idx *FTSIndex) phraseSearchInternal(ctx context.Context, phrase string, limit int) ([]FTSSearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	tokens := idx.tokenizer.Tokenize(phrase)
	if len(tokens) < 2 {
		return idx.SearchWithScores(ctx, phrase, limit)
	}

	// Get posting list for each phrase term
	termPostings := make([]*PostingList, len(tokens))
	for i, tok := range tokens {
		pl, ok := idx.invertedIndex[tok.Term]
		if !ok {
			return nil, nil // term missing -> no match
		}
		termPostings[i] = pl
	}

	// Intersect on documents that contain the first term
	var results []FTSSearchResult
	for _, posting := range termPostings[0].Postings {
		docID := posting.DocID
		if idx.docContainsPhrase(docID, tokens) {
			results = append(results, FTSSearchResult{DocID: docID, Score: float64(len(tokens))})
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	if limit > len(results) {
		limit = len(results)
	}
	return results[:limit], nil
}

func (idx *FTSIndex) docContainsPhrase(docID uint64, tokens []Token) bool {
	// Collect positions per term in this doc
	positions := make([][]int, len(tokens))
	for i, tok := range tokens {
		pl, ok := idx.invertedIndex[tok.Term]
		if !ok {
			return false
		}
		found := false
		for _, p := range pl.Postings {
			if p.DocID == docID {
				positions[i] = p.Positions
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check consecutive positions
	for _, startPos := range positions[0] {
		match := true
		for i := 1; i < len(tokens); i++ {
			expected := startPos + i
			if !containsInt(positions[i], expected) {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

func containsInt(slice []int, val int) bool {
	for _, v := range slice {
		if v == val {
			return true
		}
	}
	return false
}

// --- Boolean query ---

// BooleanQuery supports must / should / must_not semantics.
type BooleanQuery struct {
	Must    []string // all required
	Should  []string // boost score
	MustNot []string // exclude
}

// BooleanSearch executes a boolean query.
func (idx *FTSIndex) BooleanSearch(ctx context.Context, query BooleanQuery, limit int) ([]FTSSearchResult, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	mustDocs := make(map[uint64]float64)

	if len(query.Must) > 0 {
		// Start with first must term
		first := query.Must[0]
		if pl, ok := idx.invertedIndex[first]; ok {
			for _, p := range pl.Postings {
				mustDocs[p.DocID] = float64(p.TermFreq)
			}
		}

		// Intersect with remaining must terms
		for i := 1; i < len(query.Must); i++ {
			pl, ok := idx.invertedIndex[query.Must[i]]
			if !ok {
				return nil, nil // no match
			}
			termDocs := make(map[uint64]bool)
			for _, p := range pl.Postings {
				termDocs[p.DocID] = true
			}
			for docID := range mustDocs {
				if !termDocs[docID] {
					delete(mustDocs, docID)
				}
			}
		}
	}

	// Exclude must_not
	for _, term := range query.MustNot {
		if pl, ok := idx.invertedIndex[term]; ok {
			for _, p := range pl.Postings {
				delete(mustDocs, p.DocID)
			}
		}
	}

	// Boost with should terms
	for _, term := range query.Should {
		if pl, ok := idx.invertedIndex[term]; ok {
			for _, p := range pl.Postings {
				if _, ok := mustDocs[p.DocID]; ok {
					mustDocs[p.DocID] += float64(p.TermFreq)
				}
			}
		}
	}

	results := make([]FTSSearchResult, 0, len(mustDocs))
	for docID, score := range mustDocs {
		results = append(results, FTSSearchResult{DocID: docID, Score: score})
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	if limit > len(results) {
		limit = len(results)
	}
	return results[:limit], nil
}

// Verify interface compliance.
var _ InvertedIndexImpl = (*FTSIndex)(nil)
