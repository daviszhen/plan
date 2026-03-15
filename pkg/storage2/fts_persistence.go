// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// FTSIndexMeta holds metadata for a persisted FTS index
type FTSIndexMeta struct {
	Version       int     `json:"version"`
	Name          string  `json:"name"`
	ColumnIdx     int     `json:"column_idx"`
	TotalDocs     int     `json:"total_docs"`
	TotalTerms    int64   `json:"total_terms"`
	AvgDocLength  float64 `json:"avg_doc_length"`
	NumTerms      int     `json:"num_terms"`
	NumPostings   int     `json:"num_postings"`
	HasPositions  bool    `json:"has_positions"`
	TokenizerType string  `json:"tokenizer_type"`
}

// FTSIndexStore handles persistence for FTS indexes
type FTSIndexStore struct {
	basePath string
	store    ObjectStoreExt
}

// NewFTSIndexStore creates a new FTS index store
func NewFTSIndexStore(basePath string, store ObjectStoreExt) *FTSIndexStore {
	return &FTSIndexStore{
		basePath: basePath,
		store:    store,
	}
}

// SaveIndex persists an FTS index to storage
func (s *FTSIndexStore) SaveIndex(ctx context.Context, idx *FTSIndex, uuid string) error {
	indexPath := filepath.Join(s.basePath, "_indices", uuid)

	// Ensure directory exists
	if err := os.MkdirAll(indexPath, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	// Create postings directory
	postingsPath := filepath.Join(indexPath, "postings")
	if err := os.MkdirAll(postingsPath, 0755); err != nil {
		return fmt.Errorf("failed to create postings directory: %w", err)
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// 1. Save metadata
	meta := &FTSIndexMeta{
		Version:       1,
		Name:          idx.name,
		ColumnIdx:     idx.columnIdx,
		TotalDocs:     idx.totalDocs,
		TotalTerms:    idx.totalTerms,
		AvgDocLength:  idx.avgDocLength,
		NumTerms:      len(idx.invertedIndex),
		HasPositions:  true,
		TokenizerType: "simple",
	}

	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	metaPath := filepath.Join(indexPath, "meta.json")
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// 2. Build term dictionary with sorted terms for deterministic output
	terms := make([]string, 0, len(idx.invertedIndex))
	for term := range idx.invertedIndex {
		terms = append(terms, term)
	}
	sort.Strings(terms)

	termToID := make(map[string]int, len(terms))
	for i, term := range terms {
		termToID[term] = i
	}

	// Save terms dictionary
	termsData, err := json.MarshalIndent(termToID, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal terms: %w", err)
	}
	termsPath := filepath.Join(indexPath, "terms.json")
	if err := os.WriteFile(termsPath, termsData, 0644); err != nil {
		return fmt.Errorf("failed to write terms: %w", err)
	}

	// 3. Save posting lists
	for term, termID := range termToID {
		pl := idx.invertedIndex[term]
		if pl == nil {
			continue
		}

		data := encodePostingList(pl)
		filePath := filepath.Join(postingsPath, fmt.Sprintf("%04d.bin", termID))
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			return fmt.Errorf("failed to write posting list for term %q: %w", term, err)
		}
	}

	// 4. Save document lengths
	if err := s.saveDocLengths(ctx, idx, indexPath); err != nil {
		return fmt.Errorf("failed to save doc lengths: %w", err)
	}

	return nil
}

// saveDocLengths saves document length information
func (s *FTSIndexStore) saveDocLengths(ctx context.Context, idx *FTSIndex, indexPath string) error {
	// Sort doc IDs for deterministic output
	docIDs := make([]uint64, 0, len(idx.docLengths))
	for docID := range idx.docLengths {
		docIDs = append(docIDs, docID)
	}
	sort.Slice(docIDs, func(i, j int) bool { return docIDs[i] < docIDs[j] })

	// Format: [count:4][docID:8, length:4]...
	buf := make([]byte, 0, 4+len(docIDs)*12)

	// Number of entries
	countBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(countBuf, uint32(len(docIDs)))
	buf = append(buf, countBuf...)

	// Doc lengths
	for _, docID := range docIDs {
		docIDBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBuf, docID)
		buf = append(buf, docIDBuf...)

		lenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(lenBuf, uint32(idx.docLengths[docID]))
		buf = append(buf, lenBuf...)
	}

	filePath := filepath.Join(indexPath, "doc_lengths.bin")
	return os.WriteFile(filePath, buf, 0644)
}

// LoadIndex loads an FTS index from storage
func (s *FTSIndexStore) LoadIndex(ctx context.Context, uuid string) (*FTSIndex, error) {
	indexPath := filepath.Join(s.basePath, "_indices", uuid)

	// 1. Load metadata
	metaPath := filepath.Join(indexPath, "meta.json")
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta FTSIndexMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// 2. Create index object
	idx := &FTSIndex{
		name:          meta.Name,
		columnIdx:     meta.ColumnIdx,
		indexType:     InvertedIndex,
		invertedIndex: make(map[string]*PostingList),
		docLengths:    make(map[uint64]int),
		totalDocs:     meta.TotalDocs,
		totalTerms:    meta.TotalTerms,
		avgDocLength:  meta.AvgDocLength,
		tokenizer:     NewSimpleTokenizer(),
	}

	// 3. Load terms dictionary
	termsPath := filepath.Join(indexPath, "terms.json")
	termsData, err := os.ReadFile(termsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read terms: %w", err)
	}

	var termToID map[string]int
	if err := json.Unmarshal(termsData, &termToID); err != nil {
		return nil, fmt.Errorf("failed to parse terms: %w", err)
	}

	// 4. Load posting lists
	postingsPath := filepath.Join(indexPath, "postings")
	for term, termID := range termToID {
		filePath := filepath.Join(postingsPath, fmt.Sprintf("%04d.bin", termID))
		data, err := os.ReadFile(filePath)
		if err != nil {
			// Skip missing posting files
			continue
		}

		pl, err := decodePostingList(data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode posting list for term %q: %w", term, err)
		}
		idx.invertedIndex[term] = pl
	}

	// 5. Load document lengths
	if err := s.loadDocLengths(ctx, idx, indexPath); err != nil {
		return nil, fmt.Errorf("failed to load doc lengths: %w", err)
	}

	return idx, nil
}

// loadDocLengths loads document length information
func (s *FTSIndexStore) loadDocLengths(ctx context.Context, idx *FTSIndex, indexPath string) error {
	filePath := filepath.Join(indexPath, "doc_lengths.bin")
	data, err := os.ReadFile(filePath)
	if err != nil {
		// Doc lengths file is optional for backward compatibility
		return nil
	}

	if len(data) < 4 {
		return fmt.Errorf("invalid doc lengths file: too short")
	}

	count := binary.LittleEndian.Uint32(data[:4])
	offset := 4

	for i := uint32(0); i < count; i++ {
		if offset+12 > len(data) {
			return fmt.Errorf("invalid doc lengths file: unexpected end")
		}

		docID := binary.LittleEndian.Uint64(data[offset : offset+8])
		length := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		idx.docLengths[docID] = int(length)
		offset += 12
	}

	return nil
}

// DeleteIndex removes a persisted FTS index
func (s *FTSIndexStore) DeleteIndex(ctx context.Context, uuid string) error {
	indexPath := filepath.Join(s.basePath, "_indices", uuid)
	return os.RemoveAll(indexPath)
}

// ListIndexes returns all persisted FTS index UUIDs
func (s *FTSIndexStore) ListIndexes(ctx context.Context) ([]string, error) {
	indicesPath := filepath.Join(s.basePath, "_indices")
	entries, err := os.ReadDir(indicesPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read indices directory: %w", err)
	}

	var uuids []string
	for _, entry := range entries {
		if entry.IsDir() {
			// Check if it contains meta.json (FTS index marker)
			metaPath := filepath.Join(indicesPath, entry.Name(), "meta.json")
			if _, err := os.Stat(metaPath); err == nil {
				uuids = append(uuids, entry.Name())
			}
		}
	}

	return uuids, nil
}

// encodePostingList encodes a posting list to binary format
// Format: [docFreq:4][posting...]
// posting: [docID:8][termFreq:4][numPositions:4][positions:4*n]
func encodePostingList(pl *PostingList) []byte {
	// Estimate buffer size
	size := 4 // docFreq
	for _, p := range pl.Postings {
		size += 8 + 4 + 4 + len(p.Positions)*4 // docID + termFreq + numPos + positions
	}

	buf := make([]byte, 0, size)

	// DocFreq
	docFreqBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(docFreqBuf, uint32(pl.DocFreq))
	buf = append(buf, docFreqBuf...)

	for _, posting := range pl.Postings {
		// DocID
		docIDBuf := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBuf, posting.DocID)
		buf = append(buf, docIDBuf...)

		// TermFreq
		tfBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(tfBuf, uint32(posting.TermFreq))
		buf = append(buf, tfBuf...)

		// NumPositions
		numPosBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(numPosBuf, uint32(len(posting.Positions)))
		buf = append(buf, numPosBuf...)

		// Positions
		for _, pos := range posting.Positions {
			posBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(posBuf, uint32(pos))
			buf = append(buf, posBuf...)
		}
	}

	return buf
}

// decodePostingList decodes a posting list from binary format
func decodePostingList(data []byte) (*PostingList, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("invalid posting list data: too short")
	}

	pl := &PostingList{}
	pl.DocFreq = int(binary.LittleEndian.Uint32(data[:4]))

	offset := 4
	for offset < len(data) {
		if offset+16 > len(data) {
			return nil, fmt.Errorf("invalid posting list data: unexpected end")
		}

		docID := binary.LittleEndian.Uint64(data[offset : offset+8])
		termFreq := int(binary.LittleEndian.Uint32(data[offset+8 : offset+12]))
		numPositions := int(binary.LittleEndian.Uint32(data[offset+12 : offset+16]))
		offset += 16

		positions := make([]int, numPositions)
		for i := 0; i < numPositions; i++ {
			if offset+4 > len(data) {
				return nil, fmt.Errorf("invalid posting list data: unexpected end in positions")
			}
			positions[i] = int(binary.LittleEndian.Uint32(data[offset : offset+4]))
			offset += 4
		}

		pl.Postings = append(pl.Postings, Posting{
			DocID:     docID,
			TermFreq:  termFreq,
			Positions: positions,
		})
	}

	return pl, nil
}

// CompressedPostingEncoder provides VByte + Delta encoding for better compression
type CompressedPostingEncoder struct{}

// EncodeVByte encodes a uint64 using variable byte encoding
func (e *CompressedPostingEncoder) EncodeVByte(val uint64) []byte {
	var buf []byte
	for val >= 0x80 {
		buf = append(buf, byte(val)|0x80)
		val >>= 7
	}
	buf = append(buf, byte(val))
	return buf
}

// DecodeVByte decodes a VByte encoded uint64
func (e *CompressedPostingEncoder) DecodeVByte(data []byte, offset int) (uint64, int) {
	var val uint64
	shift := 0
	for offset < len(data) {
		b := data[offset]
		offset++
		val |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return val, offset
}

// EncodeCompressedPostingList encodes a posting list with delta+VByte compression
func (e *CompressedPostingEncoder) EncodeCompressedPostingList(pl *PostingList) []byte {
	var buf []byte

	// DocFreq
	buf = append(buf, e.EncodeVByte(uint64(pl.DocFreq))...)

	// Sort postings by docID for delta encoding
	sortedPostings := make([]Posting, len(pl.Postings))
	copy(sortedPostings, pl.Postings)
	sort.Slice(sortedPostings, func(i, j int) bool {
		return sortedPostings[i].DocID < sortedPostings[j].DocID
	})

	var prevDocID uint64
	for _, posting := range sortedPostings {
		// Delta-encoded DocID
		delta := posting.DocID - prevDocID
		buf = append(buf, e.EncodeVByte(delta)...)
		prevDocID = posting.DocID

		// TermFreq
		buf = append(buf, e.EncodeVByte(uint64(posting.TermFreq))...)

		// NumPositions
		buf = append(buf, e.EncodeVByte(uint64(len(posting.Positions)))...)

		// Delta-encoded positions
		prevPos := 0
		for _, pos := range posting.Positions {
			delta := pos - prevPos
			buf = append(buf, e.EncodeVByte(uint64(delta))...)
			prevPos = pos
		}
	}

	return buf
}

// DecodeCompressedPostingList decodes a delta+VByte compressed posting list
func (e *CompressedPostingEncoder) DecodeCompressedPostingList(data []byte) (*PostingList, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty posting list data")
	}

	pl := &PostingList{}
	offset := 0

	// DocFreq
	docFreq, offset := e.DecodeVByte(data, offset)
	pl.DocFreq = int(docFreq)

	var prevDocID uint64
	for offset < len(data) {
		// Delta-encoded DocID
		delta, newOffset := e.DecodeVByte(data, offset)
		offset = newOffset
		docID := prevDocID + delta
		prevDocID = docID

		// TermFreq
		termFreq, newOffset := e.DecodeVByte(data, offset)
		offset = newOffset

		// NumPositions
		numPos, newOffset := e.DecodeVByte(data, offset)
		offset = newOffset

		// Delta-encoded positions
		positions := make([]int, numPos)
		prevPos := 0
		for i := uint64(0); i < numPos; i++ {
			delta, newOffset := e.DecodeVByte(data, offset)
			offset = newOffset
			pos := prevPos + int(delta)
			positions[i] = pos
			prevPos = pos
		}

		pl.Postings = append(pl.Postings, Posting{
			DocID:     docID,
			TermFreq:  int(termFreq),
			Positions: positions,
		})
	}

	return pl, nil
}
