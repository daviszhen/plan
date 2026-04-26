// Copyright 2024 PlanDB Authors
// Licensed under the Apache License, Version 2.0 (the "License");

package storage2

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
)

// WhenMatchedAction defines the action to take when a source row matches an existing row
type WhenMatchedAction int

// generateUUID generates a random UUID string
func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

const (
	// MatchedUpdateAll updates all non-key columns with source values
	MatchedUpdateAll WhenMatchedAction = iota
	// MatchedUpdateColumns updates only specified columns
	MatchedUpdateColumns
	// MatchedDelete deletes the matched row
	MatchedDelete
	// MatchedDoNothing keeps the existing row unchanged
	MatchedDoNothing
)

// WhenNotMatchedAction defines the action to take when a source row has no match
type WhenNotMatchedAction int

const (
	// NotMatchedInsert inserts the source row as a new row
	NotMatchedInsert WhenNotMatchedAction = iota
	// NotMatchedSkip skips the source row
	NotMatchedSkip
)

// MergeInsertConfig contains configuration for merge insert operations
type MergeInsertConfig struct {
	// KeyColumns are the column indices used for matching rows
	KeyColumns []int
	// WhenMatched is the action to take when a source row matches
	WhenMatched WhenMatchedAction
	// UpdateColumns are the column indices to update (only for MatchedUpdateColumns)
	UpdateColumns []int
	// WhenNotMatched is the action to take when a source row has no match
	WhenNotMatched WhenNotMatchedAction
}

// MergeInsertResult contains the result of a merge insert operation
type MergeInsertResult struct {
	// RowsInserted is the number of new rows inserted
	RowsInserted int
	// RowsUpdated is the number of existing rows updated
	RowsUpdated int
	// RowsDeleted is the number of rows deleted
	RowsDeleted int
	// RowsUnchanged is the number of rows that were not modified
	RowsUnchanged int
}

// MergeInsertBuilder provides a fluent interface for building merge insert operations
type MergeInsertBuilder struct {
	config MergeInsertConfig
}

// NewMergeInsertBuilder creates a new MergeInsertBuilder with the specified key columns
func NewMergeInsertBuilder(keyColumns ...int) *MergeInsertBuilder {
	return &MergeInsertBuilder{
		config: MergeInsertConfig{
			KeyColumns:     keyColumns,
			WhenMatched:    MatchedUpdateAll,
			WhenNotMatched: NotMatchedInsert,
		},
	}
}

// WhenMatched sets the action for matched rows
func (b *MergeInsertBuilder) WhenMatched(action WhenMatchedAction) *MergeInsertBuilder {
	b.config.WhenMatched = action
	return b
}

// WhenMatchedUpdate sets the action to update specific columns when matched
func (b *MergeInsertBuilder) WhenMatchedUpdate(columns ...int) *MergeInsertBuilder {
	b.config.WhenMatched = MatchedUpdateColumns
	b.config.UpdateColumns = columns
	return b
}

// WhenNotMatched sets the action for non-matched rows
func (b *MergeInsertBuilder) WhenNotMatched(action WhenNotMatchedAction) *MergeInsertBuilder {
	b.config.WhenNotMatched = action
	return b
}

// Build returns the configured MergeInsertConfig
func (b *MergeInsertBuilder) Build() MergeInsertConfig {
	return b.config
}

// MergeInsertExecutor executes merge insert operations
type MergeInsertExecutor struct {
	basePath string
	store    ObjectStoreExt
	handler  CommitHandler
	config   MergeInsertConfig
}

// NewMergeInsertExecutor creates a new MergeInsertExecutor
func NewMergeInsertExecutor(basePath string, store ObjectStoreExt,
	handler CommitHandler, config MergeInsertConfig) *MergeInsertExecutor {
	return &MergeInsertExecutor{
		basePath: basePath,
		store:    store,
		handler:  handler,
		config:   config,
	}
}

// Execute performs the merge insert operation
func (e *MergeInsertExecutor) Execute(ctx context.Context, sourceData []*chunk.Chunk) (*MergeInsertResult, error) {
	if len(sourceData) == 0 {
		return &MergeInsertResult{}, nil
	}

	result := &MergeInsertResult{}

	// Get the latest version
	version, err := e.handler.ResolveLatestVersion(ctx, e.basePath)
	if err != nil {
		return nil, fmt.Errorf("resolve latest version: %w", err)
	}

	// Load the current manifest
	manifest, err := LoadManifest(ctx, e.basePath, e.handler, version)
	if err != nil {
		return nil, fmt.Errorf("load manifest: %w", err)
	}

	// Extract keys from source data
	sourceKeys := e.extractKeys(sourceData)

	// Find matching rows in the target table
	matchInfo, err := e.findMatches(ctx, manifest, sourceKeys)
	if err != nil {
		return nil, fmt.Errorf("find matches: %w", err)
	}

	// Prepare fragments for the transaction
	var newFragments []*DataFragment
	var updatedFragments []*DataFragment
	var deletedRowIDs []uint64

	// Process matched rows
	switch e.config.WhenMatched {
	case MatchedUpdateAll, MatchedUpdateColumns:
		// For updates, we need to:
		// 1. Mark old rows as deleted
		// 2. Insert new rows with updated values
		for _, match := range matchInfo.matched {
			deletedRowIDs = append(deletedRowIDs, match.rowID)
			result.RowsUpdated++
		}

	case MatchedDelete:
		for _, match := range matchInfo.matched {
			deletedRowIDs = append(deletedRowIDs, match.rowID)
			result.RowsDeleted++
		}

	case MatchedDoNothing:
		result.RowsUnchanged = len(matchInfo.matched)
	}

	// Process non-matched rows
	if e.config.WhenNotMatched == NotMatchedInsert {
		// Count non-matched source rows for insertion
		result.RowsInserted = len(matchInfo.notMatched)
	}

	// Create new fragments for inserted/updated rows
	if result.RowsInserted > 0 || result.RowsUpdated > 0 {
		combinedChunks := e.prepareInsertChunks(sourceData, matchInfo, sourceKeys)
		if len(combinedChunks) > 0 {
			frags, err := e.createFragments(ctx, combinedChunks, manifest)
			if err != nil {
				return nil, fmt.Errorf("create fragments: %w", err)
			}
			newFragments = append(newFragments, frags...)
		}
	}

	// Create deletion records for matched rows that need to be updated/deleted
	if len(deletedRowIDs) > 0 {
		updatedFrags, err := e.createDeletionRecords(ctx, manifest, deletedRowIDs)
		if err != nil {
			return nil, fmt.Errorf("create deletion records: %w", err)
		}
		updatedFragments = append(updatedFragments, updatedFrags...)
	}

	// Commit the transaction
	txnUUID := generateUUID()

	if len(newFragments) > 0 && len(updatedFragments) > 0 {
		// Combined operation: first append, then delete
		err = e.commitCombinedTransaction(ctx, version, txnUUID, newFragments, updatedFragments)
	} else if len(newFragments) > 0 {
		// Append only
		txn := NewTransactionAppend(version, txnUUID, newFragments)
		err = CommitTransaction(ctx, e.basePath, e.handler, txn)
	} else if len(updatedFragments) > 0 {
		// Delete only
		txn := NewTransactionDelete(version, txnUUID, updatedFragments, nil, "")
		err = CommitTransaction(ctx, e.basePath, e.handler, txn)
	}

	if err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return result, nil
}

// sourceRowInfo holds information about a source row
type sourceRowInfo struct {
	chunk    *chunk.Chunk
	rowIndex int
	keyHash  string
}

// mergeMatchInfo holds matching information between source and target
type mergeMatchInfo struct {
	matched    map[string]*targetRowInfo // source key -> target row info
	notMatched []string                  // source keys without matches
}

// targetRowInfo holds information about a target row
type targetRowInfo struct {
	fragmentID uint64
	rowID      uint64
	localRow   int
}

// extractKeys extracts key values from source chunks and builds a lookup map
func (e *MergeInsertExecutor) extractKeys(sourceData []*chunk.Chunk) map[string]*sourceRowInfo {
	keys := make(map[string]*sourceRowInfo)

	for _, chk := range sourceData {
		if chk == nil {
			continue
		}
		for row := 0; row < chk.Card(); row++ {
			keyHash := e.computeKeyHash(chk, row)
			keys[keyHash] = &sourceRowInfo{
				chunk:    chk,
				rowIndex: row,
				keyHash:  keyHash,
			}
		}
	}

	return keys
}

// computeKeyHash computes a hash of the key columns for a row
func (e *MergeInsertExecutor) computeKeyHash(chk *chunk.Chunk, row int) string {
	var buf bytes.Buffer

	for _, colIdx := range e.config.KeyColumns {
		if colIdx < 0 || colIdx >= chk.ColumnCount() {
			continue
		}

		vec := chk.Data[colIdx]
		val := vec.GetValue(row)

		if val == nil || val.IsNull {
			buf.WriteString("NULL")
			buf.WriteByte(0)
			continue
		}

		// Serialize the value based on type
		switch val.Typ.Id {
		case common.LTID_INTEGER, common.LTID_BIGINT, common.LTID_SMALLINT, common.LTID_TINYINT:
			binary.Write(&buf, binary.LittleEndian, val.I64)
		case common.LTID_FLOAT, common.LTID_DOUBLE:
			binary.Write(&buf, binary.LittleEndian, val.F64)
		case common.LTID_VARCHAR:
			buf.WriteString(val.Str)
			buf.WriteByte(0)
		case common.LTID_BOOLEAN:
			if val.Bool {
				buf.WriteByte(1)
			} else {
				buf.WriteByte(0)
			}
		default:
			// For other types, use string representation
			fmt.Fprintf(&buf, "%v", val)
			buf.WriteByte(0)
		}
	}

	return buf.String()
}

// findMatches scans the target table to find rows matching the source keys
func (e *MergeInsertExecutor) findMatches(ctx context.Context, manifest *Manifest,
	sourceKeys map[string]*sourceRowInfo) (*mergeMatchInfo, error) {

	info := &mergeMatchInfo{
		matched: make(map[string]*targetRowInfo),
	}

	if manifest == nil || len(manifest.Fragments) == 0 {
		// Empty table, all source rows are non-matched
		for key := range sourceKeys {
			info.notMatched = append(info.notMatched, key)
		}
		return info, nil
	}

	// Compute fragment offsets for global row ID calculation
	offsets := ComputeFragmentOffsets(manifest)

	// Scan each fragment
	for fragIdx, frag := range manifest.Fragments {
		if frag == nil || len(frag.Files) == 0 {
			continue
		}

		// Read the first data file of the fragment
		df := frag.Files[0]
		if df == nil || df.Path == "" {
			continue
		}

		fullPath := filepath.Join(e.basePath, df.Path)
		chk, err := ReadChunkFromFile(fullPath)
		if err != nil {
			return nil, fmt.Errorf("read fragment %d: %w", frag.Id, err)
		}

		// Check each row in the fragment
		for row := 0; row < chk.Card(); row++ {
			keyHash := e.computeKeyHash(chk, row)

			if _, exists := sourceKeys[keyHash]; exists {
				// Found a match
				globalRowID := offsets[fragIdx] + uint64(row)
				info.matched[keyHash] = &targetRowInfo{
					fragmentID: frag.Id,
					rowID:      globalRowID,
					localRow:   row,
				}
			}
		}
	}

	// Determine non-matched source keys
	for key := range sourceKeys {
		if _, matched := info.matched[key]; !matched {
			info.notMatched = append(info.notMatched, key)
		}
	}

	return info, nil
}

// prepareInsertChunks prepares chunks for insertion based on match info
func (e *MergeInsertExecutor) prepareInsertChunks(sourceData []*chunk.Chunk,
	info *mergeMatchInfo, sourceKeys map[string]*sourceRowInfo) []*chunk.Chunk {

	var result []*chunk.Chunk

	for _, chk := range sourceData {
		if chk == nil || chk.Card() == 0 {
			continue
		}

		// Create a new chunk with same schema
		colTypes := make([]common.LType, chk.ColumnCount())
		for i := 0; i < chk.ColumnCount(); i++ {
			colTypes[i] = chk.Data[i].Typ()
		}

		newChunk := &chunk.Chunk{}
		newChunk.Init(colTypes, chk.Card())

		rowCount := 0
		for row := 0; row < chk.Card(); row++ {
			keyHash := e.computeKeyHash(chk, row)

			shouldInsert := false
			if _, matched := info.matched[keyHash]; matched {
				// Matched row - include if updating
				if e.config.WhenMatched == MatchedUpdateAll || e.config.WhenMatched == MatchedUpdateColumns {
					shouldInsert = true
				}
			} else {
				// Non-matched row - include if inserting
				if e.config.WhenNotMatched == NotMatchedInsert {
					shouldInsert = true
				}
			}

			if shouldInsert {
				// Copy this row to the new chunk
				for col := 0; col < chk.ColumnCount(); col++ {
					val := chk.Data[col].GetValue(row)
					newChunk.Data[col].SetValue(rowCount, val)
				}
				rowCount++
			}
		}

		if rowCount > 0 {
			newChunk.SetCard(rowCount)
			result = append(result, newChunk)
		}
	}

	return result
}

// createFragments creates new fragments from chunks
func (e *MergeInsertExecutor) createFragments(ctx context.Context, chunks []*chunk.Chunk,
	manifest *Manifest) ([]*DataFragment, error) {

	var fragments []*DataFragment
	nextFragID := uint32(1)
	if manifest != nil && manifest.MaxFragmentId != nil {
		nextFragID = *manifest.MaxFragmentId + 1
	}

	for i, chk := range chunks {
		if chk == nil || chk.Card() == 0 {
			continue
		}

		fragID := uint64(nextFragID) + uint64(i)

		// Write chunk to file
		dataPath := fmt.Sprintf("data/%d/0.lance", fragID)
		fullPath := filepath.Join(e.basePath, dataPath)

		if err := WriteChunkToFile(fullPath, chk); err != nil {
			return nil, fmt.Errorf("write chunk: %w", err)
		}

		frag := &DataFragment{
			Id:           fragID,
			PhysicalRows: uint64(chk.Card()),
			Files: []*storage2pb.DataFile{
				{Path: dataPath},
			},
		}
		fragments = append(fragments, frag)
	}

	return fragments, nil
}

// createDeletionRecords creates deletion records for the specified row IDs
func (e *MergeInsertExecutor) createDeletionRecords(ctx context.Context, manifest *Manifest,
	rowIDs []uint64) ([]*DataFragment, error) {

	if len(rowIDs) == 0 || manifest == nil {
		return nil, nil
	}

	// Group row IDs by fragment
	offsets := ComputeFragmentOffsets(manifest)
	rowsByFrag := make(map[int][]uint64)

	for _, rowID := range rowIDs {
		// Find which fragment this row belongs to
		for i := 0; i < len(manifest.Fragments); i++ {
			if rowID >= offsets[i] && rowID < offsets[i+1] {
				localRow := rowID - offsets[i]
				rowsByFrag[i] = append(rowsByFrag[i], localRow)
				break
			}
		}
	}

	// Create updated fragments with deletion info
	var updatedFragments []*DataFragment

	for fragIdx, localRows := range rowsByFrag {
		frag := manifest.Fragments[fragIdx]
		if frag == nil {
			continue
		}

		// Clone the fragment
		updatedFrag := &DataFragment{
			Id:           frag.Id,
			PhysicalRows: frag.PhysicalRows,
			Files:        frag.Files,
		}

		// Create or update deletion file
		deletionFile := &storage2pb.DeletionFile{
			FileType:       storage2pb.DeletionFile_BITMAP,
			ReadVersion:    manifest.Version,
			Id:             0,
			NumDeletedRows: uint64(len(localRows)),
		}

		// Merge with existing deletion file if present
		if frag.DeletionFile != nil {
			deletionFile.NumDeletedRows += frag.DeletionFile.NumDeletedRows
		}

		updatedFrag.DeletionFile = deletionFile
		updatedFragments = append(updatedFragments, updatedFrag)

		// Write the deletion bitmap
		bitmap := NewDeletionBitmap()
		for _, localRow := range localRows {
			bitmap.MarkDeleted(localRow)
		}

		// Merge with existing bitmap if present
		if frag.DeletionFile != nil {
			existingBitmap, err := LoadFragmentDeletionBitmap(ctx, e.store, e.basePath, frag)
			if err == nil && existingBitmap != nil {
				// Merge bitmaps: copy all deleted rows from existing
				for rowID := range existingBitmap.deleted {
					bitmap.MarkDeleted(rowID)
				}
			}
		}

		// Save the deletion bitmap
		ext := "bin"
		delPath := fmt.Sprintf("%s/_deletions/%d-%d-%d.%s",
			e.basePath, frag.Id, manifest.Version, 0, ext)
		if err := SaveDeletionBitmap(ctx, e.store, delPath, bitmap); err != nil {
			return nil, fmt.Errorf("save deletion bitmap: %w", err)
		}
	}

	return updatedFragments, nil
}

// commitCombinedTransaction commits a combined append+delete transaction
func (e *MergeInsertExecutor) commitCombinedTransaction(ctx context.Context, version uint64,
	txnUUID string, newFragments, updatedFragments []*DataFragment) error {

	// First commit the append
	txn := NewTransactionAppend(version, txnUUID, newFragments)
	if err := CommitTransaction(ctx, e.basePath, e.handler, txn); err != nil {
		return err
	}

	// Then commit the deletions if any
	if len(updatedFragments) > 0 {
		newVersion := version + 1
		delTxn := NewTransactionDelete(newVersion, generateUUID(), updatedFragments, nil, "")
		if err := CommitTransaction(ctx, e.basePath, e.handler, delTxn); err != nil {
			return err
		}
	}

	return nil
}

// MergeInsert is a convenience function for performing merge insert operations
func MergeInsert(ctx context.Context, basePath string, store ObjectStoreExt,
	handler CommitHandler, sourceData []*chunk.Chunk, keyColumns []int) (*MergeInsertResult, error) {

	config := NewMergeInsertBuilder(keyColumns...).Build()
	executor := NewMergeInsertExecutor(basePath, store, handler, config)
	return executor.Execute(ctx, sourceData)
}

// MergeInsertTable is a convenience method on Table
func (t *Table) MergeInsertTable(ctx context.Context, sourceData []*chunk.Chunk,
	config MergeInsertConfig) (*MergeInsertResult, error) {

	store := NewLocalObjectStoreExt(t.BasePath, nil)
	executor := NewMergeInsertExecutor(t.BasePath, store, t.Handler, config)
	return executor.Execute(ctx, sourceData)
}
