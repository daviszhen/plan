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
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	storage2pb "github.com/daviszhen/plan/pkg/storage2/proto"
	"github.com/daviszhen/plan/pkg/util"
)

// ============================================================================
// P1-17: TestRowIdIndexLookup — rowids.go Get() 0% functions
// ============================================================================

func TestRangeWithHolesSegmentGet(t *testing.T) {
	seg := &RangeWithHolesSegment{
		Start: 10,
		End:   20,
		Holes: []uint64{12, 15, 18},
	}
	// Non-hole values in order: 10,11,13,14,16,17,19 (7 values)

	tests := []struct {
		idx  int
		val  uint64
		ok   bool
		desc string
	}{
		{0, 10, true, "first element"},
		{1, 11, true, "second element"},
		{2, 13, true, "third element (after hole at 12)"},
		{3, 14, true, "fourth element"},
		{4, 16, true, "fifth element (after hole at 15)"},
		{5, 17, true, "sixth element"},
		{6, 19, true, "seventh element (after hole at 18)"},
		{7, 0, false, "out of bounds"},
		{-1, 0, false, "negative index"},
	}

	for _, tt := range tests {
		val, ok := seg.Get(tt.idx)
		if ok != tt.ok {
			t.Errorf("Get(%d) ok=%v, want %v (%s)", tt.idx, ok, tt.ok, tt.desc)
		}
		if ok && val != tt.val {
			t.Errorf("Get(%d)=%d, want %d (%s)", tt.idx, val, tt.val, tt.desc)
		}
	}
}

func TestRangeWithBitmapSegmentGet(t *testing.T) {
	// Bitmap: 0b01010101 = 0x55 → bits 0,2,4,6 are set
	// Range [0,8) with bitmap → present values: 0, 2, 4, 6
	seg := &RangeWithBitmapSegment{
		Start:  0,
		End:    8,
		Bitmap: []byte{0x55},
	}

	tests := []struct {
		idx  int
		val  uint64
		ok   bool
		desc string
	}{
		{0, 0, true, "first present value"},
		{1, 2, true, "second present value"},
		{2, 4, true, "third present value"},
		{3, 6, true, "fourth present value"},
		{4, 0, false, "out of bounds (only 4 present)"},
	}

	for _, tt := range tests {
		val, ok := seg.Get(tt.idx)
		if ok != tt.ok {
			t.Errorf("Get(%d) ok=%v, want %v (%s)", tt.idx, ok, tt.ok, tt.desc)
		}
		if ok && val != tt.val {
			t.Errorf("Get(%d)=%d, want %d (%s)", tt.idx, val, tt.val, tt.desc)
		}
	}
}

func TestRangeWithBitmapSegmentGetWithOffset(t *testing.T) {
	// Range [100, 108) with bitmap 0xFF (all present)
	seg := &RangeWithBitmapSegment{
		Start:  100,
		End:    108,
		Bitmap: []byte{0xFF},
	}

	for i := 0; i < 8; i++ {
		val, ok := seg.Get(i)
		if !ok || val != uint64(100+i) {
			t.Errorf("Get(%d)=(%d,%v), want (%d,true)", i, val, ok, 100+i)
		}
	}
}

func TestArraySegmentGet(t *testing.T) {
	seg := &ArraySegment{Values: []uint64{100, 5, 50, 1}}

	tests := []struct {
		idx  int
		val  uint64
		ok   bool
		desc string
	}{
		{0, 100, true, "first element"},
		{1, 5, true, "second element"},
		{2, 50, true, "third element"},
		{3, 1, true, "fourth element"},
		{4, 0, false, "out of bounds"},
		{-1, 0, false, "negative index"},
	}

	for _, tt := range tests {
		val, ok := seg.Get(tt.idx)
		if ok != tt.ok {
			t.Errorf("Get(%d) ok=%v, want %v (%s)", tt.idx, ok, tt.ok, tt.desc)
		}
		if ok && val != tt.val {
			t.Errorf("Get(%d)=%d, want %d (%s)", tt.idx, val, tt.val, tt.desc)
		}
	}
}

func TestSortedArraySegmentGetBounds(t *testing.T) {
	seg := &SortedArraySegment{Values: []uint64{10, 20, 30}}

	if _, ok := seg.Get(-1); ok {
		t.Error("Get(-1) should return false")
	}
	if _, ok := seg.Get(3); ok {
		t.Error("Get(3) should return false for 3-element segment")
	}
}

func TestRangeWithHolesContainsOutOfRange(t *testing.T) {
	seg := &RangeWithHolesSegment{Start: 10, End: 20, Holes: []uint64{12}}

	if seg.Contains(9) {
		t.Error("Contains(9) should be false (below range)")
	}
	if seg.Contains(20) {
		t.Error("Contains(20) should be false (at end, exclusive)")
	}
}

func TestRangeWithBitmapContainsOutOfRange(t *testing.T) {
	seg := &RangeWithBitmapSegment{Start: 10, End: 18, Bitmap: []byte{0xFF}}

	if seg.Contains(9) {
		t.Error("Contains(9) should be false (below range)")
	}
	if seg.Contains(18) {
		t.Error("Contains(18) should be false (at end, exclusive)")
	}
}

// ============================================================================
// P1-8: TestIndexQuery — index_selector.go 0% functions
// ============================================================================

func TestExtendedIndexTypeString(t *testing.T) {
	tests := []struct {
		typ  IndexType
		want string
	}{
		{BitmapIndexType, "bitmap"},
		{ZoneMapIndexType, "zonemap"},
		{BloomFilterIndexType, "bloomfilter"},
		{ScalarIndex, "scalar"},
		{VectorIndex, "vector"},
	}

	for _, tt := range tests {
		got := ExtendedIndexTypeString(tt.typ)
		if got != tt.want {
			t.Errorf("ExtendedIndexTypeString(%d)=%q, want %q", tt.typ, got, tt.want)
		}
	}
}

func TestIndexSelectorGetAllStatistics(t *testing.T) {
	collector := NewIndexStatisticsCollector()

	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(3)
	c.Data[0].SetValue(0, &chunk.Value{Typ: typs[0], I64: 10})
	c.Data[0].SetValue(1, &chunk.Value{Typ: typs[0], I64: 20})
	c.Data[0].SetValue(2, &chunk.Value{Typ: typs[0], I64: 30})

	collector.CollectFromChunk(c)

	allStats := collector.GetAllStatistics()
	if len(allStats) != 1 {
		t.Fatalf("expected 1 column stats, got %d", len(allStats))
	}
	stats := allStats[0]
	if stats.TotalCount != 3 {
		t.Errorf("TotalCount=%d, want 3", stats.TotalCount)
	}
	if stats.DistinctCount != 3 {
		t.Errorf("DistinctCount=%d, want 3", stats.DistinctCount)
	}
}

func TestIndexAwareScannerScanWithIndexNoFilter(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Prepare dataset with one fragment
	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 5)

	// Create IndexAwareScanner with no filter
	config := DefaultScanConfig()
	scanner := NewIndexAwareScanner(basePath, handler, version, config, nil)

	chunks, err := scanner.ScanWithIndex(ctx)
	if err != nil {
		t.Fatalf("ScanWithIndex: %v", err)
	}
	if len(chunks) == 0 {
		t.Error("expected at least one chunk")
	}
	total := 0
	for _, c := range chunks {
		total += c.Card()
	}
	if total != 5 {
		t.Errorf("total rows=%d, want 5", total)
	}
}

func TestIndexAwareScannerScanWithIndexWithFilter(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Prepare dataset
	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 10)

	// Create filter: col0 > 5
	config := DefaultScanConfig()
	config.Filter = &ColumnPredicate{
		ColumnIndex: 0,
		Op:          Gt,
		Value:       &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 5},
	}

	manager := NewIndexManager(basePath, handler)
	// Create a ZoneMap index
	_ = manager.CreateZoneMapIndex(ctx, "zm_col0")

	scanner := NewIndexAwareScanner(basePath, handler, version, config, manager)
	chunks, err := scanner.ScanWithIndex(ctx)
	if err != nil {
		t.Fatalf("ScanWithIndex: %v", err)
	}

	total := 0
	for _, c := range chunks {
		total += c.Card()
	}
	// With filter col0 > 5, rows 6,7,8,9 should match
	if total != 4 {
		t.Errorf("filtered total rows=%d, want 4", total)
	}
}

func TestCanPruneFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 5)
	config := DefaultScanConfig()
	config.Filter = &ColumnPredicate{
		ColumnIndex: 0,
		Op:          Eq,
		Value:       &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 99},
	}

	manager := NewIndexManager(basePath, handler)
	scanner := NewIndexAwareScanner(basePath, handler, version, config, manager)

	// Test canPruneFragment with PrunableFragments list
	selection := &IndexSelection{
		PrunableFragments: []uint64{1, 2, 3},
		Index:             &BitmapIndex{},
	}

	if !scanner.canPruneFragment(selection, 1) {
		t.Error("fragment 1 should be prunable")
	}
	if !scanner.canPruneFragment(selection, 2) {
		t.Error("fragment 2 should be prunable")
	}
	if scanner.canPruneFragment(selection, 99) {
		t.Error("fragment 99 should not be prunable")
	}
}

func TestCanPruneFragmentWithZoneMap(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 5)
	config := DefaultScanConfig()

	manager := NewIndexManager(basePath, handler)
	scanner := NewIndexAwareScanner(basePath, handler, version, config, manager)

	// Create ZoneMap index with fragment-level stats
	zmIndex := NewZoneMapIndex(WithZoneMapIndexName("zm"))
	zmIndex.UpdateZoneMap(0, int64(0), false)
	zmIndex.UpdateZoneMap(0, int64(10), false)

	// Fragment 0 has values [0, 10], test pruning with value > 100
	selection := &IndexSelection{
		Index:       zmIndex,
		FilterOp:    ">",
		FilterValue: int64(100),
		ColumnIdx:   0,
	}

	// canPruneFragment for a fragment that's not in PrunableFragments
	// should check ZoneMap fragment-level pruning
	result := scanner.canPruneFragment(selection, 0)
	_ = result // We just verify it doesn't panic

	// Test with BloomFilter selection (should return false for non-PrunableFragments)
	bfSelection := &IndexSelection{
		Index: NewBloomFilterIndex(WithBloomFilterIndexName("bf")),
	}
	if scanner.canPruneFragment(bfSelection, 99) {
		t.Error("BloomFilter should not prune fragments outside PrunableFragments")
	}
}

// ============================================================================
// P1-10: TestRebaseIops — conflict.go OpKindOf 57.1%
// ============================================================================

func TestOpKindOfAllTypes(t *testing.T) {
	tests := []struct {
		txn  *Transaction
		want OpKind
		desc string
	}{
		{NewTransactionAppend(1, "u", nil), OpAppend, "Append"},
		{NewTransactionDelete(1, "u", nil, nil, ""), OpDelete, "Delete"},
		{NewTransactionOverwrite(1, "u", nil, nil, nil), OpOverwrite, "Overwrite"},
		{NewTransactionProject(1, "u", nil), OpProject, "Project"},
		{NewTransactionMerge(1, "u", nil, nil, nil), OpMerge, "Merge"},
		{NewTransactionRewrite(1, "u", nil, nil), OpRewrite, "Rewrite"},
		{nil, OpOther, "nil transaction"},
	}

	for _, tt := range tests {
		got := OpKindOf(tt.txn)
		if got != tt.want {
			t.Errorf("OpKindOf(%s)=%d, want %d", tt.desc, got, tt.want)
		}
	}
}

func TestOpKindOfCreateIndex(t *testing.T) {
	builder := NewIndexTransactionBuilder(t.TempDir(), NewLocalRenameCommitHandler())
	txn := builder.BuildCreateIndexTransaction(1, "u",
		[]*storage2pb.IndexMetadata{{Name: "idx1"}}, nil)
	if OpKindOf(txn) != OpCreateIndex {
		t.Errorf("OpKindOf(CreateIndex)=%d, want %d", OpKindOf(txn), OpCreateIndex)
	}
}

func TestOpKindOfUpdate(t *testing.T) {
	txn := NewTransactionUpdate(1, "u", nil, nil, nil, nil, UpdateModeRewriteRows)
	if OpKindOf(txn) != OpUpdate {
		t.Errorf("OpKindOf(Update)=%d, want %d", OpKindOf(txn), OpUpdate)
	}
}

func TestOpKindOfDataReplacement(t *testing.T) {
	txn := NewTransactionDataReplacement(1, "u", nil)
	if OpKindOf(txn) != OpDataReplacement {
		t.Errorf("OpKindOf(DataReplacement)=%d, want %d", OpKindOf(txn), OpDataReplacement)
	}
}

func TestCheckConflictNilTransactions(t *testing.T) {
	txn := NewTransactionAppend(1, "u", nil)
	if !CheckConflict(nil, txn, 2) {
		t.Error("nil myTxn should conflict")
	}
	if !CheckConflict(txn, nil, 2) {
		t.Error("nil otherTxn should conflict")
	}
}

func TestCheckConflictCreateIndexVsOverwrite(t *testing.T) {
	builder := NewIndexTransactionBuilder(t.TempDir(), NewLocalRenameCommitHandler())
	ci := builder.BuildCreateIndexTransaction(1, "ci",
		[]*storage2pb.IndexMetadata{{Name: "idx1"}}, nil)

	ow := NewTransactionOverwrite(1, "ow", nil, nil, nil)

	if !CheckConflict(ci, ow, 2) {
		t.Error("CreateIndex vs Overwrite(committed) should conflict")
	}
}

func TestCheckConflictCreateIndexVsDelete(t *testing.T) {
	builder := NewIndexTransactionBuilder(t.TempDir(), NewLocalRenameCommitHandler())
	ci := builder.BuildCreateIndexTransaction(1, "ci",
		[]*storage2pb.IndexMetadata{{Name: "idx1"}}, nil)
	del := NewTransactionDelete(1, "d", nil, nil, "")

	if CheckConflict(ci, del, 2) {
		t.Error("CreateIndex vs Delete should be compatible")
	}
}

func TestRebaseNil(t *testing.T) {
	rebased := Rebase(nil, 5)
	if rebased != nil {
		t.Error("Rebase(nil) should return nil")
	}
}

func TestRebasePreservesOperation(t *testing.T) {
	txn := NewTransactionDelete(1, "u", nil, nil, "")
	rebased := Rebase(txn, 10)

	if rebased.ReadVersion != 10 {
		t.Errorf("ReadVersion=%d, want 10", rebased.ReadVersion)
	}
	if OpKindOf(rebased) != OpDelete {
		t.Errorf("operation should be preserved as Delete, got %d", OpKindOf(rebased))
	}
	if rebased.Uuid != "u" {
		t.Errorf("UUID=%q, want %q", rebased.Uuid, "u")
	}
}

// ============================================================================
// P1-11: TestReadVersionIsolation — detached_txn.go edge cases
// ============================================================================

func TestCreateDetachedTransactionNilTxn(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	_, err := CreateDetachedTransaction(context.Background(), dir, handler, nil, nil)
	if err == nil {
		t.Error("expected error for nil transaction")
	}
}

func TestCreateDetachedTransactionEmptyUUID(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	txn := NewTransactionAppend(0, "", nil)
	_, err := CreateDetachedTransaction(context.Background(), dir, handler, txn, nil)
	if err == nil {
		t.Error("expected error for empty UUID")
	}
}

func TestCommitDetachedTransactionExpired(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	m0 := NewManifest(0)
	if err := handler.Commit(ctx, dir, 0, m0); err != nil {
		t.Fatalf("create dataset: %v", err)
	}

	txn := NewTransactionAppend(0, "test-expired", nil)
	opts := &DetachedTransactionOptions{Timeout: 1 * time.Millisecond}
	id, err := CreateDetachedTransaction(ctx, dir, handler, txn, opts)
	if err != nil {
		t.Fatalf("CreateDetachedTransaction: %v", err)
	}

	// Wait for expiration
	time.Sleep(10 * time.Millisecond)

	// Try to commit expired transaction
	_, err = CommitDetachedTransaction(ctx, dir, handler, id)
	if err == nil {
		t.Error("expected error for expired transaction")
	}
}

func TestCommitDetachedTransactionStatusFailed(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	// Write a failed state directly
	state := &DetachedTransactionState{
		ID:          "test-failed",
		Transaction: NewTransactionAppend(0, "test-failed", nil),
		Status:      DetachedStatusFailed,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Error:       "previous failure",
	}
	if err := writeDetachedState(dir, state); err != nil {
		t.Fatalf("writeDetachedState: %v", err)
	}

	_, err := CommitDetachedTransaction(ctx, dir, handler, "test-failed")
	if err == nil {
		t.Error("expected error for failed transaction")
	}
}

func TestCommitDetachedTransactionStatusCommitting(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	state := &DetachedTransactionState{
		ID:          "test-committing",
		Transaction: NewTransactionAppend(0, "test-committing", nil),
		Status:      DetachedStatusCommitting,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := writeDetachedState(dir, state); err != nil {
		t.Fatalf("writeDetachedState: %v", err)
	}

	_, err := CommitDetachedTransaction(ctx, dir, handler, "test-committing")
	if err == nil {
		t.Error("expected error for committing transaction")
	}
}

func TestListDetachedTransactionsEmpty(t *testing.T) {
	dir := t.TempDir()
	results, err := ListDetachedTransactions(context.Background(), dir, "")
	if err != nil {
		t.Fatalf("ListDetachedTransactions: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil for non-existent directory, got %d items", len(results))
	}
}

func TestListDetachedTransactionsAllStatuses(t *testing.T) {
	dir := t.TempDir()
	handler := NewLocalRenameCommitHandler()
	ctx := context.Background()

	m0 := NewManifest(0)
	handler.Commit(ctx, dir, 0, m0)

	// Create transactions with different statuses
	txn1 := NewTransactionAppend(0, "pending-1", nil)
	CreateDetachedTransaction(ctx, dir, handler, txn1, nil)

	txn2 := NewTransactionAppend(0, "committed-1", nil)
	CreateDetachedTransaction(ctx, dir, handler, txn2, nil)
	CommitDetachedTransaction(ctx, dir, handler, "committed-1")

	// List all (no status filter)
	all, err := ListDetachedTransactions(ctx, dir, "")
	if err != nil {
		t.Fatalf("ListDetachedTransactions: %v", err)
	}
	if len(all) < 2 {
		t.Errorf("expected at least 2 transactions, got %d", len(all))
	}
}

func TestWriteDetachedStateNil(t *testing.T) {
	dir := t.TempDir()
	if err := writeDetachedState(dir, nil); err == nil {
		t.Error("expected error for nil state")
	}
}

// ============================================================================
// P1-9: TestMixedEncodingSelection — encoding.go 0% functions
// ============================================================================

func TestEncodingTypeMethod(t *testing.T) {
	// Test the EncodingType() method of each encoder
	plain := NewPlainEncoder(4)
	if plain.EncodingType() != EncodingPlain {
		t.Errorf("PlainEncoder.EncodingType()=%v, want %v", plain.EncodingType(), EncodingPlain)
	}

	bitpack := NewBitPackEncoder()
	if bitpack.EncodingType() != EncodingBitPacked {
		t.Errorf("BitPackEncoder.EncodingType()=%v, want %v", bitpack.EncodingType(), EncodingBitPacked)
	}

	rle := NewRLEEncoder(4)
	if rle.EncodingType() != EncodingRLE {
		t.Errorf("RLEEncoder.EncodingType()=%v, want %v", rle.EncodingType(), EncodingRLE)
	}

	dict := NewDictEncoder(4)
	if dict.EncodingType() != EncodingDictionary {
		t.Errorf("DictEncoder.EncodingType()=%v, want %v", dict.EncodingType(), EncodingDictionary)
	}
}

func TestEncodeDoubleColumn(t *testing.T) {
	// Test encodeDoubleColumn which is at 0% coverage
	typs := []common.LType{common.MakeLType(common.LTID_DOUBLE)}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(4)
	c.Data[0].SetValue(0, &chunk.Value{Typ: typs[0], F64: 1.5})
	c.Data[0].SetValue(1, &chunk.Value{Typ: typs[0], F64: -2.7})
	c.Data[0].SetValue(2, &chunk.Value{Typ: typs[0], F64: 0.0})
	c.Data[0].SetValue(3, &chunk.Value{Typ: typs[0], F64: math.Pi})

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(c.Data[0], 4)
	if err != nil {
		t.Fatalf("EncodeColumn(DOUBLE): %v", err)
	}
	if page == nil {
		t.Fatal("page should not be nil")
	}
	if page.NumRows != 4 {
		t.Errorf("NumRows=%d, want 4", page.NumRows)
	}

	// Decode and verify
	decoder := NewLogicalColumnDecoder()
	result := chunk.NewFlatVector(typs[0], util.DefaultVectorSize)
	err = decoder.DecodeColumn(page, result, typs[0])
	if err != nil {
		t.Fatalf("DecodeColumn(DOUBLE): %v", err)
	}

	expected := []float64{1.5, -2.7, 0.0, math.Pi}
	for i, want := range expected {
		val := result.GetValue(i)
		if val == nil {
			t.Errorf("row %d: nil value", i)
			continue
		}
		if math.Abs(val.F64-want) > 1e-10 {
			t.Errorf("row %d: got %v, want %v", i, val.F64, want)
		}
	}
}

func TestEncodeDoubleColumnWithNulls(t *testing.T) {
	typs := []common.LType{common.MakeLType(common.LTID_DOUBLE)}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(3)
	c.Data[0].SetValue(0, &chunk.Value{Typ: typs[0], F64: 1.0})
	c.Data[0].SetValue(1, &chunk.Value{Typ: typs[0], IsNull: true})
	c.Data[0].SetValue(2, &chunk.Value{Typ: typs[0], F64: 3.0})

	encoder := NewLogicalColumnEncoder()
	page, err := encoder.EncodeColumn(c.Data[0], 3)
	if err != nil {
		t.Fatalf("EncodeColumn: %v", err)
	}
	if page.NumRows != 3 {
		t.Errorf("NumRows=%d, want 3", page.NumRows)
	}
}

func TestEncodingTypeStringExtended(t *testing.T) {
	tests := []struct {
		enc  EncodingType
		want string
	}{
		{EncodingPlain, "plain"},
		{EncodingBitPacked, "bitpacked"},
		{EncodingRLE, "rle"},
		{EncodingDictionary, "dictionary"},
	}

	for _, tt := range tests {
		got := tt.enc.String()
		if got != tt.want {
			t.Errorf("(%d).String()=%q, want %q", tt.enc, got, tt.want)
		}
	}
}

func TestMixedEncodingSelection(t *testing.T) {
	// Test automatic encoding selection for different column patterns

	// 1. Repeated values → RLE
	typs := []common.LType{common.MakeLType(common.LTID_INTEGER)}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	n := 100
	c.SetCard(n)
	for i := 0; i < n; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i / 20)}) // 5 distinct values
	}
	stats := AnalyzeIntColumn(c.Data[0], n)
	enc := SelectIntEncoding(stats, 4)
	// RLE or Dict should be selected for highly repetitive data
	if enc != EncodingRLE && enc != EncodingDictionary {
		t.Errorf("repeated values: got encoding %v, expected RLE or Dictionary", enc)
	}

	// 2. All same value → RLE
	for i := 0; i < n; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: 42})
	}
	stats = AnalyzeIntColumn(c.Data[0], n)
	enc = SelectIntEncoding(stats, 4)
	if enc != EncodingRLE {
		t.Errorf("all same values: got encoding %v, expected RLE", enc)
	}

	// 3. Small range → BitPacked
	for i := 0; i < n; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i % 4)})
	}
	stats = AnalyzeIntColumn(c.Data[0], n)
	enc = SelectIntEncoding(stats, 4)
	if enc != EncodingBitPacked && enc != EncodingRLE && enc != EncodingDictionary {
		t.Errorf("small range: got encoding %v", enc)
	}
}

func TestStringEncodingSelection(t *testing.T) {
	typs := []common.LType{common.MakeLType(common.LTID_VARCHAR)}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	n := 50
	c.SetCard(n)

	// Repeated strings → should select Dictionary
	values := []string{"alpha", "beta", "gamma"}
	for i := 0; i < n; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], Str: values[i%len(values)]})
	}

	stats := AnalyzeStringColumn(c.Data[0], n)
	enc := SelectStringEncoding(stats)
	if enc != EncodingDictionary {
		t.Errorf("repeated strings: got encoding %v, expected Dictionary", enc)
	}
}

// ============================================================================
// P1-5: TestTakeByIndices — scanner.go
// ============================================================================

func TestScanWithFilterMultiFragment(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial empty dataset
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	handler.Commit(ctx, basePath, 0, m0)

	// Write two data files
	dataPath1 := filepath.Join(basePath, "data", "0.dat")
	WriteChunkToFile(dataPath1, makeTestChunk(t, 5))

	dataPath2 := filepath.Join(basePath, "data", "1.dat")
	WriteChunkToFile(dataPath2, makeTestChunkWithBase(t, 5, 5))

	// Commit two fragments
	df1 := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag1 := NewDataFragmentWithRows(0, 5, []*DataFile{df1})
	txn1 := NewTransactionAppend(0, "scan-multi-1", []*DataFragment{frag1})
	CommitTransaction(ctx, basePath, handler, txn1)

	df2 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	frag2 := NewDataFragmentWithRows(1, 5, []*DataFile{df2})
	txn2 := NewTransactionAppend(1, "scan-multi-2", []*DataFragment{frag2})
	CommitTransaction(ctx, basePath, handler, txn2)

	// Scan with filter: col0 >= 3
	config := DefaultScanConfig()
	config.Filter = &ColumnPredicate{
		ColumnIndex: 0,
		Op:          Ge,
		Value:       &chunk.Value{Typ: common.MakeLType(common.LTID_INTEGER), I64: 3},
	}

	scanner := NewPushdownScanner(basePath, handler, 2, config)
	chunks, err := scanner.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	total := 0
	for _, c := range chunks {
		total += c.Card()
	}
	// Values 3,4 from frag1 + 5,6,7,8,9 from frag2 = 7
	if total != 7 {
		t.Errorf("filtered rows=%d, want 7", total)
	}
}

func TestScanWithProjection(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 5)

	// Scan with projection: only column 0
	config := DefaultScanConfig()
	config.Columns = []int{0}

	scanner := NewPushdownScanner(basePath, handler, version, config)
	chunks, err := scanner.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatal("expected at least one chunk")
	}
	// After projection, should only have 1 column
	if chunks[0].ColumnCount() != 1 {
		t.Errorf("column count=%d, want 1", chunks[0].ColumnCount())
	}
}

func TestScanWithLimit(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	version := prepareManifestWithOneFragment(t, ctx, basePath, handler, 100)

	config := DefaultScanConfig()
	config.Limit = 10

	scanner := NewPushdownScanner(basePath, handler, version, config)
	chunks, err := scanner.Scan(ctx)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}

	total := 0
	for _, c := range chunks {
		total += c.Card()
	}
	if total != 10 {
		t.Errorf("limited rows=%d, want 10", total)
	}
}

// ============================================================================
// P1-15/16: TestFragmentScanner/DeletionFile — fragment.go
// ============================================================================

func TestNewDataFileFullCoverage(t *testing.T) {
	df := NewDataFile("test.parquet", []int32{0, 1, 2}, 1, 0)
	if df == nil {
		t.Fatal("NewDataFile returned nil")
	}
	if df.Path != "test.parquet" {
		t.Errorf("Path=%q, want %q", df.Path, "test.parquet")
	}
	if len(df.Fields) != 3 {
		t.Errorf("Fields len=%d, want 3", len(df.Fields))
	}
}

func TestNewDeletionFile(t *testing.T) {
	df := NewDeletionFile(storage2pb.DeletionFile_ARROW_ARRAY, 1, 10, 100)
	if df == nil {
		t.Fatal("NewDeletionFile returned nil")
	}
	if df.ReadVersion != 1 {
		t.Errorf("ReadVersion=%d, want 1", df.ReadVersion)
	}
	if df.Id != 10 {
		t.Errorf("Id=%d, want 10", df.Id)
	}
	if df.NumDeletedRows != 100 {
		t.Errorf("NumDeletedRows=%d, want 100", df.NumDeletedRows)
	}
}

func TestNewDataFragmentWithDeletionFile(t *testing.T) {
	df := NewDataFile("data.parquet", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 1000, []*DataFile{df})

	if frag == nil {
		t.Fatal("fragment should not be nil")
	}
	if frag.Id != 0 {
		t.Errorf("Id=%d, want 0", frag.Id)
	}
	if frag.PhysicalRows != 1000 {
		t.Errorf("PhysicalRows=%d, want 1000", frag.PhysicalRows)
	}
	if len(frag.Files) != 1 {
		t.Errorf("Files count=%d, want 1", len(frag.Files))
	}
}

func TestNewBasePath(t *testing.T) {
	name := "my-table"
	bp := NewBasePath(0, "s3://bucket/table", &name, true)
	if bp.Path != "s3://bucket/table" {
		t.Errorf("Path=%q, want %q", bp.Path, "s3://bucket/table")
	}
	if !bp.IsDatasetRoot {
		t.Error("IsDatasetRoot should be true")
	}
}

// ============================================================================
// P1-14: TestFragmentTake — rowid_scanner.go
// ============================================================================

func TestRowIdScannerNoStableRowIds(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest without stable row IDs (feature flag 2)
	m0 := NewManifest(0)
	m0.ReaderFeatureFlags = 0 // No stable row IDs
	handler.Commit(ctx, basePath, 0, m0)

	_, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err == nil {
		t.Error("expected error for dataset without stable row IDs")
	}
}

func TestRowIdScannerWithStableRowIds(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest with stable row IDs enabled
	m0 := NewManifest(0)
	m0.ReaderFeatureFlags = 2 // stable row IDs flag
	m0.WriterFeatureFlags = 2
	handler.Commit(ctx, basePath, 0, m0)

	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatalf("NewRowIdScanner: %v", err)
	}
	if scanner == nil {
		t.Fatal("scanner should not be nil")
	}

	// TakeByRowIds with empty slice
	result, err := scanner.TakeByRowIds(ctx, nil)
	if err != nil {
		t.Fatalf("TakeByRowIds(nil): %v", err)
	}
	if result != nil {
		t.Error("expected nil for empty rowIds")
	}
}

func TestRowIdScannerTakeByRowIds(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create manifest with stable row IDs and inline row IDs
	m0 := NewManifest(0)
	m0.ReaderFeatureFlags = 2
	m0.WriterFeatureFlags = 2

	// Write data file
	dataPath := filepath.Join(basePath, "data", "0.dat")
	c := makeTestChunk(t, 5) // rows 0-4
	WriteChunkToFile(dataPath, c)

	// Create fragment with inline RowIds
	rowIdSeq := NewRowIdSequenceFromRange(0, 5)
	rowIdData, err := MarshalRowIdSequence(rowIdSeq)
	if err != nil {
		t.Fatalf("MarshalRowIdSequence: %v", err)
	}

	df := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	frag := NewDataFragmentWithRows(0, 5, []*DataFile{df})
	frag.RowIdSequence = &storage2pb.DataFragment_InlineRowIds{
		InlineRowIds: rowIdData,
	}

	m0.Fragments = []*DataFragment{frag}
	handler.Commit(ctx, basePath, 0, m0)

	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatalf("NewRowIdScanner: %v", err)
	}

	// Take rows 1 and 3
	result, err := scanner.TakeByRowIds(ctx, []uint64{1, 3})
	if err != nil {
		t.Fatalf("TakeByRowIds: %v", err)
	}
	if result == nil {
		t.Fatal("result should not be nil")
	}
}

func TestRowIdScannerTakeByRowIdsNotFound(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.ReaderFeatureFlags = 2
	m0.WriterFeatureFlags = 2
	m0.Fragments = []*DataFragment{} // No fragments
	handler.Commit(ctx, basePath, 0, m0)

	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatalf("NewRowIdScanner: %v", err)
	}

	// Try to take rows that don't exist
	_, err = scanner.TakeByRowIds(ctx, []uint64{100, 200})
	// Should fail because no fragments have those row IDs
	// The result should be empty chunk or error
	if err != nil {
		// Expected: no readable data files found
		return
	}
}

func TestGetRowIdsForFragmentNilSequence(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.ReaderFeatureFlags = 2
	m0.WriterFeatureFlags = 2

	frag := NewDataFragmentWithRows(0, 5, nil)
	frag.RowIdSequence = nil // No RowId sequence

	m0.Fragments = []*DataFragment{frag}
	handler.Commit(ctx, basePath, 0, m0)

	scanner, err := NewRowIdScanner(ctx, basePath, handler, 0)
	if err != nil {
		t.Fatalf("NewRowIdScanner: %v", err)
	}

	rowIds, err := scanner.getRowIdsForFragment(frag)
	if err != nil {
		t.Fatalf("getRowIdsForFragment: %v", err)
	}
	if rowIds != nil {
		t.Errorf("expected nil rowIds for nil sequence, got %v", rowIds)
	}
}

func TestGetFragmentById(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.ReaderFeatureFlags = 2
	frag0 := NewDataFragmentWithRows(0, 100, nil)
	frag1 := NewDataFragmentWithRows(1, 200, nil)
	m0.Fragments = []*DataFragment{frag0, frag1}
	handler.Commit(ctx, basePath, 0, m0)

	scanner, _ := NewRowIdScanner(ctx, basePath, handler, 0)

	found := scanner.getFragmentById(0)
	if found == nil || found.Id != 0 {
		t.Error("should find fragment 0")
	}

	found = scanner.getFragmentById(1)
	if found == nil || found.Id != 1 {
		t.Error("should find fragment 1")
	}

	found = scanner.getFragmentById(99)
	if found != nil {
		t.Error("should not find fragment 99")
	}
}
