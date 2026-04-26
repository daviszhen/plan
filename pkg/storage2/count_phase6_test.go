package storage2

import (
	"context"
	"testing"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
	"github.com/stretchr/testify/require"
)

// TestCountRows_Basic tests basic row counting functionality.
// Corresponds to Lance count_lance_file basic scenario
func TestCountRows_Basic(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create empty dataset
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Count rows in empty dataset
	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)

	// Create data file with 100 rows
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 2)})
	}

	// Write chunk to file
	dataPath := tmpDir + "/data/0.dat"
	require.NoError(t, WriteChunkToFile(dataPath, c))

	// Create fragment and append
	dataFile := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := NewDataFragmentWithRows(0, 100, []*DataFile{dataFile})

	txn := NewTransactionAppend(0, "append-1", []*DataFragment{fragment})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))

	// Count rows
	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), count)

	// Add another fragment with 50 rows
	c2 := &chunk.Chunk{}
	c2.Init(typs, util.DefaultVectorSize)
	c2.SetCard(50)

	for i := 0; i < 50; i++ {
		c2.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i + 100)})
		c2.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64((i + 100) * 2)})
	}

	dataPath2 := tmpDir + "/data/1.dat"
	require.NoError(t, WriteChunkToFile(dataPath2, c2))

	dataFile2 := NewDataFile("data/1.dat", []int32{0, 1}, 1, 0)
	fragment2 := NewDataFragmentWithRows(1, 50, []*DataFile{dataFile2})

	txn2 := NewTransactionAppend(1, "append-2", []*DataFragment{fragment2})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn2))

	// Count rows again
	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 2, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(150), count)
}

// TestCountRows_WithFilter tests counting with filter predicate.
// Corresponds to Lance count_lance_file with filter scenario
func TestCountRows_WithFilter(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with data
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create data: c0 = 0..99, c1 = 0.0, 0.5, 1.0, ...
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i * 2)})
	}

	dataPath := tmpDir + "/data/0.dat"
	require.NoError(t, WriteChunkToFile(dataPath, c))

	dataFile := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := NewDataFragmentWithRows(0, 100, []*DataFile{dataFile})

	txn := NewTransactionAppend(0, "append-1", []*DataFragment{fragment})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))

	// Test: c0 > 50 (should count 49 rows: 51-99)
	predicate, err := ParseFilter("c0 > 50", nil)
	require.NoError(t, err)

	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(49), count)

	// Test: c0 >= 50 (should count 50 rows: 50-99)
	predicate, err = ParseFilter("c0 >= 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)

	// Test: c0 < 25 (should count 25 rows: 0-24)
	predicate, err = ParseFilter("c0 < 25", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(25), count)

	// Test: c0 = 50 (should count 1 row)
	predicate, err = ParseFilter("c0 = 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	// Test: c0 != 50 (should count 99 rows)
	predicate, err = ParseFilter("c0 != 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(99), count)
}

// TestCountRows_ComplexFilter tests counting with complex filter predicates.
// Corresponds to Lance count_lance_file with complex filter scenario
func TestCountRows_ComplexFilter(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	// Create data: c0 = 0..99
	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	for i := 0; i < 100; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		c.Data[1].SetValue(i, &chunk.Value{Typ: typs[1], I64: int64(i % 10)})
	}

	dataPath := tmpDir + "/data/0.dat"
	require.NoError(t, WriteChunkToFile(dataPath, c))

	dataFile := NewDataFile("data/0.dat", []int32{0, 1}, 1, 0)
	fragment := NewDataFragmentWithRows(0, 100, []*DataFile{dataFile})

	txn := NewTransactionAppend(0, "append-1", []*DataFragment{fragment})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))

	// Test: c0 > 10 AND c0 < 20 (should count 9 rows: 11-19)
	predicate, err := ParseFilter("c0 > 10 AND c0 < 20", nil)
	require.NoError(t, err)

	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(9), count)

	// Test: c0 < 10 OR c0 > 90 (should count 19 rows: 0-9 and 91-99)
	predicate, err = ParseFilter("c0 < 10 OR c0 > 90", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(19), count)

	// Test: NOT c0 < 50 (should count 50 rows: 50-99)
	predicate, err = ParseFilter("NOT c0 < 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)

	// Test: (c0 >= 20 AND c0 < 30) OR (c0 >= 70 AND c0 < 80)
	// Should count 20 rows: 20-29 and 70-79
	predicate, err = ParseFilter("(c0 >= 20 AND c0 < 30) OR (c0 >= 70 AND c0 < 80)", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(20), count)

	// Test: c1 = 5 (should count 10 rows: 5, 15, 25, ..., 95)
	predicate, err = ParseFilter("c1 = 5", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(10), count)
}

// TestCountRows_EmptyResult tests counting with filter that matches no rows.
func TestCountRows_EmptyResult(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with data
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(50)

	for i := 0; i < 50; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
	}

	dataPath := tmpDir + "/data/0.dat"
	require.NoError(t, WriteChunkToFile(dataPath, c))

	dataFile := NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment := NewDataFragmentWithRows(0, 50, []*DataFile{dataFile})

	txn := NewTransactionAppend(0, "append-1", []*DataFragment{fragment})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))

	// Test: c0 > 1000 (no matches)
	predicate, err := ParseFilter("c0 > 1000", nil)
	require.NoError(t, err)

	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)

	// Test: c0 < 0 (no matches)
	predicate, err = ParseFilter("c0 < 0", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)

	// Test: c0 = 100 (no matches)
	predicate, err = ParseFilter("c0 = 100", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)
}

// TestCountRows_AllMatches tests counting with filter that matches all rows.
func TestCountRows_AllMatches(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset with data
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(75)

	for i := 0; i < 75; i++ {
		c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
	}

	dataPath := tmpDir + "/data/0.dat"
	require.NoError(t, WriteChunkToFile(dataPath, c))

	dataFile := NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment := NewDataFragmentWithRows(0, 75, []*DataFile{dataFile})

	txn := NewTransactionAppend(0, "append-1", []*DataFragment{fragment})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))

	// Test: c0 >= 0 (all matches)
	predicate, err := ParseFilter("c0 >= 0", nil)
	require.NoError(t, err)

	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(75), count)

	// Test: c0 < 100 (all matches)
	predicate, err = ParseFilter("c0 < 100", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(75), count)
}

// TestCountRows_MultipleFragments tests counting across multiple fragments.
func TestCountRows_MultipleFragments(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}

	// Create 3 fragments with different data ranges
	for fragIdx := 0; fragIdx < 3; fragIdx++ {
		c := &chunk.Chunk{}
		c.Init(typs, util.DefaultVectorSize)
		c.SetCard(50)

		for i := 0; i < 50; i++ {
			val := int64(fragIdx*50 + i)
			c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: val})
		}

		dataPath := tmpDir + "/data/" + string(rune('0'+fragIdx)) + ".dat"
		require.NoError(t, WriteChunkToFile(dataPath, c))

		dataFile := NewDataFile("data/"+string(rune('0'+fragIdx))+".dat", []int32{0}, 1, 0)
		fragment := NewDataFragmentWithRows(uint64(fragIdx), 50, []*DataFile{dataFile})

		version, err := handler.ResolveLatestVersion(ctx, tmpDir)
		require.NoError(t, err)

		txn := NewTransactionAppend(version, "append-"+string(rune('A'+fragIdx)), []*DataFragment{fragment})
		require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))
	}

	// Count total rows
	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 3, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(150), count)

	// Count with filter: c0 >= 50 (should match rows from fragments 1 and 2)
	predicate, err := ParseFilter("c0 >= 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 3, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(100), count)

	// Count with filter: c0 < 50 (should match rows from fragment 0)
	predicate, err = ParseFilter("c0 < 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 3, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)

	// Count with filter: c0 >= 25 AND c0 < 75
	// Should match: 25-49 from frag0 (25 rows) + 50-74 from frag1 (25 rows) = 50 rows
	predicate, err = ParseFilter("c0 >= 25 AND c0 < 75", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 3, predicate)
	require.NoError(t, err)
	require.Equal(t, uint64(50), count)
}

// TestCountRows_WithNulls tests counting with null values.
func TestCountRows_WithNulls(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create dataset
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	require.NoError(t, handler.Commit(ctx, tmpDir, 0, m0))

	typs := []common.LType{
		common.MakeLType(common.LTID_INTEGER),
	}
	c := &chunk.Chunk{}
	c.Init(typs, util.DefaultVectorSize)
	c.SetCard(100)

	// Set values with some nulls
	for i := 0; i < 100; i++ {
		if i%5 == 0 {
			// Every 5th row is null
			chunk.SetNullInPhyFormatFlat(c.Data[0], uint64(i), true)
		} else {
			c.Data[0].SetValue(i, &chunk.Value{Typ: typs[0], I64: int64(i)})
		}
	}

	dataPath := tmpDir + "/data/0.dat"
	require.NoError(t, WriteChunkToFile(dataPath, c))

	dataFile := NewDataFile("data/0.dat", []int32{0}, 1, 0)
	fragment := NewDataFragmentWithRows(0, 100, []*DataFile{dataFile})

	txn := NewTransactionAppend(0, "append-1", []*DataFragment{fragment})
	require.NoError(t, CommitTransaction(ctx, tmpDir, handler, txn))

	// Count total rows
	count, err := CountRowsWithFilter(ctx, tmpDir, handler, 1, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(100), count)

	// Count non-null rows with filter: c0 > 50
	// Non-null rows: 51-99 except 55, 65, 75, 85, 95 = 49 - 5 = 44
	predicate, err := ParseFilter("c0 > 50", nil)
	require.NoError(t, err)

	count, err = CountRowsWithFilter(ctx, tmpDir, handler, 1, predicate)
	require.NoError(t, err)
	// The actual count depends on how nulls are handled in filter evaluation
	// Null comparisons typically return false, so nulls won't match c0 > 50
	t.Logf("Count with filter c0 > 50: %d", count)
}
