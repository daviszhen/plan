package storage2

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKNNIndexManager_CreateIVFIndex(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create initial manifest
	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(context.Background(), tmpDir, 0, m0))

	manager := NewKNNIndexManager(tmpDir, handler)

	config := VectorSearchIndexConfig{
		Name:      "test_ivf",
		ColumnIdx: 0,
		Dimension: 128,
		Metric:    L2Metric,
		IndexType: "ivf",
		NList:     10,
		NProbe:    3,
	}

	idx, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)
	require.NotNil(t, idx)
	require.Equal(t, "test_ivf", idx.Name())
	require.Equal(t, L2Metric, idx.GetMetricType())
}

func TestKNNIndexManager_CreateHNSWIndex(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(context.Background(), tmpDir, 0, m0))

	manager := NewKNNIndexManager(tmpDir, handler)

	config := VectorSearchIndexConfig{
		Name:           "test_hnsw",
		ColumnIdx:      0,
		Dimension:      64,
		Metric:         CosineMetric,
		IndexType:      "hnsw",
		M:              16,
		EfConstruction: 200,
		EfSearch:       50,
	}

	idx, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)
	require.NotNil(t, idx)
	require.Equal(t, "test_hnsw", idx.Name())
	require.Equal(t, CosineMetric, idx.GetMetricType())
}

func TestKNNIndexManager_SearchIVF(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(context.Background(), tmpDir, 0, m0))

	manager := NewKNNIndexManager(tmpDir, handler)

	config := VectorSearchIndexConfig{
		Name:      "test_ivf_search",
		ColumnIdx: 0,
		Dimension: 4,
		Metric:    L2Metric,
		IndexType: "ivf",
		NList:     2,
		NProbe:    2,
	}

	_, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)

	// Create training vectors
	vectors := make(map[uint64][]float32)
	// Cluster 1: near origin
	for i := 0; i < 10; i++ {
		vectors[uint64(i)] = []float32{float32(i) * 0.1, 0, 0, 0}
	}
	// Cluster 2: near (10, 10, 10, 10)
	for i := 10; i < 20; i++ {
		vectors[uint64(i)] = []float32{10 + float32(i-10)*0.1, 10, 10, 10}
	}

	// Build index
	err = manager.BuildIndex(context.Background(), "test_ivf_search", vectors)
	require.NoError(t, err)

	// Search
	query := []float32{0.5, 0, 0, 0}
	results, err := manager.Search(context.Background(), "test_ivf_search", query, 5)
	require.NoError(t, err)
	require.LessOrEqual(t, len(results.Results), 5)
	require.Greater(t, len(results.Results), 0)
	require.Equal(t, "ivf", results.IndexType)
	require.Equal(t, L2Metric, results.Metric)

	// Results should be sorted by distance
	for i := 1; i < len(results.Results); i++ {
		require.GreaterOrEqual(t, results.Results[i].Distance, results.Results[i-1].Distance)
	}
}

func TestKNNIndexManager_SearchHNSW(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(context.Background(), tmpDir, 0, m0))

	manager := NewKNNIndexManager(tmpDir, handler)

	config := VectorSearchIndexConfig{
		Name:           "test_hnsw_search",
		ColumnIdx:      0,
		Dimension:      4,
		Metric:         L2Metric,
		IndexType:      "hnsw",
		M:              8,
		EfConstruction: 50,
		EfSearch:       20,
	}

	idx, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)

	// Insert vectors
	for i := 0; i < 50; i++ {
		vector := []float32{float32(i), float32(i * 2), float32(i * 3), float32(i * 4)}
		err := idx.Insert(uint64(i), vector)
		require.NoError(t, err)
	}

	// Search
	query := []float32{5, 10, 15, 20}
	results, err := manager.Search(context.Background(), "test_hnsw_search", query, 5)
	require.NoError(t, err)
	require.LessOrEqual(t, len(results.Results), 5)
	require.Greater(t, len(results.Results), 0)
	require.Equal(t, "hnsw", results.IndexType)

	// Results should be sorted by distance
	for i := 1; i < len(results.Results); i++ {
		require.GreaterOrEqual(t, results.Results[i].Distance, results.Results[i-1].Distance)
	}
}

func TestKNNIndexManager_DropIndex(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(context.Background(), tmpDir, 0, m0))

	manager := NewKNNIndexManager(tmpDir, handler)

	config := VectorSearchIndexConfig{
		Name:      "test_drop",
		ColumnIdx: 0,
		Dimension: 16,
		Metric:    L2Metric,
		IndexType: "ivf",
	}

	_, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)

	// Verify index exists
	_, ok := manager.GetIndex("test_drop")
	require.True(t, ok)

	// Drop index
	err = manager.DropIndex("test_drop")
	require.NoError(t, err)

	// Verify index is gone
	_, ok = manager.GetIndex("test_drop")
	require.False(t, ok)

	// Dropping non-existent index should fail
	err = manager.DropIndex("nonexistent")
	require.Error(t, err)
}

func TestKNNIndexManager_ListIndexes(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	m0 := NewManifest(0)
	m0.Fragments = []*DataFragment{}
	m0.NextRowId = 1
	require.NoError(t, handler.Commit(context.Background(), tmpDir, 0, m0))

	manager := NewKNNIndexManager(tmpDir, handler)

	// Create multiple indexes
	for i := 0; i < 3; i++ {
		config := VectorSearchIndexConfig{
			Name:      "idx_" + string(rune('a'+i)),
			ColumnIdx: 0,
			Dimension: 16,
			Metric:    L2Metric,
			IndexType: "ivf",
		}
		_, err := manager.CreateIndex(context.Background(), config)
		require.NoError(t, err)
	}

	names := manager.ListIndexes()
	require.Len(t, names, 3)
}

func TestIndexPersistence_SaveLoadIVF(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create index
	manager := NewKNNIndexManager(tmpDir, handler)
	config := VectorSearchIndexConfig{
		Name:      "persist_ivf",
		ColumnIdx: 0,
		Dimension: 8,
		Metric:    CosineMetric,
		IndexType: "ivf",
		NList:     4,
		NProbe:    2,
	}

	idx, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)

	// Insert vectors
	vectors := make(map[uint64][]float32)
	for i := 0; i < 20; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3),
			float32(i + 4), float32(i + 5), float32(i + 6), float32(i + 7)}
		vectors[uint64(i)] = vec
	}
	err = manager.BuildIndex(context.Background(), "persist_ivf", vectors)
	require.NoError(t, err)

	// Save index
	persistence := NewIndexPersistence(tmpDir, handler)
	err = persistence.SaveIndex(context.Background(), "persist_ivf", idx)
	require.NoError(t, err)

	// Verify file exists
	indexPath := filepath.Join(tmpDir, IndexDir, "persist_ivf.idx")
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	// Load index
	loadedIdx, err := persistence.LoadIndex(context.Background(), "persist_ivf", config)
	require.NoError(t, err)
	require.NotNil(t, loadedIdx)
	require.Equal(t, "persist_ivf", loadedIdx.Name())
	require.Equal(t, CosineMetric, loadedIdx.GetMetricType())

	// Verify loaded index works
	stats := loadedIdx.Statistics()
	require.Equal(t, uint64(20), stats.NumEntries)
}

func TestIndexPersistence_SaveLoadHNSW(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	// Create index
	manager := NewKNNIndexManager(tmpDir, handler)
	config := VectorSearchIndexConfig{
		Name:           "persist_hnsw",
		ColumnIdx:      0,
		Dimension:      8,
		Metric:         DotMetric,
		IndexType:      "hnsw",
		M:              8,
		EfConstruction: 50,
		EfSearch:       20,
	}

	idx, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)

	// Insert vectors
	for i := 0; i < 30; i++ {
		vec := []float32{float32(i), float32(i + 1), float32(i + 2), float32(i + 3),
			float32(i + 4), float32(i + 5), float32(i + 6), float32(i + 7)}
		err := idx.Insert(uint64(i), vec)
		require.NoError(t, err)
	}

	// Save index
	persistence := NewIndexPersistence(tmpDir, handler)
	err = persistence.SaveIndex(context.Background(), "persist_hnsw", idx)
	require.NoError(t, err)

	// Load index
	loadedIdx, err := persistence.LoadIndex(context.Background(), "persist_hnsw", config)
	require.NoError(t, err)
	require.NotNil(t, loadedIdx)
	require.Equal(t, "persist_hnsw", loadedIdx.Name())
	require.Equal(t, DotMetric, loadedIdx.GetMetricType())

	// Verify loaded index works
	stats := loadedIdx.Statistics()
	require.Equal(t, uint64(30), stats.NumEntries)
}

func TestIndexPersistence_DeleteIndex(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	manager := NewKNNIndexManager(tmpDir, handler)
	config := VectorSearchIndexConfig{
		Name:      "delete_test",
		ColumnIdx: 0,
		Dimension: 4,
		Metric:    L2Metric,
		IndexType: "ivf",
	}

	idx, err := manager.CreateIndex(context.Background(), config)
	require.NoError(t, err)

	persistence := NewIndexPersistence(tmpDir, handler)
	err = persistence.SaveIndex(context.Background(), "delete_test", idx)
	require.NoError(t, err)

	// Verify file exists
	indexPath := filepath.Join(tmpDir, IndexDir, "delete_test.idx")
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	// Delete index
	err = persistence.DeleteIndex("delete_test")
	require.NoError(t, err)

	// Verify file is gone
	_, err = os.Stat(indexPath)
	require.True(t, os.IsNotExist(err))
}

func TestIndexPersistence_ListStoredIndexes(t *testing.T) {
	tmpDir := t.TempDir()
	handler := NewLocalRenameCommitHandler()

	manager := NewKNNIndexManager(tmpDir, handler)
	persistence := NewIndexPersistence(tmpDir, handler)

	// Create and save multiple indexes
	for i := 0; i < 3; i++ {
		config := VectorSearchIndexConfig{
			Name:      "stored_" + string(rune('a'+i)),
			ColumnIdx: 0,
			Dimension: 4,
			Metric:    L2Metric,
			IndexType: "ivf",
		}
		idx, err := manager.CreateIndex(context.Background(), config)
		require.NoError(t, err)
		err = persistence.SaveIndex(context.Background(), config.Name, idx)
		require.NoError(t, err)
	}

	// List stored indexes
	names, err := persistence.ListStoredIndexes()
	require.NoError(t, err)
	require.Len(t, names, 3)
}

func TestSearchResults(t *testing.T) {
	results := &SearchResults{
		Results: []SearchResult{
			{RowID: 1, Distance: 0.1},
			{RowID: 5, Distance: 0.3},
			{RowID: 3, Distance: 0.5},
		},
		QueryTime: 1000000,
		IndexType: "hnsw",
		Metric:    CosineMetric,
	}

	require.Len(t, results.Results, 3)
	require.Equal(t, uint64(1), results.Results[0].RowID)
	require.Equal(t, float32(0.1), results.Results[0].Distance)
}

func TestVectorSearchIndexConfig(t *testing.T) {
	config := VectorSearchIndexConfig{
		Name:           "test_config",
		ColumnIdx:      2,
		Dimension:      256,
		Metric:         CosineMetric,
		IndexType:      "hnsw",
		M:              32,
		EfConstruction: 400,
		EfSearch:       100,
	}

	require.Equal(t, "test_config", config.Name)
	require.Equal(t, 2, config.ColumnIdx)
	require.Equal(t, 256, config.Dimension)
	require.Equal(t, CosineMetric, config.Metric)
	require.Equal(t, "hnsw", config.IndexType)
	require.Equal(t, 32, config.M)
	require.Equal(t, 400, config.EfConstruction)
	require.Equal(t, 100, config.EfSearch)
}
