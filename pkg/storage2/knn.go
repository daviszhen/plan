// Package storage2 provides KNN (K-Nearest Neighbors) vector search functionality.
package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SearchResult represents a single search result with row ID and distance.
type SearchResult struct {
	RowID    uint64  `json:"row_id"`
	Distance float32 `json:"distance"`
}

// SearchResults contains the results of a KNN search.
type SearchResults struct {
	Results   []SearchResult `json:"results"`
	QueryTime int64          `json:"query_time_ns"` // Query execution time in nanoseconds
	IndexType string         `json:"index_type"`
	Metric    MetricType     `json:"metric"`
}

// VectorSearchIndexConfig contains configuration for creating a vector index.
type VectorSearchIndexConfig struct {
	// Name is the unique name of the index.
	Name string `json:"name"`
	// ColumnIdx is the column index of the vector column.
	ColumnIdx int `json:"column_idx"`
	// Dimension is the dimension of the vectors.
	Dimension int `json:"dimension"`
	// Metric is the distance metric (L2, Cosine, Dot).
	Metric MetricType `json:"metric"`
	// IndexType is the type of index ("ivf" or "hnsw").
	IndexType string `json:"index_type"`
	// IVF-specific parameters
	NList  int `json:"nlist,omitempty"`  // Number of clusters for IVF
	NProbe int `json:"nprobe,omitempty"` // Number of clusters to probe during search
	// HNSW-specific parameters
	M              int `json:"m,omitempty"`               // Max connections per element
	EfConstruction int `json:"ef_construction,omitempty"` // EF during construction
	EfSearch       int `json:"ef_search,omitempty"`       // EF during search
}

// VectorSearchIndex is the interface for vector indexes that support KNN search.
type VectorSearchIndex interface {
	// ANNSearch performs approximate nearest neighbor search.
	ANNSearch(ctx context.Context, queryVector []float32, k int) ([]float32, []uint64, error)
	// Insert adds a vector to the index.
	Insert(rowID uint64, vector []float32) error
	// Train trains the index with the given vectors (for IVF).
	Train(vectors [][]float32) error
	// Name returns the index name.
	Name() string
	// GetMetricType returns the distance metric.
	GetMetricType() MetricType
	// Statistics returns index statistics.
	Statistics() IndexStats
}

// KNNIndexManager manages KNN indexes for a dataset.
type KNNIndexManager struct {
	basePath string
	handler  CommitHandler
	indexes  map[string]VectorSearchIndex
	mu       sync.RWMutex
}

// NewKNNIndexManager creates a new KNN index manager.
func NewKNNIndexManager(basePath string, handler CommitHandler) *KNNIndexManager {
	return &KNNIndexManager{
		basePath: basePath,
		handler:  handler,
		indexes:  make(map[string]VectorSearchIndex),
	}
}

// CreateIndex creates a new vector index with the given configuration.
func (m *KNNIndexManager) CreateIndex(ctx context.Context, config VectorSearchIndexConfig) (VectorSearchIndex, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if index already exists
	if _, exists := m.indexes[config.Name]; exists {
		return nil, fmt.Errorf("index %q already exists", config.Name)
	}

	var idx VectorSearchIndex

	switch config.IndexType {
	case "ivf":
		idx = m.createIVFIndex(config)
	case "hnsw":
		idx = m.createHNSWIndex(config)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", config.IndexType)
	}

	m.indexes[config.Name] = idx
	return idx, nil
}

func (m *KNNIndexManager) createIVFIndex(config VectorSearchIndexConfig) *IVFIndex {
	idx := NewIVFIndex(config.Name, config.ColumnIdx, config.Dimension, config.Metric)

	// Apply IVF-specific parameters
	if config.NList > 0 {
		idx.nlist = config.NList
		idx.centroids = make([][]float32, config.NList)
		idx.invertedLists = make([][]uint64, config.NList)
	}
	if config.NProbe > 0 {
		idx.nprobe = config.NProbe
	}

	return idx
}

func (m *KNNIndexManager) createHNSWIndex(config VectorSearchIndexConfig) *HNSWIndex {
	idx := NewHNSWIndex(config.Name, config.ColumnIdx, config.Dimension, config.Metric)

	// Apply HNSW-specific parameters
	if config.M > 0 {
		idx.M = config.M
		idx.Mmax = config.M
		idx.Mmax0 = 2 * config.M
		idx.levelMult = 1.0 / float64(config.M)
	}
	if config.EfConstruction > 0 {
		idx.efConstruction = config.EfConstruction
	}
	if config.EfSearch > 0 {
		idx.efSearch = config.EfSearch
	}

	return idx
}

// GetIndex returns an index by name.
func (m *KNNIndexManager) GetIndex(name string) (VectorSearchIndex, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	idx, ok := m.indexes[name]
	return idx, ok
}

// DropIndex drops an index by name.
func (m *KNNIndexManager) DropIndex(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.indexes[name]; !ok {
		return fmt.Errorf("index %q not found", name)
	}

	delete(m.indexes, name)
	return nil
}

// ListIndexes returns all index names.
func (m *KNNIndexManager) ListIndexes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.indexes))
	for name := range m.indexes {
		names = append(names, name)
	}
	return names
}

// AddIndex adds an existing index to the manager.
func (m *KNNIndexManager) AddIndex(name string, idx VectorSearchIndex) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.indexes[name]; exists {
		return fmt.Errorf("index %q already exists", name)
	}
	m.indexes[name] = idx
	return nil
}

// Search performs KNN search using the specified index.
func (m *KNNIndexManager) Search(ctx context.Context, indexName string, queryVector []float32, k int) (*SearchResults, error) {
	m.mu.RLock()
	idx, ok := m.indexes[indexName]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("index %q not found", indexName)
	}

	distances, rowIDs, err := idx.ANNSearch(ctx, queryVector, k)
	if err != nil {
		return nil, err
	}

	results := make([]SearchResult, len(rowIDs))
	for i, rowID := range rowIDs {
		results[i] = SearchResult{
			RowID:    rowID,
			Distance: distances[i],
		}
	}

	return &SearchResults{
		Results:   results,
		IndexType: idx.Statistics().IndexType,
		Metric:    idx.GetMetricType(),
	}, nil
}

// BuildIndex builds an index from data in the dataset.
func (m *KNNIndexManager) BuildIndex(ctx context.Context, indexName string, vectors map[uint64][]float32) error {
	m.mu.RLock()
	idx, ok := m.indexes[indexName]
	m.mu.RUnlock()

	if !ok {
		return fmt.Errorf("index %q not found", indexName)
	}

	// For IVF, train first
	if ivfIdx, ok := idx.(*IVFIndex); ok {
		vecList := make([][]float32, 0, len(vectors))
		for _, vec := range vectors {
			vecList = append(vecList, vec)
		}
		if len(vecList) > 0 {
			if err := ivfIdx.Train(vecList); err != nil {
				return fmt.Errorf("failed to train IVF index: %w", err)
			}
		}
	}

	// Insert all vectors
	for rowID, vec := range vectors {
		if err := idx.Insert(rowID, vec); err != nil {
			return fmt.Errorf("failed to insert vector for row %d: %w", rowID, err)
		}
	}

	return nil
}

// IndexPersistence handles saving and loading indexes.
type IndexPersistence struct {
	basePath string
	handler  CommitHandler
}

// NewIndexPersistence creates a new index persistence handler.
func NewIndexPersistence(basePath string, handler CommitHandler) *IndexPersistence {
	return &IndexPersistence{
		basePath: basePath,
		handler:  handler,
	}
}

// IndexDir is the directory for storing index files.
const IndexDir = "_indexes"

// SaveIndex saves an index to storage.
func (p *IndexPersistence) SaveIndex(ctx context.Context, name string, idx VectorSearchIndex) error {
	indexDir := filepath.Join(p.basePath, IndexDir)
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return fmt.Errorf("failed to create index directory: %w", err)
	}

	// Serialize index based on type
	var data []byte
	var err error

	switch i := idx.(type) {
	case *IVFIndex:
		data, err = p.serializeIVFIndex(i)
	case *HNSWIndex:
		data, err = p.serializeHNSWIndex(i)
	default:
		return fmt.Errorf("unsupported index type: %T", idx)
	}

	if err != nil {
		return fmt.Errorf("failed to serialize index: %w", err)
	}

	// Write to file
	indexPath := filepath.Join(indexDir, name+".idx")
	if err := os.WriteFile(indexPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write index file: %w", err)
	}

	return nil
}

// LoadIndex loads an index from storage.
func (p *IndexPersistence) LoadIndex(ctx context.Context, name string, config VectorSearchIndexConfig) (VectorSearchIndex, error) {
	indexPath := filepath.Join(p.basePath, IndexDir, name+".idx")
	data, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file: %w", err)
	}

	switch config.IndexType {
	case "ivf":
		return p.deserializeIVFIndex(data, config)
	case "hnsw":
		return p.deserializeHNSWIndex(data, config)
	default:
		return nil, fmt.Errorf("unsupported index type: %s", config.IndexType)
	}
}

// IVFIndexData is the serializable form of an IVF index.
type IVFIndexData struct {
	Name          string               `json:"name"`
	ColumnIdx     int                  `json:"column_idx"`
	Dimension     int                  `json:"dimension"`
	Metric        MetricType           `json:"metric"`
	NList         int                  `json:"nlist"`
	NProbe        int                  `json:"nprobe"`
	Centroids     [][]float32          `json:"centroids"`
	InvertedLists [][]uint64           `json:"inverted_lists"`
	Vectors       map[uint64][]float32 `json:"vectors"`
	NumEntries    uint64               `json:"num_entries"`
}

func (p *IndexPersistence) serializeIVFIndex(idx *IVFIndex) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	data := IVFIndexData{
		Name:          idx.name,
		ColumnIdx:     idx.columnIdx,
		Dimension:     idx.dimension,
		Metric:        idx.metricType,
		NList:         idx.nlist,
		NProbe:        idx.nprobe,
		Centroids:     idx.centroids,
		InvertedLists: idx.invertedLists,
		Vectors:       idx.vectors,
		NumEntries:    idx.stats.NumEntries,
	}

	return json.Marshal(data)
}

func (p *IndexPersistence) deserializeIVFIndex(data []byte, config VectorSearchIndexConfig) (*IVFIndex, error) {
	var idxData IVFIndexData
	if err := json.Unmarshal(data, &idxData); err != nil {
		return nil, err
	}

	idx := NewIVFIndex(idxData.Name, idxData.ColumnIdx, idxData.Dimension, idxData.Metric)
	idx.nlist = idxData.NList
	idx.nprobe = idxData.NProbe
	idx.centroids = idxData.Centroids
	idx.invertedLists = idxData.InvertedLists
	idx.vectors = idxData.Vectors
	idx.stats.NumEntries = idxData.NumEntries
	idx.updateStats()

	return idx, nil
}

// HNSWIndexData is the serializable form of an HNSW index.
type HNSWIndexData struct {
	Name        string                `json:"name"`
	ColumnIdx   int                   `json:"column_idx"`
	Dimension   int                   `json:"dimension"`
	Metric      MetricType            `json:"metric"`
	M           int                   `json:"m"`
	Mmax        int                   `json:"mmax"`
	Mmax0       int                   `json:"mmax0"`
	EfConstruct int                   `json:"ef_construction"`
	EfSearch    int                   `json:"ef_search"`
	Vectors     map[uint64][]float32  `json:"vectors"`
	Layers      []map[uint64][]uint64 `json:"layers"`
	EntryPoint  uint64                `json:"entry_point"`
	MaxLevel    int                   `json:"max_level"`
	NumEntries  uint64                `json:"num_entries"`
}

func (p *IndexPersistence) serializeHNSWIndex(idx *HNSWIndex) ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	data := HNSWIndexData{
		Name:        idx.name,
		ColumnIdx:   idx.columnIdx,
		Dimension:   idx.dimension,
		Metric:      idx.metricType,
		M:           idx.M,
		Mmax:        idx.Mmax,
		Mmax0:       idx.Mmax0,
		EfConstruct: idx.efConstruction,
		EfSearch:    idx.efSearch,
		Vectors:     idx.vectors,
		Layers:      idx.layers,
		EntryPoint:  idx.entryPoint,
		MaxLevel:    idx.maxLevel,
		NumEntries:  idx.stats.NumEntries,
	}

	return json.Marshal(data)
}

func (p *IndexPersistence) deserializeHNSWIndex(data []byte, config VectorSearchIndexConfig) (*HNSWIndex, error) {
	var idxData HNSWIndexData
	if err := json.Unmarshal(data, &idxData); err != nil {
		return nil, err
	}

	idx := NewHNSWIndex(idxData.Name, idxData.ColumnIdx, idxData.Dimension, idxData.Metric)
	idx.M = idxData.M
	idx.Mmax = idxData.Mmax
	idx.Mmax0 = idxData.Mmax0
	idx.efConstruction = idxData.EfConstruct
	idx.efSearch = idxData.EfSearch
	idx.vectors = idxData.Vectors
	idx.layers = idxData.Layers
	idx.entryPoint = idxData.EntryPoint
	idx.maxLevel = idxData.MaxLevel
	idx.stats.NumEntries = idxData.NumEntries
	idx.updateStats()

	return idx, nil
}

// DeleteIndex deletes an index file from storage.
func (p *IndexPersistence) DeleteIndex(name string) error {
	indexPath := filepath.Join(p.basePath, IndexDir, name+".idx")
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete index file: %w", err)
	}
	return nil
}

// ListStoredIndexes lists all stored index files.
func (p *IndexPersistence) ListStoredIndexes() ([]string, error) {
	indexDir := filepath.Join(p.basePath, IndexDir)
	entries, err := os.ReadDir(indexDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".idx" {
			name := entry.Name()[:len(entry.Name())-4] // Remove .idx extension
			names = append(names, name)
		}
	}
	return names, nil
}
