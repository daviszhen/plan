package storage2

import (
	"container/heap"
	"context"
	"math"
	"math/rand"
	"sync"
)

// HNSWIndex is a Hierarchical Navigable Small World index for approximate nearest neighbor search
type HNSWIndex struct {
	name       string
	columnIdx  int
	indexType  IndexType
	metricType MetricType

	// HNSW parameters
	M              int     // maximum number of connections per element
	Mmax           int     // maximum number of connections for layer 0
	Mmax0          int     // maximum number of connections for layer 0 (usually 2*M)
	efConstruction int     // size of dynamic candidate list during construction
	efSearch       int     // size of dynamic candidate list during search
	levelMult      float64 // level multiplier for layer generation

	// Index data
	vectors    map[uint64][]float32  // vector storage
	layers     []map[uint64][]uint64 // connections for each layer
	entryPoint uint64                // entry point for search
	maxLevel   int                   // maximum level in the hierarchy
	dimension  int

	mu    sync.RWMutex
	stats IndexStats
}

// HNSWNode represents a node in the HNSW graph
type HNSWNode struct {
	id        uint64
	vector    []float32
	level     int
	neighbors [][]uint64 // neighbors for each level
}

// NewHNSWIndex creates a new HNSW index
func NewHNSWIndex(name string, columnIdx int, dimension int, metric MetricType) *HNSWIndex {
	M := 16
	return &HNSWIndex{
		name:           name,
		columnIdx:      columnIdx,
		indexType:      VectorIndex,
		metricType:     metric,
		M:              M,
		Mmax:           M,
		Mmax0:          2 * M,
		efConstruction: 200,
		efSearch:       50,
		levelMult:      1.0 / math.Log(float64(M)),
		vectors:        make(map[uint64][]float32),
		layers:         make([]map[uint64][]uint64, 0),
		entryPoint:     0,
		maxLevel:       -1,
		dimension:      dimension,
		stats: IndexStats{
			NumEntries: 0,
			SizeBytes:  0,
		},
	}
}

// Name returns the index name
func (idx *HNSWIndex) Name() string {
	return idx.name
}

// Type returns the index type
func (idx *HNSWIndex) Type() IndexType {
	return idx.indexType
}

// Columns returns the column indices this index covers
func (idx *HNSWIndex) Columns() []int {
	return []int{idx.columnIdx}
}

// Search searches the index and returns matching row IDs
func (idx *HNSWIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	queryVector, ok := query.([]float32)
	if !ok {
		return nil, nil
	}

	_, rowIDs, err := idx.ANNSearch(ctx, queryVector, limit)
	return rowIDs, err
}

// Statistics returns statistics about the index
func (idx *HNSWIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// ANNSearch performs approximate nearest neighbor search
func (idx *HNSWIndex) ANNSearch(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(queryVector) != idx.dimension {
		return nil, nil, nil
	}

	if idx.maxLevel < 0 || len(idx.vectors) == 0 {
		return nil, nil, nil
	}

	// Search from top layer to layer 1
	currentNode := idx.entryPoint
	currentDist := idx.computeDistance(queryVector, idx.vectors[currentNode])

	for level := idx.maxLevel; level > 0; level-- {
		changed := true
		for changed {
			changed = false
			if neighbors, ok := idx.layers[level][currentNode]; ok {
				for _, neighbor := range neighbors {
					if dist := idx.computeDistance(queryVector, idx.vectors[neighbor]); dist < currentDist {
						currentDist = dist
						currentNode = neighbor
						changed = true
					}
				}
			}
		}
	}

	// Search at layer 0 with efSearch
	candidates := idx.searchLayer(queryVector, []uint64{currentNode}, idx.efSearch, 0)

	// Extract results
	results := make([]struct {
		rowID    uint64
		distance float32
	}, 0, len(candidates))

	for nodeID := range candidates {
		dist := idx.computeDistance(queryVector, idx.vectors[nodeID])
		results = append(results, struct {
			rowID    uint64
			distance float32
		}{rowID: nodeID, distance: dist})
	}

	// Sort by distance
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].distance < results[i].distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	// Return top k
	if limit > len(results) {
		limit = len(results)
	}

	distances := make([]float32, limit)
	rowIDs := make([]uint64, limit)
	for i := 0; i < limit; i++ {
		distances[i] = results[i].distance
		rowIDs[i] = results[i].rowID
	}

	return distances, rowIDs, nil
}

// GetMetricType returns the distance metric used by the index
func (idx *HNSWIndex) GetMetricType() MetricType {
	return idx.metricType
}

// Insert adds a vector to the index
func (idx *HNSWIndex) Insert(rowID uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vector) != idx.dimension {
		return nil
	}

	// Store the vector
	idx.vectors[rowID] = make([]float32, len(vector))
	copy(idx.vectors[rowID], vector)

	// Generate random level for new node
	level := idx.randomLevel()

	// Ensure layers exist
	for len(idx.layers) <= level {
		idx.layers = append(idx.layers, make(map[uint64][]uint64))
	}

	// Initialize connections for new node at each level
	for l := 0; l <= level; l++ {
		idx.layers[l][rowID] = make([]uint64, 0)
	}

	// If this is the first node, set it as entry point
	if idx.maxLevel < 0 {
		idx.entryPoint = rowID
		idx.maxLevel = level
		idx.stats.NumEntries++
		idx.updateStats()
		return nil
	}

	// Search from top layer to layer 1 to find entry point for layer 0
	currentNode := idx.entryPoint

	for l := idx.maxLevel; l > 0; l-- {
		if l <= level {
			// Find neighbors at this level
			neighbors := idx.searchLayer(vector, []uint64{currentNode}, idx.efConstruction, l)
			selectedNeighbors := idx.selectNeighbors(vector, neighbors, idx.M)

			// Add connections
			idx.layers[l][rowID] = selectedNeighbors

			// Update reverse connections
			for _, neighbor := range selectedNeighbors {
				idx.layers[l][neighbor] = idx.addConnection(idx.layers[l][neighbor], rowID, idx.M)
			}
		}

		// Find closest node at this level for next iteration
		if l > level {
			minDist := idx.computeDistance(vector, idx.vectors[currentNode])
			if neighbors, ok := idx.layers[l][currentNode]; ok {
				for _, neighbor := range neighbors {
					if dist := idx.computeDistance(vector, idx.vectors[neighbor]); dist < minDist {
						minDist = dist
						currentNode = neighbor
					}
				}
			}
		}
	}

	// Handle layer 0
	neighbors := idx.searchLayer(vector, []uint64{currentNode}, idx.efConstruction, 0)
	selectedNeighbors := idx.selectNeighbors(vector, neighbors, idx.Mmax0)

	idx.layers[0][rowID] = selectedNeighbors

	// Update reverse connections at layer 0
	for _, neighbor := range selectedNeighbors {
		idx.layers[0][neighbor] = idx.addConnection(idx.layers[0][neighbor], rowID, idx.Mmax0)
	}

	// Update entry point if necessary
	if level > idx.maxLevel {
		idx.maxLevel = level
		idx.entryPoint = rowID
	}

	idx.stats.NumEntries++
	idx.updateStats()
	return nil
}

// SetEfSearch sets the efSearch parameter
func (idx *HNSWIndex) SetEfSearch(ef int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.efSearch = ef
}

// Train is a no-op for HNSW index (it doesn't require training like IVF).
// This method exists to satisfy the VectorSearchIndex interface.
func (idx *HNSWIndex) Train(vectors [][]float32) error {
	// HNSW doesn't require training, just return nil
	return nil
}

// randomLevel generates a random level for a new node
func (idx *HNSWIndex) randomLevel() int {
	level := 0
	for rand.Float64() < idx.levelMult && level < 16 {
		level++
	}
	return level
}

// searchLayer performs a greedy search in a specific layer
func (idx *HNSWIndex) searchLayer(queryVector []float32, entryPoints []uint64, ef int, level int) map[uint64]bool {
	visited := make(map[uint64]bool)
	candidates := &nodeDistanceHeap{}
	results := &nodeDistanceHeap{}

	for _, entry := range entryPoints {
		visited[entry] = true
		dist := idx.computeDistance(queryVector, idx.vectors[entry])
		heap.Push(candidates, nodeDistance{nodeID: entry, distance: dist})
		heap.Push(results, nodeDistance{nodeID: entry, distance: dist})
	}

	for candidates.Len() > 0 {
		current := heap.Pop(candidates).(nodeDistance)

		if results.Len() >= ef {
			worstResult := (*results)[0]
			if current.distance > worstResult.distance {
				break
			}
		}

		if neighbors, ok := idx.layers[level][current.nodeID]; ok {
			for _, neighbor := range neighbors {
				if !visited[neighbor] {
					visited[neighbor] = true
					dist := idx.computeDistance(queryVector, idx.vectors[neighbor])

					if results.Len() < ef {
						heap.Push(candidates, nodeDistance{nodeID: neighbor, distance: dist})
						heap.Push(results, nodeDistance{nodeID: neighbor, distance: dist})
					} else if dist < (*results)[0].distance {
						heap.Pop(results)
						heap.Push(results, nodeDistance{nodeID: neighbor, distance: dist})
						heap.Push(candidates, nodeDistance{nodeID: neighbor, distance: dist})
					}
				}
			}
		}
	}

	return visited
}

// selectNeighbors selects the M closest neighbors from candidates
func (idx *HNSWIndex) selectNeighbors(queryVector []float32, candidates map[uint64]bool, M int) []uint64 {
	type neighborDist struct {
		nodeID   uint64
		distance float32
	}

	neighbors := make([]neighborDist, 0, len(candidates))
	for nodeID := range candidates {
		dist := idx.computeDistance(queryVector, idx.vectors[nodeID])
		neighbors = append(neighbors, neighborDist{nodeID: nodeID, distance: dist})
	}

	// Sort by distance
	for i := 0; i < len(neighbors)-1; i++ {
		for j := i + 1; j < len(neighbors); j++ {
			if neighbors[j].distance < neighbors[i].distance {
				neighbors[i], neighbors[j] = neighbors[j], neighbors[i]
			}
		}
	}

	// Return top M
	if M > len(neighbors) {
		M = len(neighbors)
	}

	result := make([]uint64, M)
	for i := 0; i < M; i++ {
		result[i] = neighbors[i].nodeID
	}
	return result
}

// addConnection adds a connection to a node's neighbor list, maintaining size limit
func (idx *HNSWIndex) addConnection(neighbors []uint64, newNeighbor uint64, maxConn int) []uint64 {
	// Check if already exists
	for _, n := range neighbors {
		if n == newNeighbor {
			return neighbors
		}
	}

	neighbors = append(neighbors, newNeighbor)
	if len(neighbors) > maxConn {
		// Remove the oldest connection (simple strategy)
		neighbors = neighbors[1:]
	}
	return neighbors
}

// computeDistance computes the distance between two vectors
func (idx *HNSWIndex) computeDistance(a, b []float32) float32 {
	switch idx.metricType {
	case L2Metric:
		return l2Distance(a, b)
	case CosineMetric:
		return cosineDistance(a, b)
	case DotMetric:
		return -dotProduct(a, b)
	default:
		return l2Distance(a, b)
	}
}

// updateStats updates index statistics
func (idx *HNSWIndex) updateStats() {
	idx.stats.SizeBytes = uint64(len(idx.vectors)) * uint64(idx.dimension) * 4
	idx.stats.IndexType = "hnsw"
}

// nodeDistance represents a node and its distance from a query
type nodeDistance struct {
	nodeID   uint64
	distance float32
}

// nodeDistanceHeap is a min-heap for node distances
type nodeDistanceHeap []nodeDistance

func (h nodeDistanceHeap) Len() int           { return len(h) }
func (h nodeDistanceHeap) Less(i, j int) bool { return h[i].distance < h[j].distance }
func (h nodeDistanceHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *nodeDistanceHeap) Push(x interface{}) {
	*h = append(*h, x.(nodeDistance))
}

func (h *nodeDistanceHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Ensure HNSWIndex implements VectorIndexImpl
var _ VectorIndexImpl = (*HNSWIndex)(nil)
