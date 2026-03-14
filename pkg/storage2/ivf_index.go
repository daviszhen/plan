package storage2

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// IVFIndex is an Inverted File index for approximate nearest neighbor search
type IVFIndex struct {
	name          string
	columnIdx     int
	indexType     IndexType
	metricType    MetricType
	nlist         int // number of clusters
	nprobe        int // number of clusters to search
	centroids     [][]float32
	invertedLists [][]uint64           // row IDs for each cluster
	vectors       map[uint64][]float32 // store vectors for lookup
	mu            sync.RWMutex
	stats         IndexStats
	dimension     int
}

// NewIVFIndex creates a new IVF index
func NewIVFIndex(name string, columnIdx int, dimension int, metric MetricType) *IVFIndex {
	nlist := 100 // default number of clusters
	if nlist > dimension {
		nlist = dimension
	}

	return &IVFIndex{
		name:          name,
		columnIdx:     columnIdx,
		indexType:     VectorIndex,
		metricType:    metric,
		nlist:         nlist,
		nprobe:        10, // default number of clusters to probe
		centroids:     make([][]float32, nlist),
		invertedLists: make([][]uint64, nlist),
		vectors:       make(map[uint64][]float32),
		dimension:     dimension,
		stats: IndexStats{
			NumEntries: 0,
			SizeBytes:  0,
		},
	}
}

// Name returns the index name
func (idx *IVFIndex) Name() string {
	return idx.name
}

// Type returns the index type
func (idx *IVFIndex) Type() IndexType {
	return idx.indexType
}

// Columns returns the column indices this index covers
func (idx *IVFIndex) Columns() []int {
	return []int{idx.columnIdx}
}

// Search searches the index and returns matching row IDs
func (idx *IVFIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	queryVector, ok := query.([]float32)
	if !ok {
		return nil, nil
	}

	_, rowIDs, err := idx.ANNSearch(ctx, queryVector, limit)
	return rowIDs, err
}

// Statistics returns statistics about the index
func (idx *IVFIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// ANNSearch performs approximate nearest neighbor search
func (idx *IVFIndex) ANNSearch(ctx context.Context, queryVector []float32, limit int) ([]float32, []uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(queryVector) != idx.dimension {
		return nil, nil, nil
	}

	// Find the nprobe closest centroids
	centroidDistances := make([]struct {
		idx      int
		distance float32
	}, idx.nlist)

	for i, centroid := range idx.centroids {
		if centroid != nil {
			centroidDistances[i] = struct {
				idx      int
				distance float32
			}{
				idx:      i,
				distance: idx.computeDistance(queryVector, centroid),
			}
		} else {
			centroidDistances[i] = struct {
				idx      int
				distance float32
			}{
				idx:      i,
				distance: math.MaxFloat32,
			}
		}
	}

	// Sort by distance
	sort.Slice(centroidDistances, func(i, j int) bool {
		return centroidDistances[i].distance < centroidDistances[j].distance
	})

	// Search in the closest nprobe clusters
	type result struct {
		rowID    uint64
		distance float32
	}
	var results []result

	for i := 0; i < idx.nprobe && i < idx.nlist; i++ {
		clusterIdx := centroidDistances[i].idx
		for _, rowID := range idx.invertedLists[clusterIdx] {
			if vector, ok := idx.vectors[rowID]; ok {
				distance := idx.computeDistance(queryVector, vector)
				results = append(results, result{rowID: rowID, distance: distance})
			}
		}
	}

	// Sort by distance and return top k
	sort.Slice(results, func(i, j int) bool {
		return results[i].distance < results[j].distance
	})

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
func (idx *IVFIndex) GetMetricType() MetricType {
	return idx.metricType
}

// Insert adds a vector to the index
func (idx *IVFIndex) Insert(rowID uint64, vector []float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vector) != idx.dimension {
		return nil
	}

	// Store the vector
	idx.vectors[rowID] = make([]float32, len(vector))
	copy(idx.vectors[rowID], vector)

	// Find the closest centroid
	minDistance := float32(math.MaxFloat32)
	closestCluster := 0

	for i, centroid := range idx.centroids {
		if centroid != nil {
			distance := idx.computeDistance(vector, centroid)
			if distance < minDistance {
				minDistance = distance
				closestCluster = i
			}
		}
	}

	// Add to inverted list
	idx.invertedLists[closestCluster] = append(idx.invertedLists[closestCluster], rowID)
	idx.stats.NumEntries++
	idx.updateStats()

	return nil
}

// Train trains the index with a set of vectors using k-means clustering
func (idx *IVFIndex) Train(vectors [][]float32) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(vectors) == 0 {
		return nil
	}

	// Simple k-means clustering
	// Initialize centroids randomly
	for i := 0; i < idx.nlist && i < len(vectors); i++ {
		idx.centroids[i] = make([]float32, idx.dimension)
		copy(idx.centroids[i], vectors[rand.Intn(len(vectors))])
	}

	// Run k-means iterations
	maxIterations := 10
	for iter := 0; iter < maxIterations; iter++ {
		// Assign vectors to clusters
		assignments := make([]int, len(vectors))
		for i, vector := range vectors {
			minDistance := float32(math.MaxFloat32)
			closestCluster := 0
			for j, centroid := range idx.centroids {
				if centroid != nil {
					distance := idx.computeDistance(vector, centroid)
					if distance < minDistance {
						minDistance = distance
						closestCluster = j
					}
				}
			}
			assignments[i] = closestCluster
		}

		// Update centroids
		newCentroids := make([][]float32, idx.nlist)
		counts := make([]int, idx.nlist)

		for i := range newCentroids {
			newCentroids[i] = make([]float32, idx.dimension)
		}

		for i, vector := range vectors {
			cluster := assignments[i]
			counts[cluster]++
			for j := 0; j < idx.dimension; j++ {
				newCentroids[cluster][j] += vector[j]
			}
		}

		for i := 0; i < idx.nlist; i++ {
			if counts[i] > 0 {
				for j := 0; j < idx.dimension; j++ {
					newCentroids[i][j] /= float32(counts[i])
				}
				idx.centroids[i] = newCentroids[i]
			}
		}
	}

	return nil
}

// SetNProbe sets the number of clusters to search
func (idx *IVFIndex) SetNProbe(nprobe int) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.nprobe = nprobe
}

// computeDistance computes the distance between two vectors
func (idx *IVFIndex) computeDistance(a, b []float32) float32 {
	switch idx.metricType {
	case L2Metric:
		return l2Distance(a, b)
	case CosineMetric:
		return cosineDistance(a, b)
	case DotMetric:
		return -dotProduct(a, b) // negate for ascending sort
	default:
		return l2Distance(a, b)
	}
}

// l2Distance computes L2 (Euclidean) distance
func l2Distance(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a) && i < len(b); i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return float32(math.Sqrt(float64(sum)))
}

// cosineDistance computes cosine distance (1 - cosine similarity)
func cosineDistance(a, b []float32) float32 {
	dot := dotProduct(a, b)
	normA := float32(math.Sqrt(float64(dotProduct(a, a))))
	normB := float32(math.Sqrt(float64(dotProduct(b, b))))

	if normA == 0 || normB == 0 {
		return 1.0
	}

	return 1.0 - dot/(normA*normB)
}

// dotProduct computes dot product
func dotProduct(a, b []float32) float32 {
	var sum float32
	for i := 0; i < len(a) && i < len(b); i++ {
		sum += a[i] * b[i]
	}
	return sum
}

// updateStats updates index statistics
func (idx *IVFIndex) updateStats() {
	// Approximate size calculation
	idx.stats.SizeBytes = uint64(len(idx.vectors)) * uint64(idx.dimension) * 4 // 4 bytes per float32
	idx.stats.IndexType = "ivf"
}

// Ensure IVFIndex implements VectorIndexImpl
var _ VectorIndexImpl = (*IVFIndex)(nil)
