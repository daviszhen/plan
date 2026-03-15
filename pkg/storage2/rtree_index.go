// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
)

// RTreeIndex is a spatial index for multi-dimensional bounding box queries.
// It supports efficient range queries and nearest neighbor searches for
// 2D or 3D spatial data.
type RTreeIndex struct {
	name       string
	columnIdxs []int // column indices for spatial dimensions (e.g., [x, y] or [x, y, z])
	indexType  IndexType
	root       *RTreeNode
	dimension  int // 2 for 2D, 3 for 3D
	maxEntries int // maximum entries per node (M)
	minEntries int // minimum entries per node (m)
	stats      IndexStats
	mu         sync.RWMutex
}

// BoundingBox represents a multi-dimensional bounding box
type BoundingBox struct {
	MinPoint []float64 // minimum coordinates [x_min, y_min, ...]
	MaxPoint []float64 // maximum coordinates [x_max, y_max, ...]
}

// RTreeNode represents a node in the R-tree
type RTreeNode struct {
	BBox     BoundingBox
	IsLeaf   bool
	Entries  []*RTreeEntry // leaf entries (row IDs with their bounding boxes)
	Children []*RTreeNode  // child nodes (for internal nodes)
}

// RTreeEntry represents an entry in a leaf node
type RTreeEntry struct {
	RowID uint64
	BBox  BoundingBox
}

// RTreeIndexOption configures R-tree index creation
type RTreeIndexOption func(*RTreeIndex)

// WithRTreeDimension sets the number of dimensions (2 or 3)
func WithRTreeDimension(dim int) RTreeIndexOption {
	return func(idx *RTreeIndex) {
		if dim >= 2 && dim <= 3 {
			idx.dimension = dim
		}
	}
}

// WithRTreeMaxEntries sets the maximum entries per node
func WithRTreeMaxEntries(m int) RTreeIndexOption {
	return func(idx *RTreeIndex) {
		if m >= 4 {
			idx.maxEntries = m
		}
	}
}

// NewRTreeIndex creates a new R-tree spatial index
func NewRTreeIndex(name string, columnIdxs []int, opts ...RTreeIndexOption) *RTreeIndex {
	idx := &RTreeIndex{
		name:       name,
		columnIdxs: columnIdxs,
		indexType:  ScalarIndex, // Could add SpatialIndex type
		dimension:  2,           // default 2D
		maxEntries: 50,          // default M
		minEntries: 20,          // default m (typically M/2)
		stats: IndexStats{
			IndexType: "rtree",
		},
	}

	for _, opt := range opts {
		opt(idx)
	}

	// Ensure minEntries is reasonable
	if idx.minEntries > idx.maxEntries/2 {
		idx.minEntries = idx.maxEntries / 2
	}

	// Initialize empty root as leaf
	idx.root = &RTreeNode{
		IsLeaf:  true,
		Entries: make([]*RTreeEntry, 0),
		BBox:    newEmptyBBox(idx.dimension),
	}

	return idx
}

// newEmptyBBox creates an empty bounding box for the given dimension
func newEmptyBBox(dim int) BoundingBox {
	minPt := make([]float64, dim)
	maxPt := make([]float64, dim)
	for i := 0; i < dim; i++ {
		minPt[i] = math.MaxFloat64
		maxPt[i] = -math.MaxFloat64
	}
	return BoundingBox{MinPoint: minPt, MaxPoint: maxPt}
}

// Name returns the index name
func (idx *RTreeIndex) Name() string {
	return idx.name
}

// Type returns the index type
func (idx *RTreeIndex) Type() IndexType {
	return idx.indexType
}

// Columns returns the column indices this index covers
func (idx *RTreeIndex) Columns() []int {
	return idx.columnIdxs
}

// Search performs a spatial search and returns matching row IDs
func (idx *RTreeIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	bbox, ok := query.(*BoundingBox)
	if !ok {
		return nil, fmt.Errorf("R-tree search requires BoundingBox query, got %T", query)
	}
	return idx.RangeSearch(ctx, bbox, limit)
}

// Statistics returns statistics about the index
func (idx *RTreeIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// Insert adds a new entry to the R-tree
func (idx *RTreeIndex) Insert(rowID uint64, bbox *BoundingBox) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if len(bbox.MinPoint) != idx.dimension || len(bbox.MaxPoint) != idx.dimension {
		return fmt.Errorf("bounding box dimension mismatch: got %d, want %d", len(bbox.MinPoint), idx.dimension)
	}

	entry := &RTreeEntry{
		RowID: rowID,
		BBox:  *bbox,
	}

	// Insert entry using standard R-tree algorithm
	idx.insertEntry(entry)
	idx.stats.NumEntries++

	return nil
}

// InsertPoint inserts a point (creates a zero-sized bounding box)
func (idx *RTreeIndex) InsertPoint(rowID uint64, coords []float64) error {
	if len(coords) != idx.dimension {
		return fmt.Errorf("coordinate dimension mismatch: got %d, want %d", len(coords), idx.dimension)
	}

	bbox := &BoundingBox{
		MinPoint: make([]float64, idx.dimension),
		MaxPoint: make([]float64, idx.dimension),
	}
	copy(bbox.MinPoint, coords)
	copy(bbox.MaxPoint, coords)

	return idx.Insert(rowID, bbox)
}

// insertEntry inserts an entry into the tree
func (idx *RTreeIndex) insertEntry(entry *RTreeEntry) {
	// Find the best leaf node for this entry
	leaf := idx.chooseLeaf(idx.root, &entry.BBox)

	// Insert entry into leaf
	leaf.Entries = append(leaf.Entries, entry)
	expandBBox(&leaf.BBox, &entry.BBox)

	// Handle overflow if necessary
	if len(leaf.Entries) > idx.maxEntries {
		idx.handleOverflow(leaf)
	}
}

// chooseLeaf finds the best leaf node for inserting an entry
func (idx *RTreeIndex) chooseLeaf(node *RTreeNode, bbox *BoundingBox) *RTreeNode {
	if node.IsLeaf {
		return node
	}

	// Find child whose bbox needs minimum enlargement
	var bestChild *RTreeNode
	minEnlargement := math.MaxFloat64

	for _, child := range node.Children {
		enlargement := bboxEnlargement(&child.BBox, bbox)
		if enlargement < minEnlargement {
			minEnlargement = enlargement
			bestChild = child
		} else if enlargement == minEnlargement {
			// Tie-breaker: choose smaller area
			if bboxArea(&child.BBox) < bboxArea(&bestChild.BBox) {
				bestChild = child
			}
		}
	}

	return idx.chooseLeaf(bestChild, bbox)
}

// handleOverflow handles node overflow by splitting
func (idx *RTreeIndex) handleOverflow(node *RTreeNode) {
	// Simple split: split at middle
	// A more sophisticated implementation would use quadratic or linear split
	if node.IsLeaf {
		mid := len(node.Entries) / 2
		newNode := &RTreeNode{
			IsLeaf:  true,
			Entries: make([]*RTreeEntry, len(node.Entries)-mid),
			BBox:    newEmptyBBox(idx.dimension),
		}
		copy(newNode.Entries, node.Entries[mid:])
		node.Entries = node.Entries[:mid]

		// Recalculate bounding boxes
		node.BBox = newEmptyBBox(idx.dimension)
		for _, e := range node.Entries {
			expandBBox(&node.BBox, &e.BBox)
		}
		for _, e := range newNode.Entries {
			expandBBox(&newNode.BBox, &e.BBox)
		}

		// If this is root, create new root
		if node == idx.root {
			idx.root = &RTreeNode{
				IsLeaf:   false,
				Children: []*RTreeNode{node, newNode},
				BBox:     newEmptyBBox(idx.dimension),
			}
			expandBBox(&idx.root.BBox, &node.BBox)
			expandBBox(&idx.root.BBox, &newNode.BBox)
		}
	}
}

// RangeSearch finds all entries that intersect the given bounding box
func (idx *RTreeIndex) RangeSearch(ctx context.Context, queryBBox *BoundingBox, limit int) ([]uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []uint64
	idx.rangeSearchRecursive(idx.root, queryBBox, &results, limit)
	return results, nil
}

// rangeSearchRecursive recursively searches for intersecting entries
func (idx *RTreeIndex) rangeSearchRecursive(node *RTreeNode, queryBBox *BoundingBox, results *[]uint64, limit int) {
	if limit > 0 && len(*results) >= limit {
		return
	}

	if !bboxIntersects(&node.BBox, queryBBox) {
		return
	}

	if node.IsLeaf {
		for _, entry := range node.Entries {
			if limit > 0 && len(*results) >= limit {
				return
			}
			if bboxIntersects(&entry.BBox, queryBBox) {
				*results = append(*results, entry.RowID)
			}
		}
	} else {
		for _, child := range node.Children {
			idx.rangeSearchRecursive(child, queryBBox, results, limit)
		}
	}
}

// ContainsSearch finds all entries completely contained within the query box
func (idx *RTreeIndex) ContainsSearch(ctx context.Context, queryBBox *BoundingBox, limit int) ([]uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var results []uint64
	idx.containsSearchRecursive(idx.root, queryBBox, &results, limit)
	return results, nil
}

// containsSearchRecursive recursively searches for contained entries
func (idx *RTreeIndex) containsSearchRecursive(node *RTreeNode, queryBBox *BoundingBox, results *[]uint64, limit int) {
	if limit > 0 && len(*results) >= limit {
		return
	}

	if !bboxIntersects(&node.BBox, queryBBox) {
		return
	}

	if node.IsLeaf {
		for _, entry := range node.Entries {
			if limit > 0 && len(*results) >= limit {
				return
			}
			if bboxContains(queryBBox, &entry.BBox) {
				*results = append(*results, entry.RowID)
			}
		}
	} else {
		for _, child := range node.Children {
			idx.containsSearchRecursive(child, queryBBox, results, limit)
		}
	}
}

// NearestNeighbors finds the k nearest entries to a point
func (idx *RTreeIndex) NearestNeighbors(ctx context.Context, point []float64, k int) ([]uint64, []float64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(point) != idx.dimension {
		return nil, nil, fmt.Errorf("point dimension mismatch: got %d, want %d", len(point), idx.dimension)
	}

	// Simple brute-force KNN for now
	// A more efficient implementation would use a priority queue
	var candidates []nnCandidate

	idx.collectAllEntriesNN(idx.root, &candidates, point)

	// Sort by distance
	for i := 0; i < len(candidates)-1; i++ {
		for j := i + 1; j < len(candidates); j++ {
			if candidates[j].dist < candidates[i].dist {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
		}
	}

	// Return top k
	resultCount := k
	if resultCount > len(candidates) {
		resultCount = len(candidates)
	}

	rowIDs := make([]uint64, resultCount)
	distances := make([]float64, resultCount)
	for i := 0; i < resultCount; i++ {
		rowIDs[i] = candidates[i].rowID
		distances[i] = candidates[i].dist
	}

	return rowIDs, distances, nil
}

// nnCandidate is used for nearest neighbor search
type nnCandidate struct {
	rowID uint64
	dist  float64
}

// collectAllEntriesNN collects all entries with their distances to a point
func (idx *RTreeIndex) collectAllEntriesNN(node *RTreeNode, candidates *[]nnCandidate, point []float64) {
	if node.IsLeaf {
		for _, entry := range node.Entries {
			dist := bboxMinDistance(&entry.BBox, point)
			*candidates = append(*candidates, nnCandidate{rowID: entry.RowID, dist: dist})
		}
	} else {
		for _, child := range node.Children {
			idx.collectAllEntriesNN(child, candidates, point)
		}
	}
}

// Helper functions for bounding box operations

// bboxIntersects checks if two bounding boxes intersect
func bboxIntersects(a, b *BoundingBox) bool {
	for i := 0; i < len(a.MinPoint); i++ {
		if a.MaxPoint[i] < b.MinPoint[i] || b.MaxPoint[i] < a.MinPoint[i] {
			return false
		}
	}
	return true
}

// bboxContains checks if bbox a completely contains bbox b
func bboxContains(a, b *BoundingBox) bool {
	for i := 0; i < len(a.MinPoint); i++ {
		if b.MinPoint[i] < a.MinPoint[i] || b.MaxPoint[i] > a.MaxPoint[i] {
			return false
		}
	}
	return true
}

// expandBBox expands bbox a to include bbox b
func expandBBox(a, b *BoundingBox) {
	for i := 0; i < len(a.MinPoint); i++ {
		if b.MinPoint[i] < a.MinPoint[i] {
			a.MinPoint[i] = b.MinPoint[i]
		}
		if b.MaxPoint[i] > a.MaxPoint[i] {
			a.MaxPoint[i] = b.MaxPoint[i]
		}
	}
}

// bboxArea calculates the area/volume of a bounding box
func bboxArea(bbox *BoundingBox) float64 {
	area := 1.0
	for i := 0; i < len(bbox.MinPoint); i++ {
		area *= bbox.MaxPoint[i] - bbox.MinPoint[i]
	}
	return area
}

// bboxEnlargement calculates how much bbox a would need to enlarge to include bbox b
func bboxEnlargement(a, b *BoundingBox) float64 {
	originalArea := bboxArea(a)

	// Calculate expanded area
	expandedArea := 1.0
	for i := 0; i < len(a.MinPoint); i++ {
		minVal := a.MinPoint[i]
		if b.MinPoint[i] < minVal {
			minVal = b.MinPoint[i]
		}
		maxVal := a.MaxPoint[i]
		if b.MaxPoint[i] > maxVal {
			maxVal = b.MaxPoint[i]
		}
		expandedArea *= maxVal - minVal
	}

	return expandedArea - originalArea
}

// bboxMinDistance calculates the minimum distance from a point to a bounding box
func bboxMinDistance(bbox *BoundingBox, point []float64) float64 {
	var sumSq float64
	for i := 0; i < len(point); i++ {
		var d float64
		if point[i] < bbox.MinPoint[i] {
			d = bbox.MinPoint[i] - point[i]
		} else if point[i] > bbox.MaxPoint[i] {
			d = point[i] - bbox.MaxPoint[i]
		}
		sumSq += d * d
	}
	return math.Sqrt(sumSq)
}

// Serialization support

// RTreeSerializedData represents serialized R-tree data
type RTreeSerializedData struct {
	Name       string                 `json:"name"`
	ColumnIdxs []int                  `json:"column_idxs"`
	Dimension  int                    `json:"dimension"`
	MaxEntries int                    `json:"max_entries"`
	MinEntries int                    `json:"min_entries"`
	Entries    []RTreeSerializedEntry `json:"entries"`
}

// RTreeSerializedEntry represents a serialized R-tree entry
type RTreeSerializedEntry struct {
	RowID    uint64    `json:"row_id"`
	MinPoint []float64 `json:"min_point"`
	MaxPoint []float64 `json:"max_point"`
}

// Marshal serializes the R-tree to JSON
func (idx *RTreeIndex) Marshal() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var entries []RTreeSerializedEntry
	idx.collectSerializedEntries(idx.root, &entries)

	data := RTreeSerializedData{
		Name:       idx.name,
		ColumnIdxs: idx.columnIdxs,
		Dimension:  idx.dimension,
		MaxEntries: idx.maxEntries,
		MinEntries: idx.minEntries,
		Entries:    entries,
	}

	return json.Marshal(data)
}

// collectSerializedEntries collects all entries for serialization
func (idx *RTreeIndex) collectSerializedEntries(node *RTreeNode, entries *[]RTreeSerializedEntry) {
	if node.IsLeaf {
		for _, entry := range node.Entries {
			*entries = append(*entries, RTreeSerializedEntry{
				RowID:    entry.RowID,
				MinPoint: entry.BBox.MinPoint,
				MaxPoint: entry.BBox.MaxPoint,
			})
		}
	} else {
		for _, child := range node.Children {
			idx.collectSerializedEntries(child, entries)
		}
	}
}

// Unmarshal deserializes the R-tree from JSON
func (idx *RTreeIndex) Unmarshal(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var serialized RTreeSerializedData
	if err := json.Unmarshal(data, &serialized); err != nil {
		return err
	}

	idx.name = serialized.Name
	idx.columnIdxs = serialized.ColumnIdxs
	idx.dimension = serialized.Dimension
	idx.maxEntries = serialized.MaxEntries
	idx.minEntries = serialized.MinEntries
	idx.stats.NumEntries = 0

	// Reset root
	idx.root = &RTreeNode{
		IsLeaf:  true,
		Entries: make([]*RTreeEntry, 0),
		BBox:    newEmptyBBox(idx.dimension),
	}

	// Re-insert all entries
	for _, e := range serialized.Entries {
		entry := &RTreeEntry{
			RowID: e.RowID,
			BBox: BoundingBox{
				MinPoint: e.MinPoint,
				MaxPoint: e.MaxPoint,
			},
		}
		idx.insertEntry(entry)
		idx.stats.NumEntries++
	}

	return nil
}

// Ensure RTreeIndex implements Index and Serializable
var _ Index = (*RTreeIndex)(nil)
var _ Serializable = (*RTreeIndex)(nil)
