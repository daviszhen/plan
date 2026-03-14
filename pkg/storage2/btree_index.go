package storage2

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// BTreeIndex is a B-tree based scalar index for efficient range and equality queries
type BTreeIndex struct {
	name       string
	columnIdx  int
	indexType  IndexType
	root       *BTreeNode
	mu         sync.RWMutex
	stats      IndexStats
	entries    uint64
	minValue   interface{}
	maxValue   interface{}
	distinctCount uint64
}

// BTreeNode represents a node in the B-tree
type BTreeNode struct {
	isLeaf   bool
	keys     []interface{}
	values   [][]uint64 // row IDs for each key
	children []*BTreeNode
	next     *BTreeNode // for leaf node linked list
}

const (
	// BTreeDegree is the minimum degree of the B-tree (t)
	// Each node can have at most 2t-1 keys and at least t-1 keys
	BTreeDegree = 32
)

// NewBTreeIndex creates a new B-tree index
func NewBTreeIndex(name string, columnIdx int) *BTreeIndex {
	return &BTreeIndex{
		name:      name,
		columnIdx: columnIdx,
		indexType: ScalarIndex,
		root: &BTreeNode{
			isLeaf: true,
			keys:   make([]interface{}, 0),
			values: make([][]uint64, 0),
		},
		stats: IndexStats{
			NumEntries: 0,
			SizeBytes:  0,
		},
	}
}

// Name returns the index name
func (idx *BTreeIndex) Name() string {
	return idx.name
}

// Type returns the index type
func (idx *BTreeIndex) Type() IndexType {
	return idx.indexType
}

// Columns returns the column indices this index covers
func (idx *BTreeIndex) Columns() []int {
	return []int{idx.columnIdx}
}

// Search searches the index and returns matching row IDs
func (idx *BTreeIndex) Search(ctx context.Context, query interface{}, limit int) ([]uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.equalityQuery(query)
}

// Statistics returns statistics about the index
func (idx *BTreeIndex) Statistics() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.stats
}

// RangeQuery performs a range query on the index
func (idx *BTreeIndex) RangeQuery(ctx context.Context, start, end interface{}) ([]uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []uint64
	node := idx.findLeafNode(start)
	
	for node != nil {
		for i, key := range node.keys {
			if btreeCompareValues(key, start) >= 0 && btreeCompareValues(key, end) <= 0 {
				result = append(result, node.values[i]...)
			} else if btreeCompareValues(key, end) > 0 {
				return result, nil
			}
		}
		node = node.next
	}
	
	return result, nil
}

// EqualityQuery performs an equality query on the index
func (idx *BTreeIndex) EqualityQuery(ctx context.Context, value interface{}) ([]uint64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return idx.equalityQuery(value)
}

// equalityQuery is the internal implementation of equality query
func (idx *BTreeIndex) equalityQuery(value interface{}) ([]uint64, error) {
	node := idx.findLeafNode(value)
	
	for i, key := range node.keys {
		if btreeCompareValues(key, value) == 0 {
			return node.values[i], nil
		}
	}
	
	return nil, nil
}

// Insert adds a key-value pair to the index
func (idx *BTreeIndex) Insert(key interface{}, rowID uint64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if idx.root == nil {
		idx.root = &BTreeNode{
			isLeaf: true,
			keys:   []interface{}{key},
			values: [][]uint64{{rowID}},
		}
		idx.entries = 1
		idx.updateStats()
		return nil
	}

	// Check if root is full
	if len(idx.root.keys) >= 2*BTreeDegree-1 {
		newRoot := &BTreeNode{
			isLeaf:   false,
			children: []*BTreeNode{idx.root},
		}
		idx.splitChild(newRoot, 0)
		idx.root = newRoot
	}

	idx.insertNonFull(idx.root, key, rowID)
	idx.entries++
	idx.updateMinMax(key)
	idx.updateStats()
	return nil
}

// insertNonFull inserts a key into a non-full node
func (idx *BTreeIndex) insertNonFull(node *BTreeNode, key interface{}, rowID uint64) {
	if node.isLeaf {
		// Insert into leaf node
		insertPos := sort.Search(len(node.keys), func(i int) bool {
			return btreeCompareValues(node.keys[i], key) >= 0
		})

		if insertPos < len(node.keys) && btreeCompareValues(node.keys[insertPos], key) == 0 {
			// Key already exists, append row ID
			node.values[insertPos] = append(node.values[insertPos], rowID)
		} else {
			// Insert new key
			node.keys = append(node.keys[:insertPos], append([]interface{}{key}, node.keys[insertPos:]...)...)
			node.values = append(node.values[:insertPos], append([][]uint64{{rowID}}, node.values[insertPos:]...)...)
			idx.distinctCount++
		}
	} else {
		// Find child to insert into
		childIdx := sort.Search(len(node.keys), func(i int) bool {
			return btreeCompareValues(node.keys[i], key) > 0
		})

		// Check if child is full
		if len(node.children[childIdx].keys) >= 2*BTreeDegree-1 {
			idx.splitChild(node, childIdx)
			// Recalculate child index after split
			if btreeCompareValues(key, node.keys[childIdx]) > 0 {
				childIdx++
			}
		}

		idx.insertNonFull(node.children[childIdx], key, rowID)
	}
}

// splitChild splits a full child node
func (idx *BTreeIndex) splitChild(parent *BTreeNode, childIdx int) {
	child := parent.children[childIdx]
	mid := BTreeDegree - 1

	// Create new node
	newNode := &BTreeNode{
		isLeaf: child.isLeaf,
		keys:   make([]interface{}, len(child.keys)-mid-1),
		values: make([][]uint64, len(child.values)-mid-1),
	}

	// Copy keys and values to new node
	copy(newNode.keys, child.keys[mid+1:])
	copy(newNode.values, child.values[mid+1:])

	if !child.isLeaf {
		newNode.children = make([]*BTreeNode, len(child.children)-mid-1)
		copy(newNode.children, child.children[mid+1:])
		child.children = child.children[:mid+1]
	} else {
		// Update linked list for leaf nodes
		newNode.next = child.next
		child.next = newNode
	}

	// Move middle key to parent
	parent.keys = append(parent.keys[:childIdx], append([]interface{}{child.keys[mid]}, parent.keys[childIdx:]...)...)
	parent.values = append(parent.values[:childIdx], append([][]uint64{child.values[mid]}, parent.values[childIdx:]...)...)
	parent.children = append(parent.children[:childIdx+1], append([]*BTreeNode{newNode}, parent.children[childIdx+1:]...)...)

	// Truncate child
	child.keys = child.keys[:mid]
	child.values = child.values[:mid]
}

// findLeafNode finds the leaf node that should contain the given key
func (idx *BTreeIndex) findLeafNode(key interface{}) *BTreeNode {
	node := idx.root
	for !node.isLeaf {
		childIdx := sort.Search(len(node.keys), func(i int) bool {
			return btreeCompareValues(node.keys[i], key) > 0
		})
		if childIdx >= len(node.children) {
			childIdx = len(node.children) - 1
		}
		node = node.children[childIdx]
	}
	return node
}

// updateStats updates index statistics
func (idx *BTreeIndex) updateStats() {
	idx.stats.NumEntries = idx.entries
	// Approximate size calculation
	idx.stats.SizeBytes = idx.entries * 16 // rough estimate
	idx.stats.IndexType = "btree"
	idx.stats.MinValue = idx.minValue
	idx.stats.MaxValue = idx.maxValue
	idx.stats.DistinctValues = idx.distinctCount
}

// updateMinMax updates min and max values
func (idx *BTreeIndex) updateMinMax(key interface{}) {
	if idx.minValue == nil || btreeCompareValues(key, idx.minValue) < 0 {
		idx.minValue = key
	}
	if idx.maxValue == nil || btreeCompareValues(key, idx.maxValue) > 0 {
		idx.maxValue = key
	}
}

// btreeCompareValues compares two values and returns:
// -1 if a < b
//  0 if a == b
//  1 if a > b
func btreeCompareValues(a, b interface{}) int {
	switch av := a.(type) {
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		} else if av > bv {
			return 1
		}
		return 0
	default:
		// Fallback to string comparison
		as := fmt.Sprintf("%v", a)
		bs := fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		} else if as > bs {
			return 1
		}
		return 0
	}
}

// Ensure BTreeIndex implements ScalarIndexImpl
var _ ScalarIndexImpl = (*BTreeIndex)(nil)
