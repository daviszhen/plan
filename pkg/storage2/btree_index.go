package storage2

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// BTreeIndex is a B-tree based scalar index for efficient range and equality queries
type BTreeIndex struct {
	name          string
	columnIdx     int
	indexType     IndexType
	root          *BTreeNode
	mu            sync.RWMutex
	stats         IndexStats
	entries       uint64
	minValue      interface{}
	maxValue      interface{}
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
//
//	0 if a == b
//	1 if a > b
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

// BTreeSerializedData represents serialized BTree data
type BTreeSerializedData struct {
	Name          string                 `json:"name"`
	ColumnIdx     int                    `json:"column_idx"`
	Entries       uint64                 `json:"entries"`
	DistinctCount uint64                 `json:"distinct_count"`
	MinValue      interface{}            `json:"min_value"`
	MaxValue      interface{}            `json:"max_value"`
	Data          []BTreeSerializedEntry `json:"data"`
}

// BTreeSerializedEntry represents a key-value entry in the BTree
type BTreeSerializedEntry struct {
	Key    interface{} `json:"key"`
	RowIDs []uint64    `json:"row_ids"`
}

// Marshal serializes the BTreeIndex to JSON bytes
func (idx *BTreeIndex) Marshal() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Collect all entries via in-order traversal
	var entries []BTreeSerializedEntry
	idx.collectEntriesInOrder(idx.root, &entries)

	data := BTreeSerializedData{
		Name:          idx.name,
		ColumnIdx:     idx.columnIdx,
		Entries:       idx.entries,
		DistinctCount: idx.distinctCount,
		MinValue:      idx.minValue,
		MaxValue:      idx.maxValue,
		Data:          entries,
	}

	return json.Marshal(data)
}

// collectEntriesInOrder collects all entries from the tree via in-order traversal
// This B-tree implementation stores data in both internal and leaf nodes
func (idx *BTreeIndex) collectEntriesInOrder(node *BTreeNode, entries *[]BTreeSerializedEntry) {
	if node == nil {
		return
	}

	if node.isLeaf {
		// Leaf node: collect all entries
		for i, key := range node.keys {
			*entries = append(*entries, BTreeSerializedEntry{
				Key:    key,
				RowIDs: node.values[i],
			})
		}
		return
	}

	// Internal node: in-order traversal
	// child[0], key[0], child[1], key[1], ..., child[n]
	for i := 0; i < len(node.keys); i++ {
		// Visit left child
		if i < len(node.children) {
			idx.collectEntriesInOrder(node.children[i], entries)
		}
		// Visit current key (internal nodes also have values in this B-tree)
		*entries = append(*entries, BTreeSerializedEntry{
			Key:    node.keys[i],
			RowIDs: node.values[i],
		})
	}
	// Visit rightmost child
	if len(node.children) > len(node.keys) {
		idx.collectEntriesInOrder(node.children[len(node.keys)], entries)
	}
}

// Unmarshal deserializes the BTreeIndex from JSON bytes
func (idx *BTreeIndex) Unmarshal(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var serialized BTreeSerializedData
	if err := json.Unmarshal(data, &serialized); err != nil {
		return err
	}

	// Reset the index
	idx.name = serialized.Name
	idx.columnIdx = serialized.ColumnIdx
	idx.entries = 0
	idx.distinctCount = 0
	idx.minValue = nil
	idx.maxValue = nil
	idx.root = &BTreeNode{
		isLeaf: true,
		keys:   make([]interface{}, 0),
		values: make([][]uint64, 0),
	}

	// Re-insert all entries
	for _, entry := range serialized.Data {
		// JSON unmarshals numbers as float64, convert back to int64 if needed
		key := normalizeKeyType(entry.Key)
		for _, rowID := range entry.RowIDs {
			idx.insertInternal(key, rowID)
		}
	}

	return nil
}

// normalizeKeyType converts JSON-deserialized keys back to appropriate types
func normalizeKeyType(key interface{}) interface{} {
	switch v := key.(type) {
	case float64:
		// JSON unmarshals all numbers as float64, convert to int64 if it's a whole number
		if v == float64(int64(v)) {
			return int64(v)
		}
		return v
	default:
		return key
	}
}

// insertInternal is an internal insert without locking (caller must hold lock)
func (idx *BTreeIndex) insertInternal(key interface{}, rowID uint64) {
	idx.entries++
	idx.updateMinMax(key)

	// If root is full, split it
	if len(idx.root.keys) >= 2*BTreeDegree-1 {
		oldRoot := idx.root
		idx.root = &BTreeNode{
			isLeaf:   false,
			keys:     make([]interface{}, 0),
			values:   make([][]uint64, 0),
			children: []*BTreeNode{oldRoot},
		}
		idx.splitChild(idx.root, 0)
	}

	idx.insertNonFull(idx.root, key, rowID)
	idx.updateStats()
}

// findLeftmostLeaf finds the leftmost leaf node
func (idx *BTreeIndex) findLeftmostLeaf() *BTreeNode {
	node := idx.root
	for !node.isLeaf {
		if len(node.children) == 0 {
			return nil
		}
		node = node.children[0]
	}
	return node
}

// Ensure BTreeIndex implements Serializable
var _ Serializable = (*BTreeIndex)(nil)
