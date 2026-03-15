package sdk

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage2"
)

// Scanner 提供流式数据扫描接口（当前为最小实现，不支持过滤/投影）。
type Scanner interface {
	Next() bool
	Scan(dest ...interface{}) error
	GetRow() (*Record, error)
	Close() error
	Err() error
}

// Record 表示一行数据。
type Record struct {
	Values map[string]interface{}
	// RowID is the global row ID (Lance encoding: fragmentID<<32 | rowOffset).
	// Only populated when WithRowId() is called on the ScannerBuilder.
	RowID *uint64
}

// Schema 表示一批记录的列结构（预留，当前 Scanner 不直接返回 RecordBatch）。
type Schema struct{}

// RecordBatch 表示一批记录（预留类型，当前 Scanner 不直接返回 RecordBatch）。
type RecordBatch struct {
	Records []*Record
	Schema  *Schema
}

// ScannerBuilder 用于构建 Scanner。
type ScannerBuilder struct {
	dataset      Dataset
	filter       string
	columns      []string
	limit        *int
	offset       *int
	batchSize    int
	includeRowId bool
	useIndex     string // index name to use for query
	scanInOrder  bool   // whether to return results in sorted order
}

// Scanner 创建 ScannerBuilder。
func (d *datasetImpl) Scanner() *ScannerBuilder {
	return &ScannerBuilder{
		dataset:   d,
		batchSize: 1024,
	}
}

// WithFilter sets the filter expression for the scanner.
// The filter string supports:
//   - Comparison: "c0 = 1", "c1 > 10", "c2 <= 100"
//   - String comparison: "name = 'foo'", "name LIKE 'bar%'"
//   - NULL checks: "c0 IS NULL", "c1 IS NOT NULL"
//   - IN expressions: "id IN (1, 2, 3)"
//   - Boolean operators: "c0 > 10 AND c1 < 100", "c0 = 1 OR c0 = 2", "NOT c0 = 1"
//   - Parentheses: "(c0 > 10 OR c1 > 10) AND c2 < 100"
func (b *ScannerBuilder) WithFilter(filter string) *ScannerBuilder {
	b.filter = filter
	return b
}

func (b *ScannerBuilder) WithColumns(columns ...string) *ScannerBuilder {
	b.columns = columns
	return b
}

func (b *ScannerBuilder) WithLimit(limit int) *ScannerBuilder {
	b.limit = &limit
	return b
}

func (b *ScannerBuilder) WithOffset(offset int) *ScannerBuilder {
	b.offset = &offset
	return b
}

func (b *ScannerBuilder) WithBatchSize(size int) *ScannerBuilder {
	b.batchSize = size
	return b
}

// WithRowId enables global row ID tracking in scan results.
// Each Record will have its RowID field populated with the Lance-encoded
// global row ID: (fragmentID << 32) | rowOffset.
func (b *ScannerBuilder) WithRowId() *ScannerBuilder {
	b.includeRowId = true
	return b
}

// UseIndex specifies an index to use for accelerating the scan.
// The index must exist and cover the columns used in the filter expression.
// If the index cannot be used (e.g., filter doesn't match), falls back to full scan.
func (b *ScannerBuilder) UseIndex(indexName string) *ScannerBuilder {
	b.useIndex = indexName
	return b
}

// ScanInOrder requests that results be returned in sorted order based on
// the primary key or specified index order.
func (b *ScannerBuilder) ScanInOrder() *ScannerBuilder {
	b.scanInOrder = true
	return b
}

func (b *ScannerBuilder) Build() Scanner {
	return &scannerImpl{
		dataset:      b.dataset,
		filter:       b.filter,
		columns:      b.columns,
		limit:        b.limit,
		offset:       b.offset,
		batchSize:    b.batchSize,
		includeRowId: b.includeRowId,
		useIndex:     b.useIndex,
		scanInOrder:  b.scanInOrder,
		hasMore:      true,
	}
}

type scannerImpl struct {
	dataset      Dataset
	filter       string
	columns      []string
	limit        *int
	offset       *int
	batchSize    int
	includeRowId bool
	useIndex     string
	scanInOrder  bool

	currentBatch []*Record
	currentVals  [][]interface{}
	currentIdx   int
	hasMore      bool
	err          error

	// selectedCols caches the physical column indices to read from chunks.
	// It is derived from columns on first batch load. If empty, all columns are used.
	selectedCols []int

	// filterPredicate is the compiled filter predicate
	filterPredicate storage2.FilterPredicate
	filterInited    bool

	// filterMask caches the filter evaluation result for the current chunk
	filterMask []bool
}

func (s *scannerImpl) Next() bool {
	if s.err != nil {
		return false
	}
	// 如果当前批次已读完，加载下一批
	if s.currentIdx >= len(s.currentBatch) {
		if !s.hasMore {
			return false
		}
		if err := s.loadNextBatch(); err != nil {
			s.err = err
			return false
		}
	}
	s.currentIdx++
	return s.currentIdx <= len(s.currentBatch)
}

func (s *scannerImpl) Scan(dest ...interface{}) error {
	if s.currentIdx == 0 || s.currentIdx > len(s.currentBatch) {
		return fmt.Errorf("no current row")
	}
	if len(dest) == 0 {
		return nil
	}
	rowVals := s.currentVals[s.currentIdx-1]
	for i := range dest {
		if i >= len(rowVals) {
			break
		}
		switch d := dest[i].(type) {
		case *int32:
			if v, ok := rowVals[i].(int64); ok {
				*d = int32(v)
			}
		case *int64:
			if v, ok := rowVals[i].(int64); ok {
				*d = v
			}
		case *float64:
			if v, ok := rowVals[i].(float64); ok {
				*d = v
			}
		case *string:
			if v, ok := rowVals[i].(string); ok {
				*d = v
			}
		case *bool:
			if v, ok := rowVals[i].(bool); ok {
				*d = v
			}
		default:
			// 不支持的类型，保持默认值
		}
	}
	return nil
}

func (s *scannerImpl) GetRow() (*Record, error) {
	if s.currentIdx == 0 || s.currentIdx > len(s.currentBatch) {
		return nil, fmt.Errorf("no current row")
	}
	return s.currentBatch[s.currentIdx-1], nil
}

func (s *scannerImpl) Close() error {
	// 当前实现无持久资源需要释放
	return nil
}

func (s *scannerImpl) Err() error {
	return s.err
}

func (s *scannerImpl) loadNextBatch() error {
	// 简化实现：一次性将当前版本的所有数据加载为单个 batch，
	// 忽略 filter/columns/limit/offset/batchSize。
	if !s.hasMore {
		s.currentBatch = nil
		s.currentVals = nil
		return nil
	}

	d, ok := s.dataset.(*datasetImpl)
	if !ok {
		return fmt.Errorf("unsupported dataset implementation")
	}
	ctx := context.Background()

	// Load manifest to get fragment metadata (needed for row IDs)
	manifest, err := storage2.LoadManifest(ctx, d.basePath, d.handler, d.version)
	if err != nil {
		return err
	}

	var records []*Record
	var vals [][]interface{}
	rowIndex := 0
	for _, frag := range manifest.Fragments {
		if frag == nil {
			continue
		}
		for _, df := range frag.Files {
			if df == nil || df.Path == "" {
				continue
			}
			fullPath := filepath.Join(d.basePath, df.Path)
			c, err := storage2.ReadChunkFromFile(fullPath)
			if err != nil {
				return err
			}
			if c == nil {
				continue
			}
			colCount := c.ColumnCount()

			// 初始化 filter：解析并编译过滤表达式
			if s.filter != "" && !s.filterInited {
				if err := s.initFilter(c); err != nil {
					return err
				}
			}

			// Evaluate filter on the entire chunk once
			var filterMask []bool
			if s.filterPredicate != nil {
				filterMask, err = s.filterPredicate.Evaluate(c)
				if err != nil {
					return err
				}
			}

			// 初始化列选择：若未指定 columns，则使用所有列；否则按 "c{idx}" 解析。
			if s.selectedCols == nil {
				if len(s.columns) == 0 {
					s.selectedCols = make([]int, colCount)
					for i := 0; i < colCount; i++ {
						s.selectedCols[i] = i
					}
				} else {
					for _, name := range s.columns {
						// 当前实现仅支持形如 "c0"、"c1" 的列名
						if !strings.HasPrefix(name, "c") {
							return fmt.Errorf("unsupported column name %q (expect c{index})", name)
						}
						idx, err := strconv.Atoi(strings.TrimPrefix(name, "c"))
						if err != nil || idx < 0 || idx >= colCount {
							return fmt.Errorf("column %q out of range", name)
						}
						s.selectedCols = append(s.selectedCols, idx)
					}
				}
			}

			selCount := len(s.selectedCols)
			for i := 0; i < c.Card(); i++ {
				// offset 处理：跳过前 offset 行
				if s.offset != nil && rowIndex < *s.offset {
					rowIndex++
					continue
				}

				// filter 处理：不满足条件的行直接跳过
				if filterMask != nil {
					if !filterMask[i] {
						rowIndex++
						continue
					}
				}
				// limit 处理：超过限制则结束
				if s.limit != nil && len(records) >= *s.limit {
					s.hasMore = false
					s.currentBatch = records
					s.currentVals = vals
					return nil
				}

				rowValues := make(map[string]interface{}, selCount)
				rowSlice := make([]interface{}, selCount)
				for j, colIdx := range s.selectedCols {
					val := c.Data[colIdx].GetValue(i)
					var v interface{}
					switch val.Typ.Id {
					case common.LTID_INTEGER, common.LTID_BIGINT:
						v = val.I64
					case common.LTID_DOUBLE, common.LTID_FLOAT:
						v = val.F64
					case common.LTID_VARCHAR:
						v = val.Str
					case common.LTID_BOOLEAN:
						v = val.Bool
					default:
						// 其它类型先用字符串表示
						v = val.String()
					}
					key := fmt.Sprintf("c%d", colIdx)
					rowValues[key] = v
					rowSlice[j] = v
				}

				rec := &Record{Values: rowValues}

				// Compute row ID if requested: (fragmentID << 32) | rowOffset
				if s.includeRowId {
					rid := (frag.Id << 32) | uint64(i)
					rec.RowID = &rid
				}

				records = append(records, rec)
				vals = append(vals, rowSlice)
				rowIndex++
			}
		}
	}

	s.currentBatch = records
	s.currentVals = vals
	s.hasMore = false

	// If ScanInOrder is requested, sort results by first column (c0)
	if s.scanInOrder && len(records) > 0 {
		s.sortRecords()
	}

	return nil
}

// sortRecords sorts records by the first column in ascending order
func (s *scannerImpl) sortRecords() {
	if len(s.currentBatch) == 0 || len(s.currentVals) == 0 {
		return
	}

	// Build indices and sort
	n := len(s.currentBatch)
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	// Sort by first value (c0)
	sortByFirstColumn(indices, s.currentVals)

	// Reorder both slices according to sorted indices
	newBatch := make([]*Record, n)
	newVals := make([][]interface{}, n)
	for i, idx := range indices {
		newBatch[i] = s.currentBatch[idx]
		newVals[i] = s.currentVals[idx]
	}
	s.currentBatch = newBatch
	s.currentVals = newVals
}

// sortByFirstColumn sorts indices by comparing first column values
func sortByFirstColumn(indices []int, vals [][]interface{}) {
	// Simple insertion sort for small datasets, could use sort.Slice for larger
	for i := 1; i < len(indices); i++ {
		j := i
		for j > 0 && compareValues(vals[indices[j]][0], vals[indices[j-1]][0]) < 0 {
			indices[j], indices[j-1] = indices[j-1], indices[j]
			j--
		}
	}
}

// compareValues compares two values, returns -1, 0, or 1
func compareValues(a, b interface{}) int {
	switch av := a.(type) {
	case int64:
		if bv, ok := b.(int64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case float64:
		if bv, ok := b.(float64); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	case string:
		if bv, ok := b.(string); ok {
			if av < bv {
				return -1
			} else if av > bv {
				return 1
			}
			return 0
		}
	}
	// Fallback: convert to string and compare
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	if as < bs {
		return -1
	} else if as > bs {
		return 1
	}
	return 0
}

// initFilter parses the filter string into a FilterPredicate.
// Supports complex expressions including AND, OR, NOT, comparisons, IN, LIKE, IS NULL.
func (s *scannerImpl) initFilter(firstChunk *chunk.Chunk) error {
	predicate, err := storage2.ParseFilter(s.filter, nil)
	if err != nil {
		return fmt.Errorf("failed to parse filter %q: %w", s.filter, err)
	}
	s.filterPredicate = predicate
	s.filterInited = true
	return nil
}
