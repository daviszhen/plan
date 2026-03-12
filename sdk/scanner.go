package sdk

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
	dataset   Dataset
	filter    string
	columns   []string
	limit     *int
	offset    *int
	batchSize int
}

// Scanner 创建 ScannerBuilder。
func (d *datasetImpl) Scanner() *ScannerBuilder {
	return &ScannerBuilder{
		dataset:   d,
		batchSize: 1024,
	}
}

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

func (b *ScannerBuilder) Build() Scanner {
	return &scannerImpl{
		dataset:   b.dataset,
		filter:    b.filter,
		columns:   b.columns,
		limit:     b.limit,
		offset:    b.offset,
		batchSize: b.batchSize,
		hasMore:   true,
	}
}

type scannerImpl struct {
	dataset   Dataset
	filter    string
	columns   []string
	limit     *int
	offset    *int
	batchSize int

	currentBatch []*Record
	currentVals  [][]interface{}
	currentIdx   int
	hasMore      bool
	err          error

	// selectedCols caches the physical column indices to read from chunks.
	// It is derived from columns on first batch load. If empty, all columns are used.
	selectedCols []int
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
	chunks, err := storage2.ScanChunks(ctx, d.basePath, d.handler, d.version)
	if err != nil {
		return err
	}

	var records []*Record
	var vals [][]interface{}
	rowIndex := 0
	for _, c := range chunks {
		if c == nil {
			continue
		}
		colCount := c.ColumnCount()

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
			records = append(records, &Record{Values: rowValues})
			vals = append(vals, rowSlice)
			rowIndex++
		}
	}

	s.currentBatch = records
	s.currentVals = vals
	s.hasMore = false
	return nil
}

