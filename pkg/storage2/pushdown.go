package storage2

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
)

// FilterPredicate is a predicate that can be pushed down to the scan layer
type FilterPredicate interface {
	// Evaluate evaluates the predicate on a chunk and returns a boolean vector
	// indicating which rows match the predicate
	Evaluate(c *chunk.Chunk) ([]bool, error)
	// CanPushdown returns true if this predicate can be pushed down to the scan layer
	CanPushdown() bool
	// String returns a string representation of the predicate
	String() string
}

// ComparisonOp represents comparison operators
type ComparisonOp int

const (
	Eq ComparisonOp = iota // =
	Ne                     // !=
	Lt                     // <
	Le                     // <=
	Gt                     // >
	Ge                     // >=
)

func (op ComparisonOp) String() string {
	switch op {
	case Eq:
		return "="
	case Ne:
		return "!="
	case Lt:
		return "<"
	case Le:
		return "<="
	case Gt:
		return ">"
	case Ge:
		return ">="
	default:
		return "?"
	}
}

// ColumnPredicate is a predicate on a single column
type ColumnPredicate struct {
	ColumnIndex int
	Op          ComparisonOp
	Value       *chunk.Value
}

// Evaluate implements FilterPredicate
func (p *ColumnPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	if p.ColumnIndex < 0 || p.ColumnIndex >= c.ColumnCount() {
		return nil, fmt.Errorf("column index %d out of range", p.ColumnIndex)
	}

	col := c.Data[p.ColumnIndex]
	result := make([]bool, c.Card())

	for i := 0; i < c.Card(); i++ {
		val := col.GetValue(i)
		result[i] = compareValues(val, p.Value, p.Op)
	}

	return result, nil
}

// CanPushdown implements FilterPredicate
func (p *ColumnPredicate) CanPushdown() bool {
	return true
}

// String implements FilterPredicate
func (p *ColumnPredicate) String() string {
	return fmt.Sprintf("col[%d] %s %v", p.ColumnIndex, p.Op, p.Value)
}

// AndPredicate is a conjunction of two predicates
type AndPredicate struct {
	Left, Right FilterPredicate
}

// Evaluate implements FilterPredicate
func (p *AndPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	left, err := p.Left.Evaluate(c)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.Evaluate(c)
	if err != nil {
		return nil, err
	}

	result := make([]bool, len(left))
	for i := range result {
		result[i] = left[i] && right[i]
	}

	return result, nil
}

// CanPushdown implements FilterPredicate
func (p *AndPredicate) CanPushdown() bool {
	return p.Left.CanPushdown() && p.Right.CanPushdown()
}

// String implements FilterPredicate
func (p *AndPredicate) String() string {
	return fmt.Sprintf("(%s AND %s)", p.Left, p.Right)
}

// OrPredicate is a disjunction of two predicates
type OrPredicate struct {
	Left, Right FilterPredicate
}

// Evaluate implements FilterPredicate
func (p *OrPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	left, err := p.Left.Evaluate(c)
	if err != nil {
		return nil, err
	}

	right, err := p.Right.Evaluate(c)
	if err != nil {
		return nil, err
	}

	result := make([]bool, len(left))
	for i := range result {
		result[i] = left[i] || right[i]
	}

	return result, nil
}

// CanPushdown implements FilterPredicate
func (p *OrPredicate) CanPushdown() bool {
	return p.Left.CanPushdown() && p.Right.CanPushdown()
}

// String implements FilterPredicate
func (p *OrPredicate) String() string {
	return fmt.Sprintf("(%s OR %s)", p.Left, p.Right)
}

// NotPredicate is a negation of a predicate
type NotPredicate struct {
	Inner FilterPredicate
}

// Evaluate implements FilterPredicate
func (p *NotPredicate) Evaluate(c *chunk.Chunk) ([]bool, error) {
	inner, err := p.Inner.Evaluate(c)
	if err != nil {
		return nil, err
	}

	result := make([]bool, len(inner))
	for i := range result {
		result[i] = !inner[i]
	}

	return result, nil
}

// CanPushdown implements FilterPredicate
func (p *NotPredicate) CanPushdown() bool {
	return p.Inner.CanPushdown()
}

// String implements FilterPredicate
func (p *NotPredicate) String() string {
	return fmt.Sprintf("NOT (%s)", p.Inner)
}

// compareValues compares two values using the given operator
func compareValues(a, b *chunk.Value, op ComparisonOp) bool {
	// Handle nulls
	if a == nil || b == nil {
		return false
	}

	// Normalize types for comparison - treat INTEGER and BIGINT as compatible
	aTypeId := a.Typ.Id
	bTypeId := b.Typ.Id

	// Check if both are integer types
	aIsInt := aTypeId == common.LTID_INTEGER || aTypeId == common.LTID_BIGINT
	bIsInt := bTypeId == common.LTID_INTEGER || bTypeId == common.LTID_BIGINT

	// Check if both are float types
	aIsFloat := aTypeId == common.LTID_FLOAT || aTypeId == common.LTID_DOUBLE
	bIsFloat := bTypeId == common.LTID_FLOAT || bTypeId == common.LTID_DOUBLE

	// Compare based on type
	if aIsInt && bIsInt {
		aVal := a.I64
		bVal := b.I64
		switch op {
		case Eq:
			return aVal == bVal
		case Ne:
			return aVal != bVal
		case Lt:
			return aVal < bVal
		case Le:
			return aVal <= bVal
		case Gt:
			return aVal > bVal
		case Ge:
			return aVal >= bVal
		}
	}

	if aIsFloat && bIsFloat {
		aVal := a.F64
		bVal := b.F64
		switch op {
		case Eq:
			return aVal == bVal
		case Ne:
			return aVal != bVal
		case Lt:
			return aVal < bVal
		case Le:
			return aVal <= bVal
		case Gt:
			return aVal > bVal
		case Ge:
			return aVal >= bVal
		}
	}

	// Type checking for other types
	if aTypeId != bTypeId {
		return false
	}

	switch aTypeId {
	case common.LTID_VARCHAR:
		aVal := a.Str
		bVal := b.Str
		switch op {
		case Eq:
			return aVal == bVal
		case Ne:
			return aVal != bVal
		case Lt:
			return aVal < bVal
		case Le:
			return aVal <= bVal
		case Gt:
			return aVal > bVal
		case Ge:
			return aVal >= bVal
		}
	}

	return false
}

// ScanConfig contains configuration for scanning
type ScanConfig struct {
	// BatchSize is the number of rows to read per batch
	BatchSize int
	// Filter is an optional predicate to push down
	Filter FilterPredicate
	// Columns is an optional list of column indices to project
	Columns []int
	// Limit is an optional limit on the number of rows
	Limit int
}

// DefaultScanConfig returns a default scan configuration
func DefaultScanConfig() *ScanConfig {
	return &ScanConfig{
		BatchSize: 8192,
	}
}

// PushdownScanner is a scanner that supports predicate pushdown
type PushdownScanner struct {
	basePath string
	handler  CommitHandler
	version  uint64
	config   *ScanConfig
}

// NewPushdownScanner creates a new scanner with pushdown support
func NewPushdownScanner(basePath string, handler CommitHandler, version uint64, config *ScanConfig) *PushdownScanner {
	if config == nil {
		config = DefaultScanConfig()
	}
	return &PushdownScanner{
		basePath: basePath,
		handler:  handler,
		version:  version,
		config:   config,
	}
}

// Scan performs a scan with predicate pushdown
func (s *PushdownScanner) Scan(ctx context.Context) ([]*chunk.Chunk, error) {
	m, err := LoadManifest(ctx, s.basePath, s.handler, s.version)
	if err != nil {
		return nil, err
	}

	var results []*chunk.Chunk
	var totalRows int

	for _, f := range m.Fragments {
		if f == nil {
			continue
		}

		for _, df := range f.Files {
			if df == nil || df.Path == "" {
				continue
			}

			fullPath := filepath.Join(s.basePath, df.Path)
			c, err := ReadChunkFromFile(fullPath)
			if err != nil {
				return nil, err
			}

			// Apply filter if present
			if s.config.Filter != nil && s.config.Filter.CanPushdown() {
				c, err = s.applyFilter(c, s.config.Filter)
				if err != nil {
					return nil, err
				}
			}

			// Apply projection if present
			if len(s.config.Columns) > 0 {
				c = s.applyProjection(c, s.config.Columns)
			}

			if c.Card() > 0 {
				results = append(results, c)
				totalRows += c.Card()
			}

			// Check limit
			if s.config.Limit > 0 && totalRows >= s.config.Limit {
				return s.truncateToLimit(results), nil
			}
		}
	}

	return results, nil
}

// applyFilter applies a filter predicate to a chunk
func (s *PushdownScanner) applyFilter(c *chunk.Chunk, filter FilterPredicate) (*chunk.Chunk, error) {
	mask, err := filter.Evaluate(c)
	if err != nil {
		return nil, err
	}

	// Count matching rows
	matchCount := 0
	for _, m := range mask {
		if m {
			matchCount++
		}
	}

	if matchCount == 0 {
		// Return empty chunk with same schema
		result := &chunk.Chunk{}
		typs := make([]common.LType, c.ColumnCount())
		for i := 0; i < c.ColumnCount(); i++ {
			typs[i] = c.Data[i].Typ()
		}
		result.Init(typs, c.Card())
		result.SetCard(0)
		return result, nil
	}

	// Create result chunk with matching rows
	result := &chunk.Chunk{}
	typs := make([]common.LType, c.ColumnCount())
	for i := 0; i < c.ColumnCount(); i++ {
		typs[i] = c.Data[i].Typ()
	}
	result.Init(typs, matchCount)
	result.SetCard(matchCount)

	// Copy matching rows
	outRow := 0
	for i := 0; i < c.Card(); i++ {
		if mask[i] {
			for col := 0; col < c.ColumnCount(); col++ {
				val := c.Data[col].GetValue(i)
				result.Data[col].SetValue(outRow, val)
			}
			outRow++
		}
	}

	return result, nil
}

// applyProjection applies column projection to a chunk
func (s *PushdownScanner) applyProjection(c *chunk.Chunk, columns []int) *chunk.Chunk {
	result := &chunk.Chunk{}
	typs := make([]common.LType, len(columns))
	for i, colIdx := range columns {
		typs[i] = c.Data[colIdx].Typ()
	}
	result.Init(typs, c.Card())
	result.SetCard(c.Card())

	for outCol, colIdx := range columns {
		for row := 0; row < c.Card(); row++ {
			val := c.Data[colIdx].GetValue(row)
			result.Data[outCol].SetValue(row, val)
		}
	}

	return result
}

// truncateToLimit truncates results to the limit
func (s *PushdownScanner) truncateToLimit(results []*chunk.Chunk) []*chunk.Chunk {
	if s.config.Limit <= 0 {
		return results
	}

	var totalRows int
	var truncated []*chunk.Chunk

	for _, c := range results {
		if totalRows+c.Card() <= s.config.Limit {
			truncated = append(truncated, c)
			totalRows += c.Card()
		} else {
			// Need to truncate this chunk
			remaining := s.config.Limit - totalRows
			result := &chunk.Chunk{}
			typs := make([]common.LType, c.ColumnCount())
			for i := 0; i < c.ColumnCount(); i++ {
				typs[i] = c.Data[i].Typ()
			}
			result.Init(typs, remaining)
			result.SetCard(remaining)

			for col := 0; col < c.ColumnCount(); col++ {
				for row := 0; row < remaining; row++ {
					val := c.Data[col].GetValue(row)
					result.Data[col].SetValue(row, val)
				}
			}

			truncated = append(truncated, result)
			break
		}
	}

	return truncated
}

// NewColumnPredicate creates a new column predicate
func NewColumnPredicate(columnIndex int, op ComparisonOp, value *chunk.Value) *ColumnPredicate {
	return &ColumnPredicate{
		ColumnIndex: columnIndex,
		Op:          op,
		Value:       value,
	}
}

// NewAndPredicate creates a new AND predicate
func NewAndPredicate(left, right FilterPredicate) *AndPredicate {
	return &AndPredicate{Left: left, Right: right}
}

// NewOrPredicate creates a new OR predicate
func NewOrPredicate(left, right FilterPredicate) *OrPredicate {
	return &OrPredicate{Left: left, Right: right}
}

// NewNotPredicate creates a new NOT predicate
func NewNotPredicate(inner FilterPredicate) *NotPredicate {
	return &NotPredicate{Inner: inner}
}
