// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package storage2

import (
	"context"
	"fmt"
	"sort"
)

// QueryPlan describes how a query should be executed.
type QueryPlan struct {
	// IndexName is the index to use (empty if full scan).
	IndexName string
	// IndexType is the type of the selected index.
	IndexType IndexType
	// EstCost is the estimated cost (lower is better).
	EstCost float64
	// EstRows is the estimated number of matching rows.
	EstRows uint64
	// Selectivity is the estimated fraction of rows matching (0..1).
	Selectivity float64
	// UseFullScan is true when no index is beneficial.
	UseFullScan bool
	// Predicates are the predicates that will be applied.
	Predicates []FilterPredicate
}

// String returns a human-readable description of the plan.
func (p *QueryPlan) String() string {
	if p.UseFullScan {
		return fmt.Sprintf("FullScan(cost=%.2f, rows=%d)", p.EstCost, p.EstRows)
	}
	return fmt.Sprintf("IndexScan(%s, type=%s, cost=%.2f, rows=%d, sel=%.4f)",
		p.IndexName, p.IndexType, p.EstCost, p.EstRows, p.Selectivity)
}

// TableStatistics provides table-level statistics for cost estimation.
type TableStatistics struct {
	TotalRows   uint64
	ColumnStats map[int]*ColumnStatisticsInfo
}

// ColumnStatisticsInfo holds per-column statistics for query planning.
type ColumnStatisticsInfo struct {
	DistinctCount uint64
	NullCount     uint64
	MinValue      interface{}
	MaxValue      interface{}
}

// IndexPlannerV2 is a cost-based query planner that selects the optimal
// index (or full scan) for a given predicate.
type IndexPlannerV2 struct {
	manager    *IndexManager
	tableStats *TableStatistics
}

// NewIndexPlannerV2 creates a new cost-based index planner.
func NewIndexPlannerV2(manager *IndexManager) *IndexPlannerV2 {
	return &IndexPlannerV2{
		manager: manager,
	}
}

// SetTableStats sets the table-level statistics used for cost estimation.
func (p *IndexPlannerV2) SetTableStats(stats *TableStatistics) {
	p.tableStats = stats
}

// PlanQuery produces a QueryPlan for the given predicate.
// It evaluates all candidate indexes, estimates each plan's cost, and
// returns the cheapest option (including full scan).
func (p *IndexPlannerV2) PlanQuery(ctx context.Context, predicate FilterPredicate) (*QueryPlan, error) {
	if predicate == nil {
		return &QueryPlan{
			UseFullScan: true,
			EstCost:     p.estimateFullScanCost(),
			EstRows:     p.totalRows(),
		}, nil
	}

	candidates := p.collectCandidates(predicate)

	// Evaluate each candidate
	for _, c := range candidates {
		p.estimateCost(c, predicate)
	}

	// Add full scan as a candidate
	fullScan := &QueryPlan{
		UseFullScan: true,
		EstCost:     p.estimateFullScanCost(),
		EstRows:     p.totalRows(),
		Predicates:  []FilterPredicate{predicate},
	}

	// Pick the cheapest plan
	best := fullScan
	for _, c := range candidates {
		if c.EstCost < best.EstCost {
			best = c
		}
	}

	return best, nil
}

// PlanQueryLegacy implements the original IndexPlanner.PlanQuery signature
// for backwards compatibility. It returns (matchingRowIDs, didUseIndex).
func (p *IndexPlannerV2) PlanQueryLegacy(predicate FilterPredicate) ([]uint64, bool) {
	plan, err := p.PlanQuery(context.Background(), predicate)
	if err != nil || plan == nil || plan.UseFullScan {
		return nil, false
	}

	idx, ok := p.manager.GetIndex(plan.IndexName)
	if !ok {
		return nil, false
	}

	rowIDs, err := idx.Search(context.Background(), predicate, int(plan.EstRows))
	if err != nil {
		return nil, false
	}
	return rowIDs, len(rowIDs) > 0
}

// collectCandidates finds indexes that could serve the predicate.
func (p *IndexPlannerV2) collectCandidates(predicate FilterPredicate) []*QueryPlan {
	columns := extractPredicateColumns(predicate)
	if len(columns) == 0 {
		return nil
	}

	var candidates []*QueryPlan
	for _, meta := range p.manager.ListIndexes() {
		idx, ok := p.manager.GetIndex(meta.Name)
		if !ok {
			continue
		}

		if indexCoversColumns(idx, columns) {
			candidates = append(candidates, &QueryPlan{
				IndexName:  meta.Name,
				IndexType:  meta.Type,
				Predicates: []FilterPredicate{predicate},
			})
		}
	}

	return candidates
}

// estimateCost fills in EstCost, EstRows and Selectivity for a candidate plan.
func (p *IndexPlannerV2) estimateCost(plan *QueryPlan, predicate FilterPredicate) {
	idx, ok := p.manager.GetIndex(plan.IndexName)
	if !ok {
		plan.EstCost = 1e18
		return
	}

	stats := idx.Statistics()
	sel := p.estimateSelectivity(predicate)
	plan.Selectivity = sel
	plan.EstRows = uint64(float64(p.totalRows()) * sel)

	// Cost model:
	//   indexLookup = log2(numEntries) for tree-based, or constant for bitmap/hash
	//   perRow      = cost per matching row (fetch from table)
	var indexLookupCost float64
	switch plan.IndexType {
	case ScalarIndex:
		// B-tree: O(log N) lookup + scan matching rows
		if stats.NumEntries > 0 {
			indexLookupCost = 2.0 // approximate log factor
		}
	case VectorIndex:
		// ANN: higher fixed cost
		indexLookupCost = 10.0
	case InvertedIndex:
		indexLookupCost = 5.0
	default:
		// Bitmap, ZoneMap, BloomFilter
		indexLookupCost = 1.0
	}

	fetchCostPerRow := 1.0
	plan.EstCost = indexLookupCost + float64(plan.EstRows)*fetchCostPerRow
}

// estimateSelectivity estimates the fraction of rows matching a predicate.
func (p *IndexPlannerV2) estimateSelectivity(predicate FilterPredicate) float64 {
	switch pred := predicate.(type) {
	case *ColumnPredicate:
		return p.estimateColumnSelectivity(pred)
	case *AndPredicate:
		return p.estimateSelectivity(pred.Left) * p.estimateSelectivity(pred.Right)
	case *OrPredicate:
		s1 := p.estimateSelectivity(pred.Left)
		s2 := p.estimateSelectivity(pred.Right)
		return s1 + s2 - s1*s2
	case *NotPredicate:
		return 1.0 - p.estimateSelectivity(pred.Inner)
	}
	return 1.0
}

// estimateColumnSelectivity estimates selectivity for a simple column predicate.
func (p *IndexPlannerV2) estimateColumnSelectivity(pred *ColumnPredicate) float64 {
	// Try to use column statistics if available
	if p.tableStats != nil {
		if colStats, ok := p.tableStats.ColumnStats[pred.ColumnIndex]; ok {
			switch pred.Op {
			case Eq:
				if colStats.DistinctCount > 0 {
					return 1.0 / float64(colStats.DistinctCount)
				}
			case Ne:
				if colStats.DistinctCount > 0 {
					return 1.0 - 1.0/float64(colStats.DistinctCount)
				}
			case Lt, Le, Gt, Ge:
				// Use min/max to estimate range selectivity
				if colStats.MinValue != nil && colStats.MaxValue != nil {
					return estimateRangeSelectivityFromStats(colStats, pred.Op, extractValueFromChunkValue(pred.Value))
				}
				return 0.33
			}
		}
	}

	// Default heuristics when no statistics are available
	switch pred.Op {
	case Eq:
		return 0.01
	case Ne:
		return 0.99
	case Lt, Le, Gt, Ge:
		return 0.33
	}
	return 1.0
}

// estimateRangeSelectivityFromStats uses column min/max for better range estimates.
func estimateRangeSelectivityFromStats(stats *ColumnStatisticsInfo, op ComparisonOp, value interface{}) float64 {
	if stats.MinValue == nil || stats.MaxValue == nil {
		return 0.33
	}

	switch op {
	case Lt, Le:
		if compareInterfaceValues(value, stats.MinValue) <= 0 {
			return 0.0
		}
		if compareInterfaceValues(value, stats.MaxValue) >= 0 {
			return 1.0
		}
		return estimateFraction(stats.MinValue, stats.MaxValue, value)

	case Gt, Ge:
		if compareInterfaceValues(value, stats.MaxValue) >= 0 {
			return 0.0
		}
		if compareInterfaceValues(value, stats.MinValue) <= 0 {
			return 1.0
		}
		return 1.0 - estimateFraction(stats.MinValue, stats.MaxValue, value)
	}
	return 0.33
}

// estimateFullScanCost returns the cost of a full table scan.
func (p *IndexPlannerV2) estimateFullScanCost() float64 {
	total := p.totalRows()
	if total == 0 {
		return 1.0
	}
	// Sequential scan is cheaper per row than random index fetch
	return float64(total) * 0.5
}

func (p *IndexPlannerV2) totalRows() uint64 {
	if p.tableStats != nil && p.tableStats.TotalRows > 0 {
		return p.tableStats.TotalRows
	}
	return 100000 // default assumption
}

// --- helpers ---

// extractPredicateColumns collects all column indices referenced in a predicate.
func extractPredicateColumns(predicate FilterPredicate) []int {
	seen := make(map[int]struct{})
	extractColumnsRecursive(predicate, seen)
	cols := make([]int, 0, len(seen))
	for c := range seen {
		cols = append(cols, c)
	}
	sort.Ints(cols)
	return cols
}

func extractColumnsRecursive(predicate FilterPredicate, seen map[int]struct{}) {
	switch pred := predicate.(type) {
	case *ColumnPredicate:
		seen[pred.ColumnIndex] = struct{}{}
	case *AndPredicate:
		extractColumnsRecursive(pred.Left, seen)
		extractColumnsRecursive(pred.Right, seen)
	case *OrPredicate:
		extractColumnsRecursive(pred.Left, seen)
		extractColumnsRecursive(pred.Right, seen)
	case *NotPredicate:
		extractColumnsRecursive(pred.Inner, seen)
	}
}

// indexCoversColumns returns true if the index's first column appears in the predicate columns.
func indexCoversColumns(idx Index, columns []int) bool {
	idxCols := idx.Columns()
	if len(idxCols) == 0 {
		return false
	}
	for _, c := range columns {
		if idxCols[0] == c {
			return true
		}
	}
	return false
}
