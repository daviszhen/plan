// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
)

// 通用工具函数
func isDisjoint(a, b map[uint64]bool) bool {
	for k := range a {
		if _, has := b[k]; has {
			return false
		}
	}
	return true
}

func EntriesPerBlock(width int) int {
	return int(storage.BLOCK_SIZE) / width
}

type DataType int

const (
	DataTypeInteger DataType = iota
	DataTypeVarchar
	DataTypeDecimal
	DataTypeDate
	DataTypeBool
	DataTypeInterval
	DataTypeFloat64
	DataTypeInvalid // used in binding process
)

var dataType2Str = map[DataType]string{
	DataTypeInteger: "int",
	DataTypeVarchar: "varchar",
	DataTypeDecimal: "decimal",
	DataTypeDate:    "date",
	DataTypeBool:    "bool",
	DataTypeInvalid: "invalid",
}

func (dt DataType) String() string {
	if s, ok := dataType2Str[dt]; ok {
		return s
	}
	return "invalid"
}

type Catalog struct {
	tpch map[string]*CatalogTable
}

func (c *Catalog) Table(db, table string) (*CatalogTable, error) {
	if db == "tpch" {
		if c, ok := c.tpch[table]; ok {
			return c, nil
		} else {
			panic(fmt.Errorf("table %s in database %s does not exist", table, db))
		}
	} else {
		panic(fmt.Sprintf("database %s does not exist", db))
	}
}

type Stats struct {
	RowCount float64
	ColStats []*BaseStats
}

func (s *Stats) Copy() *Stats {
	ret := &Stats{
		RowCount: s.RowCount,
	}
	ret.ColStats = make([]*BaseStats, len(s.ColStats))
	for i, stat := range s.ColStats {
		ret.ColStats[i] = stat.Copy()
	}
	return ret
}

func (s *Stats) String() string {
	if s == nil {
		return ""
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("rowcount %v\n", s.RowCount))
	for i, stat := range s.ColStats {
		sb.WriteString(fmt.Sprintf("col %v ", i))
		sb.WriteString(stat.String())
		sb.WriteByte('\n')
	}
	return sb.String()
}

func convertStats(tstats *storage.TableStats) *Stats {
	ret := &Stats{
		RowCount: float64(tstats.GetStats(0).Count()),
	}
	for _, cstats := range tstats.GetStats2() {
		ret.ColStats = append(ret.ColStats, convertStats2(cstats))
	}
	return ret
}

func convertStats2(cstats *storage.ColumnStats) *BaseStats {
	return &BaseStats{
		hasNull:       cstats.HasNull(),
		hasNoNull:     cstats.HasNoNull(),
		distinctCount: cstats.DistinctCount(),
	}
}

func convertStats3(bstats *storage.BaseStats) *BaseStats {
	return &BaseStats{
		hasNull:       bstats.HasNull(),
		hasNoNull:     bstats.HasNoNull(),
		distinctCount: bstats.DistinctCount(),
	}
}

type CatalogTable struct {
	Db         string
	Table      string
	Columns    []string
	Types      []common.LType
	PK         []int
	Column2Idx map[string]int
	Stats      *Stats
}

// WildcardMatch implements wildcard pattern match algorithm.
// pattern and target are ascii characters
// TODO: add \_ and \%
func WildcardMatch(pattern, target *common.String) bool {
	var p = 0
	var t = 0
	var positionOfPercentPlusOne int = -1
	var positionOfTargetEncounterPercent int = -1
	plen := pattern.Length()
	tlen := target.Length()
	pSlice := pattern.DataSlice()
	tSlice := target.DataSlice()
	for t < tlen {
		//%
		if p < plen && pSlice[p] == '%' {
			p++
			positionOfPercentPlusOne = p
			if p >= plen {
				//pattern end with %
				return true
			}
			//means % matches empty
			positionOfTargetEncounterPercent = t
		} else if p < plen && (pSlice[p] == '_' || pSlice[p] == tSlice[t]) { //match or _
			p++
			t++
		} else {
			if positionOfPercentPlusOne == -1 {
				//have not matched a %
				return false
			}
			if positionOfTargetEncounterPercent == -1 {
				return false
			}
			//backtrace to last % position + 1
			p = positionOfPercentPlusOne
			//means % matches multiple characters
			positionOfTargetEncounterPercent++
			t = positionOfTargetEncounterPercent
		}
	}
	//skip %
	for p < plen && pSlice[p] == '%' {
		p++
	}
	return p >= plen
}

type ColumnBind [2]uint64
type ColumnBindSet map[ColumnBind]bool
type ColumnBindMap map[ColumnBind]ColumnBind

func NewColumnBind(a, b uint64) ColumnBind {
	return ColumnBind{a, b}
}

func (bind ColumnBind) table() uint64 {
	return bind[0]
}

func (bind ColumnBind) column() uint64 {
	return bind[1]
}

func (bind ColumnBind) less(other ColumnBind) bool {
	return bind.table() < other.table() || bind.table() == other.table() && bind.column() < other.column()
}

func (bind ColumnBind) String() string {
	return fmt.Sprintf("[%d %d]", int64(bind.table()), int64(bind.column()))
}

func (set ColumnBindSet) find(bind ColumnBind) bool {
	if _, has := set[bind]; has {
		return true
	}
	return false
}

func (set ColumnBindSet) insert(bind ColumnBind) {
	set[bind] = true
}

func (set ColumnBindSet) empty() bool {
	return len(set) == 0
}

func (set ColumnBindSet) clear() {
	for key := range set {
		delete(set, key)
	}
}

func (set ColumnBindSet) hasTableId(tableId common.SourceType) bool {
	for key := range set {
		if key.table() == uint64(tableId) {
			return true
		}
	}
	return false
}

func (cmap ColumnBindMap) find(bind ColumnBind) bool {
	if _, has := cmap[bind]; has {
		return true
	}
	return false
}

func (cmap ColumnBindMap) get(bind ColumnBind) ColumnBind {
	if !cmap.find(bind) {
		panic("does no exist")
	}
	return cmap[bind]
}

func (cmap ColumnBindMap) insert(key, value ColumnBind) {
	cmap[key] = value
}
