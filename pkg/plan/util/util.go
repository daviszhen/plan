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
	"sort"
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

type ColumnBindCountMap map[ColumnBind]int

func (ccount ColumnBindCountMap) addColumnBind(bind ColumnBind) {
	if _, ok := ccount[bind]; !ok {
		ccount[bind] = 0
	}
	ccount[bind]++
}

func (ccount ColumnBindCountMap) removeColumnBind(bind ColumnBind) {
	zero := make([]ColumnBind, 0)
	if _, ok := ccount[bind]; ok {
		old := ccount[bind]
		if old < 1 {
			panic("negative bind count")
		}
		ccount[bind] = old - 1
		if old == 1 {
			zero = append(zero, bind)
		}
	}

	for _, b := range zero {
		delete(ccount, b)
	}
}

func (ccount ColumnBindCountMap) removeNotIn(other ColumnBindCountMap) {
	res := make([]ColumnBind, 0)
	for bind := range ccount {
		if _, has := other[bind]; !has {
			res = append(res, bind)
		}
	}
	for _, bind := range res {
		delete(ccount, bind)
	}
}

func (ccount ColumnBindCountMap) removeByTableIdx(tblIdx uint64, equal bool) {
	res := make([]ColumnBind, 0)
	for bind := range ccount {
		if equal {
			if bind.table() == tblIdx {
				res = append(res, bind)
			}
		} else {
			if bind.table() != tblIdx {
				res = append(res, bind)
			}
		}
	}
	for _, bind := range res {
		delete(ccount, bind)
	}
}

func (ccount ColumnBindCountMap) splitByTableIdx(tblIdx uint64) ColumnBindCountMap {
	res := make(ColumnBindCountMap, 0)
	for bind, i := range ccount {
		if bind.table() == tblIdx {
			res[bind] = i
		}
	}
	for bind := range res {
		delete(ccount, bind)
	}
	return res
}

func (ccount ColumnBindCountMap) splitByTwoTableIdxes(tblIdx, tableIdx2 uint64) ColumnBindCountMap {
	res := make(ColumnBindCountMap, 0)
	for bind, i := range ccount {
		if bind.table() == tblIdx || bind.table() == tableIdx2 {
			res[bind] = i
		}
	}
	for bind := range res {
		delete(ccount, bind)
	}
	return res
}

func (ccount ColumnBindCountMap) merge(count ColumnBindCountMap) {
	for bind, i := range count {
		ccount[bind] += i
	}
}

func (ccount ColumnBindCountMap) removeZeroCount() {
	res := make([]ColumnBind, 0)
	for bind, i := range ccount {
		if i == 0 {
			res = append(res, bind)
		}
	}
	for _, bind := range res {
		delete(ccount, bind)
	}
}

func (ccount ColumnBindCountMap) refCount(bind ColumnBind) int {
	if count, ok := ccount[bind]; ok {
		return count
	}
	return 0
}

func (ccount ColumnBindCountMap) has(bind ColumnBind) (bool, int) {
	if count, ok := ccount[bind]; ok {
		return true, count
	}
	return false, 0
}

func (ccount ColumnBindCountMap) copy() ColumnBindCountMap {
	res := make(ColumnBindCountMap)
	for bind, i := range ccount {
		res[bind] = i
	}
	return res
}

func (ccount ColumnBindCountMap) sortByColumnBind() ColumnBindPosMap {
	posMap := make(ColumnBindPosMap)
	binds := make([]ColumnBind, 0)
	for bind := range ccount {
		binds = append(binds, bind)
	}
	sort.Slice(binds, func(i, j int) bool {
		return binds[i].less(binds[j])
	})
	ret := sort.SliceIsSorted(binds, func(i, j int) bool {
		return binds[i].less(binds[j])
	})
	if !ret {
		panic("unsorted")
	}
	for i, bind := range binds {
		posMap.insert(bind, i)
	}
	return posMap
}

type ColumnBindPosMap map[ColumnBind]int

func (posmap ColumnBindPosMap) insert(bind ColumnBind, pos int) {
	posmap[bind] = pos
}
func (posmap ColumnBindPosMap) pos(bind ColumnBind) (bool, int) {
	if pos, has := posmap[bind]; has {
		return true, pos
	}
	return false, 0
}
func (posmap ColumnBindPosMap) sortByColumnBind() []ColumnBind {
	res := make([]ColumnBind, 0)
	for bind := range posmap {
		res = append(res, bind)
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].less(res[j])
	})
	return res
}

type UnorderedSet map[uint64]bool

func (set UnorderedSet) orderedKeys() []uint64 {
	keys := make([]uint64, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (set UnorderedSet) insert(keys ...uint64) {
	for _, key := range keys {
		set[key] = true
	}
}

func (set UnorderedSet) find(key uint64) bool {
	_, has := set[key]
	return has
}

func (set UnorderedSet) clear() {
	for key := range set {
		delete(set, key)
	}
}

func (set UnorderedSet) empty() bool {
	return len(set) == 0
}

func (set UnorderedSet) size() int {
	return len(set)
}
