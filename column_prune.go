package main

import (
	"fmt"
)

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
	} else {
		//panic("no bind")
	}

	for _, b := range zero {
		delete(ccount, b)
	}
}

func (ccount ColumnBindCountMap) removeNotIn(other ColumnBindCountMap) {
	res := make([]ColumnBind, 0)
	for bind, _ := range ccount {
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
	for bind, _ := range ccount {
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
	for bind, _ := range res {
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

func (ccount ColumnBindCountMap) copy() ColumnBindCountMap {
	res := make(ColumnBindCountMap)
	for bind, i := range ccount {
		res[bind] = i
	}
	return res
}

func (ccount ColumnBindCountMap) count() int {
	return len(ccount)
}

type ReferredColumnBindMap map[ColumnBind][]*Expr

func (ref ReferredColumnBindMap) addExpr(exprs ...*Expr) {
	for _, expr := range exprs {
		set := make(ColumnBindSet)
		collectColRefs(expr, set)
		for bind := range set {
			ref.insert(bind, expr)
		}
	}
}

func (ref ReferredColumnBindMap) insert(bind ColumnBind, exprs ...*Expr) {
	if _, ok := ref[bind]; !ok {
		ref[bind] = make([]*Expr, 0)
	}
	for _, expr := range exprs {
		if expr == nil {
			continue
		}
		ref[bind] = append(ref[bind], expr)
	}
}

func (ref ReferredColumnBindMap) replace(bind, newBind ColumnBind) {
	if exprs, ok := ref[bind]; ok {
		for _, expr := range exprs {
			replaceColRef(expr, bind, newBind)
		}
	}
	delete(ref, bind)
}

func (ref ReferredColumnBindMap) replaceAll(cmap ColumnBindMap) {
	for bind, newBind := range cmap {
		ref.replace(bind, newBind)
	}
}

func (ref ReferredColumnBindMap) beenReferred(bind ColumnBind) bool {
	if _, has := ref[bind]; has {
		return true
	}
	return false
}

func (ref ReferredColumnBindMap) count() int {
	return len(ref)
}

type ColumnPrune struct {
	//colref -> referenced exprs in the plan tree
	colRefs ReferredColumnBindMap
}

func NewColumnPrune() *ColumnPrune {
	return &ColumnPrune{
		colRefs: make(ReferredColumnBindMap),
	}
}

func (b *Builder) columnPrune(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	cp := NewColumnPrune()
	addRefCountOnFirstProject(cp.colRefs, root)
	root, err = cp.prune(root)
	if err != nil {
		return nil, err
	}
	if cp.colRefs.count() != 0 {
		//panic("")
	}
	//TODO: evaluate global offsets
	return root, err
}

func addRefCountOnFirstProject(colRefs ReferredColumnBindMap, root *LogicalOperator) {
	for root != nil && len(root.Children) == 1 {
		if root.Typ == LOT_Project {
			break
		}
		root = root.Children[0]
	}
	if root != nil && root.Typ == LOT_Project {
		for i := 0; i < len(root.Projects); i++ {
			bind := ColumnBind{root.Index, uint64(i)}
			colRefs.addExpr(&Expr{Typ: ET_Column, ColRef: bind})
		}
	} else {
		panic("no project")
	}
}
func addBindCountOnFirstProject(bindCount ColumnBindCountMap, root *LogicalOperator) {
	for root != nil && len(root.Children) == 1 {
		if root.Typ == LOT_Project {
			break
		}
		root = root.Children[0]
	}
	if root != nil && root.Typ == LOT_Project {
		for i := 0; i < len(root.Projects); i++ {
			bind := ColumnBind{root.Index, uint64(i)}
			bindCount.addColumnBind(bind)
		}
	} else {
		panic("no project")
	}
}

func (cp *ColumnPrune) prune(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	switch root.Typ {
	case LOT_Limit:
	case LOT_Order:
		cp.colRefs.addExpr(root.OrderBys...)
	case LOT_Project:
		cmap := make(ColumnBindMap)
		newId := uint64(0)
		removed := make([]int, 0)
		for i := 0; i < len(root.Projects); i++ {
			bind := ColumnBind{root.Index, uint64(i)}
			if !cp.colRefs.beenReferred(bind) {
				removed = append(removed, i)
				cmap[bind] = ColumnBind{root.Index, newId}
				newId++
			} else {
				cp.colRefs.addExpr(root.Projects[i])
			}
		}

		for i, child := range root.Children {
			root.Children[i], err = cp.prune(child)
			if err != nil {
				return nil, err
			}
		}
		//remove unused columns
		for i := len(removed) - 1; i >= 0; i-- {
			root.Projects = erase(root.Projects, removed[i])
		}
		cp.colRefs.replaceAll(cmap)
		if len(root.Projects) == 0 {
			return root.Children[0], err
		}
		return root, err
	case LOT_AggGroup:
		cp.colRefs.addExpr(root.GroupBys...)
		cp.colRefs.addExpr(root.Aggs...)
		cp.colRefs.addExpr(root.Filters...)
	case LOT_JOIN:
		cp.colRefs.addExpr(root.OnConds...)
	case LOT_Scan:
		cp.colRefs.addExpr(root.Filters...)
		catalogTable, err := tpchCatalog().Table(root.Database, root.Table)
		if err != nil {
			return nil, err
		}
		cmap := make(ColumnBindMap)
		needed := make([]string, 0)
		newId := 0
		for colId, colName := range catalogTable.Columns {
			bind := ColumnBind{root.Index, uint64(colId)}
			if cp.colRefs.beenReferred(bind) {
				cmap.insert(bind, ColumnBind{root.Index, uint64(newId)})
				needed = append(needed, colName)
				newId++

			}
		}
		root.Columns = needed
		outputs := make([]*Expr, 0)
		for newIdx, col := range root.Columns {
			idx := catalogTable.Column2Idx[col]
			e := &Expr{
				Typ:      ET_Column,
				DataTyp:  catalogTable.Types[idx],
				Database: root.Database,
				Table:    root.Table,
				Name:     col,
				ColRef:   [2]uint64{root.Index, uint64(newIdx)},
			}
			outputs = append(outputs, e)
		}
		root.Outputs = outputs

		cp.colRefs.replaceAll(cmap)
		return root, nil
	case LOT_Filter:
		cp.colRefs.addExpr(root.Filters...)
	default:
		panic(fmt.Sprintf("usp op type %v", root.Typ))
	}
	for i, child := range root.Children {
		root.Children[i], err = cp.prune(child)
		if err != nil {
			return nil, err
		}
	}
	return root, err
}

func (b *Builder) updateOutputs(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	update := outputsUpdater{}
	counts := make(ColumnBindCountMap)
	addBindCountOnFirstProject(counts, root)
	root, err = update.updateOutputs(root, counts)
	for bind, cnt := range root.Counts {
		if cnt != 1 {
			panic(fmt.Sprintf("bind %v in root is not 1", bind))
		}
	}
	return root, err
}

type outputsUpdater struct {
}

func (update *outputsUpdater) updateOutputs(root *LogicalOperator, upCounts ColumnBindCountMap) (*LogicalOperator, error) {
	if root == nil {
		return nil, nil
	}
	var err error
	//for Agg and Project node, the colRef above them may
	//refer them. these colRef should be removed from the
	//counts instead of transferring them to the children.
	//after the children has been updated, these colRef should
	//be restored.
	var colRefOnThisNode ColumnBindCountMap
	counts := upCounts.copy()
	resCounts := make(ColumnBindCountMap)
	defer func() {
		//check
		for bind, cnt := range resCounts {
			upCnt := upCounts.refCount(bind)
			if upCnt == 0 {
				panic(fmt.Sprintf("no %v in upCounts", bind))
			}
			if upCounts.refCount(bind) != cnt {
				panic(fmt.Sprintf("%v difers in upCounts", bind))
			}
		}
	}()

	updateChildren := func(counts ColumnBindCountMap) error {
		for i, child := range root.Children {
			root.Children[i], err = update.updateOutputs(child, counts)
			if err != nil {
				return err
			}
		}
		//for _, child := range root.Children {
		//	root.Outputs = append(root.Outputs, child.Outputs...)
		//}
		for _, child := range root.Children {
			resCounts.merge(child.Counts)
		}
		return nil
	}

	updateCounts := func(counts ColumnBindCountMap, exprs ...*Expr) error {
		//collect column referred by the exprs
		bSet := make(ColumnBindSet)
		collectColRefs2(bSet, exprs...)

		//add counts of the column referred by this node
		for bind, _ := range bSet {
			counts.addColumnBind(bind)
		}

		//recursive
		err = updateChildren(counts)
		if err != nil {
			return err
		}

		//restore colRefs
		resCounts.merge(colRefOnThisNode)
		resCounts.removeNotIn(upCounts)
		for bind, _ := range bSet {
			counts.removeColumnBind(bind)
			resCounts.removeColumnBind(bind)
		}

		root.Counts = resCounts
		return nil
	}

	switch root.Typ {
	case LOT_Limit:
		err = updateCounts(counts, nil)
		if err != nil {
			return nil, err
		}
	case LOT_Order:
		err = updateCounts(counts, root.OrderBys...)
		if err != nil {
			return nil, err
		}
	case LOT_Project:
		colRefOnThisNode = counts.splitByTableIdx(root.Index)
		err = updateCounts(counts, root.Projects...)
		if err != nil {
			return nil, err
		}

	case LOT_AggGroup:
		//remove aggExprs
		colRefOnThisNode = counts.splitByTableIdx(root.Index2)
		exprs := make([]*Expr, 0)
		exprs = append(exprs, root.GroupBys...)
		exprs = append(exprs, root.Aggs...)
		exprs = append(exprs, root.Filters...)
		err = updateCounts(counts, exprs...)
		if err != nil {
			return nil, err
		}
	case LOT_JOIN:
		err = updateCounts(counts, root.OnConds...)
		if err != nil {
			return nil, err
		}
	case LOT_Scan:
		colRefOnThisNode = counts.splitByTableIdx(root.Index)
		counts.removeByTableIdx(root.Index, false)
		resCounts.merge(colRefOnThisNode)
		resCounts.removeZeroCount()
		root.Counts = resCounts
	case LOT_Filter:
		err = updateCounts(counts, root.Filters...)
		if err != nil {
			return nil, err
		}
	default:
		panic(fmt.Sprintf("usp op type %v", root.Typ))
	}

	return root, nil
}
