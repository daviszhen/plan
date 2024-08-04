package main

import (
	"fmt"
	"sort"
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

func (ccount ColumnBindCountMap) count() int {
	return len(ccount)
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

func (posmap ColumnBindPosMap) count() int {
	return len(posmap)
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
			} else {
				cp.colRefs.addExpr(root.Projects[i])
				cmap[bind] = ColumnBind{root.Index, newId}
				newId++
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
		cp.colRefs.addExpr(root.Filters...)
		//!!!noticed that: Filters in the aggNode
		//may refer the Column in Aggs list.
		//root.Filters must be added into colRefs before
		//process root.Aggs
		cmap := make(ColumnBindMap)
		newId := uint64(0)
		removed := make([]int, 0)
		for i := 0; i < len(root.Aggs); i++ {
			bind := ColumnBind{root.Index2, uint64(i)}
			if !cp.colRefs.beenReferred(bind) {
				removed = append(removed, i)
			} else {
				cp.colRefs.addExpr(root.Aggs[i])
				cmap[bind] = ColumnBind{root.Index2, newId}
				newId++
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
			root.Aggs = erase(root.Aggs, removed[i])
		}
		cp.colRefs.replaceAll(cmap)
		if len(root.Aggs) == 0 {
			return root.Children[0], nil
		}
		return root, nil
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

func (b *Builder) generateCounts(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	update := countsUpdater{}
	counts := make(ColumnBindCountMap)
	addBindCountOnFirstProject(counts, root)
	root, err = update.generateCounts(root, counts)
	for bind, cnt := range root.Counts {
		if cnt != 1 {
			panic(fmt.Sprintf("bind %v in root is not 1", bind))
		}
	}
	return root, err
}

type countsUpdater struct {
}

func (update *countsUpdater) generateCounts(root *LogicalOperator, upCounts ColumnBindCountMap) (*LogicalOperator, error) {
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
	backupCounts := upCounts.copy()
	resCounts := make(ColumnBindCountMap)

	updateChildren := func(counts ColumnBindCountMap) error {
		for i, child := range root.Children {
			root.Children[i], err = update.generateCounts(child, counts)
			if err != nil {
				return err
			}
		}
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
		for bind := range bSet {
			counts.addColumnBind(bind)
		}

		//recursive
		err = updateChildren(counts)
		if err != nil {
			return err
		}

		//restore colRefs
		resCounts.merge(colRefOnThisNode)
		counts.merge(colRefOnThisNode)
		resCounts.removeNotIn(upCounts)
		for bind := range bSet {
			counts.removeColumnBind(bind)
			resCounts.removeColumnBind(bind)
		}

		root.Counts = resCounts
		root.ColRefToPos = resCounts.sortByColumnBind()
		return nil
	}

	switch root.Typ {
	case LOT_Limit:
		err = updateCounts(upCounts, nil)
		if err != nil {
			return nil, err
		}
	case LOT_Order:
		err = updateCounts(upCounts, root.OrderBys...)
		if err != nil {
			return nil, err
		}
	case LOT_Project:
		colRefOnThisNode = upCounts.splitByTableIdx(root.Index)
		err = updateCounts(upCounts, root.Projects...)
		if err != nil {
			return nil, err
		}

	case LOT_AggGroup:
		//remove aggExprs & group by Exprs
		colRefOnThisNode = upCounts.splitByTwoTableIdxes(root.Index, root.Index2)
		exprs := make([]*Expr, 0)
		exprs = append(exprs, root.GroupBys...)
		exprs = append(exprs, root.Aggs...)
		exprs = append(exprs, root.Filters...)
		err = updateCounts(upCounts, exprs...)
		if err != nil {
			return nil, err
		}
	case LOT_JOIN:
		err = updateCounts(upCounts, root.OnConds...)
		if err != nil {
			return nil, err
		}
	case LOT_Scan:
		resCounts = upCounts.copy()
		resCounts.removeByTableIdx(root.Index, false)
		resCounts.removeZeroCount()
		root.Counts = resCounts
		root.ColRefToPos = resCounts.sortByColumnBind()
	case LOT_Filter:
		err = updateCounts(upCounts, root.Filters...)
		if err != nil {
			return nil, err
		}
	default:
		panic(fmt.Sprintf("usp op type %v", root.Typ))
	}

	//check
	for bind, cnt := range resCounts {
		upCnt := backupCounts.refCount(bind)
		if upCnt == 0 {
			panic(fmt.Sprintf("no %v in backupCounts", bind))
		}
		if backupCounts.refCount(bind) != cnt {
			panic(fmt.Sprintf("%v count differs in backupCounts", bind))
		}
	}

	for bind, cnt := range upCounts {
		has, hcnt := backupCounts.has(bind)
		if !has {
			fmt.Printf("%v no bind %v in upCounts \n", root.Typ, bind)
			panic("xxx")
		} else if hcnt != cnt {
			fmt.Printf("%v bind %v count %d differs in upCounts %d \n",
				root.Typ, bind, cnt, hcnt)
			panic("xxx1")
		}
	}

	for bind, cnt := range backupCounts {
		has, hcnt := upCounts.has(bind)
		if !has {
			fmt.Printf("%v no bind %v in upCounts \n", root.Typ, bind)
			panic("xxx2")
		} else if hcnt != cnt {
			fmt.Printf("%v bind %v count %d differs in upCounts %d \n",
				root.Typ, bind, cnt, hcnt)
			panic("xxx3")
		}
	}

	return root, nil
}

func (b *Builder) generateOutputs(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	update := outputsUpdater{}
	root, err = update.generateOutputs(root)
	if err != nil {
		return nil, err
	}
	return root, nil
}

type SourceType int

const (
	ThisNode   SourceType = 0
	LeftChild             = -1 //also for one child
	RightChild            = -2
)

type outputsUpdater struct {
}

func (update *outputsUpdater) generateOutputs(root *LogicalOperator) (*LogicalOperator, error) {
	if root == nil {
		return nil, nil
	}
	var err error

	genChildren := func() (err error) {
		for i, child := range root.Children {
			root.Children[i], err = update.generateOutputs(child)
			if err != nil {
				return err
			}
		}
		return nil
	}

	switch root.Typ {
	case LOT_Limit:
		err = genChildren()
		if err != nil {
			return nil, err
		}

		binds := root.ColRefToPos.sortByColumnBind()
		for _, bind := range binds {
			//bind pos in the children
			st := LeftChild
			has, childPos := root.Children[0].ColRefToPos.pos(bind)
			if !has {
				if len(root.Children) > 1 {
					has, childPos = root.Children[1].ColRefToPos.pos(bind)
					if !has {
						panic(fmt.Sprintf("no such %v in children", bind))
					}
					st = RightChild
				} else {
					panic(fmt.Sprintf("no such %v in children", bind))
				}
			}

			childExpr := root.Children[0].Outputs[childPos]
			root.Outputs = append(root.Outputs, &Expr{
				Typ:      ET_Column,
				DataTyp:  childExpr.DataTyp,
				Database: childExpr.Database,
				Table:    childExpr.Table,
				Name:     childExpr.Name,
				ColRef:   ColumnBind{uint64(st), uint64(childPos)},
			})
		}
	case LOT_Order:
		err = genChildren()
		if err != nil {
			return nil, err
		}

		replaceColRef3(root.OrderBys, root.Children[0].ColRefToPos, LeftChild)

		binds := root.ColRefToPos.sortByColumnBind()
		for _, bind := range binds {
			//bind pos in the children
			st := LeftChild
			has, childPos := root.Children[0].ColRefToPos.pos(bind)
			if !has {
				if len(root.Children) > 1 {
					has, childPos = root.Children[1].ColRefToPos.pos(bind)
					if !has {
						panic(fmt.Sprintf("no such %v in children", bind))
					}
					st = RightChild
				} else {
					panic(fmt.Sprintf("no such %v in children", bind))
				}
			}

			childExpr := root.Children[0].Outputs[childPos]
			root.Outputs = append(root.Outputs, &Expr{
				Typ:      ET_Column,
				DataTyp:  childExpr.DataTyp,
				Database: childExpr.Database,
				Table:    childExpr.Table,
				Name:     childExpr.Name,
				ColRef:   ColumnBind{uint64(st), uint64(childPos)},
			})
		}

	case LOT_Project:
		err = genChildren()
		if err != nil {
			return nil, err
		}

		replaceColRef3(root.Projects, root.Children[0].ColRefToPos, LeftChild)

		binds := root.ColRefToPos.sortByColumnBind()
		for _, bind := range binds {
			if bind.table() == root.Index {
				proj := root.Projects[bind.column()]
				root.Outputs = append(root.Outputs, &Expr{
					Typ:      ET_Column,
					DataTyp:  proj.DataTyp,
					Database: proj.Database,
					Table:    proj.Table,
					Name:     proj.Name,
					ColRef:   ColumnBind{uint64(ThisNode), uint64(bind.column())},
				})
				continue
			}
			//bind pos in the children
			st := LeftChild
			has, childPos := root.Children[0].ColRefToPos.pos(bind)
			if !has {
				if len(root.Children) > 1 {
					has, childPos = root.Children[1].ColRefToPos.pos(bind)
					if !has {
						panic(fmt.Sprintf("no such %v in children", bind))
					}
					st = RightChild
				} else {
					panic(fmt.Sprintf("no such %v in children", bind))
				}
			}

			childExpr := root.Children[0].Outputs[childPos]
			root.Outputs = append(root.Outputs, &Expr{
				Typ:      ET_Column,
				DataTyp:  childExpr.DataTyp,
				Database: childExpr.Database,
				Table:    childExpr.Table,
				Name:     childExpr.Name,
				ColRef:   ColumnBind{uint64(st), uint64(childPos)},
			})
		}

	case LOT_AggGroup:
		err = genChildren()
		if err != nil {
			return nil, err
		}

		replaceColRef3(root.GroupBys, root.Children[0].ColRefToPos, LeftChild)
		replaceColRef3(root.Aggs, root.Children[0].ColRefToPos, LeftChild)
		replaceColRef3(root.Filters, root.Children[0].ColRefToPos, LeftChild)

		binds := root.ColRefToPos.sortByColumnBind()
		for _, bind := range binds {
			colIdx := len(root.Outputs)
			if bind.table() == root.Index {
				groupby := root.GroupBys[bind.column()]
				root.Outputs = append(root.Outputs, &Expr{
					Typ:      ET_Column,
					DataTyp:  groupby.DataTyp,
					Database: groupby.Database,
					Table:    groupby.Table,
					Name:     groupby.Name,
					ColRef:   ColumnBind{uint64(ThisNode), uint64(colIdx)},
				})
				continue
			}
		}
		for _, bind := range binds {
			colIdx := len(root.Outputs)
			if bind.table() == root.Index2 {
				agg := root.Aggs[bind.column()]
				root.Outputs = append(root.Outputs, &Expr{
					Typ:      ET_Column,
					DataTyp:  agg.DataTyp,
					Database: agg.Database,
					Table:    agg.Table,
					Name:     agg.Name,
					ColRef:   ColumnBind{uint64(ThisNode), uint64(colIdx)},
				})
				continue
			}
		}
		for _, bind := range binds {
			if bind.table() == root.Index || bind.table() == root.Index2 {
				continue
			}
			//bind pos in the children
			st := LeftChild
			has, childPos := root.Children[0].ColRefToPos.pos(bind)
			if !has {
				if len(root.Children) > 1 {
					has, childPos = root.Children[1].ColRefToPos.pos(bind)
					if !has {
						panic(fmt.Sprintf("no such %v in children", bind))
					}
					st = RightChild
				} else {
					panic(fmt.Sprintf("no such %v in children", bind))
				}
			}

			childExpr := root.Children[0].Outputs[childPos]
			root.Outputs = append(root.Outputs, &Expr{
				Typ:      ET_Column,
				DataTyp:  childExpr.DataTyp,
				Database: childExpr.Database,
				Table:    childExpr.Table,
				Name:     childExpr.Name,
				ColRef:   ColumnBind{uint64(st), uint64(childPos)},
			})
		}

	case LOT_JOIN:
		err = genChildren()
		if err != nil {
			return nil, err
		}

		replaceColRef3(root.OnConds, root.Children[0].ColRefToPos, LeftChild)
		replaceColRef3(root.OnConds, root.Children[1].ColRefToPos, RightChild)

		//switch onConds left & right
		for _, cond := range root.OnConds {
			lset := make(ColumnBindSet)
			rset := make(ColumnBindSet)
			switch cond.Typ {
			case ET_Func:
				switch cond.SubTyp {
				case ET_In, ET_NotIn:
					collectColRefs(cond.Children[0], lset)
					collectColRefs(cond.Children[1], rset)
				case ET_SubFunc:
				case ET_And, ET_Or, ET_Equal, ET_NotEqual, ET_Like, ET_NotLike, ET_GreaterEqual, ET_Less, ET_Greater:
					collectColRefs(cond.Children[0], lset)
					collectColRefs(cond.Children[1], rset)
				default:
					panic(fmt.Sprintf("usp %v", cond.SubTyp))
				}
			default:
				panic(fmt.Sprintf("usp operator type %d", cond.Typ))
			}

			lLeftYes := lset.hasTableId(LeftChild)
			lRightYes := lset.hasTableId(RightChild)
			rLeftYes := rset.hasTableId(LeftChild)
			rRightYes := rset.hasTableId(RightChild)
			if lLeftYes && lRightYes {
				panic("left child has two data source")
			}
			if rLeftYes && rRightYes {
				panic("right child has two data source")
			}
			if lRightYes && rLeftYes {
				//switch
				cond.Children[0], cond.Children[1] = cond.Children[1], cond.Children[0]
			}
		}

		binds := root.ColRefToPos.sortByColumnBind()
		for _, bind := range binds {
			//bind pos in the children
			st := LeftChild
			has, childPos := root.Children[0].ColRefToPos.pos(bind)
			if !has {
				if len(root.Children) > 1 {
					has, childPos = root.Children[1].ColRefToPos.pos(bind)
					if !has {
						panic(fmt.Sprintf("no such %v in children", bind))
					}
					st = RightChild
				} else {
					panic(fmt.Sprintf("no such %v in children", bind))
				}
			}

			var childExpr *Expr
			if st == LeftChild {
				childExpr = root.Children[0].Outputs[childPos]
			} else {
				childExpr = root.Children[1].Outputs[childPos]
			}

			root.Outputs = append(root.Outputs, &Expr{
				Typ:      ET_Column,
				DataTyp:  childExpr.DataTyp,
				Database: childExpr.Database,
				Table:    childExpr.Table,
				Name:     childExpr.Name,
				ColRef:   ColumnBind{uint64(st), uint64(childPos)},
			})
		}
	case LOT_Scan:
		catalogTable, err := tpchCatalog().Table(root.Database, root.Table)
		if err != nil {
			return nil, err
		}

		binds := root.ColRefToPos.sortByColumnBind()
		outputs := make([]*Expr, 0)
		for _, bind := range binds {
			colName := root.Columns[bind.column()]
			idx := catalogTable.Column2Idx[colName]
			e := &Expr{
				Typ:      ET_Column,
				DataTyp:  catalogTable.Types[idx],
				Database: root.Database,
				Table:    root.Table,
				Name:     colName,
				ColRef:   ColumnBind{uint64(ThisNode), uint64(bind.column())},
			}
			outputs = append(outputs, e)
		}
		root.Outputs = outputs

	case LOT_Filter:
		err = genChildren()
		if err != nil {
			return nil, err
		}

		replaceColRef3(root.Filters, root.Children[0].ColRefToPos, LeftChild)

		binds := root.ColRefToPos.sortByColumnBind()
		for _, bind := range binds {
			//bind pos in the children
			st := LeftChild
			has, childPos := root.Children[0].ColRefToPos.pos(bind)
			if !has {
				if len(root.Children) > 1 {
					has, childPos = root.Children[1].ColRefToPos.pos(bind)
					if !has {
						panic(fmt.Sprintf("no such %v in children", bind))
					}
					st = RightChild
				} else {
					panic(fmt.Sprintf("no such %v in children", bind))
				}
			}

			childExpr := root.Children[0].Outputs[childPos]
			root.Outputs = append(root.Outputs, &Expr{
				Typ:      ET_Column,
				DataTyp:  childExpr.DataTyp,
				Database: childExpr.Database,
				Table:    childExpr.Table,
				Name:     childExpr.Name,
				ColRef:   ColumnBind{uint64(st), uint64(childPos)},
			})
		}
	default:
		panic(fmt.Sprintf("usp op type %v", root.Typ))
	}
	checkColRefPosInNode(root)
	return root, nil
}
