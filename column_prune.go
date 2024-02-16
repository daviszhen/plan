package main

import "fmt"

type ColumnBindCountMap map[ColumnBind]int

func (ccount ColumnBindCountMap) addColumnBind(bind ColumnBind) {
	if _, ok := ccount[bind]; !ok {
		ccount[bind] = 0
	}
	ccount[bind]++
}

func (ccount ColumnBindCountMap) removeColumnBind(bind ColumnBind) {
	if _, ok := ccount[bind]; ok {
		old := ccount[bind]
		if old < 1 {
			panic("negative bind count")
		}
		ccount[bind] = old - 1
	} else {
		panic("no bind")
	}
}

func (ccount ColumnBindCountMap) refCount(bind ColumnBind) int {
	if count, ok := ccount[bind]; ok {
		return count
	}
	return 0
}

func (ccount ColumnBindCountMap) count() int {
	return len(ccount)
}

type ReferredColumnBindMap map[ColumnBind][]*Expr

func (ref ReferredColumnBindMap) addExpr(exprs ...*Expr) {
	for _, expr := range exprs {
		set := make(ColumnBindSet)
		collectColRefs(expr, set)
		for bind, _ := range set {
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
	colRefs   ReferredColumnBindMap
	colCounts ColumnBindCountMap
}

func NewColumnPrune() *ColumnPrune {
	return &ColumnPrune{
		colRefs:   make(ReferredColumnBindMap),
		colCounts: make(ColumnBindCountMap),
	}
}

func (b *Builder) columnPrune(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	cp := NewColumnPrune()
	cp.addRefCountOnFirstProject(root)
	root, err = cp.prune(root)
	if err != nil {
		return nil, err
	}
	if cp.colRefs.count() != 0 {
		//panic("")
	}
	return root, err
}

func (cp *ColumnPrune) addRefCountOnFirstProject(root *LogicalOperator) {
	for root != nil && len(root.Children) == 1 {
		if root.Typ == LOT_Project {
			break
		}
		root = root.Children[0]
	}
	if root != nil && root.Typ == LOT_Project {
		for i := 0; i < len(root.Projects); i++ {
			bind := ColumnBind{root.Index, uint64(i)}
			cp.colRefs.addExpr(&Expr{Typ: ET_Column, ColRef: bind})
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
