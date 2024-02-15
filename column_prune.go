package main

import "fmt"

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
	root, err = cp.prune(root)
	if err != nil {
		return nil, err
	}
	if cp.colRefs.count() != 0 {
		//panic("")
	}
	return root, err
}

func (cp *ColumnPrune) prune(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	switch root.Typ {
	case LOT_Limit:
		for i, child := range root.Children {
			root.Children[i], err = cp.prune(child)
			if err != nil {
				return nil, err
			}
		}
		return root, err
	case LOT_Order:
		cp.colRefs.addExpr(root.OrderBys...)
		for i, child := range root.Children {
			root.Children[i], err = cp.prune(child)
			if err != nil {
				return nil, err
			}
		}
		return root, err
	case LOT_Project:
		cp.colRefs.addExpr(root.Projects...)
		for i, child := range root.Children {
			root.Children[i], err = cp.prune(child)
			if err != nil {
				return nil, err
			}
		}
		return root, err
	case LOT_AggGroup:
		cp.colRefs.addExpr(root.GroupBys...)
		cp.colRefs.addExpr(root.Aggs...)
		cp.colRefs.addExpr(root.Filters...)
		for i, child := range root.Children {
			root.Children[i], err = cp.prune(child)
			if err != nil {
				return nil, err
			}
		}
		return root, err
	case LOT_JOIN:
		if root.JoinTyp != ET_JoinTypeInner {
			panic(fmt.Sprintf("usp join type %v", root.JoinTyp))
		}
		cp.colRefs.addExpr(root.OnConds...)
		for i, child := range root.Children {
			root.Children[i], err = cp.prune(child)
			if err != nil {
				return nil, err
			}
		}
		return root, err
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
	default:
		panic(fmt.Sprintf("usp op type %v", root.Typ))
	}
	return nil, nil
}
