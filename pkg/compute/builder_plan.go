package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
)

func (b *Builder) CreatePlan(ctx *BindContext, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	root, err = b.createFrom(b.fromExpr, root)
	if err != nil {
		return nil, err
	}

	//where
	if b.whereExpr != nil {
		root, err = b.createWhere(b.whereExpr, root)
	}

	//aggregates or group by
	if len(b.aggs) > 0 || len(b.groupbyExprs) > 0 {
		root, err = b.createAggGroup(root)
	}

	//having
	if b.havingExpr != nil {
		root, err = b.createWhere(b.havingExpr, root)
	}

	//projects
	if len(b.projectExprs) > 0 {
		root, err = b.createProject(root)
	}

	//order bys
	if len(b.orderbyExprs) > 0 {
		root, err = b.createOrderby(root)
	}

	//limit
	if b.limitCount != nil {
		root = &LogicalOperator{
			Typ:      LOT_Limit,
			Limit:    b.limitCount,
			Offset:   b.limitOffset,
			Children: []*LogicalOperator{root},
		}
	}

	return root, err
}

func (b *Builder) createFrom(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var left, right *LogicalOperator
	switch expr.Typ {
	case ET_TABLE:
		{
			tabEnt := storage.GCatalog.GetEntry(b.txn, storage.CatalogTypeTable, expr.Database, expr.Table)
			if tabEnt == nil {
				return nil, fmt.Errorf("no table %s in schema %s", expr.Database, expr.Table)
			}
			stats := convertStats(tabEnt.GetStats())
			return &LogicalOperator{
				Typ:       LOT_Scan,
				Index:     expr.Index,
				Database:  expr.Database,
				Table:     expr.Table,
				Alias:     expr.Alias,
				BelongCtx: expr.BelongCtx,
				Stats:     stats,
				TableEnt:  tabEnt,
			}, err
		}
	case ET_Join:
		left, err = b.createFrom(expr.Children[0], root)
		if err != nil {
			return nil, err
		}
		right, err = b.createFrom(expr.Children[1], root)
		if err != nil {
			return nil, err
		}
		jt := LOT_JoinTypeCross
		switch expr.JoinTyp {
		case ET_JoinTypeCross, ET_JoinTypeInner:
			jt = LOT_JoinTypeInner
		case ET_JoinTypeLeft:
			jt = LOT_JoinTypeLeft
		default:
			panic(fmt.Sprintf("usp join type %d", jt))
		}

		onExpr := expr.On.copy()
		onExpr = distributeExpr(onExpr)

		return &LogicalOperator{
			Typ:      LOT_JOIN,
			Index:    uint64(b.GetTag()),
			JoinTyp:  jt,
			OnConds:  []*Expr{onExpr.copy()},
			Children: []*LogicalOperator{left, right},
		}, err
	case ET_Subquery:
		_, root, err = b.createSubquery(expr, root)
		if err != nil {
			return nil, err
		}
		return root, err
	case ET_ValuesList:
		//is values list
		return &LogicalOperator{
			Typ:         LOT_Scan,
			Index:       expr.Index,
			Database:    expr.Database,
			Table:       expr.Table,
			Alias:       expr.Alias,
			BelongCtx:   expr.BelongCtx,
			Stats:       &Stats{},
			TableIndex:  int(expr.Index),
			ScanTyp:     ScanTypeValuesList,
			Types:       expr.Types,
			Names:       expr.Names,
			Values:      expr.Values,
			ColName2Idx: expr.ColName2Idx,
		}, err
	default:
		panic("usp")
	}
}

func (b *Builder) createWhere(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var newFilter *Expr

	//TODO:
	//1. find subquery and flatten subquery
	//1. all operators should be changed into (low priority)
	filters := splitExprByAnd(expr)
	copyExprs(filters...)
	newFilters := make([]*Expr, 0)
	for _, filter := range filters {
		newFilter, root, err = b.createSubquery(filter, root)
		if err != nil {
			return nil, err
		}
		newFilters = append(newFilters, newFilter)
	}

	newFilters = distributeExprs(newFilters...)

	return &LogicalOperator{
		Typ:      LOT_Filter,
		Filters:  copyExprs(newFilters...),
		Children: []*LogicalOperator{root},
	}, err
}

// if the expr find subquery, it flattens the subquery and replaces
// the expr.
func (b *Builder) createSubquery(expr *Expr, root *LogicalOperator) (*Expr, *LogicalOperator, error) {
	var err error
	var subRoot *LogicalOperator
	switch expr.Typ {
	case ET_Subquery:
		subBuilder := expr.SubBuilder
		subCtx := expr.SubCtx
		subRoot, err = subBuilder.CreatePlan(subCtx, nil)
		if err != nil {
			return nil, nil, err
		}
		//flatten subquery
		return b.apply(expr, root, subRoot)

	case ET_Func:
		if expr.FunImpl.IsFunction() {
			var childExpr *Expr
			args := make([]*Expr, 0)
			for _, child := range expr.Children {
				childExpr, root, err = b.createSubquery(child, root)
				if err != nil {
					return nil, nil, err
				}
				args = append(args, childExpr)
			}
			return &Expr{
				Typ:        expr.Typ,
				ConstValue: NewStringConst(expr.ConstValue.String),
				DataTyp:    expr.DataTyp,
				Children:   args,
				FunctionInfo: FunctionInfo{
					FunImpl: expr.FunImpl,
				},
				BaseInfo: BaseInfo{
					Alias: expr.Alias,
				},
			}, root, nil
		} else {
			switch expr.FunImpl._name {
			default:
				//binary operator
				var childExpr *Expr
				args := make([]*Expr, 0)
				for _, child := range expr.Children {
					childExpr, root, err = b.createSubquery(child, root)
					if err != nil {
						return nil, nil, err
					}
					args = append(args, childExpr)
				}
				return &Expr{
					Typ:        expr.Typ,
					ConstValue: NewStringConst(expr.FunImpl._name),
					DataTyp:    expr.DataTyp,
					Children:   args,
					FunctionInfo: FunctionInfo{
						FunImpl: expr.FunImpl,
					},
					BaseInfo: BaseInfo{
						Alias: expr.Alias,
					},
				}, root, nil
			case FuncIn:
				var childExpr *Expr
				args := make([]*Expr, 0)
				for _, child := range expr.Children {
					childExpr, root, err = b.createSubquery(child, root)
					if err != nil {
						return nil, nil, err
					}
					args = append(args, childExpr)
				}

				//FIXME:
				//add join conds to 'A in subquery'
				if root.Typ == LOT_JOIN &&
					root.JoinTyp == LOT_JoinTypeSEMI &&
					len(root.OnConds) == 0 {
					fbinder := FunctionBinder{}
					e1 := fbinder.BindScalarFunc(
						FuncEqual,
						copyExprs(args...),
						IsOperator(FuncEqual))
					root.OnConds = append(root.OnConds, e1)

					bExpr := &Expr{
						Typ:        ET_Const,
						DataTyp:    common.BooleanType(),
						ConstValue: NewBooleanConst(true),
					}

					retExpr := fbinder.BindScalarFunc(FuncEqual,
						[]*Expr{
							bExpr,
							copyExpr(bExpr),
						},
						IsOperator(FuncEqual),
					)

					return retExpr, root, nil
				}
				return &Expr{
					Typ:      expr.Typ,
					DataTyp:  expr.DataTyp,
					Children: args,
					FunctionInfo: FunctionInfo{
						FunImpl: expr.FunImpl,
					},
					BaseInfo: BaseInfo{
						Alias: expr.Alias,
					},
				}, root, nil
			case FuncNotIn:
				var childExpr *Expr
				args := make([]*Expr, 0)
				for _, child := range expr.Children {
					childExpr, root, err = b.createSubquery(child, root)
					if err != nil {
						return nil, nil, err
					}
					args = append(args, childExpr)
				}

				//FIXME:
				//convert semi to anti
				if root.Typ == LOT_JOIN &&
					root.JoinTyp == LOT_JoinTypeSEMI {
					root.JoinTyp = LOT_JoinTypeANTI
				}

				//FIXME:
				//add join conds to 'A not in subquery'
				if root.Typ == LOT_JOIN &&
					root.JoinTyp == LOT_JoinTypeANTI &&
					len(root.OnConds) == 0 {
					fbinder := FunctionBinder{}
					e1 := fbinder.BindScalarFunc(
						FuncEqual,
						copyExprs(args...),
						IsOperator(FuncEqual))
					root.OnConds = append(root.OnConds, e1)

					bExpr := &Expr{
						Typ:        ET_Const,
						DataTyp:    common.BooleanType(),
						ConstValue: NewBooleanConst(true),
					}

					retExpr := fbinder.BindScalarFunc(
						FuncEqual,
						[]*Expr{
							bExpr,
							copyExpr(bExpr),
						},
						IsOperator(FuncEqual))

					return retExpr, root, nil
				}

				return &Expr{
					Typ:      expr.Typ,
					DataTyp:  expr.DataTyp,
					Children: args,
					FunctionInfo: FunctionInfo{
						FunImpl: expr.FunImpl,
					},
					BaseInfo: BaseInfo{
						Alias: expr.Alias,
					},
				}, root, nil
			case FuncExists, FuncNotExists:
				var childExpr *Expr
				childExpr, root, err = b.createSubquery(expr.Children[0], root)
				if err != nil {
					return nil, nil, err
				}

				return childExpr, root, nil
			}
		}
	case ET_Column:
		return expr, root, nil
	case ET_Const:
		return expr, root, nil
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
}

// apply flattens subquery
// Based On Paper: Orthogonal Optimization of Subqueries and Aggregation
// make APPLY(expr,root,subRoot) algorithm
// expr: subquery expr
// root: root of the query that subquery belongs to
// subquery: root of the subquery
func (b *Builder) apply(expr *Expr, root, subRoot *LogicalOperator) (*Expr, *LogicalOperator, error) {
	if expr.Typ != ET_Subquery {
		panic("must be subquery")
	}

	corrExprs := collectCorrFilter(subRoot)

	if len(corrExprs) > 0 {
		var err error
		var newSub *LogicalOperator
		var corrFilters []*Expr

		//correlated subquery
		newSub, corrFilters, err = b.removeCorrFilters(subRoot)
		if err != nil {
			return nil, nil, err
		}

		//remove cor column
		nonCorrExprs, newCorrExprs := removeCorrExprs(corrFilters)

		makeMarkCondFunc := func(idx uint64, exists bool) *Expr {
			mCond := &Expr{
				Typ:     ET_Column,
				DataTyp: nonCorrExprs[0].DataTyp,
				BaseInfo: BaseInfo{
					ColRef: ColumnBind{idx, uint64(0)},
				},
			}

			left := mCond
			right := &Expr{
				Typ:        ET_Const,
				DataTyp:    common.BooleanType(),
				ConstValue: NewBooleanConst(exists),
			}
			fbinder := FunctionBinder{}
			return fbinder.BindScalarFunc(
				FuncEqual,
				[]*Expr{left, right},
				IsOperator(FuncEqual))
		}

		switch expr.SubqueryTyp {
		case ET_SubqueryTypeScalar:
			newSub = &LogicalOperator{
				Typ:     LOT_JOIN,
				Index:   uint64(b.GetTag()),
				JoinTyp: LOT_JoinTypeInner,
				OnConds: nonCorrExprs,
				Children: []*LogicalOperator{
					root, newSub,
				},
			}
		case ET_SubqueryTypeExists:
			newSub = &LogicalOperator{
				Typ:     LOT_JOIN,
				Index:   uint64(b.GetTag()),
				JoinTyp: LOT_JoinTypeMARK,
				OnConds: nonCorrExprs,
				Children: []*LogicalOperator{
					root, newSub,
				},
			}
		case ET_SubqueryTypeNotExists:
			newSub = &LogicalOperator{
				Typ:     LOT_JOIN,
				Index:   uint64(b.GetTag()),
				JoinTyp: LOT_JoinTypeAntiMARK,
				OnConds: nonCorrExprs,
				Children: []*LogicalOperator{
					root, newSub,
				},
			}
		default:
			panic("usp")
		}

		rootIndex := newSub.Index

		if len(newCorrExprs) > 0 {
			newSub = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(newCorrExprs...),
				Children: []*LogicalOperator{newSub},
			}
		}

		switch expr.SubqueryTyp {
		case ET_SubqueryTypeScalar:
			//TODO: may have multi columns
			subBuilder := expr.SubBuilder
			proj0 := subBuilder.projectExprs[0]
			colRef := &Expr{
				Typ:     ET_Column,
				DataTyp: proj0.DataTyp,
				BaseInfo: BaseInfo{
					Table: proj0.Table,
					Name:  proj0.Name,
					ColRef: ColumnBind{
						uint64(subBuilder.projectTag),
						0,
					},
				},
			}
			return colRef, newSub, nil
		case ET_SubqueryTypeExists:
			colRef := makeMarkCondFunc(rootIndex, true)
			return colRef, newSub, nil
		case ET_SubqueryTypeNotExists:
			colRef := makeMarkCondFunc(rootIndex, false)
			return colRef, newSub, nil
		default:
			panic("usp")
		}

	} else {
		var colRef *Expr
		var newRoot *LogicalOperator
		if root == nil {
			newRoot = subRoot
		} else {
			switch expr.SubqueryTyp {
			case ET_SubqueryTypeScalar:
				newRoot = &LogicalOperator{
					Typ:     LOT_JOIN,
					Index:   uint64(b.GetTag()),
					JoinTyp: LOT_JoinTypeCross,
					OnConds: nil,
					Children: []*LogicalOperator{
						root, subRoot,
					},
				}
			case ET_SubqueryTypeIn:
				newRoot = &LogicalOperator{
					Typ:     LOT_JOIN,
					Index:   uint64(b.GetTag()),
					JoinTyp: LOT_JoinTypeSEMI,
					OnConds: nil,
					Children: []*LogicalOperator{
						root, subRoot,
					},
				}
			case ET_SubqueryTypeNotIn:
				newRoot = &LogicalOperator{
					Typ:     LOT_JOIN,
					Index:   uint64(b.GetTag()),
					JoinTyp: LOT_JoinTypeANTI,
					OnConds: nil,
					Children: []*LogicalOperator{
						root, subRoot,
					},
				}
			default:
				panic("usp")
			}
		}
		// TODO: may have multi columns
		subBuilder := expr.SubBuilder
		proj0 := subBuilder.projectExprs[0]
		colRef = &Expr{
			Typ:     ET_Column,
			DataTyp: proj0.DataTyp,
			BaseInfo: BaseInfo{
				Table: proj0.Table,
				Name:  proj0.Name,
				ColRef: ColumnBind{
					uint64(subBuilder.projectTag),
					0,
				},
			},
		}
		return colRef, newRoot, nil
	}
}

func (b *Builder) removeCorrFilters(subRoot *LogicalOperator) (*LogicalOperator, []*Expr, error) {
	var childFilters []*Expr
	var err error
	corrFilters := make([]*Expr, 0)
	for i, child := range subRoot.Children {
		subRoot.Children[i], childFilters, err = b.removeCorrFilters(child)
		if err != nil {
			return nil, nil, err
		}

		corrFilters = append(corrFilters, childFilters...)
	}

	switch subRoot.Typ {
	case LOT_Project:
		b.removeCorrFiltersInProject(subRoot, corrFilters)
	case LOT_AggGroup:
		b.removeCorrFiltersInAggr(subRoot, corrFilters)
	case LOT_Filter:
		leftFilters := make([]*Expr, 0)
		for _, filter := range subRoot.Filters {
			if hasCorrCol(filter) {
				corrFilters = append(corrFilters, filter)
			} else {
				leftFilters = append(leftFilters, filter)
			}
		}
		if len(leftFilters) == 0 {
			subRoot = subRoot.Children[0]
		} else {
			subRoot.Filters = leftFilters
		}
	}
	return subRoot, corrFilters, nil
}

func (b *Builder) removeCorrFiltersInProject(subRoot *LogicalOperator, filters []*Expr) {
	for i, filter := range filters {
		filters[i] = b.replaceCorrFiltersInProject(subRoot, filter)
	}
}

func (b *Builder) replaceCorrFiltersInProject(subRoot *LogicalOperator, filter *Expr) *Expr {
	if !hasCorrCol(filter) {
		idx := len(subRoot.Projects)
		subRoot.Projects = append(subRoot.Projects, filter)
		return &Expr{
			Typ:     ET_Column,
			DataTyp: filter.DataTyp,
			BaseInfo: BaseInfo{
				ColRef: ColumnBind{subRoot.Index, uint64(idx)},
			},
		}
	}

	for i, child := range filter.Children {
		filter.Children[i] = b.replaceCorrFiltersInProject(subRoot, child)
	}

	return filter
}

func (b *Builder) removeCorrFiltersInAggr(subRoot *LogicalOperator, filters []*Expr) {
	for i, filter := range filters {
		filters[i] = b.replaceCorrFiltersInAggr(subRoot, filter)
	}
}

func (b *Builder) replaceCorrFiltersInAggr(subRoot *LogicalOperator, filter *Expr) *Expr {
	if !hasCorrCol(filter) {
		idx := len(subRoot.GroupBys)
		subRoot.GroupBys = append(subRoot.GroupBys, filter)
		return &Expr{
			Typ:     ET_Column,
			DataTyp: filter.DataTyp,
			BaseInfo: BaseInfo{
				ColRef: ColumnBind{subRoot.Index, uint64(idx)},
			},
		}
	}

	for i, child := range filter.Children {
		filter.Children[i] = b.replaceCorrFiltersInAggr(subRoot, child)
	}
	return filter
}

func (b *Builder) createAggGroup(root *LogicalOperator) (*LogicalOperator, error) {
	return &LogicalOperator{
		Typ:      LOT_AggGroup,
		Index:    uint64(b.groupTag),
		Index2:   uint64(b.aggTag),
		Aggs:     b.aggs,
		GroupBys: b.groupbyExprs,
		Children: []*LogicalOperator{root},
	}, nil
}

func (b *Builder) createProject(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var newExpr *Expr
	projects := make([]*Expr, 0)
	for _, expr := range b.projectExprs {
		newExpr, root, err = b.createSubquery(expr, root)
		if err != nil {
			return nil, err
		}
		projects = append(projects, newExpr)
	}
	return &LogicalOperator{
		Typ:      LOT_Project,
		Index:    uint64(b.projectTag),
		Projects: projects,
		Children: []*LogicalOperator{root},
	}, nil
}

func (b *Builder) createOrderby(root *LogicalOperator) (*LogicalOperator, error) {
	return &LogicalOperator{
		Typ:      LOT_Order,
		OrderBys: b.orderbyExprs,
		Children: []*LogicalOperator{root},
	}, nil
}

// collectCorrFilter collects all exprs that find correlated column.
// and does not remove these exprs.
func collectCorrFilter(root *LogicalOperator) []*Expr {
	var ret, childRet []*Expr
	for _, child := range root.Children {
		childRet = collectCorrFilter(child)
		ret = append(ret, childRet...)
	}

	switch root.Typ {
	case LOT_Filter:
		for _, filter := range root.Filters {
			if hasCorrCol(filter) {
				ret = append(ret, filter)
			}
		}
	default:
	}
	return ret
}

func hasCorrCol(expr *Expr) bool {
	switch expr.Typ {
	case ET_Column:
		return expr.Depth > 0
	case ET_Func:
		ret := false
		for _, child := range expr.Children {
			ret = ret || hasCorrCol(child)
		}
		return ret
	case ET_Const:
		return false
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
}

// ==============
// Optimize plan
// ==============
func (b *Builder) Optimize(ctx *BindContext, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var left []*Expr

	//fmt.Println("before optimize", root.String())

	//1. pushdown filter
	root, left, err = b.pushdownFilters(root, nil)
	if err != nil {
		return nil, err
	}
	if len(left) > 0 {
		root = &LogicalOperator{
			Typ:      LOT_Filter,
			Filters:  copyExprs(left...),
			Children: []*LogicalOperator{root},
		}
	}

	//fmt.Println("after pushdown filters", root.String())

	//2. join order
	root, err = b.joinOrder(root)
	if err != nil {
		return nil, err
	}

	//fmt.Println("after join order", root.String())

	//3. pushdown filter again
	root, left, err = b.pushdownFilters(root, nil)
	if err != nil {
		return nil, err
	}
	if len(left) > 0 {
		root = &LogicalOperator{
			Typ:      LOT_Filter,
			Filters:  copyExprs(left...),
			Children: []*LogicalOperator{root},
		}
	}

	//4. column prune
	root, err = b.columnPrune(root)
	if err != nil {
		return nil, err
	}

	//fmt.Println("after column prune", root.String())

	root, err = b.generateCounts(root)
	if err != nil {
		return nil, err
	}

	//fmt.Println("after generate counts", root.String())

	root, err = b.generateOutputs(root)
	if err != nil {
		return nil, err
	}
	return root, nil
}

// pushdownFilters pushes down filters to the lowest possible position.
// It returns the new root and the filters that cannot be pushed down.
func (b *Builder) pushdownFilters(root *LogicalOperator, filters []*Expr) (*LogicalOperator, []*Expr, error) {
	var err error
	var left, childLeft []*Expr
	var childRoot *LogicalOperator
	var needs []*Expr

	switch root.Typ {
	case LOT_Scan:
		for _, f := range filters {
			if onlyReferTo(f, root.Index) {
				//expr that only refer to the scan expr can be pushdown.
				root.Filters = append(root.Filters, f)
			} else {
				left = append(left, f)
			}
		}
	case LOT_JOIN:
		needs = filters
		leftTags := make(map[uint64]bool)
		rightTags := make(map[uint64]bool)
		collectTags(root.Children[0], leftTags)
		collectTags(root.Children[1], rightTags)

		root.OnConds = splitExprsByAnd(root.OnConds)
		if root.JoinTyp == LOT_JoinTypeInner || root.JoinTyp == LOT_JoinTypeLeft {
			for _, on := range root.OnConds {
				needs = append(needs, splitExprByAnd(on)...)
			}
			root.OnConds = nil
		}

		whichSides := make([]int, len(needs))
		for i, nd := range needs {
			whichSides[i] = decideSide(nd, leftTags, rightTags)
		}

		leftNeeds := make([]*Expr, 0)
		rightNeeds := make([]*Expr, 0)
		for i, nd := range needs {
			switch whichSides[i] {
			case NoneSide:
				switch root.JoinTyp {
				case LOT_JoinTypeInner:
					leftNeeds = append(leftNeeds, copyExpr(nd))
					rightNeeds = append(rightNeeds, nd)
				case LOT_JoinTypeLeft:
					leftNeeds = append(leftNeeds, nd)
				default:
					left = append(left, nd)
				}
			case LeftSide:
				leftNeeds = append(leftNeeds, nd)
			case RightSide:
				rightNeeds = append(rightNeeds, nd)
			case BothSide:
				if root.JoinTyp == LOT_JoinTypeInner || root.JoinTyp == LOT_JoinTypeLeft {
					//only equal or in can be used in On conds
					if nd.FunImpl._name == FuncEqual || nd.FunImpl._name == FuncIn {
						root.OnConds = append(root.OnConds, nd)
						break
					}
				}
				left = append(left, nd)
			default:
				panic(fmt.Sprintf("usp side %d", whichSides[i]))
			}
		}

		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], leftNeeds)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[0] = childRoot

		childRoot, childLeft, err = b.pushdownFilters(root.Children[1], rightNeeds)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[1] = childRoot

	case LOT_AggGroup:
		for _, f := range filters {
			if referTo(f, root.Index2) {
				//expr that refer to the agg exprs can not be pushdown.
				root.Filters = append(root.Filters, f)
			} else {
				//restore the real expr for the expr that refer to the expr in the group by.
				needs = append(needs, restoreExpr(f, root.Index, root.GroupBys))
			}
		}

		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[0] = childRoot
	case LOT_Project:
		//restore the real expr for the expr that refer to the expr in the project list.
		for _, f := range filters {
			needs = append(needs, restoreExpr(f, root.Index, root.Projects))
		}

		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[0] = childRoot
	case LOT_Filter:
		needs = filters
		for _, e := range root.Filters {
			needs = append(needs, splitExprByAnd(e)...)
		}
		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			root.Children[0] = childRoot
			root.Filters = childLeft
		} else {
			//remove this FILTER node
			root = childRoot
		}

	default:
		if root.Typ == LOT_Limit {
			//can not pushdown filter through LIMIT
			left, filters = filters, nil
		}
		if len(root.Children) > 0 {
			if len(root.Children) > 1 {
				panic("must be on child: " + root.Typ.String())
			}
			childRoot, childLeft, err = b.pushdownFilters(root.Children[0], filters)
			if err != nil {
				return nil, nil, err
			}
			if len(childLeft) > 0 {
				childRoot = &LogicalOperator{
					Typ:      LOT_Filter,
					Filters:  copyExprs(childLeft...),
					Children: []*LogicalOperator{childRoot},
				}
			}
			root.Children[0] = childRoot
		} else {
			left = filters
		}
	}

	return root, left, err
}
