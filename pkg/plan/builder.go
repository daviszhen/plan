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

package plan

import (
	"errors"
	"fmt"
	"strings"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

//////////////////////////////////////////////
// create plan
//////////////////////////////////////////////

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
		switch expr.SubTyp {
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
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.SubTyp.String(),
				DataTyp:  expr.DataTyp,
				Alias:    expr.Alias,
				Children: args,
				FunImpl:  expr.FunImpl,
			}, root, nil
		case ET_In:
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
					ET_Equal.String(),
					copyExprs(args...),
					ET_Equal,
					ET_Equal.isOperator())
				root.OnConds = append(root.OnConds, e1)

				bExpr := &Expr{
					Typ:     ET_BConst,
					DataTyp: common.BooleanType(),
					Bvalue:  true,
				}

				retExpr := fbinder.BindScalarFunc(ET_Equal.String(),
					[]*Expr{
						bExpr,
						copyExpr(bExpr),
					},
					ET_Equal,
					ET_Equal.isOperator(),
				)

				return retExpr, root, nil
			}
			return &Expr{
				Typ:     expr.Typ,
				SubTyp:  expr.SubTyp,
				DataTyp: expr.DataTyp,
				Alias:   expr.Alias,

				Children: args,
				FunImpl:  expr.FunImpl,
			}, root, nil
		case ET_NotIn:
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
					ET_Equal.String(),
					copyExprs(args...),
					ET_Equal,
					ET_Equal.isOperator())
				root.OnConds = append(root.OnConds, e1)

				bExpr := &Expr{
					Typ:     ET_BConst,
					DataTyp: common.BooleanType(),
					Bvalue:  true,
				}

				retExpr := fbinder.BindScalarFunc(
					ET_Equal.String(),
					[]*Expr{
						bExpr,
						copyExpr(bExpr),
					},
					ET_Equal, ET_Equal.isOperator())

				return retExpr, root, nil
			}

			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				DataTyp:  expr.DataTyp,
				Alias:    expr.Alias,
				Children: args,
				FunImpl:  expr.FunImpl,
			}, root, nil
		case ET_Exists, ET_NotExists:
			var childExpr *Expr
			childExpr, root, err = b.createSubquery(expr.Children[0], root)
			if err != nil {
				return nil, nil, err
			}

			return childExpr, root, nil

		case ET_SubFunc:
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
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.Svalue,
				DataTyp:  expr.DataTyp,
				Alias:    expr.Alias,
				Children: args,
				FunImpl:  expr.FunImpl,
			}, root, nil
		}
	case ET_Column:
		return expr, root, nil
	case ET_IConst, ET_SConst, ET_DateConst, ET_IntervalConst, ET_FConst, ET_DecConst, ET_NConst:
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
				ColRef:  ColumnBind{idx, uint64(0)},
			}

			left := mCond
			right := &Expr{
				Typ:     ET_BConst,
				DataTyp: common.BooleanType(),
				Bvalue:  exists,
			}
			fbinder := FunctionBinder{}
			return fbinder.BindScalarFunc(
				ET_Equal.String(),
				[]*Expr{left, right},
				ET_Equal,
				ET_Equal.isOperator())
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
				Table:   proj0.Table,
				Name:    proj0.Name,
				ColRef: ColumnBind{
					uint64(subBuilder.projectTag),
					0,
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
			Table:   proj0.Table,
			Name:    proj0.Name,
			ColRef: ColumnBind{
				uint64(subBuilder.projectTag),
				0,
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
			ColRef:  ColumnBind{subRoot.Index, uint64(idx)},
			DataTyp: filter.DataTyp,
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
			ColRef:  ColumnBind{subRoot.Index, uint64(idx)},
			DataTyp: filter.DataTyp,
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
	case ET_IConst, ET_SConst, ET_DateConst, ET_IntervalConst, ET_FConst, ET_BConst, ET_NConst, ET_DecConst:
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
					if nd.SubTyp == ET_Equal || nd.SubTyp == ET_In {
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

func (b *Builder) bindCaseWhen(ctx *BindContext, iwc InWhichClause, expr *pg_query.CaseWhen, depth int) (*Expr, error) {
	var err error
	var kase, when *Expr
	kase, err = b.bindExpr(ctx, iwc, expr.Expr, depth)
	if err != nil {
		return nil, err
	}
	when, err = b.bindExpr(ctx, iwc, expr.Result, depth)
	if err != nil {
		return nil, err
	}
	ret := &Expr{
		Typ:      ET_Func,
		SubTyp:   ET_CaseWhen,
		Children: []*Expr{kase, when},
	}
	return ret, nil
}

func (b *Builder) bindCaseExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.CaseExpr, depth int) (*Expr, error) {
	var err error
	var els *Expr
	var temp *Expr
	when := make([]*Expr, len(expr.Args)*2)
	var astWhen []*pg_query.Node
	if expr.Arg != nil {
		//rewrite it to CASE WHEN kase = compare value ...

		panic("usp")

		//astWhen = make([]*Ast, len(expr.Expr.When))
		//for i := 0; i < len(expr.Expr.When); i += 2 {
		//	astWhen[i] = equal(expr.Expr.Kase, expr.Expr.When[i])
		//	astWhen[i+1] = expr.Expr.When[i+1]
		//}

	} else {
		astWhen = expr.Args
	}

	for i := 0; i < len(astWhen); i++ {
		temp, err = b.bindExpr(ctx, iwc, astWhen[i], depth)
		if err != nil {
			return nil, err
		}
		when[i*2] = temp.Children[0]
		when[i*2+1] = temp.Children[1]
	}

	if expr.Defresult != nil {
		els, err = b.bindExpr(ctx, iwc, expr.Defresult, depth)
		if err != nil {
			return nil, err
		}
	} else {
		els = &Expr{
			Typ:     ET_NConst,
			DataTyp: common.Null(),
		}
	}

	retTyp := els.DataTyp

	//decide result types
	//max type of the THEN expr
	for i := 0; i < len(when); i += 2 {
		retTyp = common.MaxLType(retTyp, when[i+1].DataTyp)
	}

	//case THEN to
	for i := 0; i < len(when); i += 2 {
		when[i+1], err = AddCastToType(when[i+1], retTyp, retTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}
	}

	//cast ELSE to
	els, err = AddCastToType(els, retTyp, retTyp.Id == common.LTID_ENUM)
	if err != nil {
		return nil, err
	}

	params := []*Expr{els}
	params = append(params, when...)

	decideDataType := func(e *Expr) common.LType {
		if e == nil {
			return common.Null()
		} else {
			return e.DataTyp
		}
	}

	paramsTypes := []common.LType{
		decideDataType(els),
	}

	for i := 0; i < len(when); i += 1 {
		paramsTypes = append(paramsTypes, when[i].DataTyp)
	}

	ret, err := b.bindFunc(ET_Case.String(), ET_Case, expr.String(), params, paramsTypes, false)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *Builder) bindInExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.A_Expr, depth int) (*Expr, error) {
	var err error
	var in *Expr
	var listExpr *Expr

	in, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)
	if err != nil {
		return nil, err
	}

	listExpr, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)
	if err != nil {
		return nil, err
	}

	util.AssertFunc(listExpr.Typ == ET_List)

	argsTypes := make([]common.LType, 0)
	children := listExpr.Children
	for _, child := range listExpr.Children {
		argsTypes = append(argsTypes, child.DataTyp)
	}

	maxType := in.DataTyp
	anyVarchar := in.DataTyp.Id == common.LTID_VARCHAR
	anyEnum := in.DataTyp.Id == common.LTID_ENUM
	for i := 0; i < len(argsTypes); i++ {
		maxType = common.MaxLType(maxType, argsTypes[i])
		if argsTypes[i].Id == common.LTID_VARCHAR {
			anyVarchar = true
		}
		if argsTypes[i].Id == common.LTID_ENUM {
			anyEnum = true
		}
	}
	if anyVarchar && anyEnum {
		maxType = common.VarcharType()
	}

	paramTypes := make([]common.LType, 0)
	params := make([]*Expr, 0)

	castIn, err := AddCastToType(in, maxType, false)
	if err != nil {
		return nil, err
	}
	params = append(params, castIn)
	paramTypes = append(paramTypes, castIn.DataTyp)
	for _, child := range children {
		castChild, err := AddCastToType(child, maxType, false)
		if err != nil {
			return nil, err
		}
		params = append(params, castChild)
		paramTypes = append(paramTypes, castChild.DataTyp)
	}

	var et ET_SubTyp
	switch expr.Name[0].GetString_().GetSval() {
	case "=":
		et = ET_In
	//case AstExprSubTypeNotIn:
	//	et = ET_NotIn
	default:
		panic("unhandled default case")
	}
	//convert into ... = ... or ... = ...
	orChildren := make([]*Expr, 0)
	for i, param := range params {
		if i == 0 {
			continue
		}
		equalParams := []*Expr{params[0], param}
		equalTypes := []common.LType{paramTypes[0], paramTypes[i]}
		ret0, err := b.bindFunc(et.String(), et, expr.String(), equalParams, equalTypes, false)
		if err != nil {
			return nil, err
		}
		orChildren = append(orChildren, ret0)
	}

	bigOrExpr := combineExprsByOr(orChildren...)
	return bigOrExpr, nil
}

func collectTags(root *LogicalOperator, set map[uint64]bool) {
	if root.Index != 0 {
		set[root.Index] = true
	}
	if root.Index2 != 0 {
		set[root.Index2] = true
	}
	for _, child := range root.Children {
		collectTags(child, set)
	}
}

const (
	NoneSide       = 0
	LeftSide       = 1 << 1
	RightSide      = 1 << 2
	BothSide       = LeftSide | RightSide
	CorrelatedSide = 1 << 3
)

//////////////////////////////////////////////
// create physical plan
//////////////////////////////////////////////

func (b *Builder) CreatePhyPlan(root *LogicalOperator) (*PhysicalOperator, error) {
	if root == nil {
		return nil, nil
	}
	var err error
	children := make([]*PhysicalOperator, 0)
	for _, child := range root.Children {
		childPlan, err := b.CreatePhyPlan(child)
		if err != nil {
			return nil, err
		}
		children = append(children, childPlan)
	}
	var proot *PhysicalOperator
	switch root.Typ {
	case LOT_Project:
		proot, err = b.createPhyProject(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Filter:
		proot, err = b.createPhyFilter(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Scan:
		proot, err = b.createPhyScan(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_JOIN:
		proot, err = b.createPhyJoin(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_AggGroup:
		proot, err = b.createPhyAgg(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Order:
		proot, err = b.createPhyOrder(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Limit:
		proot, err = b.createPhyLimit(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_CreateSchema:
		proot, err = b.createPhyCreateSchema(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_CreateTable:
		proot, err = b.createPhyCreateTable(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Insert:
		proot, err = b.createPhyInsert(root, children)
		if err != nil {
			return nil, err
		}
	default:
		panic("usp")
	}
	if proot != nil {
		proot.estimatedCard = root.estimatedCard
	}

	proot.Id = b.getPhyId()
	return proot, nil
}

func (b *Builder) createPhyProject(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Project,
		Index:    root.Index,
		Projects: root.Projects,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyFilter(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{Typ: POT_Filter, Filters: root.Filters, Outputs: root.Outputs, Children: children}, nil
}

func (b *Builder) createPhyScan(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	ret := &PhysicalOperator{
		Typ:         POT_Scan,
		Index:       root.Index,
		Database:    root.Database,
		Table:       root.Table,
		Alias:       root.Alias,
		Outputs:     root.Outputs,
		Columns:     root.Columns,
		Filters:     root.Filters,
		ScanTyp:     root.ScanTyp,
		Types:       root.Types,
		ColName2Idx: root.ColName2Idx,
		Children:    children}

	switch root.ScanTyp {
	case ScanTypeValuesList:
		{
			var valuesExec *ExprExec
			var err error

			collection := NewColumnDataCollection(root.Types)
			data := &chunk.Chunk{}
			data.Init(root.Types, storage.STANDARD_VECTOR_SIZE)

			tmp := &chunk.Chunk{}
			tmp.SetCard(1)

			for i := 0; i < len(root.Values); i++ {
				valuesExec = NewExprExec(root.Values[i]...)
				err = valuesExec.executeExprs(
					[]*chunk.Chunk{tmp, nil, nil},
					data)
				if err != nil {
					return nil, err
				}
				collection.Append(data)
				data.Reset()
			}
			ret.collection = collection
		}
	case ScanTypeTable:
	case ScanTypeCopyFrom:
		ret.ScanInfo = root.ScanInfo
	}

	return ret, nil
}

func (b *Builder) createPhyJoin(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Join,
		Index:    root.Index,
		JoinTyp:  root.JoinTyp,
		OnConds:  root.OnConds,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyOrder(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Order,
		OrderBys: root.OrderBys,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyAgg(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Agg,
		Index:    root.Index,
		Index2:   root.Index2,
		Filters:  root.Filters,
		Aggs:     root.Aggs,
		GroupBys: root.GroupBys,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyLimit(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Limit,
		Outputs:  root.Outputs,
		Limit:    root.Limit,
		Offset:   root.Offset,
		Children: children}, nil
}

func (b *Builder) createPhyCreateSchema(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:         POT_CreateSchema,
		Database:    root.Database,
		IfNotExists: root.IfNotExists,
		Children:    children}, nil
}

func (b *Builder) createPhyCreateTable(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:         POT_CreateTable,
		Database:    root.Database,
		Table:       root.Table,
		IfNotExists: root.IfNotExists,
		ColDefs:     root.ColDefs,
		Constraints: root.Constraints,
		Children:    children,
	}, nil
}

func (b *Builder) buildDDL(txn *storage.Txn, ddl *pg_query.RawStmt, ctx *BindContext, depth int) (*LogicalOperator, error) {
	switch impl := ddl.GetStmt().GetNode().(type) {
	case *pg_query.Node_CreateSchemaStmt:
		return b.buildCreateSchema(
			txn,
			impl.CreateSchemaStmt,
			ctx,
			depth)
	case *pg_query.Node_CreateStmt:
		return b.buildCreateTable(
			txn,
			impl.CreateStmt,
			ctx,
			depth)
	case *pg_query.Node_InsertStmt:
		return b.buildInsert(txn, impl.InsertStmt, ctx, depth)
	case *pg_query.Node_CopyStmt:
		return b.buildCopy(txn, impl.CopyStmt, ctx, depth)
	case *pg_query.Node_SelectStmt:
		err := b.buildSelect(impl.SelectStmt, b.rootCtx, 0)
		if err != nil {
			return nil, err
		}

		lp, err := b.CreatePlan(b.rootCtx, nil)
		if err != nil {
			return nil, err
		}
		if lp == nil {
			return nil, errors.New("nil plan")
		}
		checkExprIsValid(lp)
		lp, err = b.Optimize(b.rootCtx, lp)
		if err != nil {
			return nil, err
		}
		if lp == nil {
			return nil, errors.New("nil plan")
		}
		checkExprIsValid(lp)
		return lp, nil
	default:
		return nil, fmt.Errorf("unsupport statement right now")
	}
	return nil, nil
}

func (b *Builder) buildCreateSchema(
	txn *storage.Txn,
	stmt *pg_query.CreateSchemaStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	name := stmt.Schemaname
	return &LogicalOperator{
		Typ:         LOT_CreateSchema,
		Database:    name,
		IfNotExists: stmt.IfNotExists,
	}, nil
}

func (b *Builder) buildCreateTable(
	txn *storage.Txn,
	stmt *pg_query.CreateStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {

	schema := stmt.GetRelation().GetSchemaname()
	name := stmt.GetRelation().GetRelname()

	ret := &LogicalOperator{
		Typ:         LOT_CreateTable,
		Database:    schema,
		Table:       name,
		IfNotExists: stmt.GetIfNotExists(),
	}

	colDefs := make([]*storage.ColumnDefinition, 0)
	tableCons := make([]*storage.Constraint, 0)
	for colIdx, node := range stmt.GetTableElts() {
		switch nodeImpl := node.GetNode().(type) {
		case *pg_query.Node_ColumnDef:
			colDef := nodeImpl.ColumnDef
			colDefExpr := &storage.ColumnDefinition{}

			//column name
			colDefExpr.Name = colDef.Colname
			//column type
			typName := ""
			switch len(colDef.TypeName.Names) {
			case 2:
				typName = colDef.TypeName.Names[1].GetString_().GetSval()
			case 1:
				typName = colDef.TypeName.Names[0].GetString_().GetSval()
			default:
				panic("usp")
			}
			switch strings.ToLower(typName) {
			case "int4":
				colDefExpr.Type = common.IntegerType()
			case "int8":
				colDefExpr.Type = common.BigintType()
			case "varchar":
				colDefExpr.Type = common.VarcharType()
			case "numeric":
				typMods := colDef.TypeName.GetTypmods()
				width := typMods[0].GetAConst().GetIval().GetIval()
				pres := typMods[1].GetAConst().GetIval().GetIval()
				colDefExpr.Type = common.DecimalType(int(width), int(pres))
			case "date":
				colDefExpr.Type = common.DateType()
			default:
				panic("")
			}

			//column constraint
			colCons := make([]*storage.Constraint, 0)
			for _, cons := range colDef.Constraints {
				consImpl := cons.GetConstraint()
				if consImpl != nil {
					switch consImpl.Contype {
					case pg_query.ConstrType_CONSTR_NOTNULL:
						colCons = append(colCons, storage.NewNotNullConstraint(colIdx))
					case pg_query.ConstrType_CONSTR_UNIQUE:
						colCons = append(colCons, storage.NewUniqueIndexConstraint(colIdx, false))
					case pg_query.ConstrType_CONSTR_PRIMARY:
						colCons = append(colCons, storage.NewUniqueIndexConstraint(colIdx, true))
					default:
						panic("")
					}
				}
			}
			colDefExpr.Constraints = colCons
			colDefs = append(colDefs, colDefExpr)
		case *pg_query.Node_Constraint: //table constraint
			cons := nodeImpl.Constraint
			pkNames := make([]string, 0)
			switch cons.GetContype() {
			case pg_query.ConstrType_CONSTR_PRIMARY:
				for _, key := range cons.GetKeys() {
					kname := key.GetString_().GetSval()
					pkNames = append(pkNames, kname)
				}
			default:
				panic("usp")
			}
			tableCons = append(tableCons, storage.NewUniqueIndexConstraint2(pkNames, true))
		default:
			panic("usp")
		}
	}
	ret.ColDefs = colDefs
	ret.Constraints = tableCons

	return ret, nil
}

func (b *Builder) buildInsert(
	txn *storage.Txn,
	stmt *pg_query.InsertStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	schema := stmt.GetRelation().GetSchemaname()
	if schema == "" {
		schema = "public"
	}
	name := stmt.GetRelation().GetRelname()

	return b.buildInsertInternal(
		txn,
		schema,
		name,
		stmt.Cols,
		stmt.GetSelectStmt().GetSelectStmt(),
		ctx, depth,
	)
}

func (b *Builder) buildInsertInternal(
	txn *storage.Txn,
	schema string,
	name string,
	Cols []*pg_query.Node,
	subSelect *pg_query.SelectStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	//step 0: get table
	tabEnt := storage.GCatalog.GetEntry(
		txn,
		storage.CatalogTypeTable,
		schema,
		name,
	)
	if tabEnt == nil {
		return nil, fmt.Errorf("no table %s in schema '%s'",
			name, schema)
	}
	insert := &LogicalOperator{
		Typ:        LOT_Insert,
		Database:   schema,
		Table:      name,
		TableEnt:   tabEnt,
		TableIndex: b.GetTag(),
	}

	//step 1: process columns
	//column seq no -> column idx in table
	namedColumnMap := make([]int, 0)
	if len(Cols) != 0 {
		getNameFun := func(col *pg_query.Node) string {
			resTar := col.GetResTarget()
			if resTar != nil {
				return resTar.GetName()
			}
			str := col.GetString_()
			if str != nil {
				return str.GetSval()
			}
			panic("usp")
		}
		//insert into specified columns
		//column name in stmt -> column seq no in stmt
		columnNameMap := make(map[string]int)
		for i, col := range Cols {
			colName := strings.ToLower(getNameFun(col))
			if _, has := columnNameMap[colName]; has {
				return nil, fmt.Errorf("duplicate column name %s in INSERT", colName)
			}
			columnNameMap[colName] = i
			colIdx := tabEnt.GetColumnIndex(colName)
			if colIdx == -1 {
				return nil, fmt.Errorf("invalid column %d", colIdx)
			}
			colDef := tabEnt.GetColumn(colIdx)
			insert.ExpectedTypes = append(insert.ExpectedTypes, colDef.Type)
			namedColumnMap = append(namedColumnMap, colIdx)
		}
		//
		for _, colDef := range tabEnt.GetColumns() {
			if seqNo, has := columnNameMap[colDef.Name]; !has {
				insert.ColumnIndexMap = append(insert.ColumnIndexMap, -1)
			} else {
				insert.ColumnIndexMap = append(insert.ColumnIndexMap, seqNo)
			}
		}
	} else {
		//no specified columns
		for i, colDef := range tabEnt.GetColumns() {
			namedColumnMap = append(namedColumnMap, i)
			insert.ExpectedTypes = append(insert.ExpectedTypes,
				colDef.Type)
		}
	}

	//step 2 : process default values
	//TODO:

	if subSelect == nil {
		return insert, nil
	}

	//step 3: process select stmt
	subBuilder := NewBuilder(b.txn)
	subBuilder.tag = b.tag
	subBuilder.rootCtx.parent = ctx

	expectedColumns := 0
	if len(Cols) == 0 {
		expectedColumns = len(tabEnt.GetColumns())
	} else {
		expectedColumns = len(Cols)
	}

	//value list
	if len(subSelect.ValuesLists) != 0 {
		expectedTypes := make([]common.LType, expectedColumns)
		expectedNames := make([]string, expectedColumns)

		resultColumns := len(subSelect.ValuesLists[0].GetList().GetItems())
		if expectedColumns != resultColumns {
			return nil, fmt.Errorf("table %s has %d columns but %d values were supplied",
				name,
				expectedColumns,
				resultColumns,
			)
		}

		//value list
		for colIdx := 0; colIdx < expectedColumns; colIdx++ {
			tableColIdx := namedColumnMap[colIdx]
			colDef := tabEnt.GetColumn(tableColIdx)
			expectedTypes[colIdx] = colDef.Type
			expectedNames[colIdx] = colDef.Name

			//TODO: process default value
		}
		subBuilder.expectedTypes = expectedTypes
		subBuilder.expectedNames = expectedNames
	}

	err := subBuilder.buildSelect(
		subSelect,
		subBuilder.rootCtx, 0)
	if err != nil {
		return nil, err
	}

	if len(subBuilder.projectExprs) == 0 {
		panic("select in Insert must have project list")
	}

	if expectedColumns != len(subBuilder.projectExprs) {
		return nil, fmt.Errorf("table %s has %d columns but %d values were supplied",
			name,
			expectedColumns,
			len(subBuilder.projectExprs),
		)
	}

	//build plan
	lp, err := subBuilder.CreatePlan(subBuilder.rootCtx, nil)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("sub select in insert has nil plan")
	}

	//cast types
	lp, err = b.CastLogicalOperatorToTypes(insert.ExpectedTypes, lp)
	if err != nil {
		return nil, err
	}

	checkExprIsValid(lp)
	lp, err = subBuilder.Optimize(subBuilder.rootCtx, lp)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(lp)

	insert.Children = append(insert.Children, lp)

	return insert, nil
}

func (b *Builder) buildValuesLists(
	lists []*pg_query.Node,
	ctx *BindContext,
	depth int) (*Expr, error) {
	var err error
	resultTypes := b.expectedTypes
	resultNames := b.expectedNames
	resultValues := make([][]*Expr, 0)
	var retExpr *Expr
	for _, exprList := range lists {
		items := exprList.GetList().GetItems()
		if len(resultNames) == 0 {
			for i := 0; i < len(exprList.GetList().GetItems()); i++ {
				resultNames = append(resultNames, fmt.Sprintf("col%d", i))
			}
		}
		list := make([]*Expr, 0)
		for valIdx, value := range items {
			retExpr, err = b.bindExpr(ctx, IWC_VALUES, value, depth)
			if err != nil {
				return nil, err
			}
			retExpr, err = AddCastToType(retExpr, resultTypes[valIdx], false)
			if err != nil {
				return nil, err
			}
			list = append(list, retExpr)
		}
		resultValues = append(resultValues, list)
	}

	if len(resultTypes) == 0 && len(lists) != 0 {
		panic("usp")
	}

	alias := "valueslist"
	bind := &Binding{
		typ:     BT_TABLE,
		alias:   alias,
		index:   uint64(b.GetTag()),
		typs:    util.CopyTo(resultTypes),
		names:   util.CopyTo(resultNames),
		nameMap: make(map[string]int),
	}
	for idx, name := range bind.names {
		bind.nameMap[name] = idx
	}
	err = ctx.AddBinding(alias, bind)
	if err != nil {
		return nil, err
	}

	return &Expr{
		Typ:         ET_ValuesList,
		Index:       bind.index,
		Database:    "",
		Table:       alias,
		Alias:       alias,
		BelongCtx:   ctx,
		Types:       resultTypes,
		Names:       resultNames,
		Values:      resultValues,
		ColName2Idx: bind.nameMap,
	}, err
}

func (b *Builder) CastLogicalOperatorToTypes(
	targetTyps []common.LType,
	root *LogicalOperator) (*LogicalOperator, error) {
	//srcTyps := make([]common.LType)
	//for i, output := range lp.Outputs {
	//
	//}
	var err error
	if root.Typ == LOT_Project {
		util.AssertFunc(len(root.Projects) == len(targetTyps))
		for i, proj := range root.Projects {
			if proj.DataTyp.Id != targetTyps[i].Id {
				//add cast
				root.Projects[i], err = AddCastToType(
					proj,
					targetTyps[i],
					false)
				if err != nil {
					return nil, err
				}
			}
		}
		return root, nil
	} else {
		//add cast project
		panic("usp")
	}
	return root, nil
}

func (b *Builder) createPhyInsert(
	root *LogicalOperator,
	children []*PhysicalOperator) (*PhysicalOperator, error) {
	//list := storage.NewDependList()
	//list.AddDepend(root.TableEnt)

	ret := &PhysicalOperator{
		Typ:            POT_Insert,
		TableEnt:       root.TableEnt,
		ColumnIndexMap: root.ColumnIndexMap,
		InsertTypes:    root.TableEnt.GetTypes(),
		Children:       children,
	}
	return ret, nil
}

func (b *Builder) buildCopy(
	txn *storage.Txn,
	stmt *pg_query.CopyStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	if stmt.IsFrom {
		return b.buildCopyFrom(txn, stmt, ctx, depth)
	} else {
		return b.buildCopyTo(txn, stmt, ctx, depth)
	}
}

func (b *Builder) buildCopyFrom(
	txn *storage.Txn,
	stmt *pg_query.CopyStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	schema := stmt.GetRelation().GetSchemaname()
	if schema == "" {
		schema = "public"
	}
	name := stmt.GetRelation().GetRelname()
	insert, err := b.buildInsertInternal(
		txn,
		schema,
		name,
		stmt.GetAttlist(),
		nil,
		ctx,
		depth,
	)
	if err != nil {
		return nil, err
	}
	tabEnt := storage.GCatalog.GetEntry(txn, storage.CatalogTypeTable, schema, name)
	if tabEnt == nil {
		return nil, fmt.Errorf("no table %s in schema %s", name, schema)
	}
	var expectedNames []string
	if len(insert.ColumnIndexMap) != 0 {
		expectedNames = make([]string, len(insert.ExpectedTypes))
		for i, colDef := range tabEnt.GetColumns() {
			if insert.ColumnIndexMap[i] != -1 {
				expectedNames[insert.ColumnIndexMap[i]] = colDef.Name
			}
		}
	} else {
		for _, colDef := range tabEnt.GetColumns() {
			expectedNames = append(expectedNames, colDef.Name)
		}
	}

	getOpts := func() []*ScanOption {
		opts := make([]*ScanOption, 0)
		for _, node := range stmt.GetOptions() {
			opt := &ScanOption{}
			opt.Kind = node.GetDefElem().GetDefname()
			opt.Opt = node.GetDefElem().GetArg().GetString_().GetSval()
			opts = append(opts, opt)
		}
		return opts
	}

	opts := getOpts()

	formatOpt := getFormatFun("format", opts)
	if formatOpt == nil {
		return nil, fmt.Errorf("no format option in copy from")
	}

	scanInfo := &ScanInfo{
		ReturnedTypes: insert.ExpectedTypes,
		Names:         expectedNames,
		FilePath:      stmt.GetFilename(),
		Opts:          opts,
		Format:        formatOpt.Opt,
	}

	for i := 0; i < len(insert.ExpectedTypes); i++ {
		scanInfo.ColumnIds = append(scanInfo.ColumnIds, i)
	}

	name2IdxMap := make(map[string]int)
	for i, colName := range expectedNames {
		name2IdxMap[colName] = i
	}

	scanOp := &LogicalOperator{
		Typ:         LOT_Scan,
		Index:       uint64(b.GetTag()),
		ScanTyp:     ScanTypeCopyFrom,
		ScanInfo:    scanInfo,
		ColName2Idx: name2IdxMap,
	}

	projOp := &LogicalOperator{
		Typ:      LOT_Project,
		Children: []*LogicalOperator{scanOp},
	}

	for i := 0; i < len(scanInfo.Names); i++ {
		expr := &Expr{
			Typ:     ET_Column,
			DataTyp: scanInfo.ReturnedTypes[i],
			Name:    scanInfo.Names[i],
			Alias:   scanInfo.Names[i],
			ColRef:  ColumnBind{scanOp.Index, uint64(i)},
		}
		projOp.Projects = append(projOp.Projects, expr)
	}

	tempBuilder := NewBuilder(b.txn)
	projOp, err = tempBuilder.Optimize(tempBuilder.rootCtx, projOp)
	if err != nil {
		return nil, err
	}
	if projOp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(projOp)

	insert.Children = append(insert.Children, projOp)
	return insert, nil
}

func getFormatFun(kind string, opts []*ScanOption) *ScanOption {
	for _, opt := range opts {
		if opt.Kind == kind {
			return opt
		}
	}
	return nil
}

func (b *Builder) buildCopyTo(
	txn *storage.Txn,
	stmt *pg_query.CopyStmt,
	ctx *BindContext,
	depth int) (*LogicalOperator, error) {
	return nil, fmt.Errorf("usp copy to")
}
