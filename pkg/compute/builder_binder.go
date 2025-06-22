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

package compute

import (
	"fmt"
	"strconv"

	pg_query "github.com/pganalyze/pg_query_go/v5"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

func getTableColumn(expr *pg_query.ColumnRef) (string, string) {
	cnt := len(expr.Fields)
	if cnt == 1 {
		return "", expr.Fields[0].GetString_().GetSval()
	} else if cnt == 2 {
		return expr.Fields[0].GetString_().GetSval(), expr.Fields[1].GetString_().GetSval()
	}
	panic(fmt.Sprintf("unexpected number of columns: %d %v", cnt, expr.String()))
}

func (b *Builder) bindExpr(
	ctx *BindContext,
	iwc InWhichClause,
	expr *pg_query.Node, depth int) (ret *Expr, err error) {
	//var child *Expr
	switch realExpr := expr.GetNode().(type) {
	case *pg_query.Node_ResTarget:
		panic("usp")
	case *pg_query.Node_ColumnRef:
		tableName, colName := getTableColumn(realExpr.ColumnRef)
		switch iwc {
		case IWC_WHERE:
		case IWC_ORDER:
			selIdx := b.isInSelectList(colName)
			if selIdx >= 0 {
				return b.bindToSelectList(nil, selIdx, colName), err
			}
		case IWC_GROUP:
		case IWC_SELECT:
		case IWC_HAVING:
		case IWC_JOINON:
		default:
			panic(fmt.Sprintf("usp iwc %d", iwc))
		}
		bind, d, err := ctx.GetMatchingBinding(tableName, colName)
		if err != nil {
			return nil, err
		}
		colIdx := bind.HasColumn(colName)

		switch bind.typ {
		case BT_TABLE:

		case BT_Subquery:

		default:
		}

		ret = &Expr{
			Typ:     ET_Column,
			DataTyp: bind.typs[colIdx],
			BaseInfo: BaseInfo{
				Table:  bind.alias,
				Name:   colName,
				ColRef: ColumnBind{bind.index, uint64(colIdx)},
				Depth:  d,
			},
		}

	case *pg_query.Node_BoolExpr:
		return b.bindBoolExpr(ctx, iwc, realExpr.BoolExpr, depth)
	case *pg_query.Node_SubLink:
		ret, err = b.bindSubquery(ctx, iwc, realExpr.SubLink, depth)
	case *pg_query.Node_AExpr:
		ret, err = b.bindAExpr(ctx, iwc, realExpr.AExpr, depth)
	case *pg_query.Node_AConst:
		ret, err = b.bindAConst(ctx, iwc, realExpr.AConst, depth)
	case *pg_query.Node_FuncCall:
		ret, err = b.bindFuncCall(ctx, iwc, realExpr.FuncCall, depth)
	case *pg_query.Node_SortBy:
		ret, err = b.bindSortBy(ctx, iwc, realExpr.SortBy, depth)
	case *pg_query.Node_TypeCast:
		ret, err = b.bindTypeCast(ctx, iwc, realExpr.TypeCast, depth)
	case *pg_query.Node_List:
		ret, err = b.bindList(ctx, iwc, realExpr.List, depth)
	case *pg_query.Node_CaseExpr:
		ret, err = b.bindCaseExpr(ctx, iwc, realExpr.CaseExpr, depth)
	case *pg_query.Node_CaseWhen:
		ret, err = b.bindCaseWhen(ctx, iwc, realExpr.CaseWhen, depth)
	default:
		panic(fmt.Sprintf("bindExpr: unexpected node type %T", realExpr))
	}
	return ret, err
}

func (b *Builder) bindList(ctx *BindContext, iwc InWhichClause, expr *pg_query.List, depth int) (*Expr, error) {
	lExprs := make([]*Expr, 0)
	for _, item := range expr.Items {
		itemExpr, err := b.bindExpr(ctx, iwc, item, depth)
		if err != nil {
			return nil, err
		}
		lExprs = append(lExprs, itemExpr)
	}
	return &Expr{
		Typ:      ET_List,
		Children: lExprs,
	}, nil
}

func (b *Builder) bindTypeCast(ctx *BindContext, iwc InWhichClause, expr *pg_query.TypeCast, depth int) (*Expr, error) {
	var retExpr *Expr
	var err error
	var resultTyp common.LType
	retExpr, err = b.bindExpr(ctx, iwc, expr.Arg, depth)
	if err != nil {
		return nil, err
	}

	//find type
	typName := ""
	for _, name := range expr.TypeName.Names {
		if name.GetString_().Sval == "pg_catalog" {
			continue
		}
		typName = name.GetString_().GetSval()
	}

	switch typName {
	case "date":
		resultTyp = common.DateType()
	case "interval":
		resultTyp = common.IntervalType()
	default:
		panic(fmt.Sprintf("usp typename %v", expr.TypeName))
	}

	//cast
	retExpr, err = AddCastToType(retExpr, resultTyp, resultTyp.Id == common.LTID_ENUM)
	if err != nil {
		return nil, err
	}
	return retExpr, nil
}

func (b *Builder) bindSortBy(ctx *BindContext, iwc InWhichClause, expr *pg_query.SortBy, depth int) (*Expr, error) {
	child, err := b.bindExpr(ctx, iwc, expr.Node, depth)
	if err != nil {
		return nil, err
	}

	desc := false
	switch expr.SortbyDir {
	case pg_query.SortByDir_SORTBY_DEFAULT,
		pg_query.SortByDir_SORTBY_ASC:
		desc = false
	case pg_query.SortByDir_SORTBY_DESC:
		desc = true
	default:
		panic(fmt.Sprintf("usp orderbydir %v", expr.SortbyDir))
	}

	ret := &Expr{
		Typ:     ET_Orderby,
		DataTyp: child.DataTyp,
		OrderByInfo: OrderByInfo{
			Desc: desc,
		},
		Children: []*Expr{child},
	}
	return ret, nil
}

func getFuncName(expr *pg_query.FuncCall) string {
	for _, node := range expr.Funcname {
		sval := node.GetString_().GetSval()
		if sval == "pg_catalog" {
			continue
		}
		return sval
	}
	panic("no function name")
}

func (b *Builder) bindFuncCall(ctx *BindContext, iwc InWhichClause, expr *pg_query.FuncCall, depth int) (*Expr, error) {
	var child *Expr
	var ret *Expr
	var err error
	//real function
	name := getFuncName(expr)
	if name == "count" {
		if expr.AggStar {
			colName := b.rootCtx.bindingsList[0].names[0]
			expr.Args = []*pg_query.Node{
				{
					Node: &pg_query.Node_ColumnRef{
						ColumnRef: &pg_query.ColumnRef{
							Fields: []*pg_query.Node{
								{
									Node: &pg_query.Node_String_{
										String_: &pg_query.String{
											Sval: colName,
										},
									},
								},
							},
						},
					},
				},
			}
		}
	}
	args := make([]*Expr, 0)
	argsTypes := make([]common.LType, 0)
	for _, arg := range expr.Args {
		child, err = b.bindExpr(ctx, iwc, arg, depth)
		if err != nil {
			return nil, err
		}
		args = append(args, child)
		argsTypes = append(argsTypes, child.DataTyp)
	}

	ret, err = b.bindFunc(
		name,
		name,
		args,
		argsTypes,
		expr.AggDistinct)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *Builder) bindAConst(ctx *BindContext, iwc InWhichClause, expr *pg_query.A_Const, depth int) (*Expr, error) {
	var ret *Expr
	var fval float64
	var err error

	switch realExpr := expr.GetVal().(type) {
	case *pg_query.A_Const_Sval:
		ret = &Expr{
			Typ:        ET_Const,
			DataTyp:    common.VarcharType(),
			ConstValue: NewStringConst(realExpr.Sval.Sval),
		}
	case *pg_query.A_Const_Fval:
		fval, err = strconv.ParseFloat(realExpr.Fval.Fval, 64)
		if err != nil {
			return nil, err
		}
		ret = &Expr{
			Typ:        ET_Const,
			DataTyp:    common.FloatType(),
			ConstValue: NewFloatConst(fval),
		}
	case *pg_query.A_Const_Ival:
		ret = &Expr{
			Typ:        ET_Const,
			DataTyp:    common.IntegerType(),
			ConstValue: NewIntegerConst(int64(realExpr.Ival.Ival)),
		}

	default:
		panic(fmt.Errorf("bindExpr: unexpected node type %T", realExpr))
	}
	return ret, err
}

func (b *Builder) bindAExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.A_Expr, depth int) (*Expr, error) {
	var left, right *Expr
	var resultTyp common.LType
	var err error

	//special
	switch expr.Kind {
	case pg_query.A_Expr_Kind_AEXPR_IN:
		return b.bindInExpr(ctx, iwc, expr, depth)
	case pg_query.A_Expr_Kind_AEXPR_BETWEEN:
		return b.bindBetweenExpr(ctx, iwc, expr, depth)
	default:
	}

	left, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)
	if err != nil {
		return nil, err
	}

	right, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)
	if err != nil {
		return nil, err
	}

	var funcName string
	switch expr.Kind {
	case pg_query.A_Expr_Kind_AEXPR_LIKE:
		opName := expr.Name[0].GetString_().GetSval()
		switch opName {
		case "~~":
			funcName = FuncLike
		case "!~~":
			funcName = FuncNotLike
		default:
			panic(fmt.Errorf("bindExpr: unexpected operator %q", opName))
		}

	case pg_query.A_Expr_Kind_AEXPR_OP:
		opName := expr.Name[0].GetString_().GetSval()
		switch opName {
		case "=":
			funcName = FuncEqual
		case "<>":
			funcName = FuncNotEqual
		case "+":
			funcName = FuncAdd
		case "-":
			funcName = FuncSubtract
		case "*":
			funcName = FuncMultiply
		case "/":
			funcName = FuncDivide
		case ">":
			funcName = FuncGreater
		case ">=":
			funcName = FuncGreaterEqual
		case "<":
			funcName = FuncLess
		case "<=":
			funcName = FuncLessEqual
		default:
			panic(fmt.Errorf("bindAExpr: unexpected operator '%s'", expr.Name[0].GetString_().GetSval()))
		}
	default:
		panic(fmt.Sprintf("bindExpr: unexpected kind %v", expr.Kind))
	}

	if funcName == FuncAdd &&
		(left.DataTyp.IsDate() && right.DataTyp.IsInterval() ||
			left.DataTyp.IsInterval() && right.DataTyp.IsDate()) {
		//date + interval or interval + date => date
		funcName = FuncDateAdd
	} else if funcName == FuncSubtract &&
		left.DataTyp.IsDate() &&
		right.DataTyp.IsDate() {
		//date - date => interval
	} else if funcName == FuncSubtract &&
		left.DataTyp.IsDate() &&
		right.DataTyp.IsInterval() {
		//date - interval => date
		funcName = FuncDateSub
	} else {
		resultTyp = decideResultType(left.DataTyp, right.DataTyp)

		//cast
		left, err = AddCastToType(left, resultTyp, resultTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}
		right, err = AddCastToType(right, resultTyp, resultTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}
	}

	bindFunc, err := b.bindFunc(funcName, funcName, []*Expr{left, right}, []common.LType{left.DataTyp, right.DataTyp}, false)
	if err != nil {
		return nil, err
	}
	return bindFunc, nil
}

func (b *Builder) bindBoolExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.BoolExpr, depth int) (*Expr, error) {
	var err error
	var resultTyp common.LType
	var left, cur *Expr
	var funcName string

	switch expr.Boolop {
	case pg_query.BoolExprType_AND_EXPR:
		funcName = FuncAnd
	case pg_query.BoolExprType_OR_EXPR:
		funcName = FuncOr
	case pg_query.BoolExprType_NOT_EXPR:
		funcName = FuncNot
	default:
		panic(fmt.Sprintf("usp bool expr %v", expr.String()))
	}

	left, err = b.bindExpr(ctx, iwc, expr.Args[0], depth)
	if err != nil {
		return nil, err
	}

	//special :
	// not in
	if funcName == FuncNot {
		switch left.Typ {
		case ET_Func:
			switch left.FunImpl._name {
			case FuncIn:
				//convert it to 'not in'
				left.FunImpl._name = FuncNotIn

			default:
				panic(fmt.Sprintf("usp not expr %v", left.FunImpl._name))
			}
		case ET_Subquery:
			if left.SubqueryTyp == ET_SubqueryTypeExists {
				left.SubqueryTyp = ET_SubqueryTypeNotExists
			}
		default:
			panic(fmt.Sprintf("usp not expr 2 %v", left.Typ))
		}
		return left, nil
	}

	for i := 1; i < len(expr.Args); i++ {
		cur, err = b.bindExpr(ctx, iwc, expr.Args[i], depth)
		if err != nil {
			return nil, err
		}

		resultTyp = decideResultType(left.DataTyp, cur.DataTyp)

		//cast
		left, err = AddCastToType(left, resultTyp, resultTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}
		cur, err = AddCastToType(cur, resultTyp, resultTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}

		//
		left, err = b.bindFunc(
			funcName,
			funcName,
			[]*Expr{left, cur},
			[]common.LType{left.DataTyp, cur.DataTyp},
			false)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (b *Builder) bindFunc(name string, astStr string, args []*Expr, argsTypes []common.LType, distinct bool) (*Expr, error) {
	funBinder := FunctionBinder{}
	if IsAgg(name) {
		ret := funBinder.BindAggrFunc(name, args, false)
		if distinct {
			ret.FunctionInfo.FunImpl._aggrType = DISTINCT
		}
		b.aggs = append(b.aggs, ret)
		ret = &Expr{
			Typ:     ET_Column,
			DataTyp: ret.DataTyp,
			BaseInfo: BaseInfo{
				Table:  fmt.Sprintf("AggNode_%v", b.aggTag),
				Name:   astStr,
				ColRef: ColumnBind{uint64(b.aggTag), uint64(len(b.aggs) - 1)},
				Depth:  0,
			},
		}
		return ret, nil
	} else {
		return funBinder.BindScalarFunc(name, args, IsOperator(name)), nil
	}
}

func decideResultType(left common.LType, right common.LType) common.LType {
	resultTyp := common.MaxLType(left, right)
	//adjust final result type
	switch resultTyp.Id {
	case common.LTID_DECIMAL:
		//max Width & scal of the result type
		inputTypes := []common.LType{left, right}
		maxWidth, maxScale, maxWidthOverScale := 0, 0, 0
		for _, typ := range inputTypes {
			can, width, scale := typ.GetDecimalSize()
			if !can {
				return resultTyp
			}
			maxWidth = max(width, maxWidth)
			maxScale = max(scale, maxScale)
			maxWidthOverScale = max(width-scale, maxWidthOverScale)
		}
		maxWidth = max(maxScale+maxWidthOverScale, maxWidth)
		maxWidth = min(maxWidth, common.DecimalMaxWidth)
		return common.DecimalType(maxWidth, maxScale)
	case common.LTID_VARCHAR:
		//
		if left.IsNumeric() || left.Id == common.LTID_BOOLEAN {
			return left
		} else if right.IsNumeric() || right.Id == common.LTID_BOOLEAN {
			panic("usp")
		}
		return resultTyp
	default:
		return resultTyp
	}

}

func (b *Builder) bindBetweenExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.A_Expr, depth int) (*Expr, error) {
	var betExpr *Expr
	var listExppr *Expr
	var left, right *Expr
	var err error
	var resultTyp common.LType
	betExpr, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)
	if err != nil {
		return nil, err
	}

	listExppr, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)
	if err != nil {
		return nil, err
	}

	util.AssertFunc(listExppr.Typ == ET_List)
	util.AssertFunc(len(listExppr.Children) == 2)
	left = listExppr.Children[0]
	right = listExppr.Children[1]
	{
		resultTyp = decideResultType(betExpr.DataTyp, left.DataTyp)
		resultTyp = decideResultType(resultTyp, right.DataTyp)
		//cast
		betExpr, err = AddCastToType(betExpr, resultTyp, false)
		if err != nil {
			return nil, err
		}
		left, err = AddCastToType(left, resultTyp, false)
		if err != nil {
			return nil, err
		}
		right, err = AddCastToType(right, resultTyp, false)
		if err != nil {
			return nil, err
		}
	}

	//>=
	params := []*Expr{betExpr, left}
	paramsTypes := []common.LType{betExpr.DataTyp, left.DataTyp}
	ret0, err := b.bindFunc(FuncGreaterEqual, FuncGreaterEqual, params, paramsTypes, false)
	if err != nil {
		return nil, err
	}

	//<=
	params = []*Expr{betExpr, right}
	paramsTypes = []common.LType{betExpr.DataTyp, right.DataTyp}
	ret1, err := b.bindFunc(FuncLessEqual, FuncLessEqual, params, paramsTypes, false)
	if err != nil {
		return nil, err
	}

	// >= && <=
	params = []*Expr{ret0, ret1}
	paramsTypes = []common.LType{ret0.DataTyp, ret1.DataTyp}

	ret, err := b.bindFunc(FuncAnd, FuncAnd, params, paramsTypes, false)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *Builder) bindSubquery(ctx *BindContext, iwc InWhichClause, expr *pg_query.SubLink, depth int) (*Expr, error) {
	var testExpr *Expr
	var retExpr *Expr
	var err error
	var resultTyp common.LType
	var funcName string
	if expr.Testexpr != nil {
		testExpr, err = b.bindExpr(ctx, iwc, expr.Testexpr, depth)
		if err != nil {
			return nil, err
		}
	}

	subBuilder := NewBuilder(b.txn)
	subBuilder.tag = b.tag
	subBuilder.rootCtx.parent = ctx
	err = subBuilder.buildSelect(expr.Subselect.GetSelectStmt(), subBuilder.rootCtx, 0)
	if err != nil {
		return nil, err
	}

	var typ ET_SubqueryType

	switch expr.SubLinkType {
	case pg_query.SubLinkType_ANY_SUBLINK: // IN, = ANY
		typ = ET_SubqueryTypeIn

		util.AssertFunc(testExpr != nil)

		subExpr := &Expr{
			Typ:     ET_Subquery,
			DataTyp: subBuilder.projectExprs[0].DataTyp,
			SubqueryInfo: SubqueryInfo{
				SubBuilder:  subBuilder,
				SubCtx:      subBuilder.rootCtx,
				SubqueryTyp: typ,
			},
		}

		resultTyp = decideResultType(testExpr.DataTyp, subExpr.DataTyp)
		//cast
		testExpr, err = AddCastToType(testExpr, resultTyp, resultTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}

		subExpr, err = AddCastToType(subExpr, resultTyp, resultTyp.Id == common.LTID_ENUM)
		if err != nil {
			return nil, err
		}

		funcName = FuncIn

		retExpr, err = b.bindFunc(
			funcName,
			funcName,
			[]*Expr{testExpr, subExpr},
			[]common.LType{testExpr.DataTyp, subExpr.DataTyp},
			false)
		if err != nil {
			return nil, err
		}

	case pg_query.SubLinkType_EXPR_SUBLINK:
		typ = ET_SubqueryTypeScalar

		util.AssertFunc(testExpr == nil)

		retExpr = &Expr{
			Typ:     ET_Subquery,
			DataTyp: subBuilder.projectExprs[0].DataTyp,
			SubqueryInfo: SubqueryInfo{
				SubBuilder:  subBuilder,
				SubCtx:      subBuilder.rootCtx,
				SubqueryTyp: typ,
			},
		}

	case pg_query.SubLinkType_EXISTS_SUBLINK:
		typ = ET_SubqueryTypeExists

		util.AssertFunc(testExpr == nil)

		retExpr = &Expr{
			Typ:     ET_Subquery,
			DataTyp: common.BooleanType(),
			SubqueryInfo: SubqueryInfo{
				SubBuilder:  subBuilder,
				SubCtx:      subBuilder.rootCtx,
				SubqueryTyp: typ,
			},
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.SubLinkType))
	}

	return retExpr, err
}

func (b *Builder) bindToSelectList(selectExprs []*pg_query.Node, idx int, alias string) *Expr {
	if idx < len(selectExprs) {
		alias = selectExprs[idx].String()
	}
	return &Expr{
		Typ:     ET_Column,
		DataTyp: b.projectExprs[idx].DataTyp,
		BaseInfo: BaseInfo{
			ColRef: ColumnBind{uint64(b.projectTag), uint64(idx)},
			Alias:  alias,
		},
	}
}

func (b *Builder) isInSelectList(alias string) int {
	if idx, ok := b.aliasMap[alias]; ok {
		return idx
	}
	return -1
}
