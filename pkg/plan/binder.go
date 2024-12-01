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
	"fmt"
	"strconv"

	pg_query "github.com/pganalyze/pg_query_go/v5"
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
			Table:   bind.alias,
			Name:    colName,
			ColRef:  ColumnBind{bind.index, uint64(colIdx)},
			Depth:   d,
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
	var resultTyp LType
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
		resultTyp = dateLTyp()
	case "interval":
		resultTyp = intervalLType()
	default:
		panic(fmt.Sprintf("usp typename %v", expr.TypeName))
	}

	//cast
	retExpr, err = AddCastToType(retExpr, resultTyp, resultTyp.id == LTID_ENUM)
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
		Typ:      ET_Orderby,
		DataTyp:  child.DataTyp,
		Desc:     desc,
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
			//replace * by the column 0 of the first table
			//TODO: refine
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
	argsTypes := make([]LType, 0)
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
		ET_SubFunc,
		expr.String(),
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
			Typ:     ET_SConst,
			DataTyp: varchar(),
			Svalue:  realExpr.Sval.Sval,
		}
	case *pg_query.A_Const_Fval:
		fval, err = strconv.ParseFloat(realExpr.Fval.Fval, 64)
		if err != nil {
			return nil, err
		}
		ret = &Expr{
			Typ:     ET_FConst,
			DataTyp: float(),
			Fvalue:  fval,
		}
	case *pg_query.A_Const_Ival:
		ret = &Expr{
			Typ:     ET_IConst,
			DataTyp: integer(),
			Ivalue:  int64(realExpr.Ival.Ival),
		}

	default:
		panic(fmt.Errorf("bindExpr: unexpected node type %T", realExpr))
	}
	return ret, err
}

func (b *Builder) bindAExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.A_Expr, depth int) (*Expr, error) {
	var left, right *Expr
	var resultTyp LType
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

	var et ET_SubTyp
	switch expr.Kind {
	case pg_query.A_Expr_Kind_AEXPR_LIKE:
		opName := expr.Name[0].GetString_().GetSval()
		switch opName {
		case "~~":
			et = ET_Like
		case "!~~":
			et = ET_NotLike
		default:
			panic(fmt.Errorf("bindExpr: unexpected operator %q", opName))
		}

	case pg_query.A_Expr_Kind_AEXPR_OP:
		opName := expr.Name[0].GetString_().GetSval()
		switch opName {
		case "=":
			et = ET_Equal
		case "<>":
			et = ET_NotEqual
		case "+":
			et = ET_Add
		case "-":
			et = ET_Sub
		case "*":
			et = ET_Mul
		case "/":
			et = ET_Div
		case ">":
			et = ET_Greater
		case ">=":
			et = ET_GreaterEqual
		case "<":
			et = ET_Less
		case "<=":
			et = ET_LessEqual
		default:
			panic(fmt.Errorf("bindAExpr: unexpected operator '%s'", expr.Name[0].GetString_().GetSval()))
		}
	default:
		panic(fmt.Sprintf("bindExpr: unexpected kind %v", expr.Kind))
	}

	if et == ET_Add &&
		(left.DataTyp.isDate() && right.DataTyp.isInterval() ||
			left.DataTyp.isInterval() && right.DataTyp.isDate()) {
		//date + interval or interval + date => date
		et = ET_DateAdd
	} else if et == ET_Sub &&
		left.DataTyp.isDate() &&
		right.DataTyp.isDate() {
		//date - date => interval
	} else if et == ET_Sub &&
		left.DataTyp.isDate() &&
		right.DataTyp.isInterval() {
		//date - interval => date
		et = ET_DateSub
	} else {
		resultTyp = decideResultType(left.DataTyp, right.DataTyp)

		//cast
		left, err = AddCastToType(left, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}
		right, err = AddCastToType(right, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}
	}

	bindFunc, err := b.bindFunc(et.String(), et, expr.String(), []*Expr{left, right}, []LType{left.DataTyp, right.DataTyp}, false)
	if err != nil {
		return nil, err
	}
	return bindFunc, nil
}

func (b *Builder) bindBoolExpr(ctx *BindContext, iwc InWhichClause, expr *pg_query.BoolExpr, depth int) (*Expr, error) {
	var err error
	var resultTyp LType
	var left, cur *Expr
	var et ET_SubTyp

	switch expr.Boolop {
	case pg_query.BoolExprType_AND_EXPR:
		et = ET_And
	case pg_query.BoolExprType_OR_EXPR:
		et = ET_Or
	case pg_query.BoolExprType_NOT_EXPR:
		et = ET_Not
	default:
		panic(fmt.Sprintf("usp bool expr %v", expr.String()))
	}

	left, err = b.bindExpr(ctx, iwc, expr.Args[0], depth)
	if err != nil {
		return nil, err
	}

	//TODO: refine it
	//special :
	// not in
	if et == ET_Not {
		switch left.Typ {
		case ET_Func:
			switch left.SubTyp {
			case ET_In:
				//convert it to 'not in'
				left.SubTyp = ET_NotIn

			default:
				panic(fmt.Sprintf("usp not expr %v", left.SubTyp))
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
		left, err = AddCastToType(left, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}
		cur, err = AddCastToType(cur, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}

		//
		left, err = b.bindFunc(
			et.String(),
			et,
			expr.String(),
			[]*Expr{left, cur},
			[]LType{left.DataTyp, cur.DataTyp},
			false)
		if err != nil {
			return nil, err
		}
	}
	return left, nil
}

func (b *Builder) bindFunc(name string, subTyp ET_SubTyp, astStr string, args []*Expr, argsTypes []LType, distinct bool) (*Expr, error) {
	funBinder := FunctionBinder{}
	if IsAgg(name) {
		ret := funBinder.BindAggrFunc(name, args, subTyp, false)
		if distinct {
			ret.AggrTyp = DISTINCT
		}
		b.aggs = append(b.aggs, ret)
		ret = &Expr{
			Typ:     ET_Column,
			DataTyp: ret.DataTyp,
			Table:   fmt.Sprintf("AggNode_%v", b.aggTag),
			Name:    astStr,
			ColRef:  ColumnBind{uint64(b.aggTag), uint64(len(b.aggs) - 1)},
			Depth:   0,
		}
		return ret, nil
	} else {
		return funBinder.BindScalarFunc(name, args, subTyp, subTyp.isOperator()), nil
	}
}

func decideResultType(left LType, right LType) LType {
	resultTyp := MaxLType(left, right)
	//adjust final result type
	switch resultTyp.id {
	case LTID_DECIMAL:
		//max width & scal of the result type
		inputTypes := []LType{left, right}
		maxWidth, maxScale, maxWidthOverScale := 0, 0, 0
		for _, typ := range inputTypes {
			can, width, scale := typ.getDecimalSize()
			if !can {
				return resultTyp
			}
			maxWidth = max(width, maxWidth)
			maxScale = max(scale, maxScale)
			maxWidthOverScale = max(width-scale, maxWidthOverScale)
		}
		maxWidth = max(maxScale+maxWidthOverScale, maxWidth)
		maxWidth = min(maxWidth, DecimalMaxWidth)
		return decimal(maxWidth, maxScale)
	case LTID_VARCHAR:
		//
		if left.isNumeric() || left.id == LTID_BOOLEAN {
			return left
		} else if right.isNumeric() || right.id == LTID_BOOLEAN {
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
	var resultTyp LType
	betExpr, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)
	if err != nil {
		return nil, err
	}

	listExppr, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)
	if err != nil {
		return nil, err
	}

	assertFunc(listExppr.Typ == ET_List)
	assertFunc(len(listExppr.Children) == 2)
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
	paramsTypes := []LType{betExpr.DataTyp, left.DataTyp}
	ret0, err := b.bindFunc(ET_GreaterEqual.String(), ET_GreaterEqual, expr.String(), params, paramsTypes, false)
	if err != nil {
		return nil, err
	}

	//<=
	params = []*Expr{betExpr, right}
	paramsTypes = []LType{betExpr.DataTyp, right.DataTyp}
	ret1, err := b.bindFunc(ET_LessEqual.String(), ET_LessEqual, expr.String(), params, paramsTypes, false)
	if err != nil {
		return nil, err
	}

	// >= && <=
	params = []*Expr{ret0, ret1}
	paramsTypes = []LType{ret0.DataTyp, ret1.DataTyp}

	ret, err := b.bindFunc(ET_And.String(), ET_And, expr.String(), params, paramsTypes, false)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *Builder) bindSubquery(ctx *BindContext, iwc InWhichClause, expr *pg_query.SubLink, depth int) (*Expr, error) {
	var testExpr *Expr
	var retExpr *Expr
	var err error
	var resultTyp LType
	var et ET_SubTyp
	if expr.Testexpr != nil {
		testExpr, err = b.bindExpr(ctx, iwc, expr.Testexpr, depth)
		if err != nil {
			return nil, err
		}
	}

	subBuilder := NewBuilder()
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

		assertFunc(testExpr != nil)

		subExpr := &Expr{
			Typ:         ET_Subquery,
			SubBuilder:  subBuilder,
			DataTyp:     subBuilder.projectExprs[0].DataTyp,
			SubCtx:      subBuilder.rootCtx,
			SubqueryTyp: typ,
		}

		resultTyp = decideResultType(testExpr.DataTyp, subExpr.DataTyp)
		//cast
		testExpr, err = AddCastToType(testExpr, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}

		subExpr, err = AddCastToType(subExpr, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}

		et = ET_In

		retExpr, err = b.bindFunc(
			et.String(),
			et,
			expr.String(),
			[]*Expr{testExpr, subExpr},
			[]LType{testExpr.DataTyp, subExpr.DataTyp},
			false)
		if err != nil {
			return nil, err
		}

	case pg_query.SubLinkType_EXPR_SUBLINK:
		typ = ET_SubqueryTypeScalar

		assertFunc(testExpr == nil)

		retExpr = &Expr{
			Typ:         ET_Subquery,
			SubBuilder:  subBuilder,
			DataTyp:     subBuilder.projectExprs[0].DataTyp,
			SubCtx:      subBuilder.rootCtx,
			SubqueryTyp: typ,
		}

	case pg_query.SubLinkType_EXISTS_SUBLINK:
		typ = ET_SubqueryTypeExists

		assertFunc(testExpr == nil)

		retExpr = &Expr{
			Typ:         ET_Subquery,
			SubBuilder:  subBuilder,
			DataTyp:     boolean(),
			SubCtx:      subBuilder.rootCtx,
			SubqueryTyp: typ,
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
		ColRef:  ColumnBind{uint64(b.projectTag), uint64(idx)},
		Alias:   alias,
	}
}

func (b *Builder) isInSelectList(alias string) int {
	if idx, ok := b.aliasMap[alias]; ok {
		return idx
	}
	return -1
}
