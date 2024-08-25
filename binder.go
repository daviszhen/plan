package main

import (
	"fmt"
)

func (b *Builder) bindExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (ret *Expr, err error) {
	var child *Expr
	if expr.Typ != AstTypeExpr {
		panic("need expr")
	}
	switch expr.Expr.ExprTyp {
	case AstExprTypeDecimal:
		assertFunc(expr.Expr.Scale > 0)
		ret = &Expr{
			Typ: ET_DecConst,
			DataTyp: ExprDataType{
				LTyp: decimal(expr.Expr.Width, expr.Expr.Scale),
			},
			Svalue: expr.Expr.Svalue,
		}

	case AstExprTypeNumber:
		ret = &Expr{
			Typ: ET_IConst,
			DataTyp: ExprDataType{
				LTyp: integer(),
			},
			Ivalue: expr.Expr.Ivalue,
		}

	case AstExprTypeFNumber:
		ret = &Expr{
			Typ: ET_FConst,
			DataTyp: ExprDataType{
				LTyp: float(),
			},
			Fvalue: expr.Expr.Fvalue,
		}
	case AstExprTypeString:
		ret = &Expr{
			Typ: ET_SConst,
			DataTyp: ExprDataType{
				LTyp: varchar(),
			},
			Svalue: expr.Expr.Svalue,
		}

	case AstExprTypeColumn:
		colName := expr.Expr.Svalue
		tableName := expr.Expr.Table
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

	case AstExprTypeSubquery:
		ret, err = b.bindSubquery(ctx, iwc, expr, depth)
	case AstExprTypeOrderBy:
		child, err = b.bindExpr(ctx, iwc, expr.Expr.Children[0], depth)
		if err != nil {
			return nil, err
		}

		ret = &Expr{
			Typ:      ET_Orderby,
			DataTyp:  child.DataTyp,
			Desc:     expr.Expr.Desc,
			Children: []*Expr{child},
		}
	case AstExprTypeFunc:
		switch expr.Expr.SubTyp {
		case AstExprSubTypeFunc:
			//real function
			name := expr.Expr.Svalue
			if name == "count" {
				if len(expr.Expr.Children) != 1 {
					return nil, fmt.Errorf("count must have 1 arg")
				}
				if expr.Expr.Children[0].Expr.Svalue == "*" {
					//replace * by the column 0 of the first table
					//TODO: refine
					colName := b.rootCtx.bindingsList[0].names[0]
					expr.Expr.Children[0].Expr.Svalue = colName
				}
			}
			args := make([]*Expr, 0)
			argsTypes := make([]ExprDataType, 0)
			for _, arg := range expr.Expr.Children {
				child, err = b.bindExpr(ctx, iwc, arg, depth)
				if err != nil {
					return nil, err
				}
				args = append(args, child)
				argsTypes = append(argsTypes, child.DataTyp)
			}

			ret, err = b.bindFunc(name, ET_SubFunc, expr.String(), args, argsTypes)
			if err != nil {
				return nil, err
			}
		case AstExprSubTypeIn,
			AstExprSubTypeNotIn:
			ret, err = b.bindInExpr(ctx, iwc, expr, depth)
			if err != nil {
				return nil, err
			}
		case AstExprSubTypeCase:
			ret, err = b.bindCaseExpr(ctx, iwc, expr, depth)
			if err != nil {
				return nil, err
			}
		case AstExprSubTypeExists:
			args := make([]*Expr, 0)
			argsTypes := make([]ExprDataType, 0)
			for _, arg := range expr.Expr.Children {
				child, err = b.bindExpr(ctx, iwc, arg, depth)
				if err != nil {
					return nil, err
				}
				args = append(args, child)
				argsTypes = append(argsTypes, child.DataTyp)
			}

			ret, err = b.bindFunc(ET_Exists.String(), ET_Exists, expr.String(), args, argsTypes)
			if err != nil {
				return nil, err
			}
		case AstExprSubTypeNotExists:
			args := make([]*Expr, 0)
			argsTypes := make([]ExprDataType, 0)
			for _, arg := range expr.Expr.Children {
				child, err = b.bindExpr(ctx, iwc, arg, depth)
				if err != nil {
					return nil, err
				}
				args = append(args, child)
				argsTypes = append(argsTypes, child.DataTyp)
			}

			ret, err = b.bindFunc(ET_NotExists.String(), ET_NotExists, expr.String(), args, argsTypes)
			if err != nil {
				return nil, err
			}
		case AstExprSubTypeBetween:
			ret, err = b.bindBetweenExpr(ctx, iwc, expr, depth)
			if err != nil {
				return nil, err
			}
		default:
			//binary operator
			ret, err = b.bindBinaryExpr(ctx, iwc, expr, depth)
			if err != nil {
				return nil, err
			}
		}
	case AstExprTypeDate:
		ret = &Expr{
			Typ: ET_DateConst,
			DataTyp: ExprDataType{
				LTyp: dateLTyp(),
			},
			Svalue: expr.Expr.Svalue,
		}

	case AstExprTypeInterval:
		ret = &Expr{
			Typ: ET_IntervalConst,
			DataTyp: ExprDataType{
				LTyp: intervalLType(),
			},
			Ivalue: expr.Expr.Ivalue,
			Svalue: expr.Expr.Svalue,
		}
	default:
		panic(fmt.Sprintf("usp expr type %d", expr.Expr.ExprTyp))
	}
	if len(expr.Expr.Alias.alias) != 0 {
		ret.Alias = expr.Expr.Alias.alias
	}
	return ret, err
}

func (b *Builder) bindFunc(name string, subTyp ET_SubTyp, astStr string, args []*Expr, argsTypes []ExprDataType) (*Expr, error) {
	id, err := GetFunctionId(name)
	if err != nil {
		return nil, err
	}

	impl, err := GetFunctionImpl(id, argsTypes)
	if err != nil {
		return nil, err
	}

	retTyp := impl.RetTypeDecider(argsTypes)

	ret := &Expr{
		Typ:      ET_Func,
		SubTyp:   subTyp,
		Svalue:   name,
		FuncId:   id,
		DataTyp:  retTyp,
		Children: args,
	}

	//hard code for simplicity
	if id == DATE_ADD {
		ret.DataTyp = ExprDataType{
			LTyp: dateLTyp(),
		}
	}

	if IsAgg(name) {
		b.aggs = append(b.aggs, ret)
		ret = &Expr{
			Typ:     ET_Column,
			DataTyp: ret.DataTyp,
			Table:   fmt.Sprintf("AggNode_%v", b.aggTag),
			Name:    astStr,
			ColRef:  ColumnBind{uint64(b.aggTag), uint64(len(b.aggs) - 1)},
			Depth:   0,
		}
	}
	return ret, nil
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
			//TODO: collation
		}
		return resultTyp
	default:
		return resultTyp
	}

}

func castExpr(e *Expr, target LType, tryCast bool) (*Expr, error) {
	if e.DataTyp.LTyp.equal(target) {
		return e, nil
	}
	id, err := GetFunctionId("cast")
	if err != nil {
		return nil, err
	}
	ret := &Expr{
		Typ:     ET_Func,
		SubTyp:  ET_SubFunc,
		Svalue:  "cast",
		FuncId:  id,
		DataTyp: ExprDataType{LTyp: target},
		Children: []*Expr{
			//expr to be cast
			e,
			//target type saved in DataTyp field
			{
				Typ: ET_IConst,
				DataTyp: ExprDataType{
					LTyp: target,
				},
			},
		},
	}
	return ret, nil
}

func decideBinaryOpType(opTyp AstExprSubType, resultTyp LType) (ET_SubTyp, LType) {
	var et ET_SubTyp
	var retTyp LType
	switch opTyp {
	case AstExprSubTypeAnd:
		et = ET_And
		retTyp = boolean()
	case AstExprSubTypeOr:
		et = ET_Or
		retTyp = boolean()
	case AstExprSubTypeAdd:
		et = ET_Add
		retTyp = resultTyp
	case AstExprSubTypeSub:
		et = ET_Sub
		retTyp = resultTyp
	case AstExprSubTypeMul:
		et = ET_Mul
		retTyp = resultTyp
	case AstExprSubTypeDiv:
		et = ET_Div
		retTyp = resultTyp
	default:
		panic(fmt.Sprintf("usp binary type %d", opTyp))
	}
	return et, retTyp
}

func decideCompareOpType(opTyp AstExprSubType) (ET_SubTyp, LType) {
	var et ET_SubTyp
	var retTyp LType
	switch opTyp {
	case AstExprSubTypeEqual:
		et = ET_Equal
		retTyp = boolean()
	case AstExprSubTypeNotEqual:
		et = ET_NotEqual
		retTyp = boolean()
	case AstExprSubTypeGreaterEqual:
		et = ET_GreaterEqual
		retTyp = boolean()
	case AstExprSubTypeGreater:
		et = ET_Greater
		retTyp = boolean()
	case AstExprSubTypeLess:
		et = ET_Less
		retTyp = boolean()
	case AstExprSubTypeLessEqual:
		et = ET_LessEqual
		retTyp = boolean()
	case AstExprSubTypeBetween:
		et = ET_Between
		retTyp = boolean()
	case AstExprSubTypeIn:
		et = ET_In
		retTyp = boolean()
	case AstExprSubTypeNotIn:
		et = ET_NotIn
		retTyp = boolean()
	case AstExprSubTypeLike:
		et = ET_Like
		retTyp = boolean()
	case AstExprSubTypeNotLike:
		et = ET_NotLike
		retTyp = boolean()
	default:
		panic(fmt.Sprintf("usp binary type %d", opTyp))
	}
	return et, retTyp
}

func isCompare(opTyp AstExprSubType) bool {
	switch opTyp {
	case AstExprSubTypeEqual,
		AstExprSubTypeNotEqual,
		AstExprSubTypeGreaterEqual,
		AstExprSubTypeGreater,
		AstExprSubTypeLess,
		AstExprSubTypeLessEqual,
		AstExprSubTypeBetween,
		AstExprSubTypeIn,
		AstExprSubTypeNotIn,
		AstExprSubTypeLike,
		AstExprSubTypeNotLike:
		return true
	default:
		return false
	}
	return false
}

func (b *Builder) bindBetweenExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var betExpr *Expr
	var err error
	var resultTyp LType
	{
		betExpr, err = b.bindExpr(ctx, iwc, expr.Expr.Between, depth)
		if err != nil {
			return nil, err
		}
	}
	left, err := b.bindExpr(ctx, iwc, expr.Expr.Children[0], depth)
	if err != nil {
		return nil, err
	}
	right, err := b.bindExpr(ctx, iwc, expr.Expr.Children[1], depth)
	if err != nil {
		return nil, err
	}
	{
		resultTyp = decideResultType(betExpr.DataTyp.LTyp, left.DataTyp.LTyp)
		resultTyp = decideResultType(resultTyp, right.DataTyp.LTyp)
		//cast
		betExpr, err = castExpr(betExpr, resultTyp, false)
		if err != nil {
			return nil, err
		}
		left, err = castExpr(left, resultTyp, false)
		if err != nil {
			return nil, err
		}
		right, err = castExpr(right, resultTyp, false)
		if err != nil {
			return nil, err
		}
	}

	//>=
	params := []*Expr{betExpr, left}
	paramsTypes := []ExprDataType{betExpr.DataTyp, left.DataTyp}
	ret0, err := b.bindFunc(ET_GreaterEqual.String(), ET_GreaterEqual, expr.String(), params, paramsTypes)
	if err != nil {
		return nil, err
	}

	//<=
	params = []*Expr{betExpr, right}
	paramsTypes = []ExprDataType{betExpr.DataTyp, right.DataTyp}
	ret1, err := b.bindFunc(ET_LessEqual.String(), ET_LessEqual, expr.String(), params, paramsTypes)
	if err != nil {
		return nil, err
	}

	// >= && <=
	params = []*Expr{ret0, ret1}
	paramsTypes = []ExprDataType{ret0.DataTyp, ret1.DataTyp}

	ret, err := b.bindFunc(ET_And.String(), ET_And, expr.String(), params, paramsTypes)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *Builder) bindBinaryExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var err error
	var resultTyp LType
	left, err := b.bindExpr(ctx, iwc, expr.Expr.Children[0], depth)
	if err != nil {
		return nil, err
	}
	right, err := b.bindExpr(ctx, iwc, expr.Expr.Children[1], depth)
	if err != nil {
		return nil, err
	}
	var et ET_SubTyp
	if expr.Expr.SubTyp == AstExprSubTypeAdd &&
		(left.DataTyp.LTyp.isDate() && right.DataTyp.LTyp.isInterval() ||
			left.DataTyp.LTyp.isInterval() && right.DataTyp.LTyp.isDate()) {
		//date + interval or interval + date => date
		et, _ = decideBinaryOpType(expr.Expr.SubTyp, dateLTyp())
	} else if expr.Expr.SubTyp == AstExprSubTypeSub &&
		left.DataTyp.LTyp.isDate() &&
		right.DataTyp.LTyp.isDate() {
		//date - date => interval
		et, _ = decideBinaryOpType(expr.Expr.SubTyp, intervalLType())
	} else {
		resultTyp = decideResultType(left.DataTyp.LTyp, right.DataTyp.LTyp)
		//cast
		left, err = castExpr(left, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}
		right, err = castExpr(right, resultTyp, resultTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}
		if isCompare(expr.Expr.SubTyp) {
			et, _ = decideCompareOpType(expr.Expr.SubTyp)
		} else {
			et, _ = decideBinaryOpType(expr.Expr.SubTyp, resultTyp)
		}
	}

	bindFunc, err := b.bindFunc(et.String(), et, expr.String(), []*Expr{left, right}, []ExprDataType{left.DataTyp, right.DataTyp})
	if err != nil {
		return nil, err
	}
	return bindFunc, nil
}

func (b *Builder) bindSubquery(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	subBuilder := NewBuilder()
	subBuilder.tag = b.tag
	subBuilder.rootCtx.parent = ctx
	err := subBuilder.buildSelect(expr.Expr.Children[0], subBuilder.rootCtx, 0)
	if err != nil {
		return nil, err
	}
	typ := ET_SubqueryTypeScalar
	switch expr.Expr.SubqueryTyp {
	case AstSubqueryTypeScalar:
		typ = ET_SubqueryTypeScalar
	case AstSubqueryTypeExists:
		typ = ET_SubqueryTypeExists
	case AstSubqueryTypeNotExists:
		typ = ET_SubqueryTypeNotExists
	default:
		panic(fmt.Sprintf("usp %v", expr.Expr.SubqueryTyp))
	}
	return &Expr{
		Typ:         ET_Subquery,
		SubBuilder:  subBuilder,
		DataTyp:     subBuilder.projectExprs[0].DataTyp,
		SubCtx:      subBuilder.rootCtx,
		SubqueryTyp: typ,
	}, err
}

func (b *Builder) bindToSelectList(selectExprs []*Ast, idx int, alias string) *Expr {
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
