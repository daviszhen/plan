package main

import (
	"fmt"
)

func (b *Builder) bindExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var err error
	var child *Expr
	var id FuncId
	var ret *Expr
	if expr.Typ != AstTypeExpr {
		panic("need expr")
	}
	switch expr.Expr.ExprTyp {
	// binary
	case AstExprTypeAnd,
		AstExprTypeOr,
		AstExprTypeSub,
		AstExprTypeMul,
		AstExprTypeDiv,
		AstExprTypeEqual,
		AstExprTypeLike,
		AstExprTypeNotLike,
		AstExprTypeGreaterEqual,
		AstExprTypeGreater,
		AstExprTypeLess,
		AstExprTypeBetween:
		ret, err = b.bindBinaryExpr(ctx, iwc, expr, depth)
		if err != nil {
			return nil, err
		}

	case AstExprTypeCase:
		ret, err = b.bindCaseExpr(ctx, iwc, expr, depth)
		if err != nil {
			return nil, err
		}
	case AstExprTypeNumber:
		ret = &Expr{
			Typ: ET_IConst,
			DataTyp: ExprDataType{
				Typ: DataTypeInteger,
			},
			Ivalue: expr.Expr.Ivalue,
		}

	case AstExprTypeFNumber:
		ret = &Expr{
			Typ: ET_FConst,
			DataTyp: ExprDataType{
				Typ: DataTypeFloat64,
			},
			Fvalue: expr.Expr.Fvalue,
		}
	case AstExprTypeString:
		ret = &Expr{
			Typ: ET_SConst,
			DataTyp: ExprDataType{
				Typ: DataTypeVarchar,
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

		ret = &Expr{
			Typ:     ET_Column,
			DataTyp: bind.typs[colIdx],
			Table:   bind.alias,
			Name:    colName,
			ColRef:  [2]uint64{bind.index, uint64(colIdx)},
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
		for _, arg := range expr.Expr.Children {
			child, err = b.bindExpr(ctx, iwc, arg, depth)
			if err != nil {
				return nil, err
			}
			args = append(args, child)
		}

		id, err = GetFunctionId(name)
		if err != nil {
			return nil, err
		}

		ret = &Expr{
			Typ:      ET_Func,
			Svalue:   name,
			FuncId:   id,
			DataTyp:  InvalidExprDataType,
			Children: args,
		}

		//hard code for simplicity
		if id == DATE_ADD {
			ret.DataTyp = ExprDataType{
				Typ: DataTypeDate,
			}
		}

		if IsAgg(name) {
			b.aggs = append(b.aggs, ret)
			ret = &Expr{
				Typ:     ET_Column,
				DataTyp: ret.DataTyp,
				Table:   fmt.Sprintf("AggNode_%v", b.aggTag),
				Name:    expr.String(),
				ColRef:  [2]uint64{uint64(b.aggTag), uint64(len(b.aggs) - 1)},
				Depth:   0,
			}
		}
	case AstExprTypeDate:
		ret = &Expr{
			Typ: ET_DateConst,
			DataTyp: ExprDataType{
				Typ: DataTypeDate,
			},
			Svalue: expr.Expr.Svalue,
		}

	case AstExprTypeInterval:
		ret = &Expr{
			Typ: ET_IntervalConst,
			DataTyp: ExprDataType{
				Typ: DataTypeInterval,
			},
			Ivalue: expr.Expr.Ivalue,
			Svalue: expr.Expr.Svalue,
		}
	case AstExprTypeExists:
		child, err = b.bindExpr(ctx, iwc, expr.Expr.Children[0], depth)
		if err != nil {
			return nil, err
		}
		ret = &Expr{
			Typ:      ET_Exists,
			DataTyp:  ExprDataType{Typ: DataTypeBool},
			Children: []*Expr{child},
		}
	default:
		panic(fmt.Sprintf("usp expr type %d", expr.Expr.ExprTyp))
	}
	if len(expr.Expr.Alias.alias) != 0 {
		ret.Alias = expr.Expr.Alias.alias
	}
	return ret, err
}

func (b *Builder) bindBinaryExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var between *Expr
	var err error
	if expr.Expr.ExprTyp == AstExprTypeBetween {
		between, err = b.bindExpr(ctx, iwc, expr.Expr.Between, depth)
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
	if left.DataTyp.Typ != right.DataTyp.Typ {
		if left.DataTyp.Typ == DataTypeDecimal && right.DataTyp.Typ == DataTypeInteger ||
			left.DataTyp.Typ == DataTypeInteger && right.DataTyp.Typ == DataTypeDecimal {
			//integer op decimal
		} else if left.DataTyp.Typ == DataTypeInvalid || right.DataTyp.Typ == DataTypeInvalid {

		} else if right.Typ != ET_Subquery {
			//skip subquery
			panic(fmt.Sprintf("unmatch data type %d %d", left.DataTyp.Typ, right.DataTyp.Typ))
		}
	}

	var et ET
	var edt ExprDataType
	switch expr.Expr.ExprTyp {
	case AstExprTypeAnd:
		et = ET_And
		edt.Typ = DataTypeBool
	case AstExprTypeOr:
		et = ET_Or
		edt.Typ = DataTypeBool
	case AstExprTypeSub:
		et = ET_Sub
		edt.Typ = left.DataTyp.Typ
	case AstExprTypeMul:
		et = ET_Mul
		edt.Typ = left.DataTyp.Typ
	case AstExprTypeDiv:
		et = ET_Div
		edt.Typ = left.DataTyp.Typ
	case AstExprTypeEqual:
		et = ET_Equal
		edt.Typ = DataTypeBool
	case AstExprTypeLike:
		et = ET_Like
		edt.Typ = DataTypeBool
	case AstExprTypeNotLike:
		et = ET_NotLike
		edt.Typ = DataTypeBool
	case AstExprTypeGreaterEqual:
		et = ET_GreaterEqual
		edt.Typ = DataTypeBool
	case AstExprTypeGreater:
		et = ET_Greater
		edt.Typ = DataTypeBool
	case AstExprTypeLess:
		et = ET_Less
		edt.Typ = DataTypeBool
	case AstExprTypeBetween:
		et = ET_Between
		edt.Typ = DataTypeBool
	default:
		panic(fmt.Sprintf("usp binary type %d", expr.Expr.ExprTyp))
	}
	return &Expr{
		Typ:      et,
		DataTyp:  edt,
		Between:  between,
		Children: []*Expr{left, right},
	}, err
}

func (b *Builder) bindSubquery(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	subBuilder := NewBuilder()
	subBuilder.tag = b.GetTag()
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
	default:
		panic(fmt.Sprintf("usp %v", expr.Expr.SubqueryTyp))
	}
	return &Expr{
		Typ:         ET_Subquery,
		SubBuilder:  subBuilder,
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
		DataTyp: InvalidExprDataType,
		ColRef:  [2]uint64{uint64(b.projectTag), uint64(idx)},
		Alias:   alias,
	}
}

func (b *Builder) isInSelectList(alias string) int {
	if idx, ok := b.aliasMap[alias]; ok {
		return idx
	}
	return -1
}
