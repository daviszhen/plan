package main

import (
	"fmt"
)

func (b *Builder) bindExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var err error
	var child *Expr
	var id FuncId
	if expr.Typ != AstTypeExpr {
		panic("need expr")
	}
	switch expr.Expr.ExprTyp {
	// binary
	case AstExprTypeAnd,
		AstExprTypeEqual,
		AstExprTypeLike:
		return b.bindBinaryExpr(ctx, iwc, expr, depth)

	case AstExprTypeNumber:
		return &Expr{
			Typ: ET_IConst,
			DataTyp: ExprDataType{
				Typ: DataTypeInteger,
			},
			Ivalue: expr.Expr.Ivalue,
		}, err

	case AstExprTypeString:
		return &Expr{
			Typ: ET_SConst,
			DataTyp: ExprDataType{
				Typ: DataTypeVarchar,
			},
			Svalue: expr.Expr.Svalue,
		}, err

	case AstExprTypeColumn:
		colName := expr.Expr.Svalue
		bind, d, err := ctx.GetMatchingBinding(colName)
		if err != nil {
			return nil, err
		}
		colIdx := bind.HasColumn(colName)
		switch iwc {
		case IWC_WHERE:
		case IWC_ORDER:
			selIdx := b.isInSelectList(colName)
			if selIdx >= 0 {
				return b.bindToSelectList(nil, selIdx, colName), err
			}
		case IWC_GROUP:
		case IWC_SELECT:
		default:
			panic(fmt.Sprintf("usp iwc %d", iwc))
		}
		return &Expr{
			Typ:     ET_Column,
			DataTyp: bind.typs[colIdx],
			Table:   bind.alias,
			Name:    colName,
			ColRef:  [2]uint64{bind.index, uint64(colIdx)},
			Depth:   d,
		}, err

	case AstExprTypeSubquery:
		return b.bindSubquery(ctx, iwc, expr, depth)
	case AstExprTypeOrderBy:
		child, err = b.bindExpr(ctx, iwc, expr.Expr.Children[0], depth)
		if err != nil {
			return nil, err
		}

		return &Expr{
			Typ:      ET_Orderby,
			DataTyp:  child.DataTyp,
			Desc:     expr.Expr.Desc,
			Children: []*Expr{child},
		}, err
	case AstExprTypeFunc:

		args := make([]*Expr, 0)
		for _, arg := range expr.Expr.Children {
			child, err = b.bindExpr(ctx, iwc, arg, depth)
			if err != nil {
				return nil, err
			}
			args = append(args, child)
		}
		name := expr.Expr.Svalue
		id, err = GetFunctionId(name)
		if err != nil {
			return nil, err
		}

		ret := &Expr{
			Typ:      ET_Func,
			Svalue:   name,
			FuncId:   id,
			DataTyp:  InvalidExprDataType,
			Children: args,
		}

		if IsAgg(name) {
			b.aggs = append(b.aggs, ret)
			ret = &Expr{
				Typ:     ET_Column,
				DataTyp: ret.DataTyp,
				Table:   fmt.Sprintf("AggNode_%v", b.groupTag),
				Name:    expr.String(),
				ColRef:  [2]uint64{uint64(b.groupTag), uint64(len(b.aggs) - 1)},
				Depth:   0,
			}
		}
		return ret, err
	default:
		panic(fmt.Sprintf("usp expr type %d", expr.Expr.ExprTyp))
	}
	return nil, err
}

func (b *Builder) bindBinaryExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	left, err := b.bindExpr(ctx, iwc, expr.Expr.Children[0], depth)
	if err != nil {
		return nil, err
	}
	right, err := b.bindExpr(ctx, iwc, expr.Expr.Children[1], depth)
	if err != nil {
		return nil, err
	}
	if left.DataTyp.Typ != right.DataTyp.Typ {
		//skip subquery
		if right.Typ != ET_Subquery {
			panic(fmt.Sprintf("unmatch data type %d %d", left.DataTyp.Typ, right.DataTyp.Typ))
		}
	}

	var et ET
	var edt ExprDataType
	switch expr.Expr.ExprTyp {
	case AstExprTypeAnd:
		et = ET_And
		edt.Typ = DataTypeBool
	case AstExprTypeEqual:
		et = ET_Equal
		edt.Typ = DataTypeBool
	case AstExprTypeLike:
		et = ET_Like
		edt.Typ = DataTypeBool
	default:
		panic(fmt.Sprintf("usp binary type %d", expr.Expr.ExprTyp))
	}
	return &Expr{
		Typ:      et,
		DataTyp:  edt,
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
	return &Expr{
		Typ:        ET_Subquery,
		SubBuilder: subBuilder,
		SubCtx:     subBuilder.rootCtx,
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
