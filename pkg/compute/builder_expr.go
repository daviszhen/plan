package compute

import (
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

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
