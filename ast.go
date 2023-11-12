package main

import (
	"crypto/sha256"
	bin "encoding/binary"
	"fmt"
	"math"
	"strconv"
)

type AstType int

const (
	AstTypeWith = iota
	AstTypeSelect
	AstTypeFrom
	AstTypeWhere
	AstTypeGroupBy
	AstTypeHaving
	AstTypeLimit
	AstTypeExpr
	AstTypeTable
	AstTypeNumber
)

type AstExprType int

const (
	//operator
	AstExprTypeAdd = iota
	AstExprTypeSub
	AstExprTypeMul
	AstExprTypeDiv
	AstExprTypeEqual // =
	AstExprTypeAnd
	AstExprTypeOr
	AstExprTypeNot
	AstExprTypeLike // Lik

	//table
	AstExprTypeTable
	AstExprTypeColumn
	AstExprTypeJoin

	//literal
	AstExprTypeNumber
	AstExprTypeString
	AstExprTypeNull

	//composite
	AstExprTypeParen // ()
	AstExprTypeFunc
	AstExprTypeSubquery

	AstExprTypeOrderBy
)

type AstJoinType int

const (
	AstJoinTypeCross = iota
	AstJoinTypeLeft
)

type Ast struct {
	Typ AstType

	Expr struct {
		ExprTyp AstExprType

		Table   string
		Svalue  string
		Ivalue  int64
		Fvalue  float64
		Desc    bool        // in orderby
		JoinTyp AstJoinType // join
		Alias   string

		Children []*Ast
		On       *Ast //JoinOn
	}

	With struct {
		Exprs []*Ast
	}

	Select struct {
		SelectExprs []*Ast

		From struct {
			Tables *Ast
		}

		Where struct {
			Expr *Ast
		}

		GroupBy struct {
			Exprs []*Ast
		}

		Having struct {
			Expr *Ast
		}
	}

	OrderBy struct {
		Exprs []*Ast
	}

	Limit struct {
		Offset *Ast
		Count  *Ast
	}
}

func (a *Ast) Format(ctx *FormatCtx) {
	if a == nil || a.Typ != AstTypeExpr {
		return
	}

	switch a.Expr.ExprTyp {
	case AstExprTypeColumn:
		if a.Expr.Alias != "" {
			ctx.Write(a.Expr.Alias)
		} else {
			ctx.Write(a.Expr.Svalue)
		}
	case AstExprTypeFunc:
		funcName := a.Expr.Svalue
		ctx.Write(funcName)
		ctx.Write("(")
		for i, c := range a.Expr.Children {
			if i > 0 {
				ctx.Write(",")
			}
			ctx.Write(c.String())
		}
		ctx.Write(")")
	default:
		panic(fmt.Sprintf("usp expr type %d", a.Expr.ExprTyp))
	}
}

func (a *Ast) String() string {
	ctx := &FormatCtx{}
	a.Format(ctx)
	return ctx.String()
}

func (a *Ast) Hash() string {
	if a == nil {
		return ""
	}
	if a.Typ != AstTypeExpr {
		panic("usp hash")
	}
	hash := sha256.New()

	//expr type
	hash.Write(bin.BigEndian.AppendUint64(nil, uint64(a.Expr.ExprTyp)))
	//table
	hash.Write([]byte(a.Expr.Table))
	//svalue
	hash.Write([]byte(a.Expr.Svalue))
	//ivalue
	hash.Write(bin.BigEndian.AppendUint64(nil, uint64(a.Expr.Ivalue)))
	//fvalue
	hash.Write(bin.BigEndian.AppendUint64(nil, math.Float64bits(a.Expr.Fvalue)))
	//desc bool
	hash.Write(strconv.AppendBool(nil, a.Expr.Desc))
	//join type
	hash.Write(bin.BigEndian.AppendUint64(nil, uint64(a.Expr.JoinTyp)))
	//alias
	hash.Write([]byte(a.Expr.Alias))
	//children
	for _, c := range a.Expr.Children {
		hash.Write([]byte(c.Hash()))
	}
	// join on
	hash.Write([]byte(a.Expr.On.Hash()))
	return fmt.Sprintf("%x", hash.Sum(nil))
}

type InWhichClause int

const (
	IWC_SELECT InWhichClause = iota
	IWC_WHERE
	IWC_GROUP
	IWC_HAVING
	IWC_ORDER
	IWC_LIMIT
)

func inumber(i int64) *Ast {
	num := &Ast{Typ: AstTypeExpr}
	num.Expr.ExprTyp = AstExprTypeNumber
	num.Expr.Ivalue = i
	return num
}

func sstring(s string) *Ast {
	ss := &Ast{Typ: AstTypeExpr}
	ss.Expr.ExprTyp = AstExprTypeString
	ss.Expr.Svalue = s
	return ss
}

func column(name string) *Ast {
	col := &Ast{Typ: AstTypeExpr}
	col.Expr.ExprTyp = AstExprTypeColumn
	col.Expr.Svalue = name
	return col
}

func table(table string) *Ast {
	tab := &Ast{Typ: AstTypeExpr}
	tab.Expr.ExprTyp = AstExprTypeTable
	tab.Expr.Svalue = table
	return tab
}

func crossJoin(left, right *Ast) *Ast {
	join := &Ast{Typ: AstTypeExpr}
	join.Expr.ExprTyp = AstExprTypeJoin
	join.Expr.JoinTyp = AstJoinTypeCross
	join.Expr.Children = []*Ast{left, right}
	return join
}

func binary(typ AstExprType, left, right *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = typ
	ret.Expr.Children = []*Ast{left, right}
	return ret
}

func equal(left, right *Ast) *Ast {
	return binary(AstExprTypeEqual, left, right)
}

func and(left, right *Ast) *Ast {
	return binary(AstExprTypeAnd, left, right)
}

func like(left, right *Ast) *Ast {
	return binary(AstExprTypeLike, left, right)
}

func subquery(sub *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeSubquery
	ret.Expr.Children = []*Ast{sub}
	return ret
}

func function(name string, args ...*Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.Svalue = name
	ret.Expr.Children = args
	return ret
}

func orderby(expr *Ast, desc bool) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeOrderBy
	ret.Expr.Desc = desc
	ret.Expr.Children = []*Ast{expr}
	return ret
}
