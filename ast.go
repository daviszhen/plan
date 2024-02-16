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
	AstExprTypeEqual        // =
	AstExprTypeGreaterEqual // =>
	AstExprTypeLess         // <
	AstExprTypeBetween      // [a,b]
	AstExprTypeAnd
	AstExprTypeOr
	AstExprTypeNot
	AstExprTypeLike   // Like
	AstExprTypeExists // Like

	//table
	AstExprTypeTable
	AstExprTypeColumn
	AstExprTypeJoin

	//literal
	AstExprTypeNumber
	AstExprTypeString
	AstExprTypeNull
	AstExprTypeDate
	AstExprTypeInterval

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

type AstSubqueryType int

const (
	AstSubqueryTypeScalar = iota
	AstSubqueryTypeExists
)

type Ast struct {
	Typ AstType

	Expr struct {
		ExprTyp AstExprType

		Table       string
		Svalue      string
		Ivalue      int64
		Fvalue      float64
		Desc        bool        // in orderby
		JoinTyp     AstJoinType // join
		Alias       string
		SubqueryTyp AstSubqueryType
		Between     *Ast // a part in a between b and c

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

func column2(table, name string) *Ast {
	col := &Ast{Typ: AstTypeExpr}
	col.Expr.ExprTyp = AstExprTypeColumn
	col.Expr.Table = table
	col.Expr.Svalue = name
	return col
}

func withAlias(in *Ast, alias string) *Ast {
	if in.Typ == AstTypeExpr {
		in.Expr.Alias = alias
	} else {
		panic("usp alias type ")
	}
	return in
}

func date(d string) *Ast {
	col := &Ast{Typ: AstTypeExpr}
	col.Expr.ExprTyp = AstExprTypeDate
	col.Expr.Svalue = d
	return col
}

func astList(a ...*Ast) []*Ast {
	return a
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

func crossJoinList(a ...*Ast) *Ast {
	if len(a) == 1 {
		return a[0]
	} else if len(a) == 2 {
		return crossJoin(a[0], a[1])
	}
	return crossJoin(
		crossJoinList(a[:len(a)-1]...),
		a[len(a)-1])
}

func binary(typ AstExprType, left, right *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = typ
	ret.Expr.Children = []*Ast{left, right}
	return ret
}

func add(left, right *Ast) *Ast {
	return binary(AstExprTypeAdd, left, right)
}

func equal(left, right *Ast) *Ast {
	return binary(AstExprTypeEqual, left, right)
}

func greaterEqual(left, right *Ast) *Ast {
	return binary(AstExprTypeGreaterEqual, left, right)
}

func less(left, right *Ast) *Ast {
	return binary(AstExprTypeLess, left, right)
}

func and(left, right *Ast) *Ast {
	return binary(AstExprTypeAnd, left, right)
}

func andList(a ...*Ast) *Ast {
	if len(a) == 1 {
		return a[0]
	} else if len(a) == 2 {
		return and(a[0], a[1])
	}
	return and(andList(a[:len(a)-1]...), a[len(a)-1])
}

func or(left, right *Ast) *Ast {
	return binary(AstExprTypeOr, left, right)
}

func between(a, left, right *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeBetween
	ret.Expr.Between = a
	ret.Expr.Children = []*Ast{left, right}
	return ret
}

func like(left, right *Ast) *Ast {
	return binary(AstExprTypeLike, left, right)
}

func mul(left, right *Ast) *Ast {
	return binary(AstExprTypeMul, left, right)
}

func sub(left, right *Ast) *Ast {
	return binary(AstExprTypeSub, left, right)
}

func exists(a *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeExists
	ret.Expr.Children = []*Ast{a}
	return ret
}

func subquery(sub *Ast, subqueryTyp AstSubqueryType) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeSubquery
	ret.Expr.SubqueryTyp = subqueryTyp
	ret.Expr.Children = []*Ast{sub}
	return ret
}

func interval(val int, unit string) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeInterval
	ret.Expr.Ivalue = int64(val)
	ret.Expr.Svalue = unit
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
