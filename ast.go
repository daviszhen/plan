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
	AstTypeWith AstType = iota
	AstTypeSelect
	AstTypeFrom
	AstTypeWhere
	AstTypeGroupBy
	AstTypeHaving
	AstTypeLimit
	AstTypeExpr
	AstTypeTable
	AstTypeNumber
	AstTypeCTE
)

type AstExprType int

const (
	//table
	AstExprTypeTable AstExprType = iota
	AstExprTypeColumn
	AstExprTypeJoin

	//literal
	AstExprTypeNumber
	AstExprTypeFNumber
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

type AstExprSubType int

const (
	//real function
	AstExprSubTypeInvalid AstExprSubType = iota
	AstExprSubTypeFunc
	//operator
	AstExprSubTypeAdd
	AstExprSubTypeSub
	AstExprSubTypeMul
	AstExprSubTypeDiv
	AstExprSubTypeEqual        // =
	AstExprSubTypeNotEqual     // =
	AstExprSubTypeGreater      // =>
	AstExprSubTypeGreaterEqual // =>
	AstExprSubTypeLess         // <
	AstExprSubTypeBetween      // [a,b]
	AstExprSubTypeAnd
	AstExprSubTypeOr
	AstExprSubTypeNot
	AstExprSubTypeLike      // Like
	AstExprSubTypeNotLike   // Like
	AstExprSubTypeExists    // Like
	AstExprSubTypeNotExists // Like
	AstExprSubTypeCase
	AstExprSubTypeIn
	AstExprSubTypeNotIn
)

type AstJoinType int

const (
	AstJoinTypeCross AstJoinType = iota
	AstJoinTypeLeft
	AstJoinTypeInner
)

type AstSubqueryType int

const (
	AstSubqueryTypeScalar AstSubqueryType = iota
	AstSubqueryTypeExists
	AstSubqueryTypeNotExists
	AstSubqueryTypeFrom //TODO: fixme
)

type Ast struct {
	Typ AstType

	Expr struct {
		ExprTyp AstExprType
		SubTyp  AstExprSubType

		Table   string
		Svalue  string
		Ivalue  int64
		Fvalue  float64
		Desc    bool        // in orderby
		JoinTyp AstJoinType // join
		Alias   struct {
			alias string
			cols  []string
		}
		SubqueryTyp AstSubqueryType
		Between     *Ast // a part in a between b and c
		Kase        *Ast //for case when
		Els         *Ast
		When        []*Ast
		Distinct    bool // for distinct
		In          *Ast

		Children []*Ast
		On       *Ast //JoinOn
	}

	With struct {
		Ctes []*Ast
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
		if a.Expr.Alias.alias != "" {
			ctx.Write(a.Expr.Alias.alias)
		} else {
			ctx.Write(a.Expr.Svalue)
		}
	case AstExprTypeNumber:
		ctx.Writef("%d", a.Expr.Ivalue)
	case AstExprTypeFNumber:
		ctx.Writef("%f", a.Expr.Fvalue)
	case AstExprTypeString:
		ctx.Write(a.Expr.Svalue)
	case AstExprTypeFunc:
		switch a.Expr.SubTyp {
		case AstExprSubTypeSub, AstExprSubTypeMul, AstExprSubTypeDiv, AstExprSubTypeEqual:
			ctx.Write(a.Expr.Children[0].String())
			op := ""
			switch a.Expr.SubTyp {
			case AstExprSubTypeSub:
				op = "-"
			case AstExprSubTypeMul:
				op = "*"
			case AstExprSubTypeDiv:
				op = "/"
			case AstExprSubTypeEqual:
				op = "="
			default:
				panic("usp")
			}
			ctx.Write(fmt.Sprintf(" %v ", op))
			ctx.Write(a.Expr.Children[1].String())
		case AstExprSubTypeCase:
			ctx.Write("case ")
			if a.Expr.Kase != nil {
				ctx.Write(a.Expr.Kase.String())
			}
			for i := 0; i < len(a.Expr.When); i += 2 {
				j := i + 1
				if j >= len(a.Expr.When) {
					panic("miss then")
				}
				ctx.Write(" when ")
				ctx.Write(a.Expr.When[i].String())
				ctx.Write(" then ")
				ctx.Write(a.Expr.When[j].String())
			}
			if a.Expr.Els != nil {
				ctx.Write(" else ")
				ctx.Write(a.Expr.Els.String())
			}
		case AstExprSubTypeFunc:
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
			panic(fmt.Sprintf("usp %v", a.Expr.SubTyp))
		}
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
	hash.Write([]byte(a.Expr.Alias.alias))
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
	IWC_JOINON
)

func inumber(i int64) *Ast {
	num := &Ast{Typ: AstTypeExpr}
	num.Expr.ExprTyp = AstExprTypeNumber
	num.Expr.Ivalue = i
	return num
}

func fnumber(f float64) *Ast {
	num := &Ast{Typ: AstTypeExpr}
	num.Expr.ExprTyp = AstExprTypeFNumber
	num.Expr.Fvalue = f
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

func caseWhen(kase *Ast, els *Ast, when ...*Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeCase
	ret.Expr.Kase = kase
	ret.Expr.Els = els
	ret.Expr.When = when
	return ret
}

func withAlias(in *Ast, alias string) *Ast {
	if in.Typ == AstTypeExpr {
		in.Expr.Alias.alias = alias
	} else {
		panic("usp alias type ")
	}
	return in
}

func withAlias2(in *Ast, alias string, cols ...string) *Ast {
	if in.Typ == AstTypeExpr {
		in.Expr.Alias.alias = alias
		in.Expr.Alias.cols = cols
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

func leftJoin(left, right *Ast, on *Ast) *Ast {
	join := &Ast{Typ: AstTypeExpr}
	join.Expr.ExprTyp = AstExprTypeJoin
	join.Expr.JoinTyp = AstJoinTypeLeft
	join.Expr.Children = []*Ast{left, right}
	join.Expr.On = on
	return join
}

func binary(typ AstExprSubType, left, right *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = typ
	ret.Expr.Children = []*Ast{left, right}
	return ret
}

func add(left, right *Ast) *Ast {
	return binary(AstExprSubTypeAdd, left, right)
}

func div(left, right *Ast) *Ast {
	return binary(AstExprSubTypeDiv, left, right)
}

func equal(left, right *Ast) *Ast {
	return binary(AstExprSubTypeEqual, left, right)
}

func notEqual(left, right *Ast) *Ast {
	return binary(AstExprSubTypeNotEqual, left, right)
}

func in(in *Ast, right ...*Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeIn
	ret.Expr.In = in
	ret.Expr.Children = right
	return ret
}

func notIn(in *Ast, right ...*Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeNotIn
	ret.Expr.In = in
	ret.Expr.Children = right
	return ret
}

func greaterEqual(left, right *Ast) *Ast {
	return binary(AstExprSubTypeGreaterEqual, left, right)
}

func greater(left, right *Ast) *Ast {
	return binary(AstExprSubTypeGreater, left, right)
}

func less(left, right *Ast) *Ast {
	return binary(AstExprSubTypeLess, left, right)
}

func and(left, right *Ast) *Ast {
	return binary(AstExprSubTypeAnd, left, right)
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
	return binary(AstExprSubTypeOr, left, right)
}

func between(a, left, right *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeBetween
	ret.Expr.Between = a
	ret.Expr.Children = []*Ast{left, right}
	return ret
}

func like(left, right *Ast) *Ast {
	return binary(AstExprSubTypeLike, left, right)
}

func notlike(left, right *Ast) *Ast {
	return binary(AstExprSubTypeNotLike, left, right)
}

func mul(left, right *Ast) *Ast {
	return binary(AstExprSubTypeMul, left, right)
}

func sub(left, right *Ast) *Ast {
	return binary(AstExprSubTypeSub, left, right)
}

func exists(a *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeExists
	ret.Expr.Children = []*Ast{a}
	return ret
}

func notExists(a *Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeNotExists
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
	ret.Expr.SubTyp = AstExprSubTypeFunc
	ret.Expr.Svalue = name
	ret.Expr.Children = args
	return ret
}

func functionDistinct(name string, args ...*Ast) *Ast {
	ret := &Ast{Typ: AstTypeExpr}
	ret.Expr.ExprTyp = AstExprTypeFunc
	ret.Expr.SubTyp = AstExprSubTypeFunc
	ret.Expr.Distinct = true
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
