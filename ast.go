package main

type AstType int

const (
	AstTypeWith = iota
	AstTypeSelect
	AstTypeFrom
	AstTypeWhere
	AstTypeGroupBy
	AstTypeHaving
	AstTypeOrderBy
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
	ret := &Ast{Typ: AstTypeOrderBy}
	ret.Expr.Desc = desc
	ret.Expr.Children = []*Ast{expr}
	return ret
}
