package compute

import (
	"fmt"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/huandu/go-clone"
	"github.com/xlab/treeprint"
)

type ET int

const (
	ET_Column     ET = iota //column
	ET_TABLE                //table
	ET_ValuesList           //for insert
	ET_Join                 //join
	ET_CTE

	ET_Func
	ET_Subquery

	ET_IConst    //integer
	ET_DecConst  //decimal
	ET_SConst    //string
	ET_FConst    //float
	ET_DateConst //date
	ET_IntervalConst
	ET_BConst // bool
	ET_NConst // null

	ET_Orderby
	ET_List
)

type ET_SubTyp int

const (
	//real function
	ET_Invalid ET_SubTyp = iota
	ET_SubFunc
	//operator
	ET_Add
	ET_Sub
	ET_Mul
	ET_Div
	ET_Equal
	ET_NotEqual
	ET_Greater
	ET_GreaterEqual
	ET_Less
	ET_LessEqual
	ET_And
	ET_Or
	ET_Not
	ET_Like
	ET_NotLike
	ET_Between
	ET_Case
	ET_CaseWhen
	ET_In
	ET_NotIn
	ET_Exists
	ET_NotExists
	ET_DateAdd
	ET_DateSub
	ET_Cast
	ET_Extract
	ET_Substring
)

func (et ET_SubTyp) String() string {
	switch et {
	case ET_Add:
		return "+"
	case ET_Sub:
		return "-"
	case ET_Mul:
		return "*"
	case ET_Div:
		return "/"
	case ET_Equal:
		return "="
	case ET_NotEqual:
		return "<>"
	case ET_Greater:
		return ">"
	case ET_GreaterEqual:
		return ">="
	case ET_Less:
		return "<"
	case ET_LessEqual:
		return "<="
	case ET_And:
		return "and"
	case ET_Or:
		return "or"
	case ET_Not:
		return "not"
	case ET_Like:
		return "like"
	case ET_NotLike:
		return "not like"
	case ET_Between:
		return "between"
	case ET_Case:
		return "case"
	case ET_In:
		return "in"
	case ET_NotIn:
		return "not in"
	case ET_Exists:
		return "exists"
	case ET_NotExists:
		return "not exists"
	case ET_DateAdd:
		return "date_add"
	case ET_DateSub:
		return "date_sub"
	case ET_Cast:
		return "cast"
	case ET_Extract:
		return "extract"
	case ET_Substring:
		return "substring"
	default:
		panic(fmt.Sprintf("usp %v", int(et)))
	}
}

func (et ET_SubTyp) isOperator() bool {
	switch et {
	case ET_Add:
		return true
	case ET_Sub:
		return true
	case ET_Mul:
		return true
	case ET_Div:
		return true
	case ET_Equal:
		return true
	case ET_NotEqual:
		return true
	case ET_Greater:
		return true
	case ET_GreaterEqual:
		return true
	case ET_Less:
		return true
	case ET_LessEqual:
		return true
	case ET_And:
		return true
	case ET_Or:
		return true
	case ET_Not:
		return true
	case ET_Like:
		return true
	case ET_NotLike:
		return true
	case ET_Between:
		return true
	case ET_Case:
		return true
	case ET_In:
		return true
	case ET_NotIn:
		return true
	case ET_Exists:
		return true
	case ET_NotExists:
		return true
	default:
		return false
	}
}

type ET_JoinType int

const (
	ET_JoinTypeCross ET_JoinType = iota
	ET_JoinTypeLeft
	ET_JoinTypeInner
)

type ET_SubqueryType int

const (
	ET_SubqueryTypeScalar ET_SubqueryType = iota
	ET_SubqueryTypeExists
	ET_SubqueryTypeNotExists
	ET_SubqueryTypeIn
	ET_SubqueryTypeNotIn
)

type Expr struct {
	Typ     ET
	SubTyp  ET_SubTyp
	DataTyp common.LType
	AggrTyp AggrType

	Children []*Expr

	Index       uint64
	Database    string
	Table       string     // table
	Name        string     // column
	ColRef      ColumnBind // relationTag, columnPos
	Depth       int        // > 0, correlated column
	Svalue      string
	Ivalue      int64
	Fvalue      float64
	Bvalue      bool
	Desc        bool        // in orderby
	JoinTyp     ET_JoinType // join
	Alias       string
	SubBuilder  *Builder     // builder for subquery
	SubCtx      *BindContext // context for subquery
	SubqueryTyp ET_SubqueryType
	CTEIndex    uint64

	BelongCtx   *BindContext // context for table and join
	On          *Expr        //JoinOn
	IsOperator  bool
	BindInfo    *FunctionData
	FunImpl     *FunctionV2
	Constraints []*storage.Constraint //for column def of create table
	//for insert ... values
	Types       []common.LType
	Names       []string
	Values      [][]*Expr
	ColName2Idx map[string]int
	TabEnt      *storage.CatalogEntry
}

func (e *Expr) equal(o *Expr) bool {
	if e == nil && o == nil {
		return true
	} else if e != nil && o != nil {
		if e.Typ != o.Typ {
			return false
		}
		if e.SubTyp != o.SubTyp {
			return false
		}
		if e.DataTyp != o.DataTyp {
			return false
		}
		if e.AggrTyp != o.AggrTyp {
			return false
		}
		if e.Index != o.Index {
			return false
		}
		if e.Database != o.Database {
			return false
		}
		if e.Table != o.Table {
			return false
		}
		if e.Name != o.Name {
			return false
		}
		if e.ColRef != o.ColRef {
			return false
		}
		if e.Depth != o.Depth {
			return false
		}
		if e.Svalue != o.Svalue {
			return false
		}
		if e.Ivalue != o.Ivalue {
			return false
		}
		if e.Fvalue != o.Fvalue {
			return false
		}
		if e.Bvalue != o.Bvalue {
			return false
		}
		if e.Desc != o.Desc {
			return false
		}
		if e.JoinTyp != o.JoinTyp {
			return false
		}
		if e.Alias != o.Alias {
			return false
		}
		if e.SubqueryTyp != o.SubqueryTyp {
			return false
		}
		if e.CTEIndex != o.CTEIndex {
			return false
		}
		if e.IsOperator != o.IsOperator {
			return false
		}
		if !e.On.equal(o.On) {
			return false
		}
		//children
		if len(e.Children) != len(o.Children) {
			return false
		}
		for i, child := range e.Children {
			if !child.equal(o.Children[i]) {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (e *Expr) copy() *Expr {
	if e == nil {
		return nil
	}

	if e.Typ == ET_Func && e.FunImpl == nil {
		panic("invalid fun in copy")
	}

	ret := &Expr{
		Typ:         e.Typ,
		SubTyp:      e.SubTyp,
		DataTyp:     e.DataTyp,
		AggrTyp:     e.AggrTyp,
		Index:       e.Index,
		Database:    e.Database,
		Table:       e.Table,
		Name:        e.Name,
		ColRef:      e.ColRef,
		Depth:       e.Depth,
		Svalue:      e.Svalue,
		Ivalue:      e.Ivalue,
		Fvalue:      e.Fvalue,
		Bvalue:      e.Bvalue,
		Desc:        e.Desc,
		JoinTyp:     e.JoinTyp,
		Alias:       e.Alias,
		SubBuilder:  e.SubBuilder,
		SubCtx:      e.SubCtx,
		SubqueryTyp: e.SubqueryTyp,
		CTEIndex:    e.CTEIndex,
		BelongCtx:   e.BelongCtx,
		On:          e.On.copy(),
		IsOperator:  e.IsOperator,
		BindInfo:    e.BindInfo,
		FunImpl:     e.FunImpl,
	}
	for _, child := range e.Children {
		ret.Children = append(ret.Children, child.copy())
	}
	return ret
}

func (e *Expr) Format(ctx *FormatCtx) {
	if e == nil {
		ctx.Write("")
		return
	}
	switch e.Typ {
	case ET_Column:
		//TODO:
		ctx.Writef("(%s.%s,%s,%v,%d)", e.Table, e.Name,
			e.DataTyp,
			e.ColRef, e.Depth)
	case ET_SConst:
		ctx.Writef("(%s,%s)", e.Svalue, e.DataTyp)
	case ET_IConst:
		ctx.Writef("(%d,%s)", e.Ivalue, e.DataTyp)
	case ET_DateConst:
		ctx.Writef("(%s,%s)", e.Svalue, e.DataTyp)
	case ET_IntervalConst:
		ctx.Writef("(%d %s,%s)", e.Ivalue, e.Svalue, e.DataTyp)
	case ET_BConst:
		ctx.Writef("(%v,%s)", e.Bvalue, e.DataTyp)
	case ET_FConst:
		ctx.Writef("(%v,%s)", e.Fvalue, e.DataTyp)
	case ET_DecConst:
		ctx.Writef("(%v,%s)", e.Svalue, e.DataTyp)
	case ET_TABLE:
		ctx.Writef("%s.%s", e.Database, e.Table)
	case ET_Join:
		e.Children[0].Format(ctx)
		typStr := ""
		switch e.JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.JoinTyp))
		}
		ctx.Writef(" %s ", typStr)
		e.Children[1].Format(ctx)

	case ET_Func:
		switch e.SubTyp {
		case ET_Invalid:
			panic("usp invalid expr")
		case ET_Between:
			e.Children[0].Format(ctx)
			ctx.Write(" between ")
			e.Children[1].Format(ctx)
			ctx.Write(" and ")
			e.Children[2].Format(ctx)
		case ET_Case:
			ctx.Write("case ")
			if e.Children[0] != nil {
				e.Children[0].Format(ctx)
				ctx.Writeln()
			}
			for i := 2; i < len(e.Children); i += 2 {
				ctx.Write(" when")
				e.Children[i].Format(ctx)
				ctx.Write(" then ")
				e.Children[i+1].Format(ctx)
				ctx.Writeln()
			}
			if e.Children[1] != nil {
				ctx.Write(" else ")
				e.Children[1].Format(ctx)
				ctx.Writeln()
			}
			ctx.Write("end")
		case ET_In, ET_NotIn:
			e.Children[0].Format(ctx)
			if e.SubTyp == ET_NotIn {
				ctx.Write(" not in ")
			} else {
				ctx.Write(" in ")
			}

			ctx.Write("(")
			for i := 1; i < len(e.Children); i++ {
				if i > 1 {
					ctx.Write(",")
				}
				e.Children[i].Format(ctx)
			}
			ctx.Write(")")
		case ET_Exists:
			ctx.Writef("exists(")
			e.Children[0].Format(ctx)
			ctx.Write(")")

		case ET_SubFunc:
			ctx.Writef("%s(", e.Svalue)
			for idx, child := range e.Children {
				if idx > 0 {
					ctx.Write(", ")
				}
				child.Format(ctx)
			}
			ctx.Write(")")
			ctx.Write("->")
			ctx.Writef("%s", e.DataTyp)
		default:
			//binary operator
			e.Children[0].Format(ctx)
			op := e.SubTyp.String()
			ctx.Writef(" %s ", op)
			e.Children[1].Format(ctx)
		}
	case ET_Subquery:
		ctx.Write("subquery(")
		ctx.AddOffset()
		// e.SubBuilder.Format(ctx)
		ctx.writeString(e.SubBuilder.String())
		ctx.RestoreOffset()
		ctx.Write(")")
	case ET_Orderby:
		e.Children[0].Format(ctx)
		if e.Desc {
			ctx.Write(" desc")
		}

	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}

func (e *Expr) Print(tree treeprint.Tree, meta string) {
	if e == nil {
		return
	}
	head := appendMeta(meta, e.DataTyp.String())
	switch e.Typ {
	case ET_Column:
		if e.Depth != 0 {
			tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,%v,%d)",
				e.Table, e.Name,
				e.ColRef, e.Depth))
		} else {
			tree.AddMetaNode(head, fmt.Sprintf("(%s.%s,%v)",
				e.Table, e.Name,
				e.ColRef))
		}

	case ET_SConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s)", e.Svalue))
	case ET_IConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%d)", e.Ivalue))
	case ET_DateConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s)", e.Svalue))
	case ET_IntervalConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%d %s)", e.Ivalue, e.Svalue))
	case ET_BConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%v)", e.Bvalue))
	case ET_FConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%v)", e.Fvalue))
	case ET_DecConst:
		tree.AddMetaNode(head, fmt.Sprintf("(%s %d %d)", e.Svalue, e.DataTyp.Width, e.DataTyp.Scale))
	case ET_TABLE:
		tree.AddNode(fmt.Sprintf("%s.%s", e.Database, e.Table))
	case ET_Join:
		typStr := ""
		switch e.JoinTyp {
		case ET_JoinTypeCross:
			typStr = "cross"
		case ET_JoinTypeLeft:
			typStr = "left"
		default:
			panic(fmt.Sprintf("usp join type %d", e.JoinTyp))
		}
		branch := tree.AddBranch(typStr)
		e.Children[0].Print(branch, "")
		e.Children[1].Print(branch, "")
	case ET_Func:
		var branch treeprint.Tree
		switch e.SubTyp {
		case ET_Invalid:
			panic("usp invalid expr")
		case ET_Between:
			branch = tree.AddMetaBranch(head, e.SubTyp.String())
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
			e.Children[2].Print(branch, "")
		case ET_Case:
			branch = tree.AddMetaBranch(head, e.SubTyp.String())
			if e.Children[0] != nil {
				e.Children[0].Print(branch, "")
			}
			when := branch.AddBranch("when")
			for i := 1; i < len(e.Children); i += 2 {
				e.Children[i].Print(when, "")
				e.Children[i+1].Print(when, "")
			}
			if e.Children[0] != nil {
				e.Children[0].Print(branch, "")
			}
		case ET_In, ET_NotIn:
			branch = tree.AddMetaBranch(head, e.SubTyp)
			for _, child := range e.Children {
				child.Print(branch, "")
			}
		case ET_Exists:
			branch = tree.AddMetaBranch(head, e.SubTyp)
			e.Children[0].Print(branch, "")
		case ET_SubFunc:
			dist := ""
			if e.AggrTyp == DISTINCT {
				dist = "(distinct)"
			}
			branch = tree.AddMetaBranch(head, fmt.Sprintf("%s %s", e.Svalue, dist))
			for _, child := range e.Children {
				child.Print(branch, "")
			}
		default:
			//binary operator
			branch = tree.AddMetaBranch(head, e.SubTyp)
			e.Children[0].Print(branch, "")
			e.Children[1].Print(branch, "")
		}
	case ET_Subquery:
		branch := tree.AddBranch("subquery(")
		e.SubBuilder.Print(branch)
		branch.AddNode(")")
	case ET_Orderby:
		e.Children[0].Print(tree, meta)

	default:
		panic(fmt.Sprintf("usp expr type %d", e.Typ))
	}
}

func (e *Expr) String() string {
	ctx := &FormatCtx{}
	e.Format(ctx)
	return ctx.String()
}
func copyExprs(exprs ...*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		ret = append(ret, expr.copy())
	}
	return ret
}

func findExpr(exprs []*Expr, fun func(expr *Expr) bool) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		if fun != nil && fun(expr) {
			ret = append(ret, expr)
		}
	}
	return ret
}

func checkExprIsValid(root *LogicalOperator) {
	if root == nil {
		return
	}
	checkExprs(root.Projects...)
	checkExprs(root.Filters...)
	checkExprs(root.OnConds...)
	checkExprs(root.Aggs...)
	checkExprs(root.GroupBys...)
	checkExprs(root.OrderBys...)
	checkExprs(root.Limit)
	for _, child := range root.Children {
		checkExprIsValid(child)
	}
}

func checkExprs(e ...*Expr) {
	for _, expr := range e {
		if expr == nil {
			continue
		}
		if expr.Typ == ET_Func && expr.SubTyp == ET_Invalid {
			panic("xxx")
		}
		if expr.Typ == ET_Func && expr.SubTyp == ET_Between {
			if len(expr.Children) != 3 {
				panic("invalid between")
			}
		}
		if expr.Typ == ET_Func && expr.FunImpl == nil {
			panic("invalid function")
		}
		if expr.DataTyp.Id == common.LTID_INVALID {
			panic("invalid logical type")
		}
	}
}

func collectFilterExprs(root *PhysicalOperator) []*Expr {
	if root == nil {
		return nil
	}
	ret := make([]*Expr, 0)
	ret = append(ret, root.Filters...)
	ret = append(ret, root.OnConds...)
	for _, child := range root.Children {
		ret = append(ret, collectFilterExprs(child)...)
	}
	return ret
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_And {
			return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
		}
	}
	return []*Expr{expr.copy()}
}

func splitExprsByAnd(exprs []*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, e := range exprs {
		if e == nil {
			continue
		}
		ret = append(ret, splitExprByAnd(e)...)
	}
	return ret
}

func splitExprByOr(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.SubTyp == ET_Or {
			return append(splitExprByOr(expr.Children[0]), splitExprByOr(expr.Children[1])...)
		}
	}
	return []*Expr{expr.copy()}
}

func andExpr(a, b *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(ET_And.String(), []*Expr{a, b}, ET_And, ET_And.isOperator())
}

func combineExprsByAnd(exprs ...*Expr) *Expr {
	if len(exprs) == 1 {
		return exprs[0]
	} else if len(exprs) == 2 {
		return andExpr(exprs[0], exprs[1])
	} else {
		return andExpr(
			combineExprsByAnd(exprs[:len(exprs)-1]...),
			combineExprsByAnd(exprs[len(exprs)-1]))
	}
}

func orExpr(a, b *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(ET_Or.String(), []*Expr{a, b}, ET_Or, ET_Or.isOperator())
}

func combineExprsByOr(exprs ...*Expr) *Expr {
	if len(exprs) == 1 {
		return exprs[0]
	} else if len(exprs) == 2 {
		return orExpr(exprs[0], exprs[1])
	} else {
		return orExpr(
			combineExprsByOr(exprs[:len(exprs)-1]...),
			combineExprsByOr(exprs[len(exprs)-1]))
	}
}

// removeCorrExprs remove correlated columns from exprs
// , returns non-correlated exprs and correlated exprs.
func removeCorrExprs(exprs []*Expr) ([]*Expr, []*Expr) {
	nonCorrExprs := make([]*Expr, 0)
	corrExprs := make([]*Expr, 0)
	for _, expr := range exprs {
		newExpr, hasCorCol := deceaseDepth(expr)
		if hasCorCol {
			corrExprs = append(corrExprs, newExpr)
		} else {
			nonCorrExprs = append(nonCorrExprs, newExpr)
		}
	}
	return nonCorrExprs, corrExprs
}

// deceaseDepth decrease depth of the column
// , returns new column ref and returns it is correlated or not.
func deceaseDepth(expr *Expr) (*Expr, bool) {
	hasCorCol := false
	switch expr.Typ {
	case ET_Column:
		if expr.Depth > 0 {
			expr.Depth--
			return expr, expr.Depth > 0
		}
		return expr, false

	case ET_Func:
		switch expr.SubTyp {
		case ET_And, ET_Equal, ET_Like, ET_GreaterEqual, ET_Less, ET_NotEqual:
			left, leftHasCorr := deceaseDepth(expr.Children[0])
			hasCorCol = hasCorCol || leftHasCorr
			right, rightHasCorr := deceaseDepth(expr.Children[1])
			hasCorCol = hasCorCol || rightHasCorr
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.SubTyp.String(),
				DataTyp:  expr.DataTyp,
				Children: []*Expr{left, right},
				FunImpl:  expr.FunImpl,
			}, hasCorCol
		case ET_SubFunc:
			args := make([]*Expr, 0, len(expr.Children))
			for _, child := range expr.Children {
				newChild, yes := deceaseDepth(child)
				hasCorCol = hasCorCol || yes
				args = append(args, newChild)
			}
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.Svalue,
				DataTyp:  expr.DataTyp,
				Children: args,
				FunImpl:  expr.FunImpl,
			}, hasCorCol
		default:
			panic(fmt.Sprintf("usp %v", expr.SubTyp))
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
}

func replaceColRef(e *Expr, bind, newBind ColumnBind) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		if bind == e.ColRef {
			e.ColRef = newBind
		}

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = replaceColRef(child, bind, newBind)
	}
	return e
}

func restoreExpr(e *Expr, index uint64, realExprs []*Expr) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		if index == e.ColRef[0] {
			e = realExprs[e.ColRef[1]]
		}
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = restoreExpr(child, index, realExprs)
	}
	return e
}

func referTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Column:
		return index == e.ColRef[0]
	case ET_SConst, ET_IConst, ET_DateConst, ET_FConst, ET_DecConst, ET_NConst, ET_BConst:

	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		if referTo(child, index) {
			return true
		}
	}
	return false
}

func onlyReferTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	switch e.Typ {
	case ET_Column:
		return index == e.ColRef[0]

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
		return true
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		if !onlyReferTo(child, index) {
			return false
		}
	}
	return true
}

func decideSide(e *Expr, leftTags, rightTags map[uint64]bool) int {
	var ret int
	switch e.Typ {
	case ET_Column:
		if _, has := leftTags[e.ColRef[0]]; has {
			ret |= LeftSide
		}
		if _, has := rightTags[e.ColRef[0]]; has {
			ret |= RightSide
		}
	case ET_SConst, ET_DateConst, ET_IConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		ret |= decideSide(child, leftTags, rightTags)
	}
	return ret
}

func copyExpr(e *Expr) *Expr {
	return clone.Clone(e).(*Expr)
}

func replaceColRef2(e *Expr, colRefToPos ColumnBindPosMap, st SourceType) *Expr {
	if e == nil {
		return nil
	}
	switch e.Typ {
	case ET_Column:
		has, pos := colRefToPos.pos(e.ColRef)
		if has {
			e.ColRef[0] = uint64(st)
			e.ColRef[1] = uint64(pos)
		}

	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Func:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for i, child := range e.Children {
		e.Children[i] = replaceColRef2(child, colRefToPos, st)
	}
	return e
}

func replaceColRef3(es []*Expr, colRefToPos ColumnBindPosMap, st SourceType) {
	for _, e := range es {
		replaceColRef2(e, colRefToPos, st)
	}
}

func collectColRefs(e *Expr, set ColumnBindSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		set.insert(e.ColRef)

	case ET_Func:
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:
	case ET_Orderby:
	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectColRefs(child, set)
	}
}

func collectColRefs2(set ColumnBindSet, exprs ...*Expr) {
	for _, expr := range exprs {
		collectColRefs(expr, set)
	}
}

func checkColRefPos(e *Expr, root *LogicalOperator) {
	if e == nil || root == nil {
		return
	}
	if e.Typ == ET_Column {
		if root.Typ == LOT_Scan {
			if !(e.ColRef.table() == root.Index && e.ColRef.column() < uint64(len(root.Columns))) {
				panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
			}
		} else if root.Typ == LOT_AggGroup {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				if !(e.ColRef.table() == root.Index2 && e.ColRef.column() < uint64(len(root.Aggs))) {
					panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
				}
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", e.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", e.ColRef))
				}
			default:
				if !(e.ColRef.table() == root.Index2 && e.ColRef.column() < uint64(len(root.Aggs))) {
					panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
				}
			}
		} else {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				panic(fmt.Sprintf("bind %v exists", e.ColRef))
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", e.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if e.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", e.ColRef))
				}
			default:
				panic(fmt.Sprintf("no source type %d", st))
			}
		}
	}
	for _, child := range e.Children {
		checkColRefPos(child, root)
	}
}

func checkColRefPosInExprs(es []*Expr, root *LogicalOperator) {
	for _, e := range es {
		checkColRefPos(e, root)
	}
}

func checkColRefPosInNode(root *LogicalOperator) {
	if root == nil {
		return
	}
	checkColRefPosInExprs(root.Projects, root)
	checkColRefPosInExprs(root.Filters, root)
	checkColRefPosInExprs(root.OnConds, root)
	checkColRefPosInExprs(root.Aggs, root)
	checkColRefPosInExprs(root.GroupBys, root)
	checkColRefPosInExprs(root.OrderBys, root)
	checkColRefPosInExprs([]*Expr{root.Limit}, root)
}

func collectTableRefersOfExprs(exprs []*Expr, set UnorderedSet) {
	for _, expr := range exprs {
		collectTableRefers(expr, set)
	}
}

func collectTableRefers(e *Expr, set UnorderedSet) {
	if e == nil {
		return
	}
	switch e.Typ {
	case ET_Column:
		index := e.ColRef[0]
		set.insert(index)
	case ET_SConst, ET_IConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_FConst, ET_NConst, ET_DecConst:

	case ET_Func:

	default:
		panic("usp")
	}
	for _, child := range e.Children {
		collectTableRefers(child, set)
	}
}
