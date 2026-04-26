package compute

import (
	"bytes"
	"fmt"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/huandu/go-clone"
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

	ET_Const // 新增统一的常量类型

	ET_Orderby
	ET_List
)

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
	BaseInfo
	ConstValue ConstValue // 常量值

	Typ      ET
	DataTyp  common.LType
	Index    uint64
	Children []*Expr

	// Info 存放类型特有数据
	Info any
}

// GetFuncInfo 返回函数信息，Typ 必须为 ET_Func
func (e *Expr) GetFuncInfo() *FunctionInfo {
	return e.Info.(*FunctionInfo)
}

// GetSubqueryInfo 返回子查询信息，Typ 必须为 ET_Subquery
func (e *Expr) GetSubqueryInfo() *SubqueryInfo {
	return e.Info.(*SubqueryInfo)
}

// GetJoinInfo 返回连接信息，Typ 必须为 ET_Join
func (e *Expr) GetJoinInfo() *JoinInfo {
	return e.Info.(*JoinInfo)
}

// GetTableInfo 返回表信息，Typ 必须为 ET_TABLE
func (e *Expr) GetTableInfo() *TableInfo {
	if ti, ok := e.Info.(*TableInfo); ok {
		return ti
	}
	return nil
}

// GetValuesListInfo 返回值列表信息，Typ 必须为 ET_ValuesList
func (e *Expr) GetValuesListInfo() *ValuesListInfo {
	return e.Info.(*ValuesListInfo)
}

// GetOrderByInfo 返回排序信息，Typ 必须为 ET_Orderby
func (e *Expr) GetOrderByInfo() *OrderByInfo {
	return e.Info.(*OrderByInfo)
}

// GetCTEInfo 返回 CTE 信息，Typ 必须为 ET_CTE
func (e *Expr) GetCTEInfo() *CTEInfo {
	return e.Info.(*CTEInfo)
}

// 列信息 - 用于列引用表达式
type BaseInfo struct {
	Database string // 数据库名
	Table    string // 表名
	Name     string // 列名
	Alias    string

	ColRef    ColumnBind   // 列引用(relationTag, columnPos)
	Depth     int          // 相关子查询深度(>0表示相关列)
	BelongCtx *BindContext // 所属的绑定上下文
}

func (b *BaseInfo) copy() BaseInfo {
	return BaseInfo{
		Database:  b.Database,
		Table:     b.Table,
		Name:      b.Name,
		Alias:     b.Alias,
		ColRef:    b.ColRef,
		Depth:     b.Depth,
		BelongCtx: b.BelongCtx,
	}
}

// 函数信息 - 用于函数和运算符表达式
type FunctionInfo struct {
	FunImpl  *Function     // 函数实现
	BindInfo *FunctionData // 函数绑定信息
}

func (f *FunctionInfo) copy() FunctionInfo {
	return FunctionInfo{
		FunImpl:  f.FunImpl,
		BindInfo: f.BindInfo,
	}
}

// 子查询信息 - 用于子查询表达式
type SubqueryInfo struct {
	// 子查询构建和执行
	SubBuilder  *Builder        // 子查询的构建器
	SubCtx      *BindContext    // 子查询的绑定上下文
	SubqueryTyp ET_SubqueryType // 子查询类型(标量/EXISTS/IN等)
}

func (s *SubqueryInfo) copy() SubqueryInfo {
	return SubqueryInfo{
		SubBuilder:  s.SubBuilder,
		SubCtx:      s.SubCtx,
		SubqueryTyp: s.SubqueryTyp,
	}
}

// 连接信息 - 用于连接表达式
type JoinInfo struct {
	// 连接相关
	JoinTyp ET_JoinType // 连接类型(交叉/左连接/内连接)
	On      *Expr       // 连接条件
}

func (j *JoinInfo) copy() JoinInfo {
	return JoinInfo{
		JoinTyp: j.JoinTyp,
		On:      j.On.copy(),
	}
}

// 表信息 - 用于表表达式
type TableInfo struct {
	// 表定义和约束
	TabEnt      *storage.CatalogEntry // 表目录条目
	ColName2Idx map[string]int        // 列名到索引的映射
	Constraints []*storage.Constraint // 表约束
}

func (t *TableInfo) copy() TableInfo {
	return TableInfo{
		TabEnt:      t.TabEnt,
		ColName2Idx: t.ColName2Idx,
		Constraints: t.Constraints,
	}
}

// 值列表信息 - 用于INSERT VALUES语句
type ValuesListInfo struct {
	// 值列表定义
	Types       []common.LType // 列类型
	Names       []string       // 列名
	Values      [][]*Expr      // 值列表
	ColName2Idx map[string]int // 列名到索引的映射（来自 TableInfo）
}

func (v *ValuesListInfo) copy() ValuesListInfo {
	values := make([][]*Expr, len(v.Values))
	for i, value := range v.Values {
		values[i] = make([]*Expr, len(value))
		for j, expr := range value {
			values[i][j] = expr.copy()
		}
	}
	return ValuesListInfo{
		Types:       v.Types,
		Names:       v.Names,
		Values:      values,
		ColName2Idx: v.ColName2Idx,
	}
}

// 排序信息 - 用于ORDER BY表达式
type OrderByInfo struct {
	// 排序相关
	Desc bool // 是否降序
}

func (o *OrderByInfo) copy() OrderByInfo {
	return OrderByInfo{
		Desc: o.Desc,
	}
}

// CTE信息 - 用于CTE表达式
type CTEInfo struct {
	// CTE相关
	CTEIndex uint64 // CTE索引
}

func (c *CTEInfo) copy() CTEInfo {
	return CTEInfo{
		CTEIndex: c.CTEIndex,
	}
}

func (e *Expr) equal(o *Expr) bool {
	if e == nil && o == nil {
		return true
	} else if e != nil && o != nil {
		if e.Typ != o.Typ {
			return false
		}
		if e.DataTyp != o.DataTyp {
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
		if e.Alias != o.Alias {
			return false
		}
		if e.ColRef != o.ColRef {
			return false
		}
		if e.Depth != o.Depth {
			return false
		}
		if !e.ConstValue.equal(o.ConstValue) {
			return false
		}
		// Type-specific comparisons
		switch e.Typ {
		case ET_Func:
			if e.GetFuncInfo().FunImpl._name != o.GetFuncInfo().FunImpl._name {
				return false
			}
			if e.GetFuncInfo().FunImpl._aggrType != o.GetFuncInfo().FunImpl._aggrType {
				return false
			}
		case ET_Join:
			if e.GetJoinInfo().JoinTyp != o.GetJoinInfo().JoinTyp {
				return false
			}
			if !e.GetJoinInfo().On.equal(o.GetJoinInfo().On) {
				return false
			}
		case ET_Subquery:
			if e.GetSubqueryInfo().SubqueryTyp != o.GetSubqueryInfo().SubqueryTyp {
				return false
			}
		case ET_Orderby:
			if e.GetOrderByInfo().Desc != o.GetOrderByInfo().Desc {
				return false
			}
		case ET_CTE:
			if e.GetCTEInfo().CTEIndex != o.GetCTEInfo().CTEIndex {
				return false
			}
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

	if e.Typ == ET_Func && e.GetFuncInfo().FunImpl == nil {
		panic("invalid fun in copy")
	}

	ret := &Expr{
		BaseInfo:   e.BaseInfo.copy(),
		Typ:        e.Typ,
		DataTyp:    e.DataTyp,
		Index:      e.Index,
		ConstValue: e.ConstValue.copy(),
	}
	// Copy type-specific Info
	switch e.Typ {
	case ET_Func:
		fi := e.GetFuncInfo()
		cp := fi.copy()
		ret.Info = &cp
	case ET_Subquery:
		si := e.GetSubqueryInfo()
		cp := si.copy()
		ret.Info = &cp
	case ET_Join:
		ji := e.GetJoinInfo()
		cp := ji.copy()
		ret.Info = &cp
	case ET_TABLE:
		ti := e.GetTableInfo()
		cp := ti.copy()
		ret.Info = &cp
	case ET_ValuesList:
		vli := e.GetValuesListInfo()
		cp := vli.copy()
		ret.Info = &cp
	case ET_Orderby:
		oi := e.GetOrderByInfo()
		cp := oi.copy()
		ret.Info = &cp
	case ET_CTE:
		ci := e.GetCTEInfo()
		cp := ci.copy()
		ret.Info = &cp
	}
	for _, child := range e.Children {
		ret.Children = append(ret.Children, child.copy())
	}
	return ret
}

func (e *Expr) String() string {
	opts := &ExplainOptions{}
	opts.SetDefaultValues()
	buf := &bytes.Buffer{}
	explainExpr(e, opts, buf)
	return buf.String()
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
	checkExprs(root.getOnConds()...)
	checkExprs(root.getAggs()...)
	checkExprs(root.getGroupBys()...)
	checkExprs(root.getOrderBys()...)
	if li, ok := root.Info.(*LimitOpInfo); ok {
		checkExprs(li.Limit)
	}
	for _, child := range root.Children {
		checkExprIsValid(child)
	}
}

func checkExprs(e ...*Expr) {
	for _, expr := range e {
		if expr == nil {
			continue
		}
		if expr.Typ == ET_Func && expr.GetFuncInfo().FunImpl._name == "" {
			panic("xxx")
		}
		if expr.Typ == ET_Func && expr.GetFuncInfo().FunImpl._name == FuncBetween {
			if len(expr.Children) != 3 {
				panic("invalid between")
			}
		}
		if expr.Typ == ET_Func && expr.GetFuncInfo().FunImpl == nil {
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
	ret = append(ret, root.getOnConds()...)
	for _, child := range root.Children {
		ret = append(ret, collectFilterExprs(child)...)
	}
	return ret
}

func splitExprByAnd(expr *Expr) []*Expr {
	if expr.Typ == ET_Func {
		if expr.GetFuncInfo().FunImpl._name == FuncAnd {
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
		if expr.GetFuncInfo().FunImpl._name == FuncOr {
			return append(splitExprByOr(expr.Children[0]), splitExprByOr(expr.Children[1])...)
		}
	}
	return []*Expr{expr.copy()}
}

func andExpr(a, b *Expr) *Expr {
	binder := FunctionBinder{}
	return binder.BindScalarFunc(FuncAnd, []*Expr{a, b}, IsOperator(FuncAnd))
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
	return binder.BindScalarFunc(FuncOr, []*Expr{a, b}, IsOperator(FuncOr))
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
		if expr.GetFuncInfo().FunImpl.IsFunction() {
			args := make([]*Expr, 0, len(expr.Children))
			for _, child := range expr.Children {
				newChild, yes := deceaseDepth(child)
				hasCorCol = hasCorCol || yes
				args = append(args, newChild)
			}
			return &Expr{
				Typ:        expr.Typ,
				ConstValue: NewStringConst(expr.ConstValue.String),
				DataTyp:    expr.DataTyp,
				Children:   args,
				Info: &FunctionInfo{
					FunImpl: expr.GetFuncInfo().FunImpl,
				},
			}, hasCorCol
		} else {
			switch GetOperatorType(expr.GetFuncInfo().FunImpl._name) {
			case OpTypeCompare, OpTypeLike, OpTypeLogical:
				left, leftHasCorr := deceaseDepth(expr.Children[0])
				hasCorCol = hasCorCol || leftHasCorr
				right, rightHasCorr := deceaseDepth(expr.Children[1])
				hasCorCol = hasCorCol || rightHasCorr
				return &Expr{
					Typ:        expr.Typ,
					ConstValue: NewStringConst(expr.GetFuncInfo().FunImpl._name),
					DataTyp:    expr.DataTyp,
					Children:   []*Expr{left, right},
					Info: &FunctionInfo{
						FunImpl: expr.GetFuncInfo().FunImpl,
					},
				}, hasCorCol
			default:
				panic(fmt.Sprintf("usp %v", expr.GetFuncInfo().FunImpl._name))
			}
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

	case ET_Const:
	case ET_Func:
	case ET_Orderby:
	default:
		// unknown type, skip
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
	case ET_Const:
	case ET_Func:
	default:
		// unknown type, skip
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
	case ET_Const:

	case ET_Func:
	default:
		// unknown type, skip
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

	case ET_Const:
		return true
	case ET_Func:
	default:
		// unknown type, skip
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
	case ET_Const:
	case ET_Func:
	default:
		// unknown type, skip
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

	case ET_Const:
	case ET_Func:
	case ET_Orderby:
	default:
		// unknown type, skip
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
	case ET_Const:
	case ET_Orderby:
	default:
		// unknown type, skip
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
			if !(e.ColRef.table() == root.Index && e.ColRef.column() < uint64(len(root.getScanColumns()))) {
				panic(fmt.Sprintf("no bind %v in scan %v", e.ColRef, root.Index))
			}
		} else if root.Typ == LOT_AggGroup {
			st := SourceType(e.ColRef.table())
			switch st {
			case ThisNode:
				if !(e.ColRef.table() == root.getAggTag() && e.ColRef.column() < uint64(len(root.getAggs()))) {
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
				if !(e.ColRef.table() == root.getAggTag() && e.ColRef.column() < uint64(len(root.getAggs()))) {
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
	checkColRefPosInExprs(root.getOnConds(), root)
	checkColRefPosInExprs(root.getAggs(), root)
	checkColRefPosInExprs(root.getGroupBys(), root)
	checkColRefPosInExprs(root.getOrderBys(), root)
	if li, ok := root.Info.(*LimitOpInfo); ok {
		checkColRefPosInExprs([]*Expr{li.Limit}, root)
	}
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
	case ET_Const:

	case ET_Func:

	default:
		// unknown type, skip
	}
	for _, child := range e.Children {
		collectTableRefers(child, set)
	}
}
