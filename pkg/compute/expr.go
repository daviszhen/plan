package compute

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
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

func (b *BaseInfo) Copy() BaseInfo {
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

func (f *FunctionInfo) Copy() FunctionInfo {
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

func (s *SubqueryInfo) Copy() SubqueryInfo {
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

func (j *JoinInfo) Copy() JoinInfo {
	return JoinInfo{
		JoinTyp: j.JoinTyp,
		On:      j.On.Copy(),
	}
}

// 表信息 - 用于表表达式
type TableInfo struct {
	// 表定义和约束
	TabEnt      *storage.CatalogEntry // 表目录条目
	ColName2Idx map[string]int        // 列名到索引的映射
	Constraints []*storage.Constraint // 表约束
}

func (t *TableInfo) Copy() TableInfo {
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

func (v *ValuesListInfo) Copy() ValuesListInfo {
	values := make([][]*Expr, len(v.Values))
	for i, value := range v.Values {
		values[i] = make([]*Expr, len(value))
		for j, expr := range value {
			values[i][j] = expr.Copy()
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

func (o *OrderByInfo) Copy() OrderByInfo {
	return OrderByInfo{
		Desc: o.Desc,
	}
}

// CTE信息 - 用于CTE表达式
type CTEInfo struct {
	// CTE相关
	CTEIndex uint64 // CTE索引
}

func (c *CTEInfo) Copy() CTEInfo {
	return CTEInfo{
		CTEIndex: c.CTEIndex,
	}
}

func (e *Expr) Equal(o *Expr) bool {
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
		if !e.ConstValue.Equal(o.ConstValue) {
			return false
		}
		// Type-specific comparisons
		switch e.Typ {
		case ET_Func:
			if e.FuncName() != o.FuncName() {
				return false
			}
			if e.GetFuncInfo().FunImpl.AggrType() != o.GetFuncInfo().FunImpl.AggrType() {
				return false
			}
		case ET_Join:
			if e.GetJoinInfo().JoinTyp != o.GetJoinInfo().JoinTyp {
				return false
			}
			if !e.GetJoinInfo().On.Equal(o.GetJoinInfo().On) {
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
			if !child.Equal(o.Children[i]) {
				return false
			}
		}
		return true
	} else {
		return false
	}
}

func (e *Expr) Copy() *Expr {
	if e == nil {
		return nil
	}

	if e.Typ == ET_Func && e.GetFuncInfo().FunImpl == nil {
		panic("invalid fun in copy")
	}

	ret := &Expr{
		BaseInfo:   e.BaseInfo.Copy(),
		Typ:        e.Typ,
		DataTyp:    e.DataTyp,
		Index:      e.Index,
		ConstValue: e.ConstValue.Copy(),
	}
	// Copy type-specific Info
	switch e.Typ {
	case ET_Func:
		fi := e.GetFuncInfo()
		cp := fi.Copy()
		ret.Info = &cp
	case ET_Subquery:
		si := e.GetSubqueryInfo()
		cp := si.Copy()
		ret.Info = &cp
	case ET_Join:
		ji := e.GetJoinInfo()
		cp := ji.Copy()
		ret.Info = &cp
	case ET_TABLE:
		ti := e.GetTableInfo()
		cp := ti.Copy()
		ret.Info = &cp
	case ET_ValuesList:
		vli := e.GetValuesListInfo()
		cp := vli.Copy()
		ret.Info = &cp
	case ET_Orderby:
		oi := e.GetOrderByInfo()
		cp := oi.Copy()
		ret.Info = &cp
	case ET_CTE:
		ci := e.GetCTEInfo()
		cp := ci.Copy()
		ret.Info = &cp
	}
	for _, child := range e.Children {
		ret.Children = append(ret.Children, child.Copy())
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

// Hash returns a 64-bit hash of the expression.
// Equal(a, b) implies Hash(a) == Hash(b).
func (e *Expr) Hash() uint64 {
	if e == nil {
		return 0
	}
	h := fnv.New64a()
	// hash type
	h.Write([]byte{byte(e.Typ)})
	// hash data type
	h.Write([]byte{byte(e.DataTyp.Id)})
	// hash base info fields relevant to identity
	writeString(h, e.Database)
	writeString(h, e.Table)
	writeString(h, e.Name)
	writeString(h, e.Alias)
	writeUint64(h, e.ColRef[0])
	writeUint64(h, e.ColRef[1])
	writeUint64(h, e.Index)
	// hash const value
	cvHash := e.ConstValue.Hash()
	writeUint64(h, cvHash)
	// type-specific hash
	switch e.Typ {
	case ET_Func:
		if e.GetFuncInfo().FunImpl != nil {
			writeString(h, e.FuncName())
			writeUint64(h, uint64(e.GetFuncInfo().FunImpl.FuncType()))
			writeUint64(h, uint64(e.GetFuncInfo().FunImpl.AggrType()))
		}
	case ET_Join:
		writeUint64(h, uint64(e.GetJoinInfo().JoinTyp))
	case ET_Subquery:
		writeUint64(h, uint64(e.GetSubqueryInfo().SubqueryTyp))
	case ET_Orderby:
		if e.GetOrderByInfo().Desc {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case ET_CTE:
		writeUint64(h, e.GetCTEInfo().CTEIndex)
	}
	// hash children
	for _, child := range e.Children {
		writeUint64(h, child.Hash())
	}
	return h.Sum64()
}

func writeString(h hash.Hash64, s string) {
	h.Write([]byte(s))
}

func writeUint64(h hash.Hash64, v uint64) {
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (i * 8))
	}
	h.Write(b[:])
}

func copyExprs(exprs ...*Expr) []*Expr {
	ret := make([]*Expr, 0)
	for _, expr := range exprs {
		ret = append(ret, expr.Copy())
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
		if expr.Typ == ET_Func && expr.FuncName() == "" {
			panic("xxx")
		}
		if expr.Typ == ET_Func && expr.IsBetween() {
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
	if expr.IsConjunction() {
		return append(splitExprByAnd(expr.Children[0]), splitExprByAnd(expr.Children[1])...)
	}
	return []*Expr{expr.Copy()}
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
	if expr.IsDisjunction() {
		return append(splitExprByOr(expr.Children[0]), splitExprByOr(expr.Children[1])...)
	}
	return []*Expr{expr.Copy()}
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
			left, leftHasCorr := deceaseDepth(expr.Children[0])
			hasCorCol = hasCorCol || leftHasCorr
			right, rightHasCorr := deceaseDepth(expr.Children[1])
			hasCorCol = hasCorCol || rightHasCorr
			return &Expr{
				Typ:        expr.Typ,
				ConstValue: NewStringConst(expr.FuncName()),
				DataTyp:    expr.DataTyp,
				Children:   []*Expr{left, right},
				Info: &FunctionInfo{
					FunImpl: expr.GetFuncInfo().FunImpl,
				},
			}, hasCorCol
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
}

func replaceColRef(e *Expr, bind, newBind ColumnBind) *Expr {
	return TransformExpressions(e, func(node *Expr) *Expr {
		if node.Typ == ET_Column && bind == node.ColRef {
			node.ColRef = newBind
		}
		return nil
	})
}

func restoreExpr(e *Expr, index uint64, realExprs []*Expr) *Expr {
	return TransformExpressions(e, func(node *Expr) *Expr {
		if node.Typ == ET_Column && index == node.ColRef[0] {
			return realExprs[node.ColRef[1]]
		}
		return nil
	})
}

func referTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	found := false
	ExprEnumerateNodes(e, func(node *Expr) {
		if found {
			return
		}
		if node.Typ == ET_Column && index == node.ColRef[0] {
			found = true
		}
	})
	return found
}

func onlyReferTo(e *Expr, index uint64) bool {
	if e == nil {
		return false
	}
	ok := true
	ExprEnumerateNodes(e, func(node *Expr) {
		if !ok {
			return
		}
		if node.Typ == ET_Column && index != node.ColRef[0] {
			ok = false
		}
	})
	return ok
}

func decideSide(e *Expr, leftTags, rightTags map[uint64]bool) int {
	var ret int
	ExprEnumerateNodes(e, func(node *Expr) {
		if node.Typ != ET_Column {
			return
		}
		if _, has := leftTags[node.ColRef[0]]; has {
			ret |= LeftSide
		}
		if _, has := rightTags[node.ColRef[0]]; has {
			ret |= RightSide
		}
	})
	return ret
}

func replaceColRef2(e *Expr, colRefToPos ColumnBindPosMap, st SourceType) *Expr {
	return TransformExpressions(e, func(node *Expr) *Expr {
		if node.Typ == ET_Column {
			has, pos := colRefToPos.pos(node.ColRef)
			if has {
				node.ColRef[0] = uint64(st)
				node.ColRef[1] = uint64(pos)
			}
		}
		return nil
	})
}

func replaceColRef3(es []*Expr, colRefToPos ColumnBindPosMap, st SourceType) {
	for _, e := range es {
		replaceColRef2(e, colRefToPos, st)
	}
}

func collectColRefs(e *Expr, set ColumnBindSet) {
	ExprEnumerateNodes(e, func(node *Expr) {
		if node.Typ == ET_Column {
			set.insert(node.ColRef)
		}
	})
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
	ExprEnumerateNodes(e, func(node *Expr) {
		if node.Typ != ET_Column {
			return
		}
		if root.Typ == LOT_Scan {
			if !(node.ColRef.table() == root.Index && node.ColRef.column() < uint64(len(root.getScanColumns()))) {
				panic(fmt.Sprintf("no bind %v in scan %v", node.ColRef, root.Index))
			}
		} else if root.Typ == LOT_AggGroup {
			st := SourceType(node.ColRef.table())
			switch st {
			case ThisNode:
				if !(node.ColRef.table() == root.getAggTag() && node.ColRef.column() < uint64(len(root.getAggs()))) {
					panic(fmt.Sprintf("no bind %v in scan %v", node.ColRef, root.Index))
				}
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if node.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", node.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if node.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", node.ColRef))
				}
			default:
				if !(node.ColRef.table() == root.getAggTag() && node.ColRef.column() < uint64(len(root.getAggs()))) {
					panic(fmt.Sprintf("no bind %v in scan %v", node.ColRef, root.Index))
				}
			}
		} else {
			st := SourceType(node.ColRef.table())
			switch st {
			case ThisNode:
				panic(fmt.Sprintf("bind %v exists", node.ColRef))
			case LeftChild:
				if len(root.Children) < 1 || root.Children[0] == nil {
					panic("no child")
				}
				binds := root.Children[0].ColRefToPos.sortByColumnBind()
				if node.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in child", node.ColRef))
				}
			case RightChild:
				if len(root.Children) < 2 || root.Children[1] == nil {
					panic("no right child")
				}
				binds := root.Children[1].ColRefToPos.sortByColumnBind()
				if node.ColRef.column() >= uint64(len(binds)) {
					panic(fmt.Sprintf("no bind %v in right child", node.ColRef))
				}
			default:
				panic(fmt.Sprintf("no source type %d", st))
			}
		}
	})
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
	ExprEnumerateNodes(e, func(node *Expr) {
		if node.Typ == ET_Column {
			set.insert(node.ColRef[0])
		}
	})
}

// ============================================================
// Expr property helpers — convenience queries for function type
// ============================================================

// FuncName returns the function name for ET_Func expressions, or empty string otherwise.
func (e *Expr) FuncName() string {
	if e == nil {
		return ""
	}
	if e.Typ == ET_Func && e.Info != nil {
		fi := e.GetFuncInfo()
		if fi != nil && fi.FunImpl != nil {
			return fi.FunImpl._name
		}
	}
	return ""
}

// HasValidFuncName returns true if the expression is a valid ET_Func with a non-empty name.
func (e *Expr) HasValidFuncName() bool {
	return e.FuncName() != ""
}

// ReturnType returns the data type of the expression.
func (e *Expr) ReturnType() common.LType {
	if e == nil {
		return common.LType{}
	}
	return e.DataTyp
}

// isFunc returns true if the expression is ET_Func with a valid function.
func (e *Expr) isFunc() bool {
	if e == nil || e.Typ != ET_Func || e.Info == nil {
		return false
	}
	fi := e.GetFuncInfo()
	return fi != nil && fi.FunImpl != nil
}

// IsComparison returns true if the expression is a comparison operator
// (=, <>, >, >=, <, <=, like, not like, in, not in, exists, not exists).
func (e *Expr) IsComparison() bool {
	if !e.isFunc() || !e.GetFuncInfo().FunImpl.IsOperator() {
		return false
	}
	switch GetOperatorType(e.FuncName()) {
	case OpTypeCompare, OpTypeLike:
		return true
	}
	return false
}

// IsLogicalOp returns true if the expression is a logical operator (and, or).
func (e *Expr) IsLogicalOp() bool {
	if !e.isFunc() {
		return false
	}
	return GetOperatorType(e.FuncName()) == OpTypeLogical
}

// IsConjunction returns true if the expression is an AND conjunction.
func (e *Expr) IsConjunction() bool {
	if !e.isFunc() {
		return false
	}
	return e.FuncName() == FuncAnd
}

// IsDisjunction returns true if the expression is an OR disjunction.
func (e *Expr) IsDisjunction() bool {
	if !e.isFunc() {
		return false
	}
	return e.FuncName() == FuncOr
}

// IsEqualOrIn returns true if the expression is an equality or IN operator.
func (e *Expr) IsEqualOrIn() bool {
	if !e.isFunc() {
		return false
	}
	name := e.FuncName()
	return name == FuncEqual || name == FuncIn
}

// IsInOrNotIn returns true if the expression is IN or NOT IN.
func (e *Expr) IsInOrNotIn() bool {
	if !e.isFunc() {
		return false
	}
	name := e.FuncName()
	return name == FuncIn || name == FuncNotIn
}

// IsBetween returns true if the expression is a BETWEEN operator.
func (e *Expr) IsBetween() bool {
	if !e.isFunc() {
		return false
	}
	return e.FuncName() == FuncBetween
}

// IsCaseExpr returns true if the expression is a CASE expression.
func (e *Expr) IsCaseExpr() bool {
	if !e.isFunc() {
		return false
	}
	return e.FuncName() == FuncCase
}

// IsCastExpr returns true if the expression is a CAST expression.
func (e *Expr) IsCastExpr() bool {
	if !e.isFunc() {
		return false
	}
	return e.FuncName() == FuncCast
}

// IsLikeExpr returns true if the expression is a LIKE or NOT LIKE operator.
func (e *Expr) IsLikeExpr() bool {
	if !e.isFunc() {
		return false
	}
	name := e.FuncName()
	return name == FuncLike || name == FuncNotLike
}

// IsNotIn returns true if the expression is a NOT IN operator.
func (e *Expr) IsNotIn() bool {
	if !e.isFunc() {
		return false
	}
	return e.FuncName() == FuncNotIn
}

// IsExistsExpr returns true if the expression is EXISTS or NOT EXISTS.
func (e *Expr) IsExistsExpr() bool {
	if !e.isFunc() {
		return false
	}
	name := e.FuncName()
	return name == FuncExists || name == FuncNotExists
}

// ============================================================
// Convenience matcher constructors
// ============================================================

// ComparisonOpMatcherInstance matches comparison operators
// (=, <>, >, >=, <, <=, like, not like, in, not in, exists).
func NewComparisonOpMatcher() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(
			FuncEqual, FuncNotEqual, FuncGreater, FuncGreaterEqual,
			FuncLess, FuncLessEqual, FuncLike, FuncNotLike,
			FuncIn, FuncNotIn, FuncExists, FuncNotExists,
		),
	}
}

// NewLogicalMatcher matches logical operators (and, or).
func NewLogicalMatcher() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncAnd, FuncOr),
	}
}

// NewConjunctionMatcher matches AND expressions.
func NewConjunctionMatcher() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncAnd},
	}
}

// NewDisjunctionMatcher matches OR expressions.
func NewDisjunctionMatcher() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: &SpecificFuncNameMatcher{Name: FuncOr},
	}
}

// NewInOrNotInMatcher matches IN or NOT IN expressions.
func NewInOrNotInMatcher() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncIn, FuncNotIn),
	}
}

// NewEqualOrInMatcher matches expressions that are either FuncEqual or FuncIn.
func NewEqualOrInMatcher() ExprMatcher {
	return &FuncExprMatcher{
		FuncNameMatcher: NewManyFuncNameMatcher(FuncEqual, FuncIn),
	}
}
