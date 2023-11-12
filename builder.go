package main

import (
	"errors"
	"fmt"
)

type BindingType int

const (
	BT_BASE BindingType = iota
	BT_TABLE
	BT_DUMMY
	BT_CATALOG_ENTRY
)

func (bt BindingType) String() string {
	switch bt {
	case BT_BASE:
		return "base"
	case BT_TABLE:
		return "table"
	case BT_DUMMY:
		return "dummy"
	case BT_CATALOG_ENTRY:
		return "catalog_entry"
	default:
		panic(fmt.Sprintf("usp binding type %d", bt))
	}
}

type Binding struct {
	typ      BindingType
	database string
	alias    string
	index    uint64
	typs     []ExprDataType
	names    []string
	nameMap  map[string]int
}

func (b *Binding) Format(ctx *FormatCtx) {
	ctx.Writefln("%s.%s,%s,%d", b.database, b.alias, b.typ, b.index)
	for i, n := range b.names {
		ctx.Writeln(i, n, b.typs[i])
	}
}

func (b *Binding) Bind(table, column string, depth int) (*Expr, error) {
	if idx, ok := b.nameMap[column]; ok {
		exp := &Expr{
			Typ:     ET_Column,
			DataTyp: b.typs[idx],
			Table:   table,
			Name:    column,
			ColRef:  [2]uint64{b.index, uint64(idx)},
			Depth:   depth,
		}
		return exp, nil
	}
	return nil, fmt.Errorf("table %s does not have column %s", table, column)
}

func (b *Binding) HasColumn(column string) int {
	if idx, ok := b.nameMap[column]; ok {
		return idx
	}
	return -1
}

type BindContext struct {
	parent       *BindContext
	bindings     map[string]*Binding
	bindingsList []*Binding
}

func NewBindContext(parent *BindContext) *BindContext {
	return &BindContext{
		parent:   parent,
		bindings: make(map[string]*Binding, 0),
	}
}

func (bc *BindContext) Format(ctx *FormatCtx) {
	for _, b := range bc.bindings {
		b.Format(ctx)
	}
}

func (bc *BindContext) AddBinding(alias string, b *Binding) error {
	if _, ok := bc.bindings[alias]; ok {
		return errors.New("duplicate alias " + alias)
	}
	bc.bindingsList = append(bc.bindingsList, b)
	bc.bindings[alias] = b
	return nil
}

func (bc *BindContext) AddContext(obc *BindContext) error {
	for alias, ob := range obc.bindings {
		if _, ok := bc.bindings[alias]; ok {
			return errors.New("duplicate alias " + alias)
		}
		bc.bindings[alias] = ob
	}
	bc.bindingsList = append(bc.bindingsList, obc.bindingsList...)
	return nil
}

func (bc *BindContext) RemoveContext(obList []*Binding) {
	for _, ob := range obList {
		found := -1
		for i, b := range bc.bindingsList {
			if ob.alias == b.alias {
				found = i
				break
			}
		}
		if found > -1 {
			swap(bc.bindingsList, found, len(bc.bindingsList)-1)
			bc.bindingsList = pop(bc.bindingsList)
		}

		delete(bc.bindings, ob.alias)
	}
}

func (bc *BindContext) GetBinding(name string) (*Binding, error) {
	if b, ok := bc.bindings[name]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("table %s does not exists", name)
}

func (bc *BindContext) GetMatchingBinding(column string) (*Binding, int, error) {
	var ret *Binding
	var err error
	var depth int
	for _, b := range bc.bindings {
		if b.HasColumn(column) >= 0 {
			if ret != nil {
				return nil, 0, fmt.Errorf("Ambiguous column %s in %s or %s", column, ret.alias, b.alias)
			}
			ret = b
		}
	}

	//find it in parent context
	parDepth := -1
	for p := bc.parent; p != nil && ret == nil; p = p.parent {
		ret, parDepth, err = p.GetMatchingBinding(column)
		if err != nil {
			return nil, 0, err
		}
	}

	if ret == nil {
		return nil, 0, fmt.Errorf("no table has column %s", column)
	}
	if parDepth != -1 {
		depth = parDepth + 1
	}
	return ret, depth, nil
}

func (bc *BindContext) BindColumn(table, column string, depth int) (*Expr, error) {
	b, err := bc.GetBinding(table)
	if err != nil {
		return nil, err
	}

	return b.Bind(table, column, depth)
}

var _ Format = &Builder{}

type Builder struct {
	tag        int // relation tag
	projectTag int
	groupTag   int
	rootCtx    *BindContext

	//alias of select expr -> idx of select expr
	aliasMap map[string]int
	//hash of select expr -> idx of select expr
	projectMap map[string]int

	projectExprs []*Expr
	fromExpr     *Expr
	whereExpr    *Expr
	groupbyExprs []*Expr
	orderbyExprs []*Expr
	limitCount   *Expr

	names       []string //output column names
	columnCount int      // count of the select exprs (after expanding star)
}

func NewBuilder() *Builder {
	return &Builder{
		tag:        0,
		rootCtx:    NewBindContext(nil),
		aliasMap:   make(map[string]int),
		projectMap: make(map[string]int),
	}
}

func (b *Builder) Format(ctx *FormatCtx) {
	ctx.Writeln("builder:")
	if b == nil {
		return
	}
	if b.rootCtx != nil {
		ctx.Writeln("bindings:")
		b.rootCtx.Format(ctx)
		ctx.Writeln()
	}
	ctx.Writefln("tag %d", b.tag)
	ctx.Writefln("projectTag %d", b.projectTag)
	ctx.Writefln("groupTag %d", b.groupTag)

	ctx.Writeln("aliasMap:")
	WriteMap(ctx, b.aliasMap)

	ctx.Writeln("projectMap:")
	WriteMap(ctx, b.projectMap)

	ctx.Writefln("projectExprs:")
	WriteExprs(ctx, b.projectExprs)

	ctx.Writeln("fromExpr:")
	WriteExpr(ctx, b.fromExpr)

	ctx.Writeln("whereExpr:")
	WriteExpr(ctx, b.whereExpr)

	ctx.Writefln("groupbyExprs:")
	WriteExprs(ctx, b.groupbyExprs)

	ctx.Writefln("orderbyExprs:")
	WriteExprs(ctx, b.orderbyExprs)

	ctx.Writeln("limitCount:")
	WriteExpr(ctx, b.limitCount)

	ctx.Writeln("names:")
	ctx.WriteStrings(b.names)

	ctx.Writeln("columnCount:")
	ctx.Writefln("%d", b.columnCount)
}

func (b *Builder) String() string {
	ctx := &FormatCtx{}
	b.Format(ctx)
	return ctx.String()
}

func (b *Builder) GetTag() int {
	b.tag++
	return b.tag
}

func (b *Builder) buildSelect(sel *Ast, ctx *BindContext, depth int) error {
	var err error
	b.projectTag = b.GetTag()
	b.groupTag = b.GetTag()

	//from
	b.fromExpr, err = b.buildTable(sel.Select.From.Tables, ctx)
	if err != nil {
		return err
	}

	//TODO: expand star

	//select expr alias
	for i, expr := range sel.Select.SelectExprs {
		name := expr.String()
		if expr.Expr.Alias != "" {
			b.aliasMap[expr.Expr.Alias] = i
			name = expr.Expr.Alias
		}
		b.names = append(b.names, name)
		b.projectMap[expr.Hash()] = i
	}
	b.columnCount = len(sel.Select.SelectExprs)

	//where
	if sel.Select.Where.Expr != nil {
		b.whereExpr, err = b.bindExpr(ctx, IWC_WHERE, sel.Select.Where.Expr, depth)
		if err != nil {
			return err
		}
	}

	//order by,limit,distinct
	if len(sel.OrderBy.Exprs) != 0 {
		var retExpr *Expr
		for _, expr := range sel.OrderBy.Exprs {
			retExpr, err = b.bindExpr(ctx, IWC_ORDER, expr, depth)
			if err != nil {
				return err
			}
			b.orderbyExprs = append(b.orderbyExprs, retExpr)
		}
	}

	if sel.Limit.Count != nil {
		b.limitCount, err = b.bindExpr(ctx, IWC_LIMIT, sel.Limit.Count, depth)
		if err != nil {
			return err
		}
	}

	//group by
	if len(sel.Select.GroupBy.Exprs) != 0 {
		var retExpr *Expr
		for _, expr := range sel.Select.GroupBy.Exprs {
			retExpr, err = b.bindExpr(ctx, IWC_GROUP, expr, depth)
			if err != nil {
				return err
			}
			b.groupbyExprs = append(b.groupbyExprs, retExpr)
		}
	}

	//having

	//select exprs
	for _, expr := range sel.Select.SelectExprs {
		var retExpr *Expr
		retExpr, err = b.bindExpr(ctx, IWC_GROUP, expr, depth)
		if err != nil {
			return err
		}
		b.projectExprs = append(b.projectExprs, retExpr)
	}
	return err
}

func (b *Builder) buildFrom(table *Ast, ctx *BindContext) (*Expr, error) {
	return b.buildTable(table, ctx)
}

func (b *Builder) buildTable(table *Ast, ctx *BindContext) (*Expr, error) {
	if table == nil {
		panic("need table")
	}
	switch table.Expr.ExprTyp {
	case AstExprTypeTable:
		{
			db := "tpch"
			table := table.Expr.Svalue
			tpchCatalog := tpchCatalog()
			cta, err := tpchCatalog.Table(db, table)
			if err != nil {
				return nil, err
			}
			b := &Binding{
				typ:     BT_TABLE,
				alias:   table,
				index:   uint64(b.GetTag()),
				typs:    copy(cta.Types),
				names:   copy(cta.Columns),
				nameMap: make(map[string]int),
			}
			for idx, name := range b.names {
				b.nameMap[name] = idx
			}
			err = ctx.AddBinding(table, b)
			if err != nil {
				return nil, err
			}

			return &Expr{
				Typ:       ET_TABLE,
				Database:  db,
				Table:     table,
				BelongCtx: ctx,
			}, err
		}
	case AstExprTypeJoin:
		return b.buildJoinTable(table, ctx)
	default:
		return nil, fmt.Errorf("usp table type %d", table.Typ)
	}
	return nil, nil
}

func (b *Builder) buildJoinTable(table *Ast, ctx *BindContext) (*Expr, error) {
	leftCtx := NewBindContext(ctx)
	//left
	left, err := b.buildTable(table.Expr.Children[0], leftCtx)
	if err != nil {
		return nil, err
	}

	rightCtx := NewBindContext(ctx)
	//right
	right, err := b.buildTable(table.Expr.Children[1], rightCtx)
	if err != nil {
		return nil, err
	}

	switch table.Expr.JoinTyp {
	case AstJoinTypeCross:
	default:
		return nil, fmt.Errorf("usp join type %d", table.Expr.JoinTyp)
	}

	err = ctx.AddContext(leftCtx)
	if err != nil {
		return nil, err
	}

	err = ctx.AddContext(rightCtx)
	if err != nil {
		return nil, err
	}

	ret := &Expr{
		Typ:       ET_Join,
		BelongCtx: ctx,
		Children:  []*Expr{left, right},
	}

	return ret, err
}

//////////////////////////////////////////////
// create plan
//////////////////////////////////////////////

func (b *Builder) CreatePlan(ctx *BindContext, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	root, err = b.createFrom(b.fromExpr, root)
	if err != nil {
		return nil, err
	}

	//where
	if b.whereExpr != nil {
		root, err = b.createWhere(b.whereExpr, root)
	}

	return root, err
}

func (b *Builder) createFrom(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var left, right *LogicalOperator
	switch expr.Typ {
	case ET_TABLE:
		return &LogicalOperator{
			Typ:       LOT_Scan,
			Database:  expr.Database,
			Table:     expr.Table,
			BelongCtx: expr.BelongCtx,
		}, err
	case ET_Join:
		left, err = b.createFrom(expr.Children[0], root)
		if err != nil {
			return nil, err
		}
		right, err = b.createFrom(expr.Children[1], root)
		if err != nil {
			return nil, err
		}
		jt := LOT_JoinTypeCross
		switch expr.JoinTyp {
		case ET_JoinTypeCross:
			jt = LOT_JoinTypeCross
		case ET_JoinTypeLeft:
			jt = LOT_JoinTypeLeft
		default:
			panic(fmt.Sprintf("usp join type %d", jt))
		}
		return &LogicalOperator{
			Typ:      LOT_JOIN,
			JoinTyp:  jt,
			Children: []*LogicalOperator{left, right},
		}, err
	}
	return nil, nil
}

func (b *Builder) createWhere(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error

	//TODO:
	//1. find subquery and flatten subquery
	//1. all operators should be changed into (low priority)

	return &LogicalOperator{
		Typ:    LOT_Filter,
		Filter: expr,
	}, err
}

// if the expr has subquery, it returns logical plan of the subquery
// else, it returns nil
func (b *Builder) createSubquery(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var subRoot *LogicalOperator
	switch expr.Typ {
	case ET_Subquery:
		subBuilder := expr.SubBuilder
		subCtx := expr.SubCtx
		subRoot, err = subBuilder.CreatePlan(subCtx, nil)
		if err != nil {
			return nil, err
		}
		return subRoot, err
	default:

	}
	return nil, err
}
