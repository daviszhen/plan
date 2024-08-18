package main

import (
	"errors"
	"fmt"

	"github.com/xlab/treeprint"
)

type BindingType int

const (
	BT_BASE BindingType = iota
	BT_TABLE
	BT_DUMMY
	BT_CATALOG_ENTRY
	BT_Subquery
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
	case BT_Subquery:
		return "subquery"
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

func (b *Binding) Print(tree treeprint.Tree) {
	tree = tree.AddMetaBranch(fmt.Sprintf("%s, %d", b.typ, b.index), fmt.Sprintf("%s.%s", b.database, b.alias))
	sub := tree.AddBranch("columns")
	for i, n := range b.names {
		sub.AddNode(fmt.Sprintf("%d %s %s", i, n, b.typs[i]))
	}
}

func (b *Binding) Bind(table, column string, depth int) (*Expr, error) {
	if idx, ok := b.nameMap[column]; ok {
		exp := &Expr{
			Typ:     ET_Column,
			DataTyp: b.typs[idx],
			Table:   table,
			Name:    column,
			ColRef:  ColumnBind{b.index, uint64(idx)},
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
	ctes         map[string]*Ast
	cteBindings  map[string]*Binding
}

func NewBindContext(parent *BindContext) *BindContext {
	return &BindContext{
		parent:      parent,
		bindings:    make(map[string]*Binding, 0),
		ctes:        make(map[string]*Ast),
		cteBindings: make(map[string]*Binding),
	}
}

func (bc *BindContext) Format(ctx *FormatCtx) {
	for _, b := range bc.bindings {
		b.Format(ctx)
	}
}

func (bc *BindContext) Print(tree treeprint.Tree) {
	for _, b := range bc.bindings {
		b.Print(tree)
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

func (bc *BindContext) GetMatchingBinding(table, column string) (*Binding, int, error) {
	var ret *Binding
	var err error
	var depth int
	if len(table) == 0 {
		for _, b := range bc.bindings {
			if b.HasColumn(column) >= 0 {
				if ret != nil {
					return nil, 0, fmt.Errorf("Ambiguous column %s in %s or %s", column, ret.alias, b.alias)
				}
				ret = b
			}
		}
	} else {
		if b, has := bc.bindings[table]; has {
			ret = b
		}
	}

	//find it in parent context
	parDepth := -1
	for p := bc.parent; p != nil && ret == nil; p = p.parent {
		ret, parDepth, err = p.GetMatchingBinding(table, column)
		if err != nil {
			return nil, 0, err
		}
	}

	if ret == nil {
		return nil, 0, fmt.Errorf("no table find column %s", column)
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

func (bc *BindContext) GetCteBinding(name string) *Binding {
	if b, ok := bc.cteBindings[name]; ok {
		return b
	} else {
		return nil
	}
}

var _ Format = &Builder{}

type Builder struct {
	tag        *int // relation tag
	projectTag int
	groupTag   int
	aggTag     int
	rootCtx    *BindContext
	alias      string //for subquery

	//alias of select expr -> idx of select expr
	aliasMap map[string]int
	//hash of select expr -> idx of select expr
	projectMap map[string]int

	projectExprs []*Expr
	fromExpr     *Expr
	whereExpr    *Expr
	aggs         []*Expr
	groupbyExprs []*Expr
	havingExpr   *Expr
	orderbyExprs []*Expr
	limitCount   *Expr

	names       []string //output column names
	columnCount int      // count of the select exprs (after expanding star)
}

func NewBuilder() *Builder {
	return &Builder{
		tag:        new(int),
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
	ctx.Writefln("aggTag %d", b.aggTag)

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

func (b *Builder) Print(tree treeprint.Tree) {
	if b == nil {
		return
	}
	if b.rootCtx != nil {
		sub := tree.AddBranch("bindings:")
		b.rootCtx.Print(sub)
	}

	tree.AddNode(fmt.Sprintf("tag %d", b.tag))
	tree.AddNode(fmt.Sprintf("projectTag %d", b.projectTag))
	tree.AddNode(fmt.Sprintf("groupTag %d", b.groupTag))
	tree.AddNode(fmt.Sprintf("aggTag %d", b.aggTag))

	sub := tree.AddBranch("aliasMap:")
	WriteMapTree(sub, b.aliasMap)

	sub = tree.AddBranch("projectMap:")
	WriteMapTree(sub, b.projectMap)

	sub = tree.AddBranch("projectExprs:")
	WriteExprsTree(sub, b.projectExprs)

	sub = tree.AddBranch("fromExpr:")
	WriteExprTree(sub, b.fromExpr)

	sub = tree.AddBranch("whereExpr:")
	WriteExprTree(sub, b.whereExpr)

	sub = tree.AddBranch("groupbyExprs:")
	WriteExprsTree(sub, b.groupbyExprs)

	sub = tree.AddBranch("orderbyExprs:")
	WriteExprsTree(sub, b.orderbyExprs)

	sub = tree.AddBranch("limitCount:")
	WriteExprTree(sub, b.limitCount)

	sub = tree.AddBranch("names:")
	sub.AddNode(b.names)

	sub = tree.AddBranch("columnCount:")
	sub.AddNode(fmt.Sprintf("%d", b.columnCount))
}

func (b *Builder) String() string {
	tree := treeprint.NewWithRoot("Builder:")
	b.Print(tree)
	return tree.String()
}

func (b *Builder) GetTag() int {
	*b.tag++
	return *b.tag
}

func (b *Builder) expandStar(expr *Ast) ([]*Ast, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.Typ == AstTypeExpr &&
		expr.Expr.ExprTyp == AstExprTypeColumn &&
		expr.Expr.Svalue == "*" {
		if len(b.rootCtx.bindings) == 0 {
			return nil, errors.New("no table")
		}
		ret := make([]*Ast, 0)
		for _, bind := range b.rootCtx.bindingsList {
			for _, name := range bind.names {
				ret = append(ret, column(name))
			}
		}
		return ret, nil
	}

	return []*Ast{expr}, nil
}

func (b *Builder) buildSelect(sel *Ast, ctx *BindContext, depth int) error {
	var err error
	b.projectTag = b.GetTag()
	b.groupTag = b.GetTag()
	b.aggTag = b.GetTag()

	if len(sel.With.Ctes) != 0 {
		_, err := b.buildWith(sel.With.Ctes, ctx, depth)
		if err != nil {
			return err
		}
	}

	//from
	b.fromExpr, err = b.buildTable(sel.Select.From.Tables, ctx, depth)
	if err != nil {
		return err
	}

	//expand star
	newSelectExprs := make([]*Ast, 0)
	for _, expr := range sel.Select.SelectExprs {
		ret, err := b.expandStar(expr)
		if err != nil {
			return err
		}
		newSelectExprs = append(newSelectExprs, ret...)
	}

	//select expr alias
	for i, expr := range newSelectExprs {
		name := expr.String()
		if expr.Expr.Alias.alias != "" {
			b.aliasMap[expr.Expr.Alias.alias] = i
			name = expr.Expr.Alias.alias
		}
		b.names = append(b.names, name)
		b.projectMap[expr.Hash()] = i
	}
	b.columnCount = len(newSelectExprs)

	//where
	if sel.Select.Where.Expr != nil {
		b.whereExpr, err = b.bindExpr(ctx, IWC_WHERE, sel.Select.Where.Expr, depth)
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
	if sel.Select.Having.Expr != nil {
		var retExpr *Expr
		retExpr, err = b.bindExpr(ctx, IWC_HAVING, sel.Select.Having.Expr, depth)
		if err != nil {
			return err
		}
		b.havingExpr = retExpr
	}
	//select exprs
	for _, expr := range newSelectExprs {
		var retExpr *Expr
		retExpr, err = b.bindExpr(ctx, IWC_SELECT, expr, depth)
		if err != nil {
			return err
		}
		b.projectExprs = append(b.projectExprs, retExpr)
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
	return err
}

func (b *Builder) findCte(name string, skip bool, ctx *BindContext) *Ast {
	if val, has := ctx.ctes[name]; has {
		if !skip {
			return val
		}
	}
	if ctx.parent != nil {
		return b.findCte(name, name == b.alias, ctx.parent)
	}
	return nil
}

func (b *Builder) addCte(cte *Ast, ctx *BindContext) error {
	name := cte.Expr.Alias.alias
	if _, has := ctx.ctes[name]; has {
		return errors.New(fmt.Sprintf("duplicate cte %s", name))
	}
	ctx.ctes[name] = cte
	return nil
}

func (b *Builder) buildWith(ctes []*Ast, ctx *BindContext, depth int) (*Expr, error) {
	for _, cte := range ctes {
		err := b.addCte(cte, ctx)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (b *Builder) buildTable(table *Ast, ctx *BindContext, depth int) (*Expr, error) {
	if table == nil {
		panic("need table")
	}
	switch table.Expr.ExprTyp {
	case AstExprTypeTable:
		db := "tpch"
		tableName := table.Expr.Svalue
		cte := b.findCte(tableName, tableName == b.alias, ctx)
		if cte != nil {
			//find cte binding
			cteBind := ctx.GetCteBinding(tableName)
			if cteBind != nil {
				panic("usp")
				//index := b.GetTag()
				//alias := tableName
				//if len(table.Expr.Alias.alias) != 0 {
				//	alias = table.Expr.Alias.alias
				//}
				//return &Expr{
				//	Typ:       ET_CTE,
				//	Index:     uint64(index),
				//	Database:  db,
				//	Table:     tableName,
				//	Alias:     alias,
				//	BelongCtx: ctx,
				//	CTEIndex:  cteBind.index,
				//}, nil
			} else {
				mockSub := withAlias2(
					subquery(cte.Expr.Children[0], AstSubqueryTypeFrom),
					cte.Expr.Alias.alias,
					cte.Expr.Alias.cols...)
				return b.buildTable(mockSub, ctx, depth)
			}
		}
		{
			tpchDB := tpchCatalog()
			cta, err := tpchDB.Table(db, tableName)
			if err != nil {
				return nil, err
			}
			alias := tableName
			if len(table.Expr.Alias.alias) != 0 {
				alias = table.Expr.Alias.alias
			}
			bind := &Binding{
				typ:     BT_TABLE,
				alias:   alias,
				index:   uint64(b.GetTag()),
				typs:    copyTo(cta.Types),
				names:   copyTo(cta.Columns),
				nameMap: make(map[string]int),
			}
			for idx, name := range bind.names {
				bind.nameMap[name] = idx
			}
			err = ctx.AddBinding(alias, bind)
			if err != nil {
				return nil, err
			}

			return &Expr{
				Typ:       ET_TABLE,
				Index:     bind.index,
				Database:  db,
				Table:     tableName,
				Alias:     alias,
				BelongCtx: ctx,
			}, err
		}
	case AstExprTypeJoin:
		return b.buildJoinTable(table, ctx, depth)
	case AstExprTypeSubquery:
		subBuilder := NewBuilder()
		subBuilder.tag = b.tag
		subBuilder.rootCtx.parent = ctx
		if len(table.Expr.Alias.alias) == 0 {
			return nil, errors.New("need alias for subquery")
		}
		subBuilder.alias = table.Expr.Alias.alias
		err := subBuilder.buildSelect(table.Expr.Children[0], subBuilder.rootCtx, 0)
		if err != nil {
			return nil, err
		}

		if len(subBuilder.projectExprs) == 0 {
			panic("subquery must have project list")
		}
		subTypes := make([]ExprDataType, 0)
		subNames := make([]string, 0)
		for i, expr := range subBuilder.projectExprs {
			subTypes = append(subTypes, expr.DataTyp)
			name := expr.Name
			if len(expr.Alias) != 0 {
				name = expr.Alias
			}
			if i < len(table.Expr.Alias.cols) {
				name = table.Expr.Alias.cols[i]
			}
			subNames = append(subNames, name)
		}

		bind := &Binding{
			typ:   BT_Subquery,
			alias: table.Expr.Alias.alias,
			//bind index of subquery is equal to the projectTag of subquery
			index:   uint64(subBuilder.projectTag),
			typs:    subTypes,
			names:   subNames,
			nameMap: make(map[string]int),
		}
		for idx, name := range bind.names {
			bind.nameMap[name] = idx
		}
		err = ctx.AddBinding(bind.alias, bind)
		if err != nil {
			return nil, err
		}

		return &Expr{
			Typ:        ET_Subquery,
			Index:      bind.index,
			Database:   "",
			Table:      bind.alias,
			SubBuilder: subBuilder,
			SubCtx:     subBuilder.rootCtx,
			BelongCtx:  ctx,
		}, err
	default:
		return nil, fmt.Errorf("usp table type %d", table.Typ)
	}
	return nil, nil
}

func (b *Builder) buildJoinTable(table *Ast, ctx *BindContext, depth int) (*Expr, error) {
	leftCtx := NewBindContext(ctx)
	//left
	left, err := b.buildTable(table.Expr.Children[0], leftCtx, depth)
	if err != nil {
		return nil, err
	}

	rightCtx := NewBindContext(ctx)
	//right
	right, err := b.buildTable(table.Expr.Children[1], rightCtx, depth)
	if err != nil {
		return nil, err
	}

	jt := ET_JoinTypeCross
	switch table.Expr.JoinTyp {
	case AstJoinTypeCross:
		jt = ET_JoinTypeCross
	case AstJoinTypeLeft:
		jt = ET_JoinTypeLeft
	case AstJoinTypeInner:
		jt = ET_JoinTypeInner
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

	var onExpr *Expr
	if table.Expr.On != nil {
		onExpr, err = b.bindExpr(ctx, IWC_JOINON, table.Expr.On, depth)
		if err != nil {
			return nil, err
		}
	}

	ret := &Expr{
		Typ:       ET_Join,
		JoinTyp:   jt,
		On:        onExpr,
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

	//aggregates or group by
	if len(b.aggs) > 0 || len(b.groupbyExprs) > 0 {
		root, err = b.createAggGroup(root)
	}

	//having
	if b.havingExpr != nil {
		root, err = b.createWhere(b.havingExpr, root)
	}

	//projects
	if len(b.projectExprs) > 0 {
		root, err = b.createProject(root)
	}

	//order bys
	if len(b.orderbyExprs) > 0 {
		root, err = b.createOrderby(root)
	}

	//limit
	if b.limitCount != nil {
		root = &LogicalOperator{
			Typ:      LOT_Limit,
			Limit:    b.limitCount,
			Children: []*LogicalOperator{root},
		}
	}

	return root, err
}

func (b *Builder) createFrom(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var left, right *LogicalOperator
	switch expr.Typ {
	case ET_TABLE:
		catalogOfTable, err := tpchCatalog().Table(expr.Database, expr.Table)
		if err != nil {
			return nil, err
		}
		return &LogicalOperator{
			Typ:       LOT_Scan,
			Index:     expr.Index,
			Database:  expr.Database,
			Table:     expr.Table,
			Alias:     expr.Alias,
			BelongCtx: expr.BelongCtx,
			Stats:     catalogOfTable.Stats.Copy(),
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
		case ET_JoinTypeCross, ET_JoinTypeInner:
			jt = LOT_JoinTypeInner
		case ET_JoinTypeLeft:
			jt = LOT_JoinTypeLeft
		default:
			panic(fmt.Sprintf("usp join type %d", jt))
		}

		onExpr := expr.On.copy()
		onExpr = distributeExpr(onExpr)

		return &LogicalOperator{
			Typ:      LOT_JOIN,
			JoinTyp:  jt,
			OnConds:  []*Expr{onExpr.copy()},
			Children: []*LogicalOperator{left, right},
		}, err
	case ET_Subquery:
		_, root, err = b.createSubquery(expr, root)
		if err != nil {
			return nil, err
		}
		return root, err
	default:
		panic("usp")
	}
	return nil, nil
}

func (b *Builder) createWhere(expr *Expr, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var newFilter *Expr

	//TODO:
	//1. find subquery and flatten subquery
	//1. all operators should be changed into (low priority)
	filters := splitExprByAnd(expr)
	copyExprs(filters...)
	var newFilters []*Expr
	for _, filter := range filters {
		newFilter, root, err = b.createSubquery(filter, root)
		if err != nil {
			return nil, err
		}
		newFilters = append(newFilters, newFilter)
	}

	newFilters = distributeExprs(newFilters...)

	return &LogicalOperator{
		Typ:      LOT_Filter,
		Filters:  copyExprs(newFilters...),
		Children: []*LogicalOperator{root},
	}, err
}

// if the expr find subquery, it flattens the subquery and replaces
// the expr.
func (b *Builder) createSubquery(expr *Expr, root *LogicalOperator) (*Expr, *LogicalOperator, error) {
	var err error
	var subRoot *LogicalOperator
	switch expr.Typ {
	case ET_Subquery:
		subBuilder := expr.SubBuilder
		subCtx := expr.SubCtx
		subRoot, err = subBuilder.CreatePlan(subCtx, nil)
		if err != nil {
			return nil, nil, err
		}
		//flatten subquery
		return b.apply(expr, root, subRoot)

	case ET_Func:
		switch expr.SubTyp {
		default:
			//binary operator
			var childExpr *Expr
			args := make([]*Expr, 0)
			for _, child := range expr.Children {
				childExpr, root, err = b.createSubquery(child, root)
				if err != nil {
					return nil, nil, err
				}
				args = append(args, childExpr)
			}
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.SubTyp.String(),
				DataTyp:  expr.DataTyp,
				Alias:    expr.Alias,
				FuncId:   expr.FuncId,
				Children: args,
			}, root, nil
		case ET_In, ET_NotIn:
			var childExpr *Expr
			args := make([]*Expr, 0)
			for _, child := range expr.Children {
				childExpr, root, err = b.createSubquery(child, root)
				if err != nil {
					return nil, nil, err
				}
				args = append(args, childExpr)
			}
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				DataTyp:  expr.DataTyp,
				Alias:    expr.Alias,
				FuncId:   expr.FuncId,
				Children: args,
			}, root, nil
		case ET_Exists, ET_NotExists:
			var childExpr *Expr
			childExpr, root, err = b.createSubquery(expr.Children[0], root)
			if err != nil {
				return nil, nil, err
			}

			return childExpr, root, nil

		case ET_SubFunc:
			var childExpr *Expr
			args := make([]*Expr, 0)
			for _, child := range expr.Children {
				childExpr, root, err = b.createSubquery(child, root)
				if err != nil {
					return nil, nil, err
				}
				args = append(args, childExpr)
			}
			return &Expr{
				Typ:      expr.Typ,
				SubTyp:   expr.SubTyp,
				Svalue:   expr.Svalue,
				FuncId:   expr.FuncId,
				DataTyp:  expr.DataTyp,
				Alias:    expr.Alias,
				Children: args,
			}, root, nil
		}
	case ET_Column:
		return expr, root, nil
	case ET_IConst, ET_SConst, ET_DateConst, ET_IntervalConst, ET_FConst:
		return expr, root, nil
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
	return nil, nil, err
}

// apply flattens subquery
// Based On Paper: Orthogonal Optimization of Subqueries and Aggregation
// make APPLY(expr,root,subRoot) algorithm
// expr: subquery expr
// root: root of the query that subquery belongs to
// subquery: root of the subquery
func (b *Builder) apply(expr *Expr, root, subRoot *LogicalOperator) (*Expr, *LogicalOperator, error) {
	if expr.Typ != ET_Subquery {
		panic("must be subquery")
	}

	corrExprs := collectCorrFilter(subRoot)
	//collect correlated columns in corr filters
	//nonCorrCols := make([]*Expr, 0)
	//for _, corr := range corrExprs {
	//	nonCorrCols = append(nonCorrCols, collectNonCorrColumn(corr)...)
	//}
	if len(corrExprs) > 0 {
		var err error
		var newSub *LogicalOperator
		var corrFilters []*Expr
		//for _, corrCol := range nonCorrCols {
		//	fmt.Println("===> non corr cols")
		//	fmt.Println(corrCol.String())
		//}
		//correlated subquery
		newSub, corrFilters, err = b.removeCorrFilters(subRoot)
		if err != nil {
			return nil, nil, err
		}
		//newSub, err := b.applyImpl(expr.SubqueryTyp, corrExprs, nonCorrCols, subRoot)
		//if err != nil {
		//	return nil, nil, err
		//}
		//
		//remove cor column
		nonCorrExprs, newCorrExprs := removeCorrExprs(corrFilters)

		//for _, noCorr := range nonCorrExprs {
		//	fmt.Println("===> non corr exprs")
		//	fmt.Println(noCorr.String())
		//}
		//
		//for _, nCorr := range newCorrExprs {
		//	fmt.Println("===> new corr exprs")
		//	fmt.Println(nCorr.String())
		//}

		switch expr.SubqueryTyp {
		case ET_SubqueryTypeScalar:
			newSub = &LogicalOperator{
				Typ:     LOT_JOIN,
				JoinTyp: LOT_JoinTypeInner,
				OnConds: nonCorrExprs,
				Children: []*LogicalOperator{
					root, newSub,
				},
			}
		case ET_SubqueryTypeExists:
			newSub = &LogicalOperator{
				Typ:     LOT_JOIN,
				JoinTyp: LOT_JoinTypeMARK,
				OnConds: nonCorrExprs,
				Children: []*LogicalOperator{
					root, newSub,
				},
			}
		case ET_SubqueryTypeNotExists:
			newSub = &LogicalOperator{
				Typ:     LOT_JOIN,
				JoinTyp: LOT_JoinTypeAntiMARK,
				OnConds: nonCorrExprs,
				Children: []*LogicalOperator{
					root, newSub,
				},
			}
		default:
			panic("usp")
		}

		if len(newCorrExprs) > 0 {
			newSub = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(newCorrExprs...),
				Children: []*LogicalOperator{newSub},
			}
		}

		switch expr.SubqueryTyp {
		case ET_SubqueryTypeScalar:
			//TODO: may have multi columns
			subBuilder := expr.SubBuilder
			proj0 := subBuilder.projectExprs[0]
			colRef := &Expr{
				Typ:     ET_Column,
				DataTyp: proj0.DataTyp,
				Table:   proj0.Table,
				Name:    proj0.Name,
				ColRef: ColumnBind{
					uint64(subBuilder.projectTag),
					0,
				},
			}
			return colRef, newSub, nil
		case ET_SubqueryTypeExists:
			colRef := &Expr{
				Typ:     ET_BConst,
				DataTyp: ExprDataType{LTyp: boolean()},
				Bvalue:  true,
			}
			return colRef, newSub, nil
		case ET_SubqueryTypeNotExists:
			colRef := &Expr{
				Typ:     ET_BConst,
				DataTyp: ExprDataType{LTyp: boolean()},
				Bvalue:  true,
			}
			return colRef, newSub, nil
		default:
			panic("usp")
		}

	} else {
		var colRef *Expr
		var newRoot *LogicalOperator
		if root == nil {
			newRoot = subRoot
		} else {
			newRoot = &LogicalOperator{
				Typ:     LOT_JOIN,
				JoinTyp: LOT_JoinTypeCross,
				OnConds: nil,
				Children: []*LogicalOperator{
					root, subRoot,
				},
			}
		}
		// TODO: may have multi columns
		subBuilder := expr.SubBuilder
		proj0 := subBuilder.projectExprs[0]
		colRef = &Expr{
			Typ:     ET_Column,
			DataTyp: proj0.DataTyp,
			Table:   proj0.Table,
			Name:    proj0.Name,
			ColRef: ColumnBind{
				uint64(subBuilder.projectTag),
				0,
			},
		}
		return colRef, newRoot, nil
	}
}

// TODO: wrong impl.
// need add function: check LogicalOperator find cor column
func (b *Builder) applyImpl(subqueryTyp ET_SubqueryType, corrExprs, corrCols []*Expr, subRoot *LogicalOperator) (*LogicalOperator, error) {
	var err error
	has := hasCorrColInRoot(subRoot)
	if !has {
		//remove cor column
		//nonCorrExprs, newCorrExprs := removeCorrExprs(corrExprs)
		//
		//var newRoot *LogicalOperator
		//switch subqueryTyp {
		//case ET_SubqueryTypeScalar:
		//	newRoot = &LogicalOperator{
		//		Typ:     LOT_JOIN,
		//		JoinTyp: LOT_JoinTypeInner,
		//		OnConds: nonCorrExprs,
		//		Children: []*LogicalOperator{
		//			root, subRoot,
		//		},
		//	}
		//case ET_SubqueryTypeExists:
		//	newRoot = &LogicalOperator{
		//		Typ:     LOT_JOIN,
		//		JoinTyp: LOT_JoinTypeMARK,
		//		OnConds: nonCorrExprs,
		//		Children: []*LogicalOperator{
		//			root, subRoot,
		//		},
		//	}
		//case ET_SubqueryTypeNotExists:
		//	newRoot = &LogicalOperator{
		//		Typ:     LOT_JOIN,
		//		JoinTyp: LOT_JoinTypeAntiMARK,
		//		OnConds: nonCorrExprs,
		//		Children: []*LogicalOperator{
		//			root, subRoot,
		//		},
		//	}
		//default:
		//	panic("usp")
		//}
		//
		//if len(newCorrExprs) > 0 {
		//	newRoot = &LogicalOperator{
		//		Typ:      LOT_Filter,
		//		Filters:  copyExprs(newCorrExprs...),
		//		Children: []*LogicalOperator{newRoot},
		//	}
		//}
		//return newRoot, err
		return subRoot, nil
	}
	switch subRoot.Typ {
	case LOT_Project:
		for _, proj := range subRoot.Projects {
			if hasCorrCol(proj) {
				panic("usp correlated column in project")
			}
		}
		if has {
			subRoot.Projects = append(subRoot.Projects, corrCols...)
		}
		subRoot.Children[0], err = b.applyImpl(subqueryTyp, corrExprs, corrCols, subRoot.Children[0])
		return subRoot, err
	case LOT_AggGroup:
		for _, by := range subRoot.GroupBys {
			if hasCorrCol(by) {
				panic("usp correlated column in agg")
			}
		}
		if has {
			subRoot.GroupBys = append(subRoot.GroupBys, corrCols...)
		}
		subRoot.Children[0], err = b.applyImpl(subqueryTyp, corrExprs, corrCols, subRoot.Children[0])
		return subRoot, err
	case LOT_Filter:
		filterHasCorrCol := false
		for _, filter := range subRoot.Filters {
			if hasCorrCol(filter) {
				filterHasCorrCol = true
				break
			}
		}
		if has && !filterHasCorrCol {
			subRoot.Filters = append(subRoot.Filters, corrExprs...)
		}
		subRoot.Children[0], err = b.applyImpl(subqueryTyp, corrExprs, corrCols, subRoot.Children[0])
		return subRoot, err
	}
	return subRoot, nil
}

func (b *Builder) removeCorrFilters(subRoot *LogicalOperator) (*LogicalOperator, []*Expr, error) {
	var childFilters []*Expr
	var err error
	corrFilters := make([]*Expr, 0)
	for i, child := range subRoot.Children {
		subRoot.Children[i], childFilters, err = b.removeCorrFilters(child)
		if err != nil {
			return nil, nil, err
		}

		corrFilters = append(corrFilters, childFilters...)
	}

	switch subRoot.Typ {
	case LOT_Project:
		b.removeCorrFiltersInProject(subRoot, corrFilters)
	case LOT_AggGroup:
		b.removeCorrFiltersInAggr(subRoot, corrFilters)
	case LOT_Filter:
		leftFilters := make([]*Expr, 0)
		for _, filter := range subRoot.Filters {
			if hasCorrCol(filter) {
				corrFilters = append(corrFilters, filter)
			} else {
				leftFilters = append(leftFilters, filter)
			}
		}
		if len(leftFilters) == 0 {
			subRoot = subRoot.Children[0]
		} else {
			subRoot.Filters = leftFilters
		}
	}
	return subRoot, corrFilters, nil
}

func (b *Builder) removeCorrFiltersInProject(subRoot *LogicalOperator, filters []*Expr) {
	for i, filter := range filters {
		filters[i] = b.replaceCorrFiltersInProject(subRoot, filter)
	}
}

func (b *Builder) replaceCorrFiltersInProject(subRoot *LogicalOperator, filter *Expr) *Expr {
	if !hasCorrCol(filter) {
		idx := len(subRoot.Projects)
		subRoot.Projects = append(subRoot.Projects, filter)
		return &Expr{
			Typ:     ET_Column,
			ColRef:  ColumnBind{subRoot.Index, uint64(idx)},
			DataTyp: filter.DataTyp,
		}
	}

	for i, child := range filter.Children {
		filter.Children[i] = b.replaceCorrFiltersInProject(subRoot, child)
	}

	return filter
}

func (b *Builder) removeCorrFiltersInAggr(subRoot *LogicalOperator, filters []*Expr) {
	for i, filter := range filters {
		filters[i] = b.replaceCorrFiltersInAggr(subRoot, filter)
	}
}

func (b *Builder) replaceCorrFiltersInAggr(subRoot *LogicalOperator, filter *Expr) *Expr {
	if !hasCorrCol(filter) {
		idx := len(subRoot.GroupBys)
		subRoot.GroupBys = append(subRoot.GroupBys, filter)
		return &Expr{
			Typ:     ET_Column,
			ColRef:  ColumnBind{subRoot.Index, uint64(idx)},
			DataTyp: filter.DataTyp,
		}
	}

	for i, child := range filter.Children {
		filter.Children[i] = b.replaceCorrFiltersInAggr(subRoot, child)
	}
	return filter
}

func (b *Builder) createAggGroup(root *LogicalOperator) (*LogicalOperator, error) {
	return &LogicalOperator{
		Typ:      LOT_AggGroup,
		Index:    uint64(b.groupTag),
		Index2:   uint64(b.aggTag),
		Aggs:     b.aggs,
		GroupBys: b.groupbyExprs,
		Children: []*LogicalOperator{root},
	}, nil
}

func (b *Builder) createProject(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var newExpr *Expr
	projects := make([]*Expr, 0)
	for _, expr := range b.projectExprs {
		newExpr, root, err = b.createSubquery(expr, root)
		if err != nil {
			return nil, err
		}
		projects = append(projects, newExpr)
	}
	return &LogicalOperator{
		Typ:      LOT_Project,
		Index:    uint64(b.projectTag),
		Projects: projects,
		Children: []*LogicalOperator{root},
	}, nil
}

func (b *Builder) createOrderby(root *LogicalOperator) (*LogicalOperator, error) {
	return &LogicalOperator{
		Typ:      LOT_Order,
		OrderBys: b.orderbyExprs,
		Children: []*LogicalOperator{root},
	}, nil
}

// collectCorrFilter collects all exprs that find correlated column.
// and does not remove these exprs.
func collectCorrFilter(root *LogicalOperator) []*Expr {
	var ret, childRet []*Expr
	for _, child := range root.Children {
		childRet = collectCorrFilter(child)
		ret = append(ret, childRet...)
	}

	switch root.Typ {
	case LOT_Filter:
		for _, filter := range root.Filters {
			if hasCorrCol(filter) {
				ret = append(ret, filter)
			}
		}
	default:
	}
	return ret
}

// collectCorrColumn collects all correlated columns
func collectCorrColumn(expr *Expr) []*Expr {
	var ret []*Expr
	switch expr.Typ {
	case ET_Column:
		if expr.Depth > 0 {
			return []*Expr{expr}
		}
	case ET_Func:
		for _, child := range expr.Children {
			ret = append(ret, collectCorrColumn(child)...)
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
	return ret
}

// collectNonCorrColumn collects all non correlated columns
func collectNonCorrColumn(expr *Expr) []*Expr {
	var ret []*Expr
	switch expr.Typ {
	case ET_Column:
		if expr.Depth == 0 {
			return []*Expr{expr}
		}
	case ET_Func:
		for _, child := range expr.Children {
			ret = append(ret, collectNonCorrColumn(child)...)
		}
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
	return ret
}

func hasCorrCol(expr *Expr) bool {
	switch expr.Typ {
	case ET_Column:
		return expr.Depth > 0
	case ET_Func:
		ret := false
		for _, child := range expr.Children {
			ret = ret || hasCorrCol(child)
		}
		return ret
	case ET_IConst, ET_SConst, ET_DateConst, ET_IntervalConst, ET_FConst, ET_BConst, ET_NConst:
		return false
	default:
		panic(fmt.Sprintf("usp %v", expr.Typ))
	}
	return false
}

// hasCorrColInRoot checks if the plan with root find the correlated column.
func hasCorrColInRoot(root *LogicalOperator) bool {
	switch root.Typ {
	case LOT_Project:
		for _, proj := range root.Projects {
			if hasCorrCol(proj) {
				panic("usp correlated column in project")
			}
		}

	case LOT_AggGroup:
		for _, by := range root.GroupBys {
			if hasCorrCol(by) {
				panic("usp correlated column in agg")
			}
		}

	case LOT_Filter:
		for _, filter := range root.Filters {
			if hasCorrCol(filter) {
				return true
			}
		}
	}

	for _, child := range root.Children {
		if hasCorrColInRoot(child) {
			return true
		}
	}
	return false
}

// ==============
// Optimize plan
// ==============
func (b *Builder) Optimize(ctx *BindContext, root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	var left []*Expr
	////0. apply distribute
	//root, err = b.applyDistribute(root)
	//if err != nil {
	//	return nil, err
	//}
	//fmt.Println("Before pushdown filter\n", root.String())
	//1. pushdown filter
	root, left, err = b.pushdownFilters(root, nil)
	if err != nil {
		return nil, err
	}
	if len(left) > 0 {
		root = &LogicalOperator{
			Typ:      LOT_Filter,
			Filters:  copyExprs(left...),
			Children: []*LogicalOperator{root},
		}
	}

	//fmt.Println("After pushdown filter\n", root.String())

	//2. join order
	root, err = b.joinOrder(root)
	if err != nil {
		return nil, err
	}

	//fmt.Println("After join reorder\n", root.String())

	//3. pushdown filter again
	root, left, err = b.pushdownFilters(root, nil)
	if err != nil {
		return nil, err
	}
	if len(left) > 0 {
		root = &LogicalOperator{
			Typ:      LOT_Filter,
			Filters:  copyExprs(left...),
			Children: []*LogicalOperator{root},
		}
	}

	//fmt.Println("After pushdown filter 2\n", root.String())

	//4. column prune
	root, err = b.columnPrune(root)
	if err != nil {
		return nil, err
	}
	//fmt.Println("After prune\n", root.String())
	root, err = b.generateCounts(root)
	if err != nil {
		return nil, err
	}
	//fmt.Println("After generateCounts\n", root.String())
	root, err = b.generateOutputs(root)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func (b *Builder) applyDistribute(root *LogicalOperator) (*LogicalOperator, error) {
	var err error
	if root == nil {
		return root, nil
	}
	root.Filters = distributeExprs(root.Filters...)
	root.OnConds = distributeExprs(root.OnConds...)
	for i, child := range root.Children {
		root.Children[i], err = b.applyDistribute(child)
		if err != nil {
			return nil, err
		}
	}
	return root, nil
}

// pushdownFilters pushes down filters to the lowest possible position.
// It returns the new root and the filters that cannot be pushed down.
func (b *Builder) pushdownFilters(root *LogicalOperator, filters []*Expr) (*LogicalOperator, []*Expr, error) {
	var err error
	var left, childLeft []*Expr
	var childRoot *LogicalOperator
	var needs []*Expr

	switch root.Typ {
	case LOT_Scan:
		for _, f := range filters {
			if onlyReferTo(f, root.Index) {
				//expr that only refer to the scan expr can be pushdown.
				root.Filters = append(root.Filters, f)
			} else {
				left = append(left, f)
			}
		}
	case LOT_JOIN:
		needs = filters
		leftTags := make(map[uint64]bool)
		rightTags := make(map[uint64]bool)
		collectTags(root.Children[0], leftTags)
		collectTags(root.Children[1], rightTags)

		root.OnConds = splitExprsByAnd(root.OnConds)
		if root.JoinTyp == LOT_JoinTypeInner {
			for _, on := range root.OnConds {
				needs = append(needs, splitExprByAnd(on)...)
			}
			root.OnConds = nil
		}

		whichSides := make([]int, len(needs))
		for i, nd := range needs {
			whichSides[i] = decideSide(nd, leftTags, rightTags)
		}

		leftNeeds := make([]*Expr, 0)
		rightNeeds := make([]*Expr, 0)
		for i, nd := range needs {
			switch whichSides[i] {
			case NoneSide:
				switch root.JoinTyp {
				case LOT_JoinTypeInner:
					leftNeeds = append(leftNeeds, copyExpr(nd))
					rightNeeds = append(rightNeeds, nd)
				case LOT_JoinTypeLeft:
					leftNeeds = append(leftNeeds, nd)
				default:
					left = append(left, nd)
				}
			case LeftSide:
				leftNeeds = append(leftNeeds, nd)
			case RightSide:
				rightNeeds = append(rightNeeds, nd)
			case BothSide:
				if root.JoinTyp == LOT_JoinTypeInner {
					//only equal or in can be used in On conds
					if nd.SubTyp == ET_Equal || nd.SubTyp == ET_In {
						root.OnConds = append(root.OnConds, nd)
						break
					}
				}
				left = append(left, nd)
			default:
				panic(fmt.Sprintf("usp side %d", whichSides[i]))
			}
		}

		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], leftNeeds)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[0] = childRoot

		childRoot, childLeft, err = b.pushdownFilters(root.Children[1], rightNeeds)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[1] = childRoot

	case LOT_AggGroup:
		for _, f := range filters {
			if referTo(f, root.Index2) {
				//expr that refer to the agg exprs can not be pushdown.
				root.Filters = append(root.Filters, f)
			} else {
				//restore the real expr for the expr that refer to the expr in the group by.
				needs = append(needs, restoreExpr(f, root.Index, root.GroupBys))
			}
		}

		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[0] = childRoot
	case LOT_Project:
		//restore the real expr for the expr that refer to the expr in the project list.
		for _, f := range filters {
			needs = append(needs, restoreExpr(f, root.Index, root.Projects))
		}

		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			childRoot = &LogicalOperator{
				Typ:      LOT_Filter,
				Filters:  copyExprs(childLeft...),
				Children: []*LogicalOperator{childRoot},
			}
		}
		root.Children[0] = childRoot
	case LOT_Filter:
		needs = filters
		for _, e := range root.Filters {
			needs = append(needs, splitExprByAnd(e)...)
		}
		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)
		if err != nil {
			return nil, nil, err
		}
		if len(childLeft) > 0 {
			root.Children[0] = childRoot
			root.Filters = childLeft
		} else {
			//remove this FILTER node
			root = childRoot
		}

	default:
		if root.Typ == LOT_Limit {
			//can not pushdown filter through LIMIT
			left, filters = filters, nil
		}
		if len(root.Children) > 0 {
			if len(root.Children) > 1 {
				panic("must be on child: " + root.Typ.String())
			}
			childRoot, childLeft, err = b.pushdownFilters(root.Children[0], filters)
			if err != nil {
				return nil, nil, err
			}
			if len(childLeft) > 0 {
				childRoot = &LogicalOperator{
					Typ:      LOT_Filter,
					Filters:  copyExprs(childLeft...),
					Children: []*LogicalOperator{childRoot},
				}
			}
			root.Children[0] = childRoot
		} else {
			left = filters
		}
	}

	return root, left, err
}

func (b *Builder) bindCaseExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var err error
	var els *Expr
	when := make([]*Expr, len(expr.Expr.When))
	var astWhen []*Ast
	if expr.Expr.Kase != nil {
		//rewrite it to CASE WHEN kase = compare value ...
		astWhen = make([]*Ast, len(expr.Expr.When))
		for i := 0; i < len(expr.Expr.When); i += 2 {
			astWhen[i] = equal(expr.Expr.Kase, expr.Expr.When[i])
			astWhen[i+1] = expr.Expr.When[i+1]
		}

	} else {
		astWhen = expr.Expr.When
	}

	for i := 0; i < len(astWhen); i += 2 {
		if i+1 >= len(astWhen) {
			panic("miss then")
		}
		when[i], err = b.bindExpr(ctx, iwc, astWhen[i], depth)
		if err != nil {
			return nil, err
		}
		when[i+1], err = b.bindExpr(ctx, iwc, astWhen[i+1], depth)
		if err != nil {
			return nil, err
		}
	}

	if expr.Expr.Els != nil {
		els, err = b.bindExpr(ctx, iwc, expr.Expr.Els, depth)
		if err != nil {
			return nil, err
		}
	} else {
		els = &Expr{
			Typ: ET_NConst,
			DataTyp: ExprDataType{
				LTyp: null(),
			},
		}
	}

	retTyp := els.DataTyp.LTyp

	//decide result types
	//max type of the THEN expr
	for i := 0; i < len(when); i += 2 {
		retTyp = MaxLType(retTyp, when[i+1].DataTyp.LTyp)
	}

	//case THEN to
	for i := 0; i < len(when); i += 2 {
		when[i+1], err = castExpr(when[i+1], retTyp, retTyp.id == LTID_ENUM)
		if err != nil {
			return nil, err
		}
	}

	//cast ELSE to
	els, err = castExpr(els, retTyp, retTyp.id == LTID_ENUM)
	if err != nil {
		return nil, err
	}

	params := []*Expr{els}
	params = append(params, when...)

	decideDataType := func(e *Expr) ExprDataType {
		if e == nil {
			return ExprDataType{LTyp: null()}
		} else {
			return e.DataTyp
		}
	}

	paramsTypes := []ExprDataType{
		decideDataType(els),
	}

	for i := 0; i < len(when); i += 1 {
		paramsTypes = append(paramsTypes, when[i].DataTyp)
	}

	ret, err := b.bindFunc(ET_Case.String(), ET_Case, expr.String(), params, paramsTypes)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (b *Builder) bindInExpr(ctx *BindContext, iwc InWhichClause, expr *Ast, depth int) (*Expr, error) {
	var err error
	var in *Expr
	var children []*Expr

	in, err = b.bindExpr(ctx, iwc, expr.Expr.In, depth)
	if err != nil {
		return nil, err
	}
	argsTypes := make([]LType, 0)
	children = make([]*Expr, len(expr.Expr.Children))
	for i, child := range expr.Expr.Children {
		children[i], err = b.bindExpr(ctx, iwc, child, depth)
		if err != nil {
			return nil, err
		}
		argsTypes = append(argsTypes, children[i].DataTyp.LTyp)
	}

	maxType := in.DataTyp.LTyp
	anyVarchar := in.DataTyp.LTyp.id == LTID_VARCHAR
	anyEnum := in.DataTyp.LTyp.id == LTID_ENUM
	for i := 0; i < len(argsTypes); i++ {
		maxType = MaxLType(maxType, argsTypes[i])
		if argsTypes[i].id == LTID_VARCHAR {
			anyVarchar = true
		}
		if argsTypes[i].id == LTID_ENUM {
			anyEnum = true
		}
	}
	if anyVarchar && anyEnum {
		maxType = varchar()
	}

	paramTypes := make([]ExprDataType, 0)
	params := make([]*Expr, 0)

	castIn, err := castExpr(in, maxType, false)
	if err != nil {
		return nil, err
	}
	params = append(params, castIn)
	paramTypes = append(paramTypes, castIn.DataTyp)
	for _, child := range children {
		castChild, err := castExpr(child, maxType, false)
		if err != nil {
			return nil, err
		}
		params = append(params, castChild)
		paramTypes = append(paramTypes, castChild.DataTyp)
	}

	var et ET_SubTyp
	switch expr.Expr.SubTyp {
	case AstExprSubTypeIn:
		et = ET_In
	case AstExprSubTypeNotIn:
		et = ET_NotIn
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
		equalTypes := []ExprDataType{paramTypes[0], paramTypes[i]}
		ret0, err := b.bindFunc(et.String(), et, expr.String(), equalParams, equalTypes)
		if err != nil {
			return nil, err
		}
		orChildren = append(orChildren, ret0)
	}

	bigOrExpr := combineExprsByOr(orChildren...)
	return bigOrExpr, nil
}

func collectTags(root *LogicalOperator, set map[uint64]bool) {
	if root.Index != 0 {
		set[root.Index] = true
	}
	if root.Index2 != 0 {
		set[root.Index2] = true
	}
	for _, child := range root.Children {
		collectTags(child, set)
	}
}

const (
	NoneSide       = 0
	LeftSide       = 1 << 1
	RightSide      = 1 << 2
	BothSide       = LeftSide | RightSide
	CorrelatedSide = 1 << 3
)

//////////////////////////////////////////////
// create physical plan
//////////////////////////////////////////////

func (b *Builder) CreatePhyPlan(root *LogicalOperator) (*PhysicalOperator, error) {
	if root == nil {
		return nil, nil
	}
	var err error
	children := make([]*PhysicalOperator, 0)
	for _, child := range root.Children {
		childPlan, err := b.CreatePhyPlan(child)
		if err != nil {
			return nil, err
		}
		children = append(children, childPlan)
	}
	var proot *PhysicalOperator
	switch root.Typ {
	case LOT_Project:
		proot, err = b.createPhyProject(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Filter:
		proot, err = b.createPhyFilter(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Scan:
		proot, err = b.createPhyScan(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_JOIN:
		proot, err = b.createPhyJoin(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_AggGroup:
		proot, err = b.createPhyAgg(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Order:
		proot, err = b.createPhyOrder(root, children)
		if err != nil {
			return nil, err
		}
	case LOT_Limit:
		proot, err = b.createPhyTopN(root, children)
		if err != nil {
			return nil, err
		}
	}
	if proot != nil {
		proot.estimatedCard = root.estimatedCard
	}

	return proot, nil
}

func (b *Builder) createPhyProject(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Project,
		Index:    root.Index,
		Projects: root.Projects,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyFilter(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{Typ: POT_Filter, Filters: root.Filters, Outputs: root.Outputs, Children: children}, nil
}

func (b *Builder) createPhyScan(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Scan,
		Index:    root.Index,
		Database: root.Database,
		Table:    root.Table,
		Alias:    root.Alias,
		Outputs:  root.Outputs,
		Columns:  root.Columns,
		Filters:  root.Filters,
		Children: children}, nil
}

func (b *Builder) createPhyJoin(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Join,
		JoinTyp:  root.JoinTyp,
		OnConds:  root.OnConds,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyOrder(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Order,
		OrderBys: root.OrderBys,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyAgg(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{
		Typ:      POT_Agg,
		Index:    root.Index,
		Index2:   root.Index2,
		Filters:  root.Filters,
		Aggs:     root.Aggs,
		GroupBys: root.GroupBys,
		Outputs:  root.Outputs,
		Children: children}, nil
}

func (b *Builder) createPhyTopN(root *LogicalOperator, children []*PhysicalOperator) (*PhysicalOperator, error) {
	return &PhysicalOperator{Typ: POT_Limit, Outputs: root.Outputs, Limit: root.Limit, Children: children}, nil
}
