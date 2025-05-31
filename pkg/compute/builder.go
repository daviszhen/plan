package compute

import (
	"errors"
	"fmt"

	pg_query "github.com/pganalyze/pg_query_go/v5"
	"github.com/xlab/treeprint"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
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
	typs     []common.LType
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
	ctes         map[string]*pg_query.CommonTableExpr
	cteBindings  map[string]*Binding
}

func NewBindContext(parent *BindContext) *BindContext {
	return &BindContext{
		parent:      parent,
		bindings:    make(map[string]*Binding, 0),
		ctes:        make(map[string]*pg_query.CommonTableExpr, 0),
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
			util.Swap(bc.bindingsList, found, len(bc.bindingsList)-1)
			bc.bindingsList = util.Pop(bc.bindingsList)
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
					return nil, 0, fmt.Errorf("ambiguous column %s in %s or %s", column, ret.alias, b.alias)
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
	limitOffset  *Expr

	//for insert
	expectedTypes []common.LType
	expectedNames []string

	names       []string //output column names
	columnCount int      // count of the select exprs (after expanding star)
	phyId       int
	txn         *storage.Txn
}

func NewBuilder(txn *storage.Txn) *Builder {
	return &Builder{
		tag:        new(int),
		rootCtx:    NewBindContext(nil),
		aliasMap:   make(map[string]int),
		projectMap: make(map[string]int),
		txn:        txn,
	}
}

func (b *Builder) getPhyId() int {
	old := b.phyId
	b.phyId++
	return old
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
