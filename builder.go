package main

import "errors"

type Binding struct {
	tag         int
	table       string
	cols        []string
	typs        []*ExprDataType
	name2ColPos map[string]int
}

type BindContext struct {
	parent, left, right *BindContext

	bindings       []*Binding
	bindingByTag   map[int]*Binding
	bindingByTable map[string]*Binding
}

func NewBindContext(parent *BindContext) *BindContext {
	return &BindContext{
		parent:         parent,
		bindings:       make([]*Binding, 0),
		bindingByTag:   make(map[int]*Binding),
		bindingByTable: make(map[string]*Binding),
	}
}

type Builder struct {
	tag int // relation tag
}

func NewBuilder() *Builder {
	return &Builder{tag: 0}
}

func (b *Builder) GetTag() int {
	b.tag++
	return b.tag
}

func (b *Builder) buildFrom(table *Ast, ctx *BindContext) (int, error) {
	return b.buildTable(table, ctx)
}

func (b *Builder) buildTable(table *Ast, ctx *BindContext) (int, error) {
	if table == nil {
		panic("need table")
	}
	switch table.Typ {
	case AstExprTypeTable:
		{
			//dbName := "tpch"
			//tableName := table.Expr.Svalue

		}
	case AstExprTypeJoin:
		return b.buildTable(table, ctx)
	default:
		return 0, errors.New("usp table type")
	}
	return 0, nil
}

func (b *Builder) buildJoinTable(table *Ast, ctx *BindContext) (int, error) {
	panic("todo")
}
