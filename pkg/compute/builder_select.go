package compute

import (
	"errors"
	"fmt"

	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
	pg_query "github.com/pganalyze/pg_query_go/v5"
)

func (b *Builder) expandStar(expr *pg_query.ResTarget) ([]*pg_query.ResTarget, error) {
	if expr == nil {
		return nil, nil
	}
	if expr.GetVal().GetColumnRef() != nil {
		colRef := expr.GetVal().GetColumnRef()
		star := colRef.GetFields()[0].GetAStar()
		if star != nil {
			if len(b.rootCtx.bindings) == 0 {
				return nil, errors.New("no table")
			}
			ret := make([]*pg_query.ResTarget, 0)
			for _, bind := range b.rootCtx.bindingsList {
				for _, name := range bind.names {
					colNode := &pg_query.Node_String_{
						String_: &pg_query.String{
							Sval: name,
						},
					}

					ret = append(ret, &pg_query.ResTarget{
						Val: &pg_query.Node{
							Node: &pg_query.Node_ColumnRef{
								ColumnRef: &pg_query.ColumnRef{
									Fields: []*pg_query.Node{
										{
											Node: colNode,
										},
									},
								},
							},
						},
					})
				}
			}
			return ret, nil
		}
	}
	return []*pg_query.ResTarget{expr}, nil
}

func (b *Builder) buildSelect(sel *pg_query.SelectStmt, ctx *BindContext, depth int) error {
	var err error
	b.projectTag = b.GetTag()
	b.groupTag = b.GetTag()
	b.aggTag = b.GetTag()

	if sel.WithClause != nil {
		_, err := b.buildWith(sel.WithClause, ctx, depth)
		if err != nil {
			return err
		}
	}

	if len(sel.FromClause) != 0 {
		//from
		b.fromExpr, err = b.buildTables(sel.FromClause, ctx, depth)
		if err != nil {
			return err
		}
	} else if len(sel.ValuesLists) != 0 {
		b.fromExpr, err = b.buildValuesLists(sel.ValuesLists, ctx, depth)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("usp table ref")
	}

	//expand star
	newSelectExprs := make([]*pg_query.ResTarget, 0)

	targetList := sel.TargetList
	if len(targetList) == 0 {
		//mock star
		star := &pg_query.ResTarget{
			Val: &pg_query.Node{
				Node: &pg_query.Node_ColumnRef{
					ColumnRef: &pg_query.ColumnRef{
						Fields: []*pg_query.Node{
							{
								Node: &pg_query.Node_AStar{
									AStar: &pg_query.A_Star{},
								},
							},
						},
					},
				},
			},
		}
		targetList = append(targetList, &pg_query.Node{
			Node: &pg_query.Node_ResTarget{
				ResTarget: star,
			},
		})
	}

	for _, expr := range targetList {
		ret, err := b.expandStar(expr.GetResTarget())
		if err != nil {
			return err
		}
		newSelectExprs = append(newSelectExprs, ret...)
	}

	//select expr alias
	for i, expr := range newSelectExprs {
		name := ""
		colRef := expr.GetVal().GetColumnRef()
		if colRef != nil {
			name = colRef.GetFields()[0].GetString_().GetSval()
		} else {
			name = expr.GetVal().String()
		}

		if expr.Name != "" {
			b.aliasMap[expr.Name] = i
			name = expr.Name
		}
		b.names = append(b.names, name)
		b.projectMap[expr.String()] = i
	}
	b.columnCount = len(newSelectExprs)

	//where
	if sel.WhereClause != nil {
		b.whereExpr, err = b.bindExpr(ctx,
			IWC_WHERE,
			sel.WhereClause,
			depth)
		if err != nil {
			return err
		}
	}

	//group by
	if len(sel.GroupClause) != 0 {
		var retExpr *Expr
		for _, expr := range sel.GroupClause {
			retExpr, err = b.bindExpr(ctx, IWC_GROUP, expr, depth)
			if err != nil {
				return err
			}
			b.groupbyExprs = append(b.groupbyExprs, retExpr)
		}
	}

	//having
	if sel.HavingClause != nil {
		var retExpr *Expr
		retExpr, err = b.bindExpr(ctx, IWC_HAVING, sel.HavingClause, depth)
		if err != nil {
			return err
		}
		b.havingExpr = retExpr
	}
	//select exprs
	var retExpr *Expr
	for i, expr := range newSelectExprs {
		retExpr, err = b.bindExpr(ctx, IWC_SELECT, expr.GetVal(), depth)
		if err != nil {
			return err
		}
		retExpr.Alias = b.names[i]
		b.projectExprs = append(b.projectExprs, retExpr)
	}

	//order by,limit,distinct
	if len(sel.SortClause) != 0 {
		for _, expr := range sel.SortClause {
			retExpr, err = b.bindExpr(ctx, IWC_ORDER, expr, depth)
			if err != nil {
				return err
			}
			b.orderbyExprs = append(b.orderbyExprs, retExpr)
		}
	}

	if sel.LimitOffset != nil || sel.LimitCount != nil {
		if sel.LimitCount != nil {
			b.limitCount, err = b.bindExpr(ctx, IWC_LIMIT, sel.LimitCount, depth)
			if err != nil {
				return err
			}
		}
	}
	return err
}

func (b *Builder) findCte(name string, skip bool, ctx *BindContext) *pg_query.CommonTableExpr {
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

func (b *Builder) addCte(cte *pg_query.CommonTableExpr, ctx *BindContext) error {
	name := cte.Ctename
	if _, has := ctx.ctes[name]; has {
		return fmt.Errorf("duplicate cte %s", name)
	}
	ctx.ctes[name] = cte
	return nil
}

func (b *Builder) buildWith(with *pg_query.WithClause, ctx *BindContext, depth int) (*Expr, error) {
	for _, cte := range with.Ctes {
		err := b.addCte(cte.GetCommonTableExpr(), ctx)
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}

func (b *Builder) buildTable(table *pg_query.Node, ctx *BindContext, depth int) (*Expr, error) {
	if table == nil {
		panic("need table")
	}

	switch rangeNode := table.GetNode().(type) {
	case *pg_query.Node_RangeVar:

		tableAst := rangeNode.RangeVar
		db := tableAst.GetSchemaname()
		tableName := tableAst.Relname
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
				//TODO:refine it
				nodeRangeSub := &pg_query.Node_RangeSubselect{}
				nodeRangeSub.RangeSubselect = &pg_query.RangeSubselect{
					Alias: &pg_query.Alias{
						Aliasname: cte.Ctename,
					},
					Subquery: cte.Ctequery,
				}
				node := &pg_query.Node{
					Node: nodeRangeSub,
				}
				return b.buildTable(node, ctx, depth)
			}
		}
		{
			if db == "" {
				db = "public"
			}
			tabEnt := storage.GCatalog.GetEntry(b.txn, storage.CatalogTypeTable, db, tableName)
			if tabEnt == nil {
				return nil, fmt.Errorf("no table %s in schema %s", tableName, db)
			}
			alias := tableName
			if tableAst.Alias != nil {
				alias = tableAst.Alias.Aliasname
			}
			typs := tabEnt.GetTypes()
			cols := tabEnt.GetColumnNames()
			bind := &Binding{
				typ:     BT_TABLE,
				alias:   alias,
				index:   uint64(b.GetTag()),
				typs:    util.CopyTo(typs),
				names:   util.CopyTo(cols),
				nameMap: make(map[string]int),
			}
			for idx, name := range bind.names {
				bind.nameMap[name] = idx
			}
			err := ctx.AddBinding(alias, bind)
			if err != nil {
				return nil, err
			}

			return &Expr{
				Typ:   ET_TABLE,
				Index: bind.index,
				BaseInfo: BaseInfo{
					Database:  db,
					Table:     tableName,
					Alias:     alias,
					BelongCtx: ctx,
				},
				TableInfo: TableInfo{
					TabEnt: tabEnt,
				},
			}, err
		}
	case *pg_query.Node_JoinExpr:
		return b.buildJoinTable(rangeNode.JoinExpr, ctx, depth)
	case *pg_query.Node_RangeSubselect:
		subqueryAst := rangeNode.RangeSubselect
		subBuilder := NewBuilder(b.txn)
		subBuilder.tag = b.tag
		subBuilder.rootCtx.parent = ctx
		if len(subqueryAst.Alias.Aliasname) == 0 {
			return nil, errors.New("need alias for subquery")
		}
		subBuilder.alias = subqueryAst.Alias.Aliasname
		err := subBuilder.buildSelect(subqueryAst.Subquery.GetSelectStmt(), subBuilder.rootCtx, 0)
		if err != nil {
			return nil, err
		}

		if len(subBuilder.projectExprs) == 0 {
			panic("subquery must have project list")
		}
		subTypes := make([]common.LType, 0)
		subNames := make([]string, 0)
		for i, expr := range subBuilder.projectExprs {
			subTypes = append(subTypes, expr.DataTyp)
			name := expr.Name
			if len(expr.Alias) != 0 {
				name = expr.Alias
			}
			if i < len(subqueryAst.Alias.Colnames) {
				name = subqueryAst.Alias.Colnames[i].GetString_().GetSval()
			}
			subNames = append(subNames, name)
		}

		bind := &Binding{
			typ:   BT_Subquery,
			alias: subqueryAst.Alias.Aliasname,
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
			Typ:   ET_Subquery,
			Index: bind.index,
			BaseInfo: BaseInfo{
				Database:  "",
				Table:     bind.alias,
				BelongCtx: ctx,
			},
			SubqueryInfo: SubqueryInfo{
				SubBuilder: subBuilder,
				SubCtx:     subBuilder.rootCtx,
			},
		}, err
	default:
		return nil, fmt.Errorf("usp table type %v", table.String())
	}
}

func (b *Builder) buildTables(tables []*pg_query.Node, ctx *BindContext, depth int) (*Expr, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables")
	} else if len(tables) == 1 {
		return b.buildTable(tables[0], ctx, depth)
	} else if len(tables) == 2 {
		leftCtx := NewBindContext(ctx)
		//left
		left, err := b.buildTable(tables[0], leftCtx, depth)
		if err != nil {
			return nil, err
		}

		rightCtx := NewBindContext(ctx)
		//right
		right, err := b.buildTable(tables[1], rightCtx, depth)
		if err != nil {
			return nil, err
		}

		return b.mergeTwoTable(
			leftCtx,
			rightCtx,
			left,
			right,
			ET_JoinTypeCross,
			ctx,
			depth)
	} else {
		nodeCnt := len(tables)
		leftCtx := NewBindContext(ctx)
		//left
		left, err := b.buildTables(tables[:nodeCnt-1], leftCtx, depth)
		if err != nil {
			return nil, err
		}

		rightCtx := NewBindContext(ctx)
		//right
		right, err := b.buildTable(tables[nodeCnt-1], rightCtx, depth)
		if err != nil {
			return nil, err
		}

		return b.mergeTwoTable(
			leftCtx,
			rightCtx,
			left,
			right,
			ET_JoinTypeCross,
			ctx,
			depth)
	}
}

func (b *Builder) mergeTwoTable(
	leftCtx, rightCtx *BindContext,
	left, right *Expr,
	jt ET_JoinType,
	ctx *BindContext, depth int) (*Expr, error) {

	switch jt {
	case ET_JoinTypeCross, ET_JoinTypeLeft, ET_JoinTypeInner:
	default:
		return nil, fmt.Errorf("usp join type %d", jt)
	}

	err := ctx.AddContext(leftCtx)
	if err != nil {
		return nil, err
	}

	err = ctx.AddContext(rightCtx)
	if err != nil {
		return nil, err
	}

	ret := &Expr{
		Typ:      ET_Join,
		Children: []*Expr{left, right},
		BaseInfo: BaseInfo{
			BelongCtx: ctx,
		},
		JoinInfo: JoinInfo{
			JoinTyp: jt,
		},
	}

	return ret, err
}

func (b *Builder) buildJoinTable(join *pg_query.JoinExpr, ctx *BindContext, depth int) (*Expr, error) {
	leftCtx := NewBindContext(ctx)
	//left
	left, err := b.buildTable(join.Larg, leftCtx, depth)
	if err != nil {
		return nil, err
	}

	rightCtx := NewBindContext(ctx)
	//right
	right, err := b.buildTable(join.Rarg, rightCtx, depth)
	if err != nil {
		return nil, err
	}

	var jt ET_JoinType
	switch join.Jointype {
	case pg_query.JoinType_JOIN_FULL:
		jt = ET_JoinTypeCross
	case pg_query.JoinType_JOIN_LEFT:
		jt = ET_JoinTypeLeft
	case pg_query.JoinType_JOIN_INNER:
		jt = ET_JoinTypeInner
	default:
		return nil, fmt.Errorf("usp join type %d", join.Jointype)
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
	if join.Quals != nil {
		onExpr, err = b.bindExpr(ctx, IWC_JOINON, join.Quals, depth)
		if err != nil {
			return nil, err
		}
	}

	ret := &Expr{
		Typ:      ET_Join,
		Children: []*Expr{left, right},
		BaseInfo: BaseInfo{
			BelongCtx: ctx,
		},
		JoinInfo: JoinInfo{
			JoinTyp: jt,
			On:      onExpr,
		},
	}

	return ret, err
}
