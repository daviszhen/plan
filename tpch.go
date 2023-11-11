package main

func tpchQ2() *Ast {
	ret := &Ast{Typ: AstTypeSelect}

	//1. select list
	selectList := make([]*Ast, 8)
	ret.Select.SelectExprs = selectList

	//s_acctbal
	selectList[0] = column("s_acctbal")

	//s_name
	selectList[1] = column("s_name")

	//n_name
	selectList[2] = column("n_name")

	//p_partkey
	selectList[3] = column("p_partkey")

	//p_mfgr
	selectList[4] = column("p_mfgr")

	//s_address
	selectList[5] = column("s_address")

	//s_phone
	selectList[6] = column("s_phone")

	//s_comment
	selectList[7] = column("s_comment")

	//2. from
	//part
	part := table("part")

	//supplier
	supplier := table("supplier")

	//partsupp
	partsupp := table("partsupp")

	//nation
	nation := table("nation")

	//region
	region := table("region")

	//part,supplier,partsupp,nation,region
	ret.Select.From.Tables = crossJoin(
		crossJoin(
			crossJoin(
				crossJoin(part, supplier),
				partsupp),
			nation),
		region)

	//3. where
	w1 := equal(column("p_partkey"), column("ps_partkey"))
	w2 := equal(column("s_suppkey"), column("ps_suppkey"))
	w3 := equal(column("p_size"), inumber(48))
	w4 := like(column("p_type"), sstring("%TIN"))
	w5 := equal(column("s_nationkey"), column("n_nationkey"))
	w6 := equal(column("n_regionkey"), column("r_regionkey"))
	w7 := equal(column("r_name"), sstring("MIDDLE"))
	w8 := equal(column("ps_supplycost"), subquery(q1Subquery()))

	ret.Select.Where.Expr = and(
		and(
			and(
				and(
					and(
						and(
							and(w1, w2),
							w3),
						w4),
					w5),
				w6),
			w7),
		w8)

	//4. order by
	ret.OrderBy.Exprs = make([]*Ast, 4)
	ret.OrderBy.Exprs[0] = orderby(column("s_acctbal"), true)
	ret.OrderBy.Exprs[1] = orderby(column("n_name"), false)
	ret.OrderBy.Exprs[2] = orderby(column("s_name"), false)
	ret.OrderBy.Exprs[3] = orderby(column("p_partkey"), false)

	//5. limit
	ret.Limit.Count = inumber(100)
	return ret
}

func q1Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	//1.select list
	selectList := make([]*Ast, 1)
	ret.Select.SelectExprs = selectList
	selectList[0] = function("min", column("ps_supplycost"))

	//2. from
	ret.Select.From.Tables = crossJoin(
		crossJoin(
			crossJoin(
				table("partsupp"),
				table("supplier")),
			table("nation")),
		table("region"))

	//3. where
	w1 := equal(column("p_partkey"), column("ps_partkey"))
	w2 := equal(column("s_suppkey"), column("ps_suppkey"))
	w3 := equal(column("s_nationkey"), column("n_nationkey"))
	w4 := equal(column("n_regionkey"), column("r_regionkey"))
	w5 := equal(column("r_name"), sstring("MIDDLE EAST"))

	ret.Select.Where.Expr = and(
		and(
			and(
				and(w1, w2),
				w3),
			w4),
		w5)
	return ret
}

func tpchCatalog() *Catalog {
	cat := &Catalog{
		tpch: make(map[string]*CatalogTable),
	}
	cat.tpch["part"] = &CatalogTable{
		Db:    "tpch",
		Table: "part",
		Columns: []string{
			"p_partkey",
			"p_name",
			"p_mfgr",
			"p_brand",
			"p_type",
			"p_size",
			"p_container",
			"p_retailprice",
			"p_comment",
		},
		Types: []*ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 55},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeVarchar, NotNull: true, Width: 10},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 10},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeVarchar, NotNull: true, Width: 23},
		},
		PK: []int{
			0,
		},
		Column2Idx: map[string]int{
			"p_partkey":     0,
			"p_name":        1,
			"p_mfgr":        2,
			"p_brand":       3,
			"p_type":        4,
			"p_size":        5,
			"p_container":   6,
			"p_retailprice": 7,
			"p_comment":     8,
		},
	}
	cat.tpch["supplier"] = &CatalogTable{
		Db:    "tpch",
		Table: "supplier",
		Columns: []string{
			"s_suppkey",
		},
		Types:      []*ExprDataType{},
		PK:         []int{},
		Column2Idx: map[string]int{},
	}
	return cat
}
