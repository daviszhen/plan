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
	w8 := equal(column("ps_supplycost"), subquery(q2Subquery()))

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

func q2Subquery() *Ast {
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
	//tpch 1g
	cat := &Catalog{
		tpch: make(map[string]*CatalogTable),
	}
	// part
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
		Types: []ExprDataType{
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
		Stats: &Stats{
			RowCount: 200000,
		},
	}
	// supplier
	cat.tpch["supplier"] = &CatalogTable{
		Db:    "tpch",
		Table: "supplier",
		Columns: []string{
			"s_suppkey",
			"s_name",
			"s_address",
			"s_nationkey",
			"s_phone",
			"s_acctbal",
			"s_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeVarchar, NotNull: true, Width: 40},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 15},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeVarchar, NotNull: true, Width: 101},
		},
		PK: []int{0},
		Column2Idx: map[string]int{
			"s_suppkey":   0,
			"s_name":      1,
			"s_address":   2,
			"s_nationkey": 3,
			"s_phone":     4,
			"s_acctbal":   5,
			"s_comment":   6,
		},
		Stats: &Stats{
			RowCount: 10000,
		},
	}
	// partsupp
	cat.tpch["partsupp"] = &CatalogTable{
		Db:    "tpch",
		Table: "partsupp",
		Columns: []string{
			"ps_partkey",
			"ps_suppkey",
			"ps_availqty",
			"ps_supplycost",
			"ps_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeVarchar, NotNull: true, Width: 199},
		},
		PK: []int{0, 1},
		Column2Idx: map[string]int{
			"ps_partkey":    0,
			"ps_suppkey":    1,
			"ps_availqty":   2,
			"ps_supplycost": 3,
			"ps_comment":    4,
		},
		Stats: &Stats{
			RowCount: 800000,
		},
	}
	// nation
	cat.tpch["nation"] = &CatalogTable{
		Db:    "tpch",
		Table: "nation",
		Columns: []string{
			"n_nationkey",
			"n_name",
			"n_regionkey",
			"n_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 152},
		},
		PK: []int{0},
		Column2Idx: map[string]int{
			"n_nationkey": 0,
			"n_name":      1,
			"n_regionkey": 2,
			"n_comment":   3,
		},
		Stats: &Stats{
			RowCount: 25,
		},
	}
	// region
	cat.tpch["region"] = &CatalogTable{
		Db:    "tpch",
		Table: "region",
		Columns: []string{
			"r_regionkey",
			"r_name",
			"r_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeVarchar, NotNull: true, Width: 152},
		},
		PK: []int{0},
		Column2Idx: map[string]int{
			"r_regionkey": 0,
			"r_name":      1,
			"r_comment":   2,
		},
		Stats: &Stats{
			RowCount: 5,
		},
	}
	return cat
}
