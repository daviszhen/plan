package main

// correlated
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
	w8 := equal(column("ps_supplycost"), subquery(q2Subquery(), AstSubqueryTypeScalar))

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

// correlated
func tpchQ4() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("o_orderpriority"),
		withAlias(function("count", column("*")), "order_count"),
	)
	ret.Select.From.Tables = table("orders")
	w1 := greaterEqual(column("o_orderdate"), date("1997-07-01"))
	w2 := less(column("o_orderdate"),
		function("date_add",
			date("1997-07-01"),
			interval(3, "month")))
	w3 := exists(subquery(q4Subquery(), AstSubqueryTypeExists))
	ret.Select.Where.Expr = and(and(w1, w2), w3)
	ret.Select.GroupBy.Exprs = astList(column("o_orderpriority"))
	ret.OrderBy.Exprs = astList(orderby(column("o_orderpriority"), false))
	return ret
}

func q4Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(column("*"))
	ret.Select.From.Tables = table("lineitem")
	w1 := equal(column("l_orderkey"), column("o_orderkey"))
	w2 := less(column("l_commitdate"), column("l_receiptdate"))
	ret.Select.Where.Expr = and(w1, w2)
	return ret
}

func tpchQ7() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("supp_nation"),
		column("cust_nation"),
		column("l_year"),
		withAlias(function("sum", column("volume")), "revenue"),
	)
	ret.Select.From.Tables = withAlias(
		subquery(q7Subquery(), AstSubqueryTypeFrom),
		"shipping")
	ret.Select.GroupBy.Exprs = astList(
		column("supp_nation"),
		column("cust_nation"),
		column("l_year"))
	ret.OrderBy.Exprs = astList(
		orderby(column("supp_nation"), false),
		orderby(column("cust_nation"), false),
		orderby(column("l_year"), false))
	return ret
}

func q7Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(column2("n1", "n_name"), "supp_nation"),
		withAlias(column2("n2", "n_name"), "cust_nation"),
		withAlias(
			function("extract",
				sstring("year"),
				column("l_shipdate"),
			),
			"l_year"),
		withAlias(
			mul(
				column("l_extendedprice"),
				sub(
					inumber(1),
					column("l_discount"),
				),
			),
			"volume"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("supplier"),
		table("lineitem"),
		table("orders"),
		table("customer"),
		withAlias(table("nation"), "n1"),
		withAlias(table("nation"), "n2"),
	)

	w1 := equal(column("s_suppkey"), column("l_suppkey"))
	w2 := equal(column("o_orderkey"), column("l_orderkey"))
	w3 := equal(column("c_custkey"), column("o_custkey"))
	w4 := equal(column("s_nationkey"), column2("n1", "n_nationkey"))
	w5 := equal(column("c_nationkey"), column2("n2", "n_nationkey"))
	w6 :=
		or(
			and(
				equal(
					column2("n1", "n_name"),
					sstring("FRANCE")),
				equal(
					column2("n2", "n_name"),
					sstring("ARGENTINA"))),
			and(
				equal(
					column2("n1", "n_name"),
					sstring("ARGENTINA")),
				equal(
					column2("n2", "n_name"),
					sstring("FRANCE"))))
	w7 := between(column("l_shipdate"), date("1995-01-01"), date("1996-12-31"))
	ret.Select.Where.Expr = andList(w1, w2, w3, w4, w5, w6, w7)
	return ret
}

func tpchQ8() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("o_year"),
		withAlias(
			div(
				function("sum",
					caseWhen(nil,
						inumber(0),
						equal(column("nation"), sstring("ARGENTINA")), column("volume"),
					)),
				function("sum", column("volume")),
			),
			"mkt_share"),
	)
	ret.Select.From.Tables = withAlias(subquery(q8Subquery(), AstSubqueryTypeFrom), "all_nations")
	ret.Select.GroupBy.Exprs = astList(column("o_year"))
	ret.OrderBy.Exprs = astList(orderby(column("o_year"), false))
	return ret
}

func q8Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(
			function("extract",
				sstring("year"),
				column("o_orderdate")),
			"o_year"),
		withAlias(
			mul(
				column("l_extendedprice"),
				sub(
					inumber(1),
					column("l_discount"),
				),
			),
			"volume"),
		withAlias(
			column2("n2", "n_name"),
			"nation"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("part"),
		table("supplier"),
		table("lineitem"),
		table("orders"),
		table("customer"),
		withAlias(table("nation"), "n1"),
		withAlias(table("nation"), "n2"),
		table("region"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("p_partkey"), column("l_partkey")),
		equal(column("s_suppkey"), column("l_suppkey")),
		equal(column("l_orderkey"), column("o_orderkey")),
		equal(column("o_custkey"), column("c_custkey")),
		equal(column("c_nationkey"), column2("n1", "n_nationkey")),
		equal(column2("n1", "n_regionkey"), column("r_regionkey")),
		equal(column("r_name"), sstring("AMERICA")),
		equal(column("s_nationkey"), column2("n2", "n_nationkey")),
		between(column("o_orderdate"), date("1995-01-01"), date("1996-12-31")),
		equal(column("p_type"), sstring("ECONOMY BURNISHED TIN")),
	)
	return ret
}

func tpchQ9() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("nation"),
		column("o_year"),
		withAlias(
			function("sum", column("amount")),
			"sum_profit"),
	)
	ret.Select.From.Tables = withAlias(
		subquery(q9Subquery(), AstSubqueryTypeFrom),
		"profit")
	ret.Select.GroupBy.Exprs = astList(
		column("nation"),
		column("o_year"),
	)
	ret.OrderBy.Exprs = astList(
		orderby(column("nation"), false),
		orderby(column("o_year"), true),
	)
	return ret
}

func q9Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(column("n_name"), "nation"),
		withAlias(
			function("extract", sstring("year"), column("o_orderdate")),
			"o_year"),
		withAlias(
			sub(
				mul(
					column("l_extendedprice"),
					sub(
						inumber(1),
						column("l_discount"),
					),
				),
				mul(
					column("ps_supplycost"),
					column("l_quantity"),
				),
			),
			"amount"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("part"),
		table("supplier"),
		table("lineitem"),
		table("partsupp"),
		table("orders"),
		table("nation"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("s_suppkey"), column("l_suppkey")),
		equal(column("ps_suppkey"), column("l_suppkey")),
		equal(column("ps_partkey"), column("l_partkey")),
		equal(column("p_partkey"), column("l_partkey")),
		equal(column("o_orderkey"), column("l_orderkey")),
		equal(column("s_nationkey"), column("n_nationkey")),
		like(column("p_name"), sstring("%pink%")),
	)
	return ret
}

func tpchQ11() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("ps_partkey"),
		withAlias(
			function("sum",
				mul(
					column("ps_supplycost"),
					column("ps_availqty"),
				),
			),
			"value"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("partsupp"),
		table("supplier"),
		table("nation"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("ps_suppkey"), column("s_suppkey")),
		equal(column("s_nationkey"), column("n_nationkey")),
		equal(column("n_name"), sstring("JAPAN")),
	)
	ret.Select.GroupBy.Exprs = astList(column("ps_partkey"))
	ret.Select.Having.Expr = greater(
		function("sum",
			mul(
				column("ps_supplycost"),
				column("ps_availqty"),
			),
		),
		subquery(q11Subquery(), AstSubqueryTypeScalar),
	)
	ret.OrderBy.Exprs = astList(orderby(column("value"), true))
	return ret
}

func q11Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		mul(
			function("sum",
				mul(
					column("ps_supplycost"),
					column("ps_availqty"),
				),
			),
			fnumber(0.0001000000),
		),
	)
	ret.Select.From.Tables = crossJoinList(
		table("partsupp"),
		table("supplier"),
		table("nation"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("ps_suppkey"), column("s_suppkey")),
		equal(column("s_nationkey"), column("n_nationkey")),
		equal(column("n_name"), sstring("JAPAN")),
	)
	return ret
}

func tpchQ13() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("c_count"),
		withAlias(
			function("count", column("*")),
			"custdist"),
	)
	ret.Select.From.Tables = withAlias2(
		subquery(q13Subquery(), AstSubqueryTypeFrom),
		"c_orders",
		"c_custkey", "c_count",
	)
	ret.Select.GroupBy.Exprs = astList(column("c_count"))
	ret.OrderBy.Exprs = astList(
		orderby(column("custdist"), true),
		orderby(column("c_count"), true))
	return ret
}

func q13Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("c_custkey"),
		function("count",
			column("o_orderkey")))
	ret.Select.From.Tables = leftJoin(
		table("customer"),
		table("orders"),
		andList(
			equal(
				column("c_custkey"),
				column("o_custkey"),
			),
			notlike(
				column("o_comment"),
				sstring("%pending%accounts%"),
			),
		),
	)
	ret.Select.GroupBy.Exprs = astList(column("c_custkey"))
	return ret
}

func tpchQ15() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.With.Ctes = append(ret.With.Ctes, q15Cte())
	ret.Select.SelectExprs = astList(
		column("s_suppkey"),
		column("s_name"),
		column("s_address"),
		column("s_phone"),
		column("total_revenue"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("supplier"),
		table("q15_revenue0"),
	)
	ret.Select.Where.Expr = andList(
		equal(
			column("s_suppkey"),
			column("supplier_no"),
		),
		equal(
			column("total_revenue"),
			subquery(q15Subquery(), AstSubqueryTypeScalar),
		),
	)
	ret.OrderBy.Exprs = astList(column("s_suppkey"))
	return ret
}

func q15Cte() *Ast {
	ret := &Ast{Typ: AstTypeCTE}
	ret.Expr.Alias.alias = "q15_revenue0"
	ret.Expr.Children = []*Ast{q15CteImpl()}
	return ret
}

func q15CteImpl() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(
			column("l_suppkey"),
			"supplier_no"),
		withAlias(
			function("sum",
				mul(
					column("l_extendedprice"),
					sub(
						inumber(1),
						column("l_discount"),
					),
				),
			),
			"total_revenue"),
	)
	ret.Select.From.Tables = table("lineitem")
	ret.Select.Where.Expr = andList(
		greaterEqual(
			column("l_shipdate"),
			date("1995-12-01"),
		),
		less(
			column("l_shipdate"),
			function("date_add",
				date("1995-12-01"),
				interval(3, "month"),
			),
		),
	)
	ret.Select.GroupBy.Exprs = astList(column("l_suppkey"))
	return ret
}

func q15Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		function("max",
			column("total_revenue")),
	)
	ret.Select.From.Tables = table("q15_revenue0")
	return ret
}

func tpchQ16() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("p_brand"),
		column("p_type"),
		column("p_size"),
		withAlias(
			functionDistinct("count", column("ps_suppkey")),
			"supplier_cnt"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("partsupp"),
		table("part"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("p_partkey"), column("ps_partkey")),
		notEqual(column("p_brand"), sstring("Brand#35")),
		notEqual(column("p_type"), sstring("ECONOMY BURNISHED%")),
		in(column("p_size"),
			inumber(14),
			inumber(7),
			inumber(21),
			inumber(24),
			inumber(35),
			inumber(33),
			inumber(2),
			inumber(20),
		),
		notIn(column("ps_suppkey"),
			subquery(q16Subquery(), AstSubqueryTypeScalar),
		),
	)
	ret.Select.GroupBy.Exprs = astList(
		column("p_brand"),
		column("p_type"),
		column("p_size"),
	)
	ret.OrderBy.Exprs = astList(
		orderby(column("supplier_cnt"), true),
		orderby(column("p_brand"), false),
		orderby(column("p_type"), false),
		orderby(column("p_size"), false),
	)
	return ret
}

func q16Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("s_suppkey"),
	)
	ret.Select.From.Tables = table("supplier")
	ret.Select.Where.Expr = like(
		column("s_comment"),
		sstring("%Customer%Complaints%"),
	)
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
			ColStats: []*BaseStats{
				{distinctCount: 200000},
				{distinctCount: 199997},
				{distinctCount: 5},
				{distinctCount: 25},
				{distinctCount: 150},
				{distinctCount: 50},
				{distinctCount: 40},
				{distinctCount: 20899},
				{distinctCount: 131753},
			},
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
			ColStats: []*BaseStats{
				{distinctCount: 10000},
				{distinctCount: 10000},
				{distinctCount: 10000},
				{distinctCount: 25},
				{distinctCount: 10000},
				{distinctCount: 9955},
				{distinctCount: 10000},
			},
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
			ColStats: []*BaseStats{
				{distinctCount: 200000},
				{distinctCount: 10000},
				{distinctCount: 9999},
				{distinctCount: 99865},
				{distinctCount: 799124},
			},
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
			ColStats: []*BaseStats{
				{distinctCount: 25},
				{distinctCount: 25},
				{distinctCount: 5},
				{distinctCount: 25},
			},
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
			ColStats: []*BaseStats{
				{distinctCount: 5},
				{distinctCount: 5},
				{distinctCount: 5},
			},
		},
	}
	// orders
	cat.tpch["orders"] = &CatalogTable{
		Db:    "tpch",
		Table: "orders",
		Columns: []string{
			"o_orderkey",
			"o_custkey",
			"o_orderstatus",
			"o_totalprice",
			"o_orderdate",
			"o_orderpriority",
			"o_clerk",
			"o_shippriority",
			"o_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 1},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeDate, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 15},
			{Typ: DataTypeVarchar, NotNull: true, Width: 15},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 79},
		},
		PK: []int{0},
		Column2Idx: map[string]int{
			"o_orderkey":      0,
			"o_custkey":       1,
			"o_orderstatus":   2,
			"o_totalprice":    3,
			"o_orderdate":     4,
			"o_orderpriority": 5,
			"o_clerk":         6,
			"o_shippriority":  7,
			"o_comment":       8,
		},
		Stats: &Stats{
			RowCount: 1500000,
			ColStats: []*BaseStats{
				{distinctCount: 1500000},
				{distinctCount: 99996},
				{distinctCount: 3},
				{distinctCount: 1464556},
				{distinctCount: 2406},
				{distinctCount: 5},
				{distinctCount: 1000},
				{distinctCount: 1},
				{distinctCount: 1482071},
			},
		},
	}

	// lineitem
	cat.tpch["lineitem"] = &CatalogTable{
		Db:    "tpch",
		Table: "lineitem",
		Columns: []string{
			"l_orderkey",
			"l_partkey",
			"l_suppkey",
			"l_linenumber",
			"l_quantity",
			"l_extendedprice",
			"l_discount",
			"l_tax",
			"l_returnflag",
			"l_linestatus",
			"l_shipdate",
			"l_commitdate",
			"l_receiptdate",
			"l_shipinstruct",
			"l_shipmode",
			"l_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeVarchar, NotNull: true, Width: 1},
			{Typ: DataTypeVarchar, NotNull: true, Width: 1},
			{Typ: DataTypeDate, NotNull: true},
			{Typ: DataTypeDate, NotNull: true},
			{Typ: DataTypeDate, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeVarchar, NotNull: true, Width: 10},
			{Typ: DataTypeVarchar, NotNull: true, Width: 44},
		},
		PK: []int{0, 3},
		Column2Idx: map[string]int{
			"l_orderkey":      0,
			"l_partkey":       1,
			"l_suppkey":       2,
			"l_linenumber":    3,
			"l_quantity":      4,
			"l_extendedprice": 5,
			"l_discount":      6,
			"l_tax":           7,
			"l_returnflag":    8,
			"l_linestatus":    9,
			"l_shipdate":      10,
			"l_commitdate":    11,
			"l_receiptdate":   12,
			"l_shipinstruct":  13,
			"l_shipmode":      14,
			"l_comment":       15,
		},
		Stats: &Stats{
			RowCount: 6001215,
			ColStats: []*BaseStats{
				{distinctCount: 1500000},
				{distinctCount: 200000},
				{distinctCount: 10000},
				{distinctCount: 7},
				{distinctCount: 50},
				{distinctCount: 933900},
				{distinctCount: 11},
				{distinctCount: 9},
				{distinctCount: 3},
				{distinctCount: 2},
				{distinctCount: 2526},
				{distinctCount: 2466},
				{distinctCount: 2554},
				{distinctCount: 4},
				{distinctCount: 7},
				{distinctCount: 4580667},
			},
		},
	}

	//customer
	cat.tpch["customer"] = &CatalogTable{
		Db:    "tpch",
		Table: "customer",
		Columns: []string{
			"c_custkey",
			"c_name",
			"c_address",
			"c_nationkey",
			"c_phone",
			"c_acctbal",
			"c_mktsegment",
			"c_comment",
		},
		Types: []ExprDataType{
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 25},
			{Typ: DataTypeVarchar, NotNull: true, Width: 40},
			{Typ: DataTypeInteger, NotNull: true},
			{Typ: DataTypeVarchar, NotNull: true, Width: 15},
			{Typ: DataTypeDecimal, NotNull: true, Width: 15, Scale: 2},
			{Typ: DataTypeVarchar, NotNull: true, Width: 10},
			{Typ: DataTypeVarchar, NotNull: true, Width: 117},
		},
		PK: []int{0},
		Column2Idx: map[string]int{
			"c_custkey":    0,
			"c_name":       1,
			"c_address":    2,
			"c_nationkey":  3,
			"c_phone":      4,
			"c_acctbal":    5,
			"c_mktsegment": 6,
			"c_comment":    7,
		},
		Stats: &Stats{
			RowCount: 150000,
			ColStats: []*BaseStats{
				{distinctCount: 150000},
				{distinctCount: 150000},
				{distinctCount: 150000},
				{distinctCount: 25},
				{distinctCount: 150000},
				{distinctCount: 140187},
				{distinctCount: 5},
				{distinctCount: 149968},
			},
		},
	}
	return cat
}
