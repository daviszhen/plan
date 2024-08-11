package main

func tpchQ1() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("l_returnflag"),
		column("l_linestatus"),
		withAlias(
			function("sum",
				column("l_quantity")),
			"sum_qty"),
		withAlias(
			function("sum",
				column("l_extendedprice")),
			"sum_base_price"),
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
			"sum_disc_price"),
		withAlias(
			function("sum",
				mul(
					mul(
						column("l_extendedprice"),
						sub(
							inumber(1),
							column("l_discount"),
						),
					),
					add(
						inumber(1),
						column("l_tax"),
					),
				),
			),
			"sum_charge"),
		withAlias(
			function("avg",
				column("l_quantity")),
			"avg_qty"),
		withAlias(
			function("avg",
				column("l_extendedprice")),
			"avg_price"),
		withAlias(
			function("avg",
				column("l_discount")),
			"avg_disc"),
		withAlias(
			function("count",
				column("*")),
			"count_order"),
	)
	ret.Select.From.Tables = table("lineitem")
	ret.Select.Where.Expr = lessEqual(
		column("l_shipdate"),
		sub(
			date("1998-12-01"),
			interval(112, "day"),
		),
	)
	ret.Select.GroupBy.Exprs = astList(
		column("l_returnflag"),
		column("l_linestatus"))
	ret.OrderBy.Exprs = astList(
		orderby(column("l_returnflag"), false),
		orderby(column("l_linestatus"), false),
	)
	return ret
}

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

func tpchQ3() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("l_orderkey"),
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
			"revenue"),
		column("o_orderdate"),
		column("o_shippriority"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("customer"),
		table("orders"),
		table("lineitem"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("c_mktsegment"), sstring("HOUSEHOLD")),
		equal(column("c_custkey"), column("o_custkey")),
		equal(column("l_orderkey"), column("o_orderkey")),
		less(column("o_orderdate"), date("1995-03-29")),
		greater(column("l_shipdate"), date("1995-03-29")),
	)
	ret.Select.GroupBy.Exprs = astList(
		column("l_orderkey"),
		column("o_orderdate"),
		column("o_shippriority"),
	)
	ret.OrderBy.Exprs = astList(
		orderby(column("revenue"), true),
		orderby(column("o_orderdate"), false),
	)
	ret.Limit.Count = inumber(10)
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

func tpchQ5() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("n_name"),
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
			"revenue"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("customer"),
		table("orders"),
		table("lineitem"),
		table("supplier"),
		table("nation"),
		table("region"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("c_custkey"), column("o_custkey")),
		equal(column("l_orderkey"), column("o_orderkey")),
		equal(column("l_suppkey"), column("s_suppkey")),
		equal(column("c_nationkey"), column("s_nationkey")),
		equal(column("s_nationkey"), column("n_nationkey")),
		equal(column("n_regionkey"), column("r_regionkey")),
		equal(column("r_name"), sstring("AMERICA")),
		greaterEqual(column("o_orderdate"), date("1994-01-01")),
		less(
			column("o_orderdate"),
			add(
				date("1994-01-01"),
				interval(1, "year"),
			),
		),
	)
	ret.Select.GroupBy.Exprs = astList(column("n_name"))
	ret.OrderBy.Exprs = astList(
		orderby(column("revenue"), true))
	return ret
}

func tpchQ6() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(
			function("sum",
				mul(
					column("l_extendedprice"),
					column("l_discount"),
				),
			),
			"revenue"),
	)
	ret.Select.From.Tables = table("lineitem")
	ret.Select.Where.Expr = andList(
		greaterEqual(column("l_shipdate"), date("1994-01-01")),
		less(
			column("l_shipdate"),
			add(
				date("1994-01-01"),
				interval(1, "year"),
			),
		),
		between(
			column("l_discount"),
			sub(fnumber(0.03), fnumber(0.01)),
			add(fnumber(0.03), fnumber(0.01)),
		),
		less(column("l_quantity"), inumber(24)),
	)
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

func tpchQ10() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("c_custkey"),
		column("c_name"),
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
			"revenue"),
		column("c_acctbal"),
		column("n_name"),
		column("c_address"),
		column("c_phone"),
		column("c_comment"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("customer"),
		table("orders"),
		table("lineitem"),
		table("nation"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("c_custkey"), column("o_custkey")),
		equal(column("l_orderkey"), column("o_orderkey")),
		greaterEqual(column("o_orderdate"), date("1993-03-01")),
		less(column("o_orderdate"), add(date("1993-03-01"), interval(3, "month"))),
		equal(column("l_returnflag"), sstring("R")),
		equal(column("c_nationkey"), column("n_nationkey")),
	)
	ret.Select.GroupBy.Exprs = astList(
		column("c_custkey"),
		column("c_name"),
		column("c_acctbal"),
		column("c_phone"),
		column("n_name"),
		column("c_address"),
		column("c_comment"),
	)
	ret.OrderBy.Exprs = astList(
		orderby(column("revenue"), true),
	)
	ret.Limit.Count = inumber(20)
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

func tpchQ12() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("l_shipmode"),
		withAlias(
			function("sum",
				caseWhen(
					nil,
					inumber(0),
					or(
						equal(column("o_orderpriority"), sstring("1-URGENT")),
						equal(column("o_orderpriority"), sstring("2-HIGH")),
					),
					inumber(1),
				)),
			"high_line_count"),
		withAlias(
			function("sum",
				caseWhen(
					nil,
					inumber(0),
					or(
						notEqual(column("o_orderpriority"), sstring("1-URGENT")),
						notEqual(column("o_orderpriority"), sstring("2-HIGH")),
					),
					inumber(1),
				)),
			"low_line_count"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("orders"),
		table("lineitem"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("o_orderkey"), column("l_orderkey")),
		in(column("l_shipmode"), sstring("FOB"), sstring("TRUCK")),
		less(column("l_commitdate"), column("l_receiptdate")),
		less(column("l_shipdate"), column("l_commitdate")),
		greaterEqual(column("l_receiptdate"), date("1996-01-01")),
		less(column("l_receiptdate"), add(date("1996-01-01"), interval(1, "year"))),
	)
	ret.Select.GroupBy.Exprs = astList(
		column("l_shipmode"),
	)
	ret.OrderBy.Exprs = astList(
		orderby(column("l_shipmode"), false),
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

func tpchQ14() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(
			div(
				mul(
					fnumber(100),
					function("sum",
						caseWhen(
							nil,
							inumber(0),
							like(column("p_type"), sstring("PROMO%")),
							mul(
								column("l_extendedprice"),
								sub(
									inumber(1),
									column("l_discount"),
								),
							),
						),
					),
				),
				function("sum",
					mul(
						column("l_extendedprice"),
						sub(
							inumber(1),
							column("l_discount"),
						),
					),
				),
			),
			"promo_revenue"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("lineitem"),
		table("part"),
	)
	ret.Select.Where.Expr = andList(
		equal(column("l_partkey"), column("p_partkey")),
		greaterEqual(column("l_shipdate"), date("1996-04-01")),
		less(column("l_shipdate"), add(date("1996-04-01"), interval(1, "month"))),
	)
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
	ret.OrderBy.Exprs = astList(
		orderby(column("s_suppkey"), false),
	)
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
		notlike(column("p_type"), sstring("ECONOMY BURNISHED%")),
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

func tpchQ17() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(
			div(
				function("sum",
					column("l_extendedprice")),
				fnumber(7.0)),
			"avg_yearly"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("lineitem"),
		table("part"))
	ret.Select.Where.Expr = andList(
		equal(column("p_partkey"), column("l_partkey")),
		equal(column("p_brand"), sstring("Brand#54")),
		equal(column("p_container"), sstring("LG BAG")),
		less(column("l_quantity"), subquery(q17Subquery(), AstSubqueryTypeScalar)),
	)
	return ret
}

func q17Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		mul(
			fnumber(0.2),
			function("avg", column("l_quantity")),
		),
	)
	ret.Select.From.Tables = table("lineitem")
	ret.Select.Where.Expr = equal(column("l_partkey"), column("p_partkey"))
	return ret
}

func tpchQ18() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("c_name"),
		column("c_custkey"),
		column("o_orderkey"),
		column("o_orderdate"),
		column("o_totalprice"),
		function("sum", column("l_quantity")))
	ret.Select.From.Tables = crossJoinList(
		table("customer"),
		table("orders"),
		table("lineitem"),
	)
	ret.Select.Where.Expr = andList(
		in(column("o_orderkey"), subquery(q18Subquery(), AstSubqueryTypeScalar)),
		equal(column("c_custkey"), column("o_custkey")),
		equal(column("o_orderkey"), column("l_orderkey")),
	)
	ret.Select.GroupBy.Exprs = astList(
		column("c_name"),
		column("c_custkey"),
		column("o_orderkey"),
		column("o_orderdate"),
		column("o_totalprice"))
	ret.OrderBy.Exprs = astList(
		orderby(column("o_totalprice"), true),
		orderby(column("o_orderdate"), false),
	)
	ret.Limit.Count = inumber(100)
	return ret
}

func q18Subquery() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("l_orderkey"),
	)
	ret.Select.From.Tables = table("lineitem")
	ret.Select.GroupBy.Exprs = astList(column("l_orderkey"))
	ret.Select.Having.Expr = greater(
		function("sum", column("l_quantity")),
		inumber(314))
	return ret
}

func tpchQ19() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
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
			"revenue"),
	)
	ret.Select.From.Tables = crossJoinList(
		table("lineitem"),
		table("part"),
	)
	ret.Select.Where.Expr = orList(
		andList(
			equal(column("p_partkey"), column("l_partkey")),
			equal(column("p_brand"), sstring("Brand#23")),
			in(column("p_container"), sstring("SM CASE"), sstring("SM BOX"), sstring("SM PACK"), sstring("SM PKG")),
			greaterEqual(column("l_quantity"), inumber(5)),
			lessEqual(column("l_quantity"), inumber(15)),
			between(column("p_size"), inumber(1), inumber(5)),
			in(column("l_shipmode"), sstring("AIR"), sstring("AIR REG")),
			equal(column("l_shipinstruct"), sstring("DELIVER IN PERSON")),
		),
		andList(
			equal(column("p_partkey"), column("l_partkey")),
			equal(column("p_brand"), sstring("Brand#15")),
			in(column("p_container"), sstring("MED BAG"), sstring("MED BOX"), sstring("MED PKG"), sstring("MED PACK")),
			greaterEqual(column("l_quantity"), inumber(14)),
			lessEqual(column("l_quantity"), inumber(24)),
			between(column("p_size"), inumber(1), inumber(10)),
			in(column("l_shipmode"), sstring("AIR"), sstring("AIR REG")),
			equal(column("l_shipinstruct"), sstring("DELIVER IN PERSON")),
		),
		andList(
			equal(column("p_partkey"), column("l_partkey")),
			equal(column("p_brand"), sstring("Brand#44")),
			in(column("p_container"), sstring("LG CASE"), sstring("LG BOX"), sstring("LG PACK"), sstring("LG PKG")),
			greaterEqual(column("l_quantity"), inumber(28)),
			lessEqual(column("l_quantity"), inumber(38)),
			between(column("p_size"), inumber(1), inumber(15)),
			in(column("l_shipmode"), sstring("AIR"), sstring("AIR REG")),
			equal(column("l_shipinstruct"), sstring("DELIVER IN PERSON")),
		),
	)
	return ret
}

func tpchQ20() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("s_name"),
		column("s_address"))
	ret.Select.From.Tables = crossJoinList(
		table("supplier"),
		table("nation"))
	ret.Select.Where.Expr = andList(
		in(
			column("s_suppkey"),
			subquery(q20Subquery1(), AstSubqueryTypeScalar),
		),
		equal(column("s_nationkey"), column("n_nationkey")),
		equal(column("n_name"), sstring("VIETNAM")),
	)
	ret.OrderBy.Exprs = astList(
		orderby(column("s_name"), false))
	return ret
}

func q20Subquery1() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(column("ps_suppkey"))
	ret.Select.From.Tables = table("partsupp")
	ret.Select.Where.Expr = andList(
		in(
			column("ps_partkey"),
			subquery(q20Subquery21(), AstSubqueryTypeScalar),
		),
		greater(
			column("ps_availqty"),
			subquery(q20Subquery22(), AstSubqueryTypeScalar),
		))
	return ret
}

func q20Subquery21() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(column("p_partkey"))
	ret.Select.From.Tables = table("part")
	ret.Select.Where.Expr = like(column("p_name"), sstring("lime%"))
	return ret
}

func q20Subquery22() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		mul(
			fnumber(0.5),
			function("sum", column("l_quantity")),
		),
	)
	ret.Select.From.Tables = table("lineitem")
	ret.Select.Where.Expr = andList(
		equal(column("l_partkey"), column("ps_partkey")),
		equal(column("l_suppkey"), column("ps_suppkey")),
		greaterEqual(column("l_shipdate"), date("1993-01-01")),
		less(column("l_shipdate"),
			add(
				date("1993-01-01"),
				interval(1, "year"))),
	)
	return ret
}

func tpchQ21() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("s_name"),
		withAlias(function("count", column("*")), "numwait"))
	ret.Select.From.Tables = crossJoinList(
		table("supplier"),
		withAlias(table("lineitem"), "l1"),
		table("orders"),
		table("nation"))
	ret.Select.Where.Expr = andList(
		equal(column("s_suppkey"), column2("l1", "l_suppkey")),
		equal(column("o_orderkey"), column2("l1", "l_orderkey")),
		equal(column("o_orderstatus"), sstring("F")),
		greater(column2("l1", "l_receiptdate"), column2("l1", "l_commitdate")),
		exists(subquery(q21Subquery1(), AstSubqueryTypeExists)),
		notExists(subquery(q21Subquery2(), AstSubqueryTypeNotExists)),
		equal(column("s_nationkey"), column("n_nationkey")),
		equal(column("n_name"), sstring("BRAZIL")),
	)
	ret.Select.GroupBy.Exprs = astList(column("s_name"))
	ret.OrderBy.Exprs = astList(
		orderby(column("numwait"), true),
		orderby(column("s_name"), false),
	)
	ret.Limit.Count = inumber(100)
	return ret
}

func q21Subquery1() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(column("*"))
	ret.Select.From.Tables = withAlias(table("lineitem"), "l2")
	ret.Select.Where.Expr = andList(
		equal(column2("l2", "l_orderkey"), column2("l1", "l_orderkey")),
		notEqual(column2("l2", "l_suppkey"), column2("l1", "l_suppkey")),
	)
	return ret
}

func q21Subquery2() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(column("*"))
	ret.Select.From.Tables = withAlias(table("lineitem"), "l3")
	ret.Select.Where.Expr = andList(
		equal(column2("l3", "l_orderkey"), column2("l1", "l_orderkey")),
		notEqual(column2("l3", "l_suppkey"), column2("l1", "l_suppkey")),
		greater(column2("l3", "l_receiptdate"), column2("l3", "l_commitdate")),
	)
	return ret
}

func tpchQ22() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("cntrycode"),
		withAlias(
			function("count", column("*")),
			"numcust"),
		withAlias(
			function("sum", column("c_acctbal")),
			"totacctbal"),
	)
	ret.Select.From.Tables = withAlias(
		subquery(q22Subquery1(), AstSubqueryTypeFrom),
		"custsale")
	ret.Select.GroupBy.Exprs = astList(column("cntrycode"))
	ret.OrderBy.Exprs = astList(
		orderby(column("cntrycode"), false))
	return ret
}

func q22Subquery1() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		withAlias(
			function("substring",
				column("c_phone"),
				inumber(1),
				inumber(2),
			),
			"cntrycode"),
		column("c_acctbal"),
	)
	ret.Select.From.Tables = table("customer")
	ret.Select.Where.Expr = andList(
		in(
			function("substring",
				column("c_phone"),
				inumber(1),
				inumber(2),
			),
			sstring("10"),
			sstring("11"),
			sstring("26"),
			sstring("22"),
			sstring("19"),
			sstring("20"),
			sstring("27"),
		),
		greater(column("c_acctbal"), subquery(q22Subquery21(), AstSubqueryTypeScalar)),
		notExists(subquery(q22Subquery22(), AstSubqueryTypeNotExists)),
	)
	return ret
}

func q22Subquery21() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		function("avg", column("c_acctbal")))
	ret.Select.From.Tables = table("customer")
	ret.Select.Where.Expr = andList(
		greater(column("c_acctbal"), fnumber(0)),
		in(
			function("substring",
				column("c_phone"),
				inumber(1),
				inumber(2),
			),
			sstring("10"),
			sstring("11"),
			sstring("26"),
			sstring("22"),
			sstring("19"),
			sstring("20"),
			sstring("27"),
		),
	)
	return ret
}

func q22Subquery22() *Ast {
	ret := &Ast{Typ: AstTypeSelect}
	ret.Select.SelectExprs = astList(
		column("*"))
	ret.Select.From.Tables = table("orders")
	ret.Select.Where.Expr = equal(column("o_custkey"), column("c_custkey"))
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
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(55), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: varchar2(10), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(10), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: varchar2(23), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: varchar2(40), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(15), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: varchar2(101), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: varchar2(199), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(152), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: varchar2(152), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(1), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: dateLTyp(), NotNull: true},
			{LTyp: varchar2(15), NotNull: true},
			{LTyp: varchar2(15), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(79), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: varchar2(1), NotNull: true},
			{LTyp: varchar2(1), NotNull: true},
			{LTyp: dateLTyp(), NotNull: true},
			{LTyp: dateLTyp(), NotNull: true},
			{LTyp: dateLTyp(), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: varchar2(10), NotNull: true},
			{LTyp: varchar2(44), NotNull: true},
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
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(25), NotNull: true},
			{LTyp: varchar2(40), NotNull: true},
			{LTyp: integer(), NotNull: true},
			{LTyp: varchar2(15), NotNull: true},
			{LTyp: decimal(15, 2), NotNull: true},
			{LTyp: varchar2(10), NotNull: true},
			{LTyp: varchar2(117), NotNull: true},
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
