// Copyright 2023-2024 daviszhen
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/daviszhen/plan/pkg/common"
)

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
		Types: []common.LType{
			common.IntegerType(),
			common.VarcharType2(55),
			common.VarcharType2(25),
			common.VarcharType2(10),
			common.VarcharType2(25),
			common.IntegerType(),
			common.VarcharType2(10),
			common.DecimalType(15, 2),
			common.VarcharType2(23),
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
		Types: []common.LType{
			common.IntegerType(),
			common.VarcharType2(25),
			common.VarcharType2(40),
			common.IntegerType(),
			common.VarcharType2(15),
			common.DecimalType(15, 2),
			common.VarcharType2(101),
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
		Types: []common.LType{
			common.IntegerType(),
			common.IntegerType(),
			common.IntegerType(),
			common.DecimalType(15, 2),
			common.VarcharType2(199),
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
		Types: []common.LType{
			common.IntegerType(),
			common.VarcharType2(25),
			common.IntegerType(),
			common.VarcharType2(152),
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
		Types: []common.LType{
			common.IntegerType(),
			common.VarcharType2(25),
			common.VarcharType2(152),
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
		Types: []common.LType{
			common.IntegerType(),
			common.IntegerType(),
			common.VarcharType2(1),
			common.DecimalType(15, 2),
			common.DateType(),
			common.VarcharType2(15),
			common.VarcharType2(15),
			common.IntegerType(),
			common.VarcharType2(79),
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
		Types: []common.LType{
			common.IntegerType(),
			common.IntegerType(),
			common.IntegerType(),
			common.IntegerType(),
			common.IntegerType(),
			common.DecimalType(15, 2),
			common.DecimalType(15, 2),
			common.DecimalType(15, 2),
			common.VarcharType2(1),
			common.VarcharType2(1),
			common.DateType(),
			common.DateType(),
			common.DateType(),
			common.VarcharType2(25),
			common.VarcharType2(10),
			common.VarcharType2(44),
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
		Types: []common.LType{
			common.IntegerType(),
			common.VarcharType2(25),
			common.VarcharType2(40),
			common.IntegerType(),
			common.VarcharType2(15),
			common.DecimalType(15, 2),
			common.VarcharType2(10),
			common.VarcharType2(117),
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
