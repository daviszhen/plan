package main

import (
	"fmt"
	"math"
	"testing"
	"time"

	dec "github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"
)

func findOperator(root *PhysicalOperator, fun func(root *PhysicalOperator) bool) []*PhysicalOperator {
	ret := make([]*PhysicalOperator, 0)
	if fun != nil && fun(root) {
		ret = append(ret, root)
	}
	for _, child := range root.Children {
		ret = append(ret, findOperator(child, fun)...)
	}
	return ret
}

const (
	//maxTestCnt = 20
	maxTestCnt = math.MaxInt
)

func runOps(t *testing.T, ops []*PhysicalOperator) {
	for _, op := range ops {

		//if i != 2 {
		//	continue
		//}

		//fmt.Println(op.String())

		run := &Runner{
			op:    op,
			state: &OperatorState{},
		}
		err := run.Init()
		assert.NoError(t, err)

		rowCnt := 0
		for {
			if rowCnt >= maxTestCnt {
				break
			}
			output := &Chunk{}
			output.setCap(defaultVectorSize)
			result, err := run.Execute(nil, output, run.state)
			assert.NoError(t, err)

			if err != nil {
				break
			}
			if result == Done {
				break
			}
			if output.card() > 0 {
				assertFunc(output.card() != 0)
				assert.NotEqual(t, 0, output.card())
				rowCnt += output.card()
				if !gConf.SkipOutput {
					output.print()
				}
			}
		}
		fmt.Println("row Count", rowCnt)
		run.Close()
	}
}

func wantedOp(root *PhysicalOperator, pt POT) bool {
	if root == nil {
		return false
	}
	if root.Typ == pt {
		return true
	}
	return false
}

func Test_1g_q20_scanExec(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Scan)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_projectExec(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) && wantedOp(root.Children[0], POT_Scan)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_innerJoin(t *testing.T) {
	/*
		equal to:

		select
			lr.s_suppkey,
			lr.s_name,
			lr.s_address
		from
		(
			select
				s_suppkey,
				s_name,
				s_address,
				s_nationkey
			from
				supplier
		) lr
		join
		(
			select
			n_nationkey
			from
				nation
			where n_name = 'VIETNAM'
		) rr
		on lr.s_nationkey = rr.n_nationkey

		result: tpch1g 399 rows
	*/
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Join) &&
				wantedOp(root.Children[0], POT_Scan) &&
				wantedOp(root.Children[1], POT_Scan)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_innerJoin2(t *testing.T) {
	/*
		equal to:
		select
			lr.ps_partkey,
			lr.ps_suppkey,
			lr.ps_availqty
		from
			(
				select
					ps_partkey,
					ps_suppkey,
					ps_availqty
				from partsupp
			) lr
			join
			(
				select
					p_partkey
				from part
				where p_name like 'lime%'
			) rr
			on lr.ps_partkey = rr.p_partkey

		result: tpch1g 8644 rows
	*/
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Join) &&
				wantedOp(root.Children[0], POT_Scan) &&
				wantedOp(root.Children[1], POT_Project)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_innerJoin3(t *testing.T) {
	/*
		equal to:

			select
				rr.ps_partkey,
				rr.ps_suppkey,
				rr.ps_availqty,
				lr.l_quantity
			from
				(
					select
						lineitem.l_partkey,
						lineitem.l_suppkey,
						lineitem.l_quantity
					from lineitem
					where
						l_shipdate >= date '1993-01-01' and
						l_shipdate < date '1993-01-01' + interval '1' year

				) lr
			join
				(
					select
						partsupp.ps_partkey,
						partsupp.ps_suppkey,
						partsupp.ps_availqty
					from
						partsupp join part on partsupp.ps_partkey = part.p_partkey
					where part.p_name like 'lime%'
				) rr
				on lr.l_partkey = rr.ps_partkey and
					lr.l_suppkey = rr.ps_suppkey

		result: tpch1g 9767 rows
	*/
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Join) &&
				wantedOp(root.Children[0], POT_Scan) &&
				wantedOp(root.Children[1], POT_Join)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_HashAggr(t *testing.T) {
	/*
		PhysicalPlan:
		└── Aggregate:
		    ├── [outputs]
		    │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		    ├── [estCard]  24004860
		    ├── groupExprs, index 15
		    │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		    │   └── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		    ├── aggExprs, index 16
		    │   └── [0  {(LTID_INTEGER INT32 0 0),null}]  sum
		    │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[-1 3],0)
		    ├── filters
		    │   └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >
		    │       ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		    │       │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		    │       │   └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		    │       └── [ {(LTID_FLOAT FLOAT 0 0),not null}]  *
		    │           ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0.5)
		    │           └── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		    │               ├── [ {(LTID_INTEGER INT32 0 0),null}]  (AggNode_16.sum(l_quantity),[16 0],0)
		    │               └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		    └── Join (inner):
		        ├── [outputs]
		        │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		        │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		        │   ├── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-2 2],0)
		        │   └── [3  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[-1 2],0)
		        ├── [estCard]  24004860
		        ├── [On]
		        │   ├── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		        │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[-1 0],0)
		        │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		        │   ├── [1  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		        │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[-1 1],0)
		        │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		        │   ├── [2  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		        │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[-1 0],0)
		        │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		        │   └── [3  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		        │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[-1 1],0)
		        │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		        ├── Scan:
		        │   ├── [outputs]
		        │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[0 0],0)
		        │   │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[0 1],0)
		        │   │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[0 2],0)
		        │   ├── [estCard]  6001215
		        │   ├── [index]  17
		        │   ├── [table]  tpch.lineitem
		        │   ├── [columns]
		        │   │   col 0 l_partkey
		        │   │   col 1 l_suppkey
		        │   │   col 2 l_quantity
		        │   │   col 3 l_shipdate
		        │   │
		        │   └── filters
		        │       ├── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >=
		        │       │   ├── [ {(LTID_DATE DATE 0 0),not null}]  (lineitem.l_shipdate,[17 3],0)
		        │       │   └── [ {(LTID_DATE DATE 0 0),null}]  (1993-01-01)
		        │       └── [1  {(LTID_BOOLEAN BOOL 0 0),null}]  <
		        │           ├── [ {(LTID_DATE DATE 0 0),not null}]  (lineitem.l_shipdate,[17 3],0)
		        │           └── [ {(LTID_DATE DATE 0 0),not null}]  +
		        │               ├── [ {(LTID_DATE DATE 0 0),null}]  (1993-01-01)
		        │               └── [ {(LTID_INTERVAL INTERVAL 0 0),null}]  (1 year)
		        └── Join (inner):
		            ├── [outputs]
		            │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		            │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		            │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		            ├── [estCard]  800000
		            ├── [On]
		            │   └── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  in
		            │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		            │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[-2 0],0)
		            ├── Scan:
		            │   ├── [outputs]
		            │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[0 0],0)
		            │   │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[0 1],0)
		            │   │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[0 2],0)
		            │   ├── [estCard]  800000
		            │   ├── [index]  9
		            │   ├── [table]  tpch.partsupp
		            │   ├── [columns]
		            │   │   col 0 ps_partkey
		            │   │   col 1 ps_suppkey
		            │   │   col 2 ps_availqty
		            │   │
		            │   └── filters
		            └── Project:
		                ├── [outputs]
		                │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[0 0],0)
		                ├── [estCard]  200000
		                ├── [index]  10
		                ├── [exprs]
		                │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[-1 0],0)
		                └── Scan:
		                    ├── [outputs]
		                    │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[0 0],0)
		                    ├── [estCard]  0
		                    ├── [index]  13
		                    ├── [table]  tpch.part
		                    ├── [columns]
		                    │   col 0 p_partkey
		                    │   col 1 p_name
		                    │
		                    └── filters
		                        └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  like
		                            ├── [ {(LTID_VARCHAR VARCHAR 55 0),not null}]  (part.p_name,[13 1],0)
		                            └── [ {(LTID_VARCHAR VARCHAR 0 0),null}]  (lime%)

	*/
	/*
		1. after sort and dedup the result.
		it is equal to(also sort and dedup): result rows count 4454
			select
			-- 		partsupp.ps_partkey,
					distinct partsupp.ps_suppkey
			-- 		partsupp.ps_availqty,
			-- 		gby.qty,
			-- 		gby.qty * 2,
			-- 		cnt

				from
					partsupp
					join
					(
						select
							agg.ps_partkey,
							agg.ps_suppkey,
							0.5 * sum(l_quantity) as qty,
							count(*) as cnt
						from

								(
									select
										rr.ps_partkey as ps_partkey,
										rr.ps_suppkey as ps_suppkey,
										rr.ps_availqty as ps_availqty,
										lr.l_quantity as l_quantity
									from
										(
											select
												lineitem.l_partkey,
												lineitem.l_suppkey,
												lineitem.l_quantity
											from lineitem
											where
												l_shipdate >= date '1993-01-01' and
												l_shipdate < date '1993-01-01' + interval '1' year

										) lr
									join
										(
											select
												partsupp.ps_partkey,
												partsupp.ps_suppkey,
												partsupp.ps_availqty
											from
												partsupp join part on partsupp.ps_partkey = part.p_partkey
											where part.p_name like 'lime%'
										) rr
										on lr.l_partkey = rr.ps_partkey and
											lr.l_suppkey = rr.ps_suppkey

								) as agg
						group by agg.ps_partkey,agg.ps_suppkey
					) as gby
					on
						partsupp.ps_partkey = gby.ps_partkey
						and
						partsupp.ps_suppkey = gby.ps_suppkey
				where partsupp.ps_availqty > gby.qty
				order by
			-- 		partsupp.ps_partkey,
					partsupp.ps_suppkey
			-- 		partsupp.ps_availqty,
			-- 		gby.qty

		2. research sql: result rows count 5838

			select
				partsupp.ps_partkey,
				partsupp.ps_suppkey,
				partsupp.ps_availqty,
				gby.qty,
				gby.qty * 2,
				cnt
			from
				partsupp
				join
				(
					select
						agg.ps_partkey,
						agg.ps_suppkey,
						0.5 * sum(l_quantity) as qty,
						count(*) as cnt
					from

							(
								select
									rr.ps_partkey as ps_partkey,
									rr.ps_suppkey as ps_suppkey,
									rr.ps_availqty as ps_availqty,
									lr.l_quantity as l_quantity
								from
									(
										select
											lineitem.l_partkey,
											lineitem.l_suppkey,
											lineitem.l_quantity
										from lineitem
										where
											l_shipdate >= date '1993-01-01' and
											l_shipdate < date '1993-01-01' + interval '1' year

									) lr
								join
									(
										select
											partsupp.ps_partkey,
											partsupp.ps_suppkey,
											partsupp.ps_availqty
										from
											partsupp join part on partsupp.ps_partkey = part.p_partkey
										where part.p_name like 'lime%'
									) rr
									on lr.l_partkey = rr.ps_partkey and
										lr.l_suppkey = rr.ps_suppkey

							) as agg
					group by agg.ps_partkey,agg.ps_suppkey
				) as gby
				on
					partsupp.ps_partkey = gby.ps_partkey
					and
					partsupp.ps_suppkey = gby.ps_suppkey
			where partsupp.ps_availqty > gby.qty
			order by
				partsupp.ps_partkey,
				partsupp.ps_suppkey ,
				partsupp.ps_availqty,
				gby.qty

		3. research sql : result rows count9747

			select
				sum(sumCnt.cnt)
			from

			(
				select
					partsupp.ps_partkey,
					partsupp.ps_suppkey,
					partsupp.ps_availqty,
					gby.qty,
					gby.qty * 2,
					cnt
				from
					partsupp
					join
					(
						select
							agg.ps_partkey,
							agg.ps_suppkey,
							0.5 * sum(l_quantity) as qty,
							count(*) as cnt
						from

								(
									select
										rr.ps_partkey as ps_partkey,
										rr.ps_suppkey as ps_suppkey,
										rr.ps_availqty as ps_availqty,
										lr.l_quantity as l_quantity
									from
										(
											select
												lineitem.l_partkey,
												lineitem.l_suppkey,
												lineitem.l_quantity
											from lineitem
											where
												l_shipdate >= date '1993-01-01' and
												l_shipdate < date '1993-01-01' + interval '1' year

										) lr
									join
										(
											select
												partsupp.ps_partkey,
												partsupp.ps_suppkey,
												partsupp.ps_availqty
											from
												partsupp join part on partsupp.ps_partkey = part.p_partkey
											where part.p_name like 'lime%'
										) rr
										on lr.l_partkey = rr.ps_partkey and
											lr.l_suppkey = rr.ps_suppkey

								) as agg
						group by agg.ps_partkey,agg.ps_suppkey
					) as gby
					on
						partsupp.ps_partkey = gby.ps_partkey
						and
						partsupp.ps_suppkey = gby.ps_suppkey
				where partsupp.ps_availqty > gby.qty
				order by
					partsupp.ps_partkey,
					partsupp.ps_suppkey ,
					partsupp.ps_availqty,
					gby.qty

			) as sumCnt

		result:
	*/
	pplan := runTest2(t, tpchQ20())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Agg)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_ProjectAndAggr(t *testing.T) {
	/*
		PhysicalPlan:
		└── Project:
		    ├── [outputs]
		    │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[0 0],0)
		    ├── [estCard]  24004860
		    ├── [index]  6
		    ├── [exprs]
		    │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 0],0)
		    └── Aggregate:
		        ├── [outputs]
		        │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		        ├── [estCard]  24004860
		        ├── groupExprs, index 15
		        │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		        │   └── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		        ├── aggExprs, index 16
		        │   └── [0  {(LTID_INTEGER INT32 0 0),null}]  sum
		        │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[-1 3],0)
		        ├── filters
		        │   └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >
		        │       ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		        │       │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		        │       │   └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		        │       └── [ {(LTID_FLOAT FLOAT 0 0),not null}]  *
		        │           ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0.5)
		        │           └── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		        │               ├── [ {(LTID_INTEGER INT32 0 0),null}]  (AggNode_16.sum(l_quantity),[16 0],0)
		        │               └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		        └── Join (inner):
		            ├── [outputs]
		            │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		            │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		            │   ├── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-2 2],0)
		            │   └── [3  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[-1 2],0)
		            ├── [estCard]  24004860
		            ├── [On]
		            │   ├── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		            │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[-1 0],0)
		            │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		            │   ├── [1  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		            │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[-1 1],0)
		            │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		            │   ├── [2  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		            │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[-1 0],0)
		            │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		            │   └── [3  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		            │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[-1 1],0)
		            │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		            ├── Scan:
		            │   ├── [outputs]
		            │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[0 0],0)
		            │   │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[0 1],0)
		            │   │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[0 2],0)
		            │   ├── [estCard]  6001215
		            │   ├── [index]  17
		            │   ├── [table]  tpch.lineitem
		            │   ├── [columns]
		            │   │   col 0 l_partkey
		            │   │   col 1 l_suppkey
		            │   │   col 2 l_quantity
		            │   │   col 3 l_shipdate
		            │   │
		            │   └── filters
		            │       ├── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >=
		            │       │   ├── [ {(LTID_DATE DATE 0 0),not null}]  (lineitem.l_shipdate,[17 3],0)
		            │       │   └── [ {(LTID_DATE DATE 0 0),null}]  (1993-01-01)
		            │       └── [1  {(LTID_BOOLEAN BOOL 0 0),null}]  <
		            │           ├── [ {(LTID_DATE DATE 0 0),not null}]  (lineitem.l_shipdate,[17 3],0)
		            │           └── [ {(LTID_DATE DATE 0 0),not null}]  +
		            │               ├── [ {(LTID_DATE DATE 0 0),null}]  (1993-01-01)
		            │               └── [ {(LTID_INTERVAL INTERVAL 0 0),null}]  (1 year)
		            └── Join (inner):
		                ├── [outputs]
		                │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		                │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		                │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		                ├── [estCard]  800000
		                ├── [On]
		                │   └── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  in
		                │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		                │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[-2 0],0)
		                ├── Scan:
		                │   ├── [outputs]
		                │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[0 0],0)
		                │   │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[0 1],0)
		                │   │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[0 2],0)
		                │   ├── [estCard]  800000
		                │   ├── [index]  9
		                │   ├── [table]  tpch.partsupp
		                │   ├── [columns]
		                │   │   col 0 ps_partkey
		                │   │   col 1 ps_suppkey
		                │   │   col 2 ps_availqty
		                │   │
		                │   └── filters
		                └── Project:
		                    ├── [outputs]
		                    │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[0 0],0)
		                    ├── [estCard]  200000
		                    ├── [index]  10
		                    ├── [exprs]
		                    │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[-1 0],0)
		                    └── Scan:
		                        ├── [outputs]
		                        │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[0 0],0)
		                        ├── [estCard]  0
		                        ├── [index]  13
		                        ├── [table]  tpch.part
		                        ├── [columns]
		                        │   col 0 p_partkey
		                        │   col 1 p_name
		                        │
		                        └── filters
		                            └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  like
		                                ├── [ {(LTID_VARCHAR VARCHAR 55 0),not null}]  (part.p_name,[13 1],0)
		                                └── [ {(LTID_VARCHAR VARCHAR 0 0),null}]  (lime%)



	*/

	pplan := runTest2(t, tpchQ20())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) && wantedOp(root.Children[0], POT_Agg)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_innerJoin4(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)
	/*
		PhysicalPlan:
		└── Join (inner):
		    ├── [outputs]
		    │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[-2 0],0)
		    │   ├── [1  {(LTID_VARCHAR VARCHAR 25 0),not null}]  (supplier.s_name,[-2 1],0)
		    │   ├── [2  {(LTID_VARCHAR VARCHAR 40 0),not null}]  (supplier.s_address,[-2 2],0)
		    │   └── [3  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 0],0)
		    ├── [estCard]  24004860
		    ├── [On]
		    │   └── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  in
		    │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 0],0)
		    │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[-2 0],0)
		    ├── Project:
		    │   ├── [outputs]
		    │   │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[0 0],0)
		    │   ├── [estCard]  24004860
		    │   ├── [index]  6
		    │   ├── [exprs]
		    │   │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 0],0)
		    │   └── Aggregate:
		    │       ├── [outputs]
		    │       │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		    │       ├── [estCard]  24004860
		    │       ├── groupExprs, index 15
		    │       │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		    │       │   └── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		    │       ├── aggExprs, index 16
		    │       │   └── [0  {(LTID_INTEGER INT32 0 0),null}]  sum
		    │       │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[-1 3],0)
		    │       ├── filters
		    │       │   └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >
		    │       │       ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		    │       │       │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		    │       │       │   └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		    │       │       └── [ {(LTID_FLOAT FLOAT 0 0),not null}]  *
		    │       │           ├── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0.5)
		    │       │           └── [ {(LTID_FLOAT FLOAT 0 0),null}]  cast
		    │       │               ├── [ {(LTID_INTEGER INT32 0 0),null}]  (AggNode_16.sum(l_quantity),[16 0],0)
		    │       │               └── [ {(LTID_FLOAT FLOAT 0 0),null}]  (0)
		    │       └── Join (inner):
		    │           ├── [outputs]
		    │           │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		    │           │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		    │           │   ├── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-2 2],0)
		    │           │   └── [3  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[-1 2],0)
		    │           ├── [estCard]  24004860
		    │           ├── [On]
		    │           │   ├── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		    │           │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[-1 0],0)
		    │           │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		    │           │   ├── [1  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		    │           │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[-1 1],0)
		    │           │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		    │           │   ├── [2  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		    │           │   │   ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[-1 0],0)
		    │           │   │   └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-2 0],0)
		    │           │   └── [3  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		    │           │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[-1 1],0)
		    │           │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-2 1],0)
		    │           ├── Scan:
		    │           │   ├── [outputs]
		    │           │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_partkey,[0 0],0)
		    │           │   │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_suppkey,[0 1],0)
		    │           │   │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (lineitem.l_quantity,[0 2],0)
		    │           │   ├── [estCard]  6001215
		    │           │   ├── [index]  17
		    │           │   ├── [table]  tpch.lineitem
		    │           │   ├── [columns]
		    │           │   │   col 0 l_partkey
		    │           │   │   col 1 l_suppkey
		    │           │   │   col 2 l_quantity
		    │           │   │   col 3 l_shipdate
		    │           │   │
		    │           │   └── filters
		    │           │       ├── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  >=
		    │           │       │   ├── [ {(LTID_DATE DATE 0 0),not null}]  (lineitem.l_shipdate,[17 3],0)
		    │           │       │   └── [ {(LTID_DATE DATE 0 0),null}]  (1993-01-01)
		    │           │       └── [1  {(LTID_BOOLEAN BOOL 0 0),null}]  <
		    │           │           ├── [ {(LTID_DATE DATE 0 0),not null}]  (lineitem.l_shipdate,[17 3],0)
		    │           │           └── [ {(LTID_DATE DATE 0 0),not null}]  +
		    │           │               ├── [ {(LTID_DATE DATE 0 0),null}]  (1993-01-01)
		    │           │               └── [ {(LTID_INTERVAL INTERVAL 0 0),null}]  (1 year)
		    │           └── Join (inner):
		    │               ├── [outputs]
		    │               │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		    │               │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[-1 1],0)
		    │               │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[-1 2],0)
		    │               ├── [estCard]  800000
		    │               ├── [On]
		    │               │   └── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  in
		    │               │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[-1 0],0)
		    │               │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[-2 0],0)
		    │               ├── Scan:
		    │               │   ├── [outputs]
		    │               │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_partkey,[0 0],0)
		    │               │   │   ├── [1  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_suppkey,[0 1],0)
		    │               │   │   └── [2  {(LTID_INTEGER INT32 0 0),not null}]  (partsupp.ps_availqty,[0 2],0)
		    │               │   ├── [estCard]  800000
		    │               │   ├── [index]  9
		    │               │   ├── [table]  tpch.partsupp
		    │               │   ├── [columns]
		    │               │   │   col 0 ps_partkey
		    │               │   │   col 1 ps_suppkey
		    │               │   │   col 2 ps_availqty
		    │               │   │
		    │               │   └── filters
		    │               └── Project:
		    │                   ├── [outputs]
		    │                   │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[0 0],0)
		    │                   ├── [estCard]  200000
		    │                   ├── [index]  10
		    │                   ├── [exprs]
		    │                   │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[-1 0],0)
		    │                   └── Scan:
		    │                       ├── [outputs]
		    │                       │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (part.p_partkey,[0 0],0)
		    │                       ├── [estCard]  0
		    │                       ├── [index]  13
		    │                       ├── [table]  tpch.part
		    │                       ├── [columns]
		    │                       │   col 0 p_partkey
		    │                       │   col 1 p_name
		    │                       │
		    │                       └── filters
		    │                           └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  like
		    │                               ├── [ {(LTID_VARCHAR VARCHAR 55 0),not null}]  (part.p_name,[13 1],0)
		    │                               └── [ {(LTID_VARCHAR VARCHAR 0 0),null}]  (lime%)
		    └── Join (inner):
		        ├── [outputs]
		        │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[-1 0],0)
		        │   ├── [1  {(LTID_VARCHAR VARCHAR 25 0),not null}]  (supplier.s_name,[-1 1],0)
		        │   └── [2  {(LTID_VARCHAR VARCHAR 40 0),not null}]  (supplier.s_address,[-1 2],0)
		        ├── [estCard]  10000
		        ├── [On]
		        │   └── [0  {(LTID_BOOLEAN BOOL 0 0),null}]  =
		        │       ├── [ {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_nationkey,[-1 3],0)
		        │       └── [ {(LTID_INTEGER INT32 0 0),not null}]  (nation.n_nationkey,[-2 0],0)
		        ├── Scan:
		        │   ├── [outputs]
		        │   │   ├── [0  {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_suppkey,[0 0],0)
		        │   │   ├── [1  {(LTID_VARCHAR VARCHAR 25 0),not null}]  (supplier.s_name,[0 1],0)
		        │   │   ├── [2  {(LTID_VARCHAR VARCHAR 40 0),not null}]  (supplier.s_address,[0 2],0)
		        │   │   └── [3  {(LTID_INTEGER INT32 0 0),not null}]  (supplier.s_nationkey,[0 3],0)
		        │   ├── [estCard]  10000
		        │   ├── [index]  4
		        │   ├── [table]  tpch.supplier
		        │   ├── [columns]
		        │   │   col 0 s_suppkey
		        │   │   col 1 s_name
		        │   │   col 2 s_address
		        │   │   col 3 s_nationkey
		        │   │
		        │   └── filters
		        └── Scan:
		            ├── [outputs]
		            │   └── [0  {(LTID_INTEGER INT32 0 0),not null}]  (nation.n_nationkey,[0 0],0)
		            ├── [estCard]  25
		            ├── [index]  5
		            ├── [table]  tpch.nation
		            ├── [columns]
		            │   col 0 n_nationkey
		            │   col 1 n_name
		            │
		            └── filters
		                └── [0  {(LTID_BOOLEAN BOOL 0 0),not null}]  =
		                    ├── [ {(LTID_VARCHAR VARCHAR 25 0),not null}]  (nation.n_name,[5 1],0)
		                    └── [ {(LTID_VARCHAR VARCHAR 0 0),null}]  (VIETNAM)
	*/
	/*
		equal to (after sort and dedup):
		select
			distinct
			rt2.s_suppkey,
			rt2.s_name,
			rt2.s_address,
			lf2.ps_suppkey
		from
			(
				select
					partsupp.ps_suppkey

				from
					partsupp
					join
					(
						select
							agg.ps_partkey,
							agg.ps_suppkey,
							0.5 * sum(l_quantity) as qty,
							count(*) as cnt
						from

								(
									select
										rr.ps_partkey as ps_partkey,
										rr.ps_suppkey as ps_suppkey,
										rr.ps_availqty as ps_availqty,
										lr.l_quantity as l_quantity
									from
										(
											select
												lineitem.l_partkey,
												lineitem.l_suppkey,
												lineitem.l_quantity
											from lineitem
											where
												l_shipdate >= date '1993-01-01' and
												l_shipdate < date '1993-01-01' + interval '1' year

										) lr
									join
										(
											select
												partsupp.ps_partkey,
												partsupp.ps_suppkey,
												partsupp.ps_availqty
											from
												partsupp join part on partsupp.ps_partkey = part.p_partkey
											where part.p_name like 'lime%'
										) rr
										on lr.l_partkey = rr.ps_partkey and
											lr.l_suppkey = rr.ps_suppkey

								) as agg
						group by agg.ps_partkey,agg.ps_suppkey
					) as gby
					on
						partsupp.ps_partkey = gby.ps_partkey
						and
						partsupp.ps_suppkey = gby.ps_suppkey
				where partsupp.ps_availqty > gby.qty
			) as lf2
			join
			(
				select
						lr.s_suppkey,
						lr.s_name,
						lr.s_address
					from
					(
						select
							s_suppkey,
							s_name,
							s_address,
							s_nationkey
						from
							supplier
					) lr
					join
					(
						select
						n_nationkey
						from
							nation
						where n_name = 'VIETNAM'
					) rr
					on lr.s_nationkey = rr.n_nationkey
			) as rt2
			on lf2.ps_suppkey = rt2.s_suppkey
			order by
				s_suppkey

	*/

	pplan := runTest2(t, tpchQ20())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Join) &&
				wantedOp(root.Children[0], POT_Project) &&
				wantedOp(root.Children[1], POT_Join)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_filter(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ20())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Filter) &&
				wantedOp(root.Children[0], POT_Join)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_projectAndFilter(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ20())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) &&
				wantedOp(root.Children[0], POT_Filter)
		},
	)
	runOps(t, ops)
}

func Test_1g_q20_order(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ20())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Order)
		},
	)
	runOps(t, ops)
}

func Test_date(t *testing.T) {
	i := 9568
	ti := time.Date(1970, 1, 1+i, 0, 0, 0, 0, time.UTC)
	fmt.Println(ti.Date())
}

func Test_1g_q19_scan1(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ19())
	fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Scan) && root.Table == "part"
		},
	)
	runOps(t, ops)
}

func Test_1g_q19_scan2(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ19())
	fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Scan) && root.Table == "lineitem"
		},
	)
	runOps(t, ops)
}

func Test_decimal(t *testing.T) {
	i := 2116823
	val := dec.MustNew(int64(i), 2)
	fmt.Println(val)
}

func Test_1g_q19_crossJoin(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ19())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Join)
		},
	)
	gConf.EnableMaxScanRows = true
	gConf.SkipOutput = true
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q19_filter(t *testing.T) {
	//disable go gc to avoid recycle the unsafe.pointer from make
	//debug.SetGCPercent(-1)
	//debug.SetMemoryLimit(math.MaxInt64)

	pplan := runTest2(t, tpchQ19())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Filter)
		},
	)
	gConf.EnableMaxScanRows = true
	defer func() {
		gConf.EnableMaxScanRows = false
	}()
	runOps(t, ops)
}
