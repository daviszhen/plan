package main

import (
	"fmt"
	"testing"

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
	maxTestCnt = 20
	//maxTestCnt = math.MaxInt
)

func runOps(t *testing.T, ops []*PhysicalOperator) {
	for _, op := range ops {

		//if i != 2 {
		//	continue
		//}

		fmt.Println(op.String())

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
				output.print()
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

func Test_scanExec(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Scan)
		},
	)
	runOps(t, ops)
}

func Test_projectExec(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) && wantedOp(root.Children[0], POT_Scan)
		},
	)
	runOps(t, ops)
}

func Test_innerJoin(t *testing.T) {
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

func Test_innerJoin2(t *testing.T) {
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

func Test_innerJoin3(t *testing.T) {
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
