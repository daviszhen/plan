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
