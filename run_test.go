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

func Test_scanExec(t *testing.T) {
	pplan := runTest2(t, tpchQ20())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			if root == nil {
				return false
			}
			if root.Typ == POT_Scan {
				return true
			}
			return false
		},
	)

	const (
		maxCnt = 20
	)

	for i, op := range ops {

		//i = 1: finished
		if i != 1 {
			continue
		}

		fmt.Println(op.String())

		run := &Runner{
			op:    op,
			state: &OperatorState{},
		}
		err := run.Init()
		assert.NoError(t, err)

		rowCnt := 0
		for {
			if rowCnt >= maxCnt {
				break
			}
			output := &Chunk{}
			result, err := run.Execute(nil, output, run.state)
			assert.NoError(t, err)

			if err != nil {
				break
			}
			if result == Done {
				break
			}
			rowCnt += output.card()
			output.print()
		}
		fmt.Println("row Count", rowCnt)
		run.Close()
	}
}
