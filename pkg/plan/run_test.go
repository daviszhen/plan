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

func Test_1g_q20_order(t *testing.T) {
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

func Test_1g_q19_aggr(t *testing.T) {
	pplan := runTest2(t, tpchQ19())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Agg)
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	//gConf.MaxScanRows = 100000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q18_proj_aggr_filter(t *testing.T) {
	pplan := runTest2(t, tpchQ18())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) &&
				wantedOp(root.Children[0], POT_Agg) &&
				wantedOp(root.Children[0].Children[0], POT_Join)
			//return wantedOp(root, POT_Order)

			//return wantedOp(root, POT_Agg) &&
			//	wantedOp(root.Children[0], POT_Scan)
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q17_proj_aggr(t *testing.T) {
	pplan := runTest2(t, tpchQ17())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) &&
				wantedOp(root.Children[0], POT_Agg) &&
				wantedOp(root.Children[0].Children[0], POT_Filter)
			//return wantedOp(root, POT_Agg) &&
			//	wantedOp(root.Children[0], POT_Scan)
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q16(t *testing.T) {
	pplan := runTest2(t, tpchQ16())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			//return wantedOp(root, POT_Agg) &&
			//	wantedOp(root.Children[0], POT_Filter)
			return wantedOp(root, POT_Order)
			//return wantedOp(root, POT_Filter)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Project) &&
			//	wantedOp(root.Children[1], POT_Join)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Scan) &&
			//	wantedOp(root.Children[1], POT_Scan)
			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Scan)
			//return wantedOp(root, POT_Scan) &&
			//	len(root.Filters) > 1
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q15(t *testing.T) {
	pplan := runTest2(t, tpchQ15())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Agg) &&
			//	wantedOp(root.Children[0].Children[0], POT_Project)

			//return wantedOp(root, POT_Agg) &&
			//	wantedOp(root.Children[0], POT_Project) &&
			//	wantedOp(root.Children[0].Children[0], POT_Agg)

			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Project) &&
			//	wantedOp(root.Children[1], POT_Join)

			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Agg) &&
			//	wantedOp(root.Children[0].Children[0], POT_Scan)

			return wantedOp(root, POT_Order)
			//return wantedOp(root, POT_Filter)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Project) &&
			//	wantedOp(root.Children[1], POT_Scan)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Scan) &&
			//	wantedOp(root.Children[1], POT_Scan)
			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Scan)
			//return wantedOp(root, POT_Scan) &&
			//	len(root.Filters) > 1
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q14(t *testing.T) {
	pplan := runTest2(t, tpchQ14())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Project) &&
				wantedOp(root.Children[0], POT_Agg)

		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q12(t *testing.T) {
	pplan := runTest2(t, tpchQ12())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Order)

		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q11(t *testing.T) {
	pplan := runTest2(t, tpchQ11())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Order)
			//return wantedOp(root, POT_Agg) && len(root.GroupBys) != 0
			//return wantedOp(root, POT_Filter)
			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Agg)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Agg)
			//return wantedOp(root, POT_Join) && root.JoinTyp == LOT_JoinTypeCross
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q10(t *testing.T) {
	pplan := runTest2(t, tpchQ10())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			//return wantedOp(root, POT_Order)
			return wantedOp(root, POT_Limit)
			//return wantedOp(root, POT_Agg) && len(root.GroupBys) != 0
			//return wantedOp(root, POT_Filter)
			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Agg)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Agg)
			//return wantedOp(root, POT_Join) && root.JoinTyp == LOT_JoinTypeCross
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q9(t *testing.T) {
	pplan := runTest2(t, tpchQ9())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Order)
			//return wantedOp(root, POT_Limit)
			//return wantedOp(root, POT_Agg) && len(root.GroupBys) != 0
			//return wantedOp(root, POT_Filter)
			//return wantedOp(root, POT_Project) &&
			//	wantedOp(root.Children[0], POT_Join)
			//return wantedOp(root, POT_Join) &&
			//	wantedOp(root.Children[0], POT_Agg)
			//return wantedOp(root, POT_Join) && root.JoinTyp == LOT_JoinTypeCross
		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func Test_1g_q5(t *testing.T) {
	pplan := runTest2(t, tpchQ5())
	//fmt.Println(pplan.String())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantedOp(root, POT_Order)

		},
	)
	//gConf.EnableMaxScanRows = true
	//gConf.SkipOutput = true
	gConf.MaxScanRows = 1000000
	defer func() {
		gConf.EnableMaxScanRows = false
		gConf.SkipOutput = false
	}()
	runOps(t, ops)
}

func TestName(t *testing.T) {
	dec := dec.MustParse("0.0001000000")
	fmt.Println(dec)
}

func Test_right(t *testing.T) {

}
