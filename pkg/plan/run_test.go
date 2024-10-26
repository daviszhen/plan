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

	"github.com/BurntSushi/toml"
	dec "github.com/govalues/decimal"
	"github.com/stretchr/testify/assert"

	"github.com/daviszhen/plan/pkg/util"
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
	maxTestCnt = math.MaxInt
)

func runOps(
	t *testing.T,
	conf *util.Config,
	serial Serialize,
	ops []*PhysicalOperator) {
	_, err := toml.DecodeFile("./config.toml", gConf)
	if err != nil {
		fmt.Println(err)
	}
	for _, op := range ops {

		if conf.Debug.PrintPlan {
			fmt.Println(op.String())
		}

		run := &Runner{
			op:    op,
			state: &OperatorState{},
			cfg:   conf,
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

				if serial != nil {
					err = output.serialize(serial)
					assert.NoError(t, err)
				}

				rowCnt += output.card()
				if conf.Debug.PrintResult {
					output.print()
				}
			}
		}
		fmt.Println("row Count", rowCnt)
		run.Close()
	}
}

func Test_1g_q20_order(t *testing.T) {
	pplan := runTest2(t, tpchQ20())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)
		},
	)
	runOps(t, gConf, nil, ops)
}

func Test_1g_q19_aggr(t *testing.T) {
	pplan := runTest2(t, tpchQ19())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Agg)
		},
	)

	defer func() {
		gConf.Debug.EnableMaxScanRows = false
		gConf.Debug.PrintResult = false
	}()
	runOps(t, gConf, nil, ops)
}

func Test_1g_q18_proj_aggr_filter(t *testing.T) {
	pplan := runTest2(t, tpchQ18())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)
		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_prepare_q18(t *testing.T) {
	pplan := runTest2(t, tpchQ18())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantId(root, 8)
		},
	)
	fname := "./test/q18_out"
	serial, err := NewFileSerialize(fname)
	assert.NoError(t, err, fname)
	assert.NotNil(t, serial)
	defer serial.Close()
	runOps(t, gConf, serial, ops)
}

func Test_q18_with_stub(t *testing.T) {
	pplan := runTest2(t, tpchQ18())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantId(root, 9)
		},
	)

	assertFunc(len(ops) == 1)

	//replace child by stub
	stubOp := &PhysicalOperator{
		Typ:     POT_Stub,
		Outputs: ops[0].Children[0].Outputs,
		Table:   "./test/q18_out",
	}
	ops[0].Children[0] = stubOp
	runOps(t, gConf, nil, ops)
}

func Test_1g_q17_proj_aggr(t *testing.T) {
	pplan := runTest2(t, tpchQ17())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Project) &&
				wantOp(root.Children[0], POT_Agg) &&
				wantOp(root.Children[0].Children[0], POT_Filter)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q16(t *testing.T) {
	pplan := runTest2(t, tpchQ16())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {

			return wantOp(root, POT_Order)

			//	len(root.Filters) > 1
		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q15(t *testing.T) {
	pplan := runTest2(t, tpchQ15())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {

			return wantOp(root, POT_Order)

			//	len(root.Filters) > 1
		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q14(t *testing.T) {
	pplan := runTest2(t, tpchQ14())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Project) &&
				wantOp(root.Children[0], POT_Agg)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q12(t *testing.T) {
	pplan := runTest2(t, tpchQ12())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q11(t *testing.T) {
	pplan := runTest2(t, tpchQ11())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q10(t *testing.T) {
	pplan := runTest2(t, tpchQ10())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {

			return wantOp(root, POT_Limit)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q9(t *testing.T) {
	pplan := runTest2(t, tpchQ9())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q8(t *testing.T) {
	pplan := runTest2(t, tpchQ8())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q7(t *testing.T) {
	pplan := runTest2(t, tpchQ7())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q6(t *testing.T) {
	pplan := runTest2(t, tpchQ6())
	ops := []*PhysicalOperator{
		pplan,
	}
	runOps(t, gConf, nil, ops)
}

func Test_1g_q5(t *testing.T) {
	pplan := runTest2(t, tpchQ5())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q4(t *testing.T) {
	pplan := runTest2(t, tpchQ4())

	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)

		},
	)

	runOps(t, gConf, nil, ops)
}

func Test_1g_q3(t *testing.T) {
	pplan := runTest2(t, tpchQ3())
	ops := []*PhysicalOperator{
		pplan,
	}
	runOps(t, gConf, nil, ops)
}

func Test_1g_q2(t *testing.T) {
	pplan := runTest2(t, tpchQ2())
	ops := []*PhysicalOperator{
		pplan,
	}
	runOps(t, gConf, nil, ops)
}

func Test_1g_q1(t *testing.T) {
	pplan := runTest2(t, tpchQ1())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)
		},
	)
	runOps(t, gConf, nil, ops)
}

func Test_1g_q21(t *testing.T) {
	pplan := runTest2(t, tpchQ21())
	ops := []*PhysicalOperator{
		pplan,
	}
	runOps(t, gConf, nil, ops)
}

func Test_1g_q22(t *testing.T) {
	pplan := runTest2(t, tpchQ22())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)
		},
	)
	runOps(t, gConf, nil, ops)
}

func Test_1g_q13(t *testing.T) {
	pplan := runTest2(t, tpchQ13())
	ops := findOperator(
		pplan,
		func(root *PhysicalOperator) bool {
			return wantOp(root, POT_Order)
		},
	)
	runOps(t, gConf, nil, ops)
}

func TestName(t *testing.T) {
	dec := dec.MustParse("0.0001000000")
	fmt.Println(dec)
}

func Test_date(t *testing.T) {
	i := 9568
	ti := time.Date(1970, 1, 1+i, 0, 0, 0, 0, time.UTC)
	fmt.Println(ti.Date())

	t2 := time.Date(1993, 3, 1, 0, 0, 0, 0, time.UTC)
	t3 := t2.AddDate(0, 3, 0)
	fmt.Println(t3.Date())
}
