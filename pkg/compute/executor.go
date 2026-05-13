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

package compute

import (
	"context"
	"errors"
	"fmt"
	"os"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/lib/pq/oid"
	pg_query "github.com/pganalyze/pg_query_go/v5"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/parser"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

func InitRunner(cfg *util.Config, txn *storage.Txn, query string) (*Runner, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	stmts, err := parser.Parse(query)
	if err != nil {
		return nil, err
	}

	if len(stmts) != 1 {
		return nil, fmt.Errorf("multiple statements in one request")
	}

	var root *PhysicalOperator
	root, err = genDDLPhyPlan(txn, stmts[0])
	if err != nil {
		return nil, err
	}
	if root == nil {
		return nil, fmt.Errorf("nil plan")
	}

	rootExec, err := buildOperatorExec(root, cfg, txn)
	if err != nil {
		return nil, err
	}

	run := &Runner{
		op:   root,
		root: rootExec,
		cfg:  cfg,
		Txn:  txn,
	}
	err = run.Init()
	if err != nil {
		return nil, err
	}

	return run, nil
}

func genPhyPlan(txn *storage.Txn, ast *pg_query.SelectStmt) (*PhysicalOperator, error) {
	builder := NewBuilder(txn)
	err := builder.buildSelect(ast, builder.rootCtx, 0)
	if err != nil {
		return nil, err
	}

	lp, err := builder.CreatePlan(builder.rootCtx, nil)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(lp)
	lp, err = builder.Optimize(builder.rootCtx, lp)
	if err != nil {
		return nil, err
	}
	if lp == nil {
		return nil, errors.New("nil plan")
	}
	checkExprIsValid(lp)
	if true {
		planStr, err := ExplainLogicalPlan(lp)
		if err != nil {
			return nil, err
		}
		fmt.Println("Logical Plan:\n" + planStr)
	}
	pp, err := builder.CreatePhyPlan(lp)
	if err != nil {
		return nil, err
	}
	if pp == nil {
		return nil, errors.New("nil physical plan")
	}
	return pp, nil
}

func execOps(
	conf *util.Config,
	txn *storage.Txn,
	serial util.Serialize,
	resFile *os.File,
	ops []*PhysicalOperator) error {
	for _, op := range ops {
		if conf.Debug.PrintPlan {
			planStr, err := ExplainPhysicalPlan(op)
			if err != nil {
				return err
			}
			fmt.Println(planStr)
		}

		opExec, err := buildOperatorExec(op, conf, txn)
		if err != nil {
			return err
		}

		run := &Runner{
			op:   op,
			root: opExec,
			cfg:  conf,
			Txn:  txn,
		}
		fmt.Println("[execOps] runner init")
		err = run.Init()
		if err != nil {
			return err
		}

		rowCnt := 0
		batchCnt := 0
		fmt.Println("[execOps] start execute loop")
		for {
			if rowCnt >= conf.Debug.MaxOutputRowCount && conf.Debug.MaxOutputRowCount != -1 {
				break
			}
			output := &chunk.Chunk{}
			output.SetCap(util.DefaultVectorSize)
			result, err := run.Execute(output)
			if err != nil {
				return err
			}
			batchCnt++
			if result == Done {
				fmt.Printf("[execOps] done after %d batches, %d rows\n", batchCnt, rowCnt)
				break
			}
			if output.Card() > 0 {
				util.AssertFunc(output.Card() != 0)

				if serial != nil {
					err = output.Serialize(serial)
					if err != nil {
						return err
					}
				}

				if resFile != nil {
					err = output.SaveToFile(resFile)
					if err != nil {
						return err
					}
				}

				rowCnt += output.Card()
				if conf.Debug.PrintResult {
					output.Print()
				}
			}
		}
		if conf.Debug.PrintPlan {
			planStr, err := ExplainPhysicalPlan(op)
			if err != nil {
				return err
			}
			fmt.Println(planStr)
		}
		run.Close()
	}
	return nil
}

func ensureOutputChunk(op *PhysicalOperator, output *chunk.Chunk) {
	if len(output.Data) != 0 {
		return
	}
	types := make([]common.LType, 0, len(op.Outputs))
	for _, out := range op.Outputs {
		types = append(types, out.DataTyp)
	}
	output.Init(types, util.DefaultVectorSize)
}

func wantOp(root *PhysicalOperator, pt POT) bool {
	if root == nil {
		return false
	}
	return root.Typ == pt
}

func wantId(root *PhysicalOperator, id int) bool {
	if root == nil {
		return false
	}
	return root.Id == id
}

// Runner is a lightweight facade that wraps the root OperatorExec.
// It preserves backward compatibility for callers like cmd/main/main.go.
type Runner struct {
	cfg  *util.Config
	Txn  *storage.Txn
	op   *PhysicalOperator
	root OperatorExec
}

func (run *Runner) Init() error {
	if run.root == nil {
		return fmt.Errorf("nil root executor")
	}
	return nil
}

func (run *Runner) Execute(output *chunk.Chunk) (OperatorResult, error) {
	if run.root == nil {
		return InvalidOpResult, fmt.Errorf("nil root executor")
	}
	outputTypes := make([]common.LType, 0, len(run.op.Outputs))
	for _, out := range run.op.Outputs {
		outputTypes = append(outputTypes, out.DataTyp)
	}
	output.Init(outputTypes, util.DefaultVectorSize)
	return run.root.Execute(nil, output)
}

func (run *Runner) Columns() wire.Columns {
	cols := make(wire.Columns, 0)
	for _, output := range run.op.Outputs {
		col := wire.Column{
			Oid:   oid.T_varchar, //FIXME:
			Width: int16(output.DataTyp.Width),
		}
		cols = append(cols, col)
	}
	return cols
}

func (run *Runner) Run(
	ctx context.Context,
	writer wire.DataWriter) error {
	if run.cfg.Debug.PrintPlan {
		planStr, err := ExplainPhysicalPlan(run.op)
		if err != nil {
			return err
		}
		fmt.Println(planStr)
	}

	for {
		output := &chunk.Chunk{}
		output.SetCap(util.DefaultVectorSize)
		result, err := run.root.Execute(nil, output)
		if err != nil {
			return err
		}
		if result == Done {
			break
		}
		if output.Card() > 0 {
			err = output.SaveToWriter(writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (run *Runner) Close() error {
	if run.root == nil {
		return nil
	}
	return run.root.Close()
}

func buildOperatorExec(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn) (OperatorExec, error) {
	children := make([]OperatorExec, len(op.Children))
	for i, childOp := range op.Children {
		childExec, err := buildOperatorExec(childOp, cfg, txn)
		if err != nil {
			return nil, err
		}
		children[i] = childExec
	}

	var exec OperatorExec
	var err error
	switch op.Typ {
	case POT_Scan:
		exec, err = newScanExecutor(op, cfg, txn, children)
	case POT_Project:
		exec, err = newProjectExecutor(op, cfg, txn, children)
	case POT_Join:
		exec, err = newJoinExecutor(op, cfg, txn, children)
	case POT_Agg:
		exec, err = newAggExecutor(op, cfg, txn, children)
	case POT_Filter:
		exec, err = newFilterExecutor(op, cfg, txn, children)
	case POT_Order:
		exec, err = newOrderExecutor(op, cfg, txn, children)
	case POT_Limit:
		exec, err = newLimitExecutor(op, cfg, txn, children)
	case POT_Stub:
		exec, err = newStubExecutor(op, cfg, txn, children)
	case POT_CreateSchema:
		exec, err = newCreateSchemaExecutor(op, cfg, txn, children)
	case POT_CreateTable:
		exec, err = newCreateTableExecutor(op, cfg, txn, children)
	case POT_Insert:
		exec, err = newInsertExecutor(op, cfg, txn, children)
	default:
		return nil, fmt.Errorf("unsupported physical operator type: %v", op.Typ)
	}
	if err != nil {
		return nil, err
	}
	if err := exec.Init(); err != nil {
		return nil, err
	}
	return exec, nil
}
