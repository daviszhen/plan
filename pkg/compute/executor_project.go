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
	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type projectExecutor struct {
	op         *PhysicalOperator
	projTypes  []common.LType
	projExec   *ExprExec
	outputExec *ExprExec
	children   []OperatorExec
}

func newProjectExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*projectExecutor, error) {
	return &projectExecutor{
		op:       op,
		children: children,
	}, nil
}

func (e *projectExecutor) Init() error {
	projTypes := make([]common.LType, 0)
	for _, proj := range e.op.Projects {
		projTypes = append(projTypes, proj.DataTyp)
	}
	e.projTypes = projTypes
	e.projExec = NewExprExec(e.op.Projects...)
	e.outputExec = NewExprExec(e.op.Outputs...)
	return nil
}

func (e *projectExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	childChunk := &chunk.Chunk{}
	var res OperatorResult
	var err error
	if len(e.children) != 0 {
		res, err = e.children[0].Execute(nil, childChunk)
		if err != nil {
			return InvalidOpResult, err
		}
		if res == InvalidOpResult {
			return InvalidOpResult, nil
		}
	}

	projChunk := &chunk.Chunk{}
	projChunk.Init(e.projTypes, util.DefaultVectorSize)
	err = e.projExec.executeExprs([]*chunk.Chunk{childChunk, nil, nil}, projChunk)
	if err != nil {
		return InvalidOpResult, err
	}

	err = e.outputExec.executeExprs([]*chunk.Chunk{childChunk, nil, projChunk}, output)
	if err != nil {
		return InvalidOpResult, err
	}

	return res, nil
}

func (e *projectExecutor) Close() error {
	return nil
}
