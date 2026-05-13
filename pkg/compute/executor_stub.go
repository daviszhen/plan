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
	"github.com/daviszhen/plan/pkg/storage"
	"github.com/daviszhen/plan/pkg/util"
)

type stubExecutor struct {
	op         *PhysicalOperator
	deserial   util.Deserialize
	maxRowCnt  int
	rowReadCnt int
	children   []OperatorExec
}

func newStubExecutor(op *PhysicalOperator, cfg *util.Config, txn *storage.Txn, children []OperatorExec) (*stubExecutor, error) {
	return &stubExecutor{
		op:       op,
		children: children,
	}, nil
}

func (e *stubExecutor) Init() error {
	si := e.op.Info.(*StubInfo)
	deserial, err := util.NewFileDeserialize(si.Table)
	if err != nil {
		return err
	}
	e.deserial = deserial
	e.maxRowCnt = e.op.ChunkCount
	return nil
}

func (e *stubExecutor) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
	ensureOutputChunk(e.op, output)
	if e.maxRowCnt != 0 && e.rowReadCnt >= e.maxRowCnt {
		return Done, nil
	}
	err := output.Deserialize(e.deserial)
	if err != nil {
		return InvalidOpResult, err
	}
	if output.Card() == 0 {
		return Done, nil
	}
	e.rowReadCnt += output.Card()
	return haveMoreOutput, nil
}

func (e *stubExecutor) Close() error {
	if e.deserial != nil {
		return e.deserial.Close()
	}
	return nil
}
