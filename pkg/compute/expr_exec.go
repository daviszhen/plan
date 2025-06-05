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
	"fmt"
	"time"

	"github.com/daviszhen/plan/pkg/chunk"
	"github.com/daviszhen/plan/pkg/common"
	"github.com/daviszhen/plan/pkg/util"
)

type ExprState struct {
	_expr               *Expr
	_execState          *ExprExecState
	_children           []*ExprState
	_types              []common.LType
	_interChunk         *chunk.Chunk
	_trueSel, _falseSel *chunk.SelectVector //for CASE WHEN
}

func NewExprState(expr *Expr, eeState *ExprExecState) *ExprState {
	return &ExprState{
		_expr:       expr,
		_execState:  eeState,
		_interChunk: &chunk.Chunk{},
	}
}

func (es *ExprState) addChild(child *Expr) {
	es._types = append(es._types, child.DataTyp)
	es._children = append(es._children, initExprState(child, es._execState))
}

func (es *ExprState) finalize() {
	if len(es._types) == 0 {
		return
	}
	es._interChunk.Init(es._types, util.DefaultVectorSize)
}

type ExprExecState struct {
	_root *ExprState
	_exec *ExprExec
}

type ExprExec struct {
	_exprs      []*Expr
	_chunk      []*chunk.Chunk
	_execStates []*ExprExecState
}

func NewExprExec(es ...*Expr) *ExprExec {
	exec := &ExprExec{}
	for _, e := range es {
		if e == nil {
			continue
		}
		exec.addExpr(e)
	}
	return exec
}

func (exec *ExprExec) addExpr(expr *Expr) {
	exec._exprs = append(exec._exprs, expr)
	eeState := &ExprExecState{}
	eeState._exec = exec
	eeState._root = initExprState(expr, eeState)
	exec._execStates = append(exec._execStates, eeState)
}

func (exec *ExprExec) executeExprs(data []*chunk.Chunk, result *chunk.Chunk) error {
	for i := 0; i < len(exec._exprs); i++ {
		err := exec.executeExprI(data, i, result.Data[i])
		if err != nil {
			return err
		}
	}
	for _, d := range data {
		if d == nil {
			continue
		}
		result.SetCard(d.Card())
		break
	}

	return nil
}

func (exec *ExprExec) executeExprI(data []*chunk.Chunk, exprId int, result *chunk.Vector) error {
	exec._chunk = data
	cnt := 1
	if len(exec._chunk) != 0 {
		for _, chunk := range exec._chunk {
			if chunk == nil {
				continue
			}
			cnt = chunk.Card()
			break
		}

	}
	return exec.execute(
		exec._exprs[exprId],
		exec._execStates[exprId]._root,
		nil,
		cnt,
		result,
	)
}

func (exec *ExprExec) execute(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {
	if count == 0 {
		return nil
	}
	switch expr.Typ {
	case ET_Column:
		return exec.executeColumnRef(expr, eState, sel, count, result)
	case ET_Func:
		if expr.FunImpl._name == FuncCase {
			return exec.executeCase(expr, eState, sel, count, result)
		} else {
			return exec.executeFunc(expr, eState, sel, count, result)
		}
	case ET_Const:
		return exec.executeConst(expr, eState, sel, count, result)
	default:
		panic(fmt.Sprintf("%d", expr.Typ))
	}
}
func (exec *ExprExec) executeCase(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {
	var err error
	eState._interChunk.Reset()
	curTrueSel := eState._trueSel
	curFalseSel := eState._falseSel
	curSel := sel
	curCount := count
	//[0] ELSE expr
	for i := 1; i < len(expr.Children); i += 2 {
		when := expr.Children[i]
		then := expr.Children[i+1]
		whenState := eState._children[i]
		thenState := eState._children[i+1]
		interRes := eState._interChunk.Data[i+1]
		tCnt, err := exec.execSelectExpr(
			when,
			whenState,
			curSel,
			curCount,
			curTrueSel,
			curFalseSel,
		)
		if err != nil {
			return err
		}
		if tCnt == 0 { //all false
			continue
		}
		fCnt := curCount - tCnt
		if fCnt == 0 && curCount == count {
			//first WHEN are all true.
			//execute the THEN in first WHEN
			err = exec.execute(
				then,
				thenState,
				sel,
				count,
				result,
			)
			if err != nil {
				return err
			}
			return err
		} else {
			err = exec.execute(
				then,
				thenState,
				curTrueSel,
				tCnt,
				interRes,
			)
			if err != nil {
				return err
			}
			FillSwitch(
				interRes,
				result,
				curTrueSel,
				tCnt,
			)
		}
		curSel = curFalseSel
		curCount = fCnt
		if fCnt == 0 {
			break
		}
	}
	if curCount > 0 {
		elseState := eState._children[0]
		if curCount == count {
			//all WHEN are false
			err = exec.execute(
				expr.Children[0],
				elseState,
				sel,
				count,
				result,
			)
			if err != nil {
				return err
			}
			return err
		} else {
			interRes := eState._interChunk.Data[0]
			util.AssertFunc(curSel != nil)
			err = exec.execute(
				expr.Children[0],
				elseState,
				curSel,
				curCount,
				interRes,
			)
			if err != nil {
				return err
			}
			FillSwitch(interRes, result, curSel, curCount)
		}
	}
	if sel != nil {
		result.SliceOnSelf(sel, count)
	}
	return nil
}

func (exec *ExprExec) executeColumnRef(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {
	data := exec._chunk
	tabId := int64(expr.ColRef.table())
	if tabId >= 0 {
		//this node
		tabId = 2
	} else {
		tabId = -tabId
		tabId -= 1
	}
	colIdx := expr.ColRef.column()
	if sel != nil {
		result.Slice(data[tabId].Data[colIdx], sel, count)
	} else {
		result.Reference(data[tabId].Data[colIdx])
	}
	return nil
}
func (exec *ExprExec) executeConst(expr *Expr, state *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {
	switch expr.Typ {
	case ET_Const:
		switch expr.ConstValue.Type {
		case ConstTypeInteger,
			ConstTypeFloat,
			ConstTypeString,
			ConstTypeBoolean,
			ConstTypeNull:
			val := &chunk.Value{
				Typ:  expr.DataTyp,
				I64:  expr.ConstValue.Integer,
				F64:  expr.ConstValue.Float,
				Str:  expr.ConstValue.String,
				Bool: expr.ConstValue.Boolean,
			}
			result.ReferenceValue(val)
		case ConstTypeDecimal:
			val := &chunk.Value{
				Typ: expr.DataTyp,
				Str: expr.ConstValue.Decimal,
			}
			result.ReferenceValue(val)
		case ConstTypeDate:
			d, err := time.Parse(time.DateOnly, expr.ConstValue.Date)
			if err != nil {
				return err
			}
			//TODO: to date
			val := &chunk.Value{
				Typ:   expr.DataTyp,
				I64:   int64(d.Year()),
				I64_1: int64(d.Month()),
				I64_2: int64(d.Day()),
			}
			result.ReferenceValue(val)
		case ConstTypeInterval:
			val := &chunk.Value{
				Typ: expr.DataTyp,
				I64: expr.ConstValue.Interval.Value,
				Str: expr.ConstValue.Interval.Unit,
			}
			result.ReferenceValue(val)
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
	return nil
}

func (exec *ExprExec) executeFunc(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {
	var err error
	eState._interChunk.Reset()
	for i, child := range expr.Children {
		err = exec.execute(child,
			eState._children[i],
			sel,
			count,
			eState._interChunk.Data[i])
		if err != nil {
			return err
		}
	}
	eState._interChunk.SetCard(count)
	if expr.FunImpl._boundCastInfo != nil {
		params := &CastParams{}
		expr.FunImpl._boundCastInfo._fun(eState._interChunk.Data[0], result, count, params)
	} else {
		expr.FunImpl._scalar(eState._interChunk, eState, result)
	}

	return nil
}

func (exec *ExprExec) executeSelect(datas []*chunk.Chunk, sel *chunk.SelectVector) (int, error) {
	card := 0
	for _, data := range datas {
		if data == nil {
			continue
		}
		card = data.Card()
		break
	}
	if len(exec._exprs) == 0 {
		return card, nil
	}

	exec._chunk = datas
	return exec.execSelectExpr(
		exec._exprs[0],
		exec._execStates[0]._root,
		nil,
		card,
		sel,
		nil,
	)
}

func (exec *ExprExec) execSelectExpr(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector) (retCount int, err error) {
	if count == 0 {
		return 0, nil
	}
	switch expr.Typ {
	case ET_Func:
		switch GetOperatorType(expr.FunImpl._name) {
		case OpTypeCompare,
			OpTypeLike:
			return exec.execSelectCompare(expr, eState, sel, count, trueSel, falseSel)
		case OpTypeLogical:
			switch expr.FunImpl._name {
			case FuncAnd:
				return exec.execSelectAnd(expr, eState, sel, count, trueSel, falseSel)
			case FuncOr:
				return exec.execSelectOr(expr, eState, sel, count, trueSel, falseSel)
			default:
				panic("usp")
			}
		default:
			panic("usp")
		}

	default:
		panic("usp")
	}
}

func (exec *ExprExec) execSelectCompare(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector) (int, error) {
	var err error
	eState._interChunk.Reset()
	for i, child := range expr.Children {
		err = exec.execute(child,
			eState._children[i],
			sel,
			count,
			eState._interChunk.Data[i])
		if err != nil {
			return 0, err
		}
	}

	switch expr.Typ {
	case ET_Func:
		switch GetOperatorType(expr.FunImpl._name) {
		case OpTypeCompare, OpTypeLike:
			return selectOperation(
				eState._interChunk.Data[0],
				eState._interChunk.Data[1],
				sel,
				count,
				trueSel,
				falseSel,
				expr.FunImpl._name,
			), nil
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}

}

func (exec *ExprExec) execSelectAnd(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector) (int, error) {
	var err error
	curSel := sel
	curCount := count
	falseCount := 0
	trueCount := 0
	var tempFalse *chunk.SelectVector
	if falseSel != nil {
		tempFalse = chunk.NewSelectVector(util.DefaultVectorSize)
	}
	if trueSel == nil {
		trueSel = chunk.NewSelectVector(util.DefaultVectorSize)
	}

	for i, child := range expr.Children {
		trueCount, err = exec.execSelectExpr(child,
			eState._children[i],
			curSel,
			curCount,
			trueSel,
			tempFalse)
		if err != nil {
			return 0, err
		}
		fCount := curCount - trueCount
		if fCount > 0 && falseSel != nil {
			//move failed into false sel
			for j := 0; j < fCount; j++ {
				falseSel.SetIndex(falseCount, tempFalse.GetIndex(j))
				falseCount++
			}
		}
		curCount = trueCount
		if curCount == 0 {
			break
		}
		if curCount < count {
			curSel = trueSel
		}
	}

	return curCount, nil
}

func (exec *ExprExec) execSelectOr(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector) (int, error) {
	var err error
	curSel := sel
	curCount := count
	resCount := 0
	trueCount := 0

	var tempTrue *chunk.SelectVector
	var tempFalse *chunk.SelectVector
	if trueSel != nil {
		tempTrue = chunk.NewSelectVector(util.DefaultVectorSize)
	}

	if falseSel == nil {
		tempFalse = chunk.NewSelectVector(util.DefaultVectorSize)
		falseSel = tempFalse
	}

	for i, child := range expr.Children {
		trueCount, err = exec.execSelectExpr(
			child,
			eState._children[i],
			curSel,
			curCount,
			tempTrue,
			falseSel)
		if err != nil {
			return 0, err
		}
		if trueCount > 0 {
			if trueSel != nil {
				for j := 0; j < trueCount; j++ {
					trueSel.SetIndex(resCount, tempTrue.GetIndex(j))
					resCount++
				}
			}
			curCount -= trueCount
			curSel = falseSel
		}
	}

	return resCount, nil
}

func initExprState(expr *Expr, eeState *ExprExecState) (ret *ExprState) {
	switch expr.Typ {
	case ET_Column:
		ret = NewExprState(expr, eeState)
	case ET_Join:
	case ET_Func:
		ret = NewExprState(expr, eeState)
		for _, child := range expr.Children {
			ret.addChild(child)
		}
		if expr.FunImpl._name == FuncCase {
			ret._trueSel = chunk.NewSelectVector(util.DefaultVectorSize)
			ret._falseSel = chunk.NewSelectVector(util.DefaultVectorSize)
		}
	case ET_Const:
		ret = NewExprState(expr, eeState)
	case ET_Orderby:
		//TODO: asc or desc
		ret = NewExprState(expr, eeState)
		ret.addChild(expr.Children[0])
	default:
		panic("usp")
	}
	ret.finalize()
	return
}

func FillSwitch(
	vec *chunk.Vector,
	res *chunk.Vector,
	sel *chunk.SelectVector,
	count int,
) {
	switch res.Typ().GetInternalType() {
	case common.INT32:
		TemplatedFillLoop[int32](vec, res, sel, count)
	case common.DECIMAL:
		TemplatedFillLoop[common.Decimal](vec, res, sel, count)
	default:
		panic("usp")
	}
}

func TemplatedFillLoop[T any](
	vec *chunk.Vector,
	res *chunk.Vector,
	sel *chunk.SelectVector,
	count int,
) {
	res.SetPhyFormat(chunk.PF_FLAT)
	resSlice := chunk.GetSliceInPhyFormatFlat[T](res)
	resBitmap := chunk.GetMaskInPhyFormatFlat(res)
	if vec.PhyFormat().IsConst() {
		srcSlice := chunk.GetSliceInPhyFormatConst[T](vec)
		if chunk.IsNullInPhyFormatConst(vec) {
			for i := 0; i < count; i++ {
				resBitmap.SetInvalid(uint64(sel.GetIndex(i)))
			}
		} else {
			for i := 0; i < count; i++ {
				resSlice[sel.GetIndex(i)] = srcSlice[0]
			}
		}
	} else {
		var vdata chunk.UnifiedFormat
		vec.ToUnifiedFormat(count, &vdata)
		srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](&vdata)
		for i := 0; i < count; i++ {
			srcIdx := vdata.Sel.GetIndex(i)
			resIdx := sel.GetIndex(i)
			resSlice[resIdx] = srcSlice[srcIdx]
			resBitmap.Set(uint64(resIdx), vdata.Mask.RowIsValid(uint64(srcIdx)))
		}
	}
}

func (e *Expr) IsNull() bool {
	return e.Typ == ET_Const && e.ConstValue.Type == ConstTypeNull
}
