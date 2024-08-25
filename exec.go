package main

import (
	"fmt"
	"time"
)

type ExprState struct {
	_expr               *Expr
	_execState          *ExprExecState
	_children           []*ExprState
	_types              []LType
	_interChunk         *Chunk
	_trueSel, _falseSel *SelectVector //for CASE WHEN
}

func NewExprState(expr *Expr, eeState *ExprExecState) *ExprState {
	return &ExprState{
		_expr:       expr,
		_execState:  eeState,
		_interChunk: &Chunk{},
	}
}

func (es *ExprState) addChild(child *Expr) {
	es._types = append(es._types, child.DataTyp.LTyp)
	es._children = append(es._children, initExprState(child, es._execState))
}

func (es *ExprState) finalize() {
	if len(es._types) == 0 {
		return
	}
	es._interChunk.init(es._types, defaultVectorSize)
}

type ExprExecState struct {
	_root *ExprState
	_exec *ExprExec
}

const (
	//_chunk[0] : result of left child
	//_chunk[1] : result of right child
	//_chunk[2] : result of this node
	chunkOffset = 2
)

type ExprExec struct {
	_exprs      []*Expr
	_chunk      []*Chunk
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

func (exec *ExprExec) executeExprs(data []*Chunk, result *Chunk) error {
	for i := 0; i < len(exec._exprs); i++ {
		err := exec.executeExprI(data, i, result._data[i])
		if err != nil {
			return err
		}
	}
	for _, d := range data {
		if d == nil {
			continue
		}
		result.setCard(d.card())
		break
	}

	return nil
}

func (exec *ExprExec) executeExpr(data []*Chunk, result *Vector) error {
	return exec.executeExprI(data, 0, result)
}

func (exec *ExprExec) executeExprI(data []*Chunk, exprId int, result *Vector) error {
	exec._chunk = data
	cnt := 1
	if len(exec._chunk) != 0 {
		for _, chunk := range exec._chunk {
			if chunk == nil {
				continue
			}
			cnt = chunk.card()
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

func (exec *ExprExec) execute(expr *Expr, eState *ExprState, sel *SelectVector, count int, result *Vector) error {
	if count == 0 {
		return nil
	}
	switch expr.Typ {
	case ET_Column:
		return exec.executeColumnRef(expr, eState, sel, count, result)
	case ET_Func:
		if expr.SubTyp == ET_Case {
			return exec.executeCase(expr, eState, sel, count, result)
		} else {
			return exec.executeFunc(expr, eState, sel, count, result)
		}
	case ET_IConst, ET_SConst, ET_FConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_NConst, ET_DecConst:
		return exec.executeConst(expr, eState, sel, count, result)
	default:
		panic(fmt.Sprintf("%d", expr.Typ))
	}
	return nil
}
func (exec *ExprExec) executeCase(expr *Expr, eState *ExprState, sel *SelectVector, count int, result *Vector) error {
	var err error
	eState._interChunk.reset()
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
		interRes := eState._interChunk._data[i+1]
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
			interRes := eState._interChunk._data[0]
			assertFunc(curSel != nil)
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
		result.sliceOnSelf(sel, count)
	}
	return nil
}
func (exec *ExprExec) executeCompare(expr *Expr, eState *ExprState, sel *SelectVector, count int, result *Vector) error {
	var err error
	eState._interChunk.reset()
	for i, child := range expr.Children {
		err = exec.execute(child,
			eState._children[i],
			sel,
			count,
			eState._interChunk._data[i])
		if err != nil {
			return err
		}
	}

	switch expr.Typ {
	case ET_Func:
		switch expr.SubTyp {
		case ET_Equal, ET_In:
			compareOperations(eState._interChunk._data[0], eState._interChunk._data[1], result, count, expr.SubTyp)
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}

	return nil
}

func (exec *ExprExec) executeColumnRef(expr *Expr, eState *ExprState, sel *SelectVector, count int, result *Vector) error {
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
		result.slice(data[tabId]._data[colIdx], sel, count)
	} else {
		result.reference(data[tabId]._data[colIdx])
	}
	return nil
}
func (exec *ExprExec) executeConst(expr *Expr, state *ExprState, sel *SelectVector, count int, result *Vector) error {
	switch expr.Typ {
	case ET_IConst, ET_SConst, ET_FConst, ET_BConst, ET_NConst, ET_DecConst:
		val := &Value{
			_typ: expr.DataTyp.LTyp,
			_i64: expr.Ivalue,
			_f64: expr.Fvalue,
			_str: expr.Svalue,
		}
		result.referenceValue(val)
	case ET_DateConst:
		d, err := time.Parse(time.DateOnly, expr.Svalue)
		if err != nil {
			return err
		}
		//TODO: to date
		val := &Value{
			_typ:   expr.DataTyp.LTyp,
			_i64:   int64(d.Year()),
			_i64_1: int64(d.Month()),
			_i64_2: int64(d.Day()),
		}
		result.referenceValue(val)
	case ET_IntervalConst:
		val := &Value{
			_typ: expr.DataTyp.LTyp,
			_i64: expr.Ivalue,
			_f64: expr.Fvalue,
			_str: expr.Svalue,
		}
		result.referenceValue(val)
	default:
		panic("usp")
	}
	return nil
}

func (exec *ExprExec) executeFunc(expr *Expr, eState *ExprState, sel *SelectVector, count int, result *Vector) error {
	var err error
	argsTypes := make([]ExprDataType, 0)
	eState._interChunk.reset()
	for i, child := range expr.Children {
		argsTypes = append(argsTypes, child.DataTyp)
		err = exec.execute(child,
			eState._children[i],
			sel,
			count,
			eState._interChunk._data[i])
		if err != nil {
			return err
		}
	}
	eState._interChunk.setCard(count)
	impl, err := GetFunctionImpl(expr.FuncId, argsTypes)
	if err != nil {
		return err
	}
	if impl == nil {
		panic(fmt.Sprintf("no function impl: %v %v", expr.FuncId, argsTypes))
	}

	body := impl.Body()
	err = body(eState._interChunk, eState, count, result)
	return err
}

func (exec *ExprExec) executeSelect(datas []*Chunk, sel *SelectVector) (int, error) {
	card := 0
	for _, data := range datas {
		if data == nil {
			continue
		}
		card = data.card()
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

func (exec *ExprExec) execSelectExpr(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (retCount int, err error) {
	if count == 0 {
		return 0, nil
	}
	switch expr.Typ {
	case ET_Func:
		switch expr.SubTyp {
		case ET_Equal,
			ET_NotEqual,
			ET_NotIn,
			ET_Greater,
			ET_GreaterEqual,
			ET_Less,
			ET_LessEqual,
			ET_Like,
			ET_NotLike,
			ET_In:
			return exec.execSelectCompare(expr, eState, sel, count, trueSel, falseSel)
		case ET_And:
			return exec.execSelectAnd(expr, eState, sel, count, trueSel, falseSel)
		case ET_Or:
			return exec.execSelectOr(expr, eState, sel, count, trueSel, falseSel)
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
	return 0, nil
}

func (exec *ExprExec) execSelectCompare(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (int, error) {
	var err error
	eState._interChunk.reset()
	for i, child := range expr.Children {
		err = exec.execute(child,
			eState._children[i],
			sel,
			count,
			eState._interChunk._data[i])
		if err != nil {
			return 0, err
		}
	}

	switch expr.Typ {
	case ET_Func:
		switch expr.SubTyp {
		case ET_Equal,
			ET_NotEqual,
			ET_NotIn,
			ET_Greater,
			ET_GreaterEqual,
			ET_Less,
			ET_LessEqual,
			ET_Like,
			ET_NotLike,
			ET_In:
			return selectOperation(
				eState._interChunk._data[0],
				eState._interChunk._data[1],
				sel,
				count,
				trueSel,
				falseSel,
				expr.SubTyp,
			), nil
		default:
			panic("usp")
		}
	default:
		panic("usp")
	}

	return 0, nil
}

func (exec *ExprExec) execSelectAnd(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (int, error) {
	var err error
	curSel := sel
	curCount := count
	falseCount := 0
	trueCount := 0
	var tempFalse *SelectVector
	if falseSel != nil {
		tempFalse = NewSelectVector(defaultVectorSize)
	}
	if trueSel == nil {
		trueSel = NewSelectVector(defaultVectorSize)
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
				falseSel.setIndex(falseCount, tempFalse.getIndex(j))
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

func (exec *ExprExec) execSelectOr(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (int, error) {
	var err error
	curSel := sel
	curCount := count
	resCount := 0
	trueCount := 0

	var tempTrue *SelectVector
	var tempFalse *SelectVector
	if trueSel != nil {
		tempTrue = NewSelectVector(defaultVectorSize)
	}

	if falseSel == nil {
		tempFalse = NewSelectVector(defaultVectorSize)
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
					trueSel.setIndex(resCount, tempTrue.getIndex(j))
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
		if expr.SubTyp == ET_Case {
			ret._trueSel = NewSelectVector(defaultVectorSize)
			ret._falseSel = NewSelectVector(defaultVectorSize)
		}
	case ET_IConst, ET_SConst, ET_FConst, ET_DateConst, ET_IntervalConst, ET_BConst, ET_NConst, ET_DecConst:
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
	vec *Vector,
	res *Vector,
	sel *SelectVector,
	count int,
) {
	switch res.typ().getInternalType() {
	case INT32:
		TemplatedFillLoop[int32](vec, res, sel, count)
	case DECIMAL:
		TemplatedFillLoop[Decimal](vec, res, sel, count)
	default:
		panic("usp")
	}
}

func TemplatedFillLoop[T any](
	vec *Vector,
	res *Vector,
	sel *SelectVector,
	count int,
) {
	res.setPhyFormat(PF_FLAT)
	resSlice := getSliceInPhyFormatFlat[T](res)
	resBitmap := getMaskInPhyFormatFlat(res)
	if vec.phyFormat().isConst() {
		srcSlice := getSliceInPhyFormatConst[T](vec)
		if isNullInPhyFormatConst(vec) {
			for i := 0; i < count; i++ {
				resBitmap.setInvalid(uint64(sel.getIndex(i)))
			}
		} else {
			for i := 0; i < count; i++ {
				resSlice[sel.getIndex(i)] = srcSlice[0]
			}
		}
	} else {
		var vdata UnifiedFormat
		vec.toUnifiedFormat(count, &vdata)
		srcSlice := getSliceInPhyFormatUnifiedFormat[T](&vdata)
		for i := 0; i < count; i++ {
			srcIdx := vdata._sel.getIndex(i)
			resIdx := sel.getIndex(i)
			resSlice[resIdx] = srcSlice[srcIdx]
			resBitmap.set(uint64(resIdx), vdata._mask.rowIsValid(uint64(srcIdx)))
		}
	}
}
