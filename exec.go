package main

type ExprState struct {
	_expr       *Expr
	_execState  *ExprExecState
	_children   []*ExprState
	_types      []LType
	_interChunk *Chunk
}

func NewExprState(expr *Expr, eeState *ExprExecState) *ExprState {
	return &ExprState{
		_expr:      expr,
		_execState: eeState,
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
	es._interChunk.init(es._types)
}

type ExprExecState struct {
	_root *ExprState
	_exec *ExprExec
}

type ExprExec struct {
	_exprs      []*Expr
	_chunk      *Chunk
	_execStates []*ExprExecState
}

func NewExprExec(es ...*Expr) *ExprExec {
	exec := &ExprExec{}
	for _, e := range es {
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

func (exec *ExprExec) execute(expr *Expr, eState *ExprState, sel *SelectVector, count int, result *Vector) error {
	return nil
}

func (exec *ExprExec) selectExpr(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (retCount int, err error) {
	if count == 0 {
		return 0, nil
	}
	switch expr.Typ {
	case ET_Func:
		switch expr.SubTyp {
		case ET_Equal:
			return exec.selectCompare(expr, eState, sel, count, trueSel, falseSel)
		case ET_And:
			return exec.selectAnd(expr, eState, sel, count, trueSel, falseSel)

		default:
			panic("usp")
		}
	default:
		panic("usp")
	}
	return 0, nil
}

func (exec *ExprExec) selectCompare(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (int, error) {
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
		case ET_Equal:
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

func (exec *ExprExec) selectAnd(expr *Expr, eState *ExprState, sel *SelectVector, count int, trueSel, falseSel *SelectVector) (int, error) {
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
		trueCount, err = exec.selectExpr(child,
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

func initExprState(expr *Expr, eeState *ExprExecState) (ret *ExprState) {
	switch expr.Typ {
	case ET_Column:
		ret = NewExprState(expr, eeState)
	case ET_Join:
	case ET_Func:
		switch expr.SubTyp {
		case ET_SubFunc:
			ret = NewExprState(expr, eeState)
			for _, child := range expr.Children {
				ret.addChild(child)
			}
		case ET_Equal:
			ret = NewExprState(expr, eeState)
			for _, child := range expr.Children {
				ret.addChild(child)
			}
		case ET_And:
			ret = NewExprState(expr, eeState)
			for _, child := range expr.Children {
				ret.addChild(child)
			}
		case ET_Invalid, ET_Add, ET_Sub, ET_Mul, ET_Div, ET_NotEqual, ET_Greater, ET_GreaterEqual, ET_Less, ET_LessEqual, ET_Or, ET_Not, ET_Like, ET_NotLike, ET_Between, ET_Case, ET_In, ET_NotIn, ET_Exists, ET_NotExists:
		default:
			panic("usp")
		}
	case ET_IConst, ET_SConst, ET_FConst, ET_DateConst, ET_IntervalConst, ET_BConst:
		ret = NewExprState(expr, eeState)
	case ET_Orderby:
	default:
		panic("usp")
	}
	ret.finalize()
	return
}
