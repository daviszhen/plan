package main

type ExprState struct {
	_expr       *Expr
	_execState  *ExprExecutorState
	_children   []*Expr
	_interChunk *Chunk
}

type ExprExecutorState struct {
	_root *ExprState
	_exec *ExprExecutor
}

type ExprExecutor struct {
	_exprs []*Expr
	_chunk *Chunk
}

func (exec *ExprExecutor) ExecuteExpr(expr *Expr, state *ExprState, sel *SelectVector, count int, result *Vector) error {
	return nil
}
