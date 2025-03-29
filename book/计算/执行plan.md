# 简介
介绍物理查询计划执行原理。

整体结构：
- 向量化火山执行框架。
- 表达式执行器。
- 关系算子的实现。

# 执行框架
采用向量化火山执行模型。容易理解实现。

物理查询计划是一个物理算子树。

物理算子执行方式：
- 每次调用生成一批结果。
- 先执行子结点的算子，拿到一批结果。
- 利用子结点算子的结果，执行本结点算子。
- 生成本结点算子的一批结果
- 返回

算子执行接口及其实现Runner
```go
type OperatorExec interface {  
    Init() error  
    Execute(input, output *chunk.Chunk, 
	    state *OperatorState) (OperatorResult, error)  
    Close() error  
}

type Runner struct {
	op    *PhysicalOperator
	...
}

```

初始化执行框架：
- 为物理查询计划的根结点创建Runner对象。
- 递归初始化Runner.
	- 初始化子结点
	- 依据算子的类型，执行对应的init函数
```go
func (run *Runner) Init() error {  
	//step 1: 初始化子结点
    err := run.initChildren()  
    //step 2：初始化具体的算子
    switch run.op.Typ {  
    case POT_Scan:  
       return run.scanInit()  
    case POT_Project:  
       return run.projInit()  
    case POT_Join:  
       return run.joinInit()  
    case POT_Agg:  
       return run.aggrInit()  
    case POT_Filter:  
       return run.filterInit()  
    case POT_Order:  
       return run.orderInit()  
    case POT_Limit:  
       return run.limitInit() 
    }  
}

//初始化算子子结点
func (run *Runner) initChildren() error {  
    run.children = []*Runner{}  
    for _, child := range run.op.Children {  
	    //初始化算子子结点
       childRun := &Runner{  
          op:    child,  
          Txn:   run.Txn,  
          state: &OperatorState{},  
          cfg:   run.cfg,  
       }  
       err := childRun.Init()  
       run.children = append(run.children, childRun)  
    }  
    return nil  
}
```

类似，执行算子逻辑：
```go
func (run *Runner) Execute(input, output *chunk.Chunk, state *OperatorState) (OperatorResult, error) {  
	//step 1：初始化输出chunk
    output.Init(run.outputTypes, util.DefaultVectorSize)  
    //step 2：执行每种类型的算子
    switch run.op.Typ {  
    case POT_Scan:  
       return run.scanExec(output, state)  
    case POT_Project:  
       return run.projExec(output, state)  
    case POT_Join:  
       return run.joinExec(output, state)  
    case POT_Agg:  
       return run.aggrExec(output, state)  
    case POT_Filter:  
       return run.filterExec(output, state)  
    case POT_Order:  
       return run.orderExec(output, state)  
    case POT_Limit:  
       return run.limitExec(output, state)    
    }  
}

```


# 表达式执行器
表达式求值是算子执行的重要部分。几乎每个算子都需要对表达式求值。通常，表达式的值作为进一步运算的基础。初始化算子时，必须初始化需要的表达式执行器。

表达式执行器的组成部分：
- 表达式。一组要执行的表达式。
- 输入数据。
- 执行状态。中间值和结果。
	- 树形结构。与表达式的结构同构。
	- 组成部分：
		- 表达式
		- 子表达式的执行状态和数据类型
		- 子表达式的执行结果

```go

type ExprExec struct {  
    _exprs      []*Expr //表达式 
    _chunk      []*chunk.Chunk //输入数据 
    _execStates []*ExprExecState  //执行状态
}

type ExprExecState struct {  
    _root *ExprState  
    _exec *ExprExec  
}  

//执行状态
type ExprState struct {  
    _expr               *Expr  
    _execState          *ExprExecState  
    _children           []*ExprState  
    _types              []common.LType  
    _interChunk         *chunk.Chunk  
    _trueSel, _falseSel *chunk.SelectVector //for CASE WHEN  
}

```

## 初始化
表达式执行器初始化过程：
- 不停的向表达式执行器中增加表达式
- 为表达式初始化执行状态

表达式是一种递归结构。也需要递归初始化表达式的执行状态。
- 列引用，常数表达式。直接初始化。
- 函数
	- 初始化当前状态。
	- 再初始化子表达式的状态。

```go
func initExprState(expr *Expr, eeState *ExprExecState) (ret *ExprState) {
	switch typ{
	case column ref,constant expr:
		初始化执行状态；
	case 函数：
		初始化执行状态；
		初始化参数表达式的执行状态；
		
	}
	初始化子表达式的结果空间。
}

```

## 执行
两个入口：
- 常规入口。输入一组chunk，得出表达式的结果chunk。
- select入口。用于执行filter表达式。结果是那些行被留下，那些行被过滤掉

### 常规入口

依次执行每个表达式。输入数据都是相同的。

```go
func (exec *ExprExec) executeExprs(data []*chunk.Chunk, result *chunk.Chunk) error {
	for i := 0; i < len(exec._exprs); i++ {  
	    err := exec.executeExprI(data, i, result.Data[i])  
	}
}

func (exec *ExprExec) executeExprI(data []*chunk.Chunk, exprId int, result *chunk.Vector) error {  
    exec._chunk = data  
    return exec.execute(  
       exec._exprs[exprId],  
       exec._execStates[exprId]._root,  
       nil,  
       cnt,  
       result,  
    )  
}
```

实质执行表达式，并得出结果的是execute函数。按表达式类型分别执行
- 列引用。从输入数据中取对应的列vector。
- 函数。细分为case...when和普通函数。
- 常数。取常数值。

列引用和常数简单，不细说。
```go
func (exec *ExprExec) execute(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {  
    if count == 0 {  
       return nil  
    }  
    switch expr.Typ {  
    case 列引用:  
       return exec.executeColumnRef(expr, eState, sel, count, result)  
    case 函数:  
       if expr.SubTyp == ET_Case {  //case ... when
          return exec.executeCase(expr, eState, sel, count, result)  
       } else {  
          return exec.executeFunc(expr, eState, sel, count, result)  
       }  
    case 常数:  
       return exec.executeConst(expr, eState, sel, count, result)  
    }  
}

```

普通函数的执行（case...when是运算符。都用函数表达。除此之外的函数。）
- 执行参数表达式，获取参数值。
- 执行函数体，得到函数结果。函数体执行复杂后续专门讲。
```go
func (exec *ExprExec) executeFunc(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {  
	//执行参数表达式
    for i, child := range expr.Children {  
       err = exec.execute(child,  //参数表达式
          eState._children[i],  //参数表达式的执行状态
          sel,  
          count,  
          eState._interChunk.Data[i])   //参数表达式结果
    }  

	//执行函数体。
	expr.FunImpl._scalar(eState._interChunk, eState, result)  
    return nil  
}

```

case...when的执行。不容易理解。
首先要理解case...when的执行过程：
- 输入数据为vector,有多行数据。vector的每行输入数据，
	- 逐个执行when表达式，直到其值为true. 此时，执行对应的then表达式。其结果为此行输入数据的计算结果。
	- 如果没有when表达式值为true。那么执行else表达式。其结果为此行输入数据的计算结果。
	- vector的不同行输入数据，对同一个when表达式，其结果可能不同。那么，每执行完一个when表达式。只需要在下一个when表达式上输入那些到目前为止都为false的行。
case...when的执行，需要用到select入口（下一节会讲，这里不细说）。
```go

func (exec *ExprExec) executeCase(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, result *chunk.Vector) error {
	//step 1：
	逐个执行when表达式：
		tCnt, err := exec.execSelectExpr(  
		    when,  
		    whenState,  
		    curSel,  
		    curCount,  
		    curTrueSel,  
		    curFalseSel,  
		)
		if vector的所有行在此when表达式上都为true {
			对vector所有行执行then表达式，得出最终结果；
			返回；
		}else{
			对when表达式结果为true的行执行then表达式，得出这些行的最终结果；
			并且这些行不会再输入后续的when表达式；
		}
		到目前为止when表达式值都为false的行进入下一个when表达式；
		如果没有，执行结束；

	//step 2：情况
	//情况1：when表达式都执行完了。还有数据行在所有when表达式上都为false.
	//情况2：所有数据行都得出false.
	//或者上述一起发生
	//为false的数据行，输入else表达式，得出最终结果；
}

```

#### 函数体框架
参数值vector可能有多种物理表示。设计一套通用的函数体框架妥善表达函数实现，且兼顾性能和维护性。

这里仅考虑scalar函数的实现。聚合函数是不同的实现方案。
TODO：

### select入口
用于执行filter表达式。得出那些filter值为true行。select执行后，返回值为true的select vector和值为true的个数。

根据运算符，区分执行过程：
- 比较运算符。=，<>,in,not in,<,<=,>,>=等
- and，or。

```go
func (exec *ExprExec) executeSelect(datas []*chunk.Chunk, sel *chunk.SelectVector) (int, error) {
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
    switch expr.Typ {  
    case 函数:  
       switch expr.SubTyp {  
       case 比较运算符:  
          return exec.execSelectCompare(expr, eState, sel, count, trueSel, falseSel)  
       case and运算符:  
          return exec.execSelectAnd(expr, eState, sel, count, trueSel, falseSel)  
       case or运算符:  
          return exec.execSelectOr(expr, eState, sel, count, trueSel, falseSel)  
       } 
    }  
}

```

#### 比较运算符框架
两个参数值vector的物理表示可能不同。设计一套通用的运算符框架妥善表达其实现。能用一套代码实现多种比较运算符。

这里仅考虑二元比较运算符。

TODO：

#### and运算符

#### or运算符

# 算子

## scan

## project

## join

## agg

## filter

## order

## limit

# 小结