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

##### scalar函数

scalar函数的实现结构。以二元函数为例子，介绍其方案。

二元函数的抽象为下面的接口。左右参数和结果。`BinaryWrapper` 辅助对象接口，暂时不用关注。
```go
type BinaryOp[T any, S any, R any] func(left *T, right *S, result *R)

type BinaryWrapper[T any, S any, R any] interface {  
    operation(left *T, right *S, result *R, mask *util.Bitmap, idx int,  
       fun BinaryFunc[T, S, R])  
  
    addsNulls() bool  
}
```
再以加法为例。两个元素的加法实现：
```go
func addInt8(left, right, result *int8) {  
    *result = *left + *right  
}  
  
func addInt16(left, right, result *int16) {  
    *result = *left + *right  
}  
  
func addInt32(left, right, result *int32) {  
    *result = *left + *right  
}
...
```

二元函数框架，分为几层：
- 左右参数的物理表达。constant,flat,dict等
	- 左右都是constant
	- 左为constant,右为flat
	- 左为flat,右为constant
	- 左为flat,右为flat
	- 其他情况

参数意义：
- left,right 二元函数参数
- result 结果
- count 参数值个数
- fun 二元函数单行元素的实现
- wrapper 辅助类


 重点介绍`binaryExecFlat`和`binaryExecGeneric` 
```go
func binaryExecSwitch[T any, S any, R any](  
    left, right, result *chunk.Vector,  
    count int,  
    fun BinaryFunc[T, S, R],  
    wrapper BinaryWrapper[T, S, R],  
) {  
    if 左右都是constant {  
       binaryExecConst[T, S, R](...)  
    } else if 左为flat,右为constant {  
       binaryExecFlat[T, S, R](...,false, true)  
    } else if 左为constant,右为flat {  
       binaryExecFlat[T, S, R](..., true, false)  
    } else if 左为flat,右为flat {  
       binaryExecFlat[T, S, R](..., false, false)  
    } else {  
       binaryExecGeneric[T, S, R](...)  
    }  
}
```

###### `binaryExecFlat`的结构

参数意义：
- lconst,rconst。 左参数为constant。右参数为constant

优先处理NULL和Mask.

分类情况：
- 左参数constant且值为NULL。结果是NULL
- 右参数constant且值为NULL。结果是NULL
- 左参数constant。结果Mask是右参数的Mask
- 右参数constant。结果Mask是左参数的Mask
- 其它情况，结果Mask是左右参数的Mask的合并

```go
func binaryExecFlat[T any, S any, R any](  
    left, right, result *chunk.Vector,  
    count int,  
    fun BinaryFunc[T, S, R],  
    wrapper BinaryWrapper[T, S, R],  
    lconst, rconst bool,  
){
	//左参数constant且值为NULL 或 右参数constant且值为NULL，结果是NULL
	//左参数constant。结果Mask是右参数的Mask
	//右参数constant。结果Mask是左参数的Mask
	//其它情况，结果Mask是左右参数的Mask的合并
	binaryExecFlatLoop(...)
}


```

主体在函数`binaryExecFlatLoop`中完成。
参数意义：
- ldata,rdata,resData 左右参数、结果参数的数组。
- mask 参数NULL值

实现思路：
- 结果部分为NULL时，
	- 每8行一组。每组执行。
		- 全不为NULL时，每行数值，依次执行函数体
		- 全为NULL时，结果为NULL
		- 部分为NULL时，不为NULL的行，执行函数体
- 结果全不为NULL时，每行数值，依次执行函数体
```go
func binaryExecFlatLoop[T any, S any, R any](  
    ldata []T, rdata []S,  
    resData []R,  
    count int,  
    mask *util.Bitmap,  
    fun BinaryFunc[T, S, R],  
    wrapper BinaryWrapper[T, S, R],  
    lconst, rconst bool,  
) {
	if 结果部分为NULL {
		全不为NULL时，每行数值，依次执行函数体
		全为NULL时，结果为NULL
		部分为NULL时，不为NULL的行，执行函数体
	}else{
		结果全不为NULL时，每行数值，依次执行函数体
	}

}
```

###### `binaryExecGeneric`的结构

实现思路：左右参数转为`UnifiedFormat`。
函数主体在`binaryExecGenericLoop`。
函数参数比较多，参数意义：
- lsel,rsel。左右参数的select vector。
- lmask,rmask，resMask。左右参数mask和结果mask。

逻辑结构：
- 左参数不全为NULL。取有效行，计算结果。
- 右参数不全为NULL。取有效行，计算结果。
- 左右参数全不为NULL。取所有行，计算结果。

```go
func binaryExecGenericLoop[T any, S any, R any](  
    ldata []T, rdata []S,  
    resData []R,  
    lsel *chunk.SelectVector,  
    rsel *chunk.SelectVector,  
    count int,  
    lmask *util.Bitmap,  
    rmask *util.Bitmap,  
    resMask *util.Bitmap,  
    fun BinaryFunc[T, S, R],  
    wrapper BinaryWrapper[T, S, R],  
) {  
    if 左参数不全为NULL 或 右参数不全为NULL {  
       取有效行，计算结果。
       ...
	   wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, fun)   ...
    } else {  
       //左右参数全不为NULL。
       取所有行，计算结果；
       ...
       wrapper.operation(&ldata[lidx], &rdata[ridx], &resData[i], resMask, i, fun)；   
       ...
    }  
}
```

一元函数与二元函数有类似的方案. 逻辑上要简化很多,这里不再细说.

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
这里仅考虑二元比较运算符。

`execSelectCompare`为比较运算符的执行入口。两个参数值vector的物理表示可能不同。为它们设计一套执行框架。

执行框架的结构：
- 入口分类：函数`selectOperation`。分类的目的是要确定运算符在特定数据类型下的具体实现。
	- 第一层：运算符类型
	- 第二层：数据类型。在bind阶段已经保证左右参数的数据类型相同。
- 左右参数不同物理表示的分类：函数`selectBinary`。分类的目的是根据两种物理表示的组合选择合适的比较运算符的实现。
	- 左右都是constant。函数`selectConst`。
	- 左是constant，右是flat。函数`selectFlat`。
	- 左是flat，右是constant。函数`selectFlat`。
	- 左是flat，右是flat。函数`selectFlat`。
	- 其它组合。函数`selectGeneric`。
- 比较逻辑的实现方案：
	- `selectConst`
	- `selectFlat`
	- `selectGeneric`
```go
func selectOperation(left, right *chunk.Vector, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector, subTyp ET_SubTyp) int {
	switch subTyp {
    case ET_Equal://相等运算符
       switch left.Typ().GetInternalType() {
        case common.INT32:
            return selectBinary[int32](left, right, sel, count, trueSel, falseSel, equalOp[int32]{})
        case ...://其它数据类型
	    }
	case ...://其它运算符
    }
}

func selectBinary[T any](left, right *chunk.Vector, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector, cmpOp CompareOp[T]) int {
    if sel == nil {
        sel = chunk.IncrSelectVectorInPhyFormatFlat()
    }

    if 左右都是constant {
        return selectConst[T](left, right, sel, count, trueSel, falseSel, cmpOp)
    } else if 左是constant，右是flat {
        return selectFlat[T](left, right, sel, count, trueSel, falseSel, cmpOp, true, false)
    } else if 左是flat，右是constant {
        return selectFlat[T](left, right, sel, count, trueSel, falseSel, cmpOp, false, true)
    } else if 左是flat，右是flat {
        return selectFlat[T](left, right, sel, count, trueSel, falseSel, cmpOp, false, false)
    } else {//其它组合
        return selectGeneric[T](left, right, sel, count, trueSel, falseSel, cmpOp)
    }
}
```

##### selectFlat
 
 参数意义：
 - left，right：运算数
 - sel：输入参数的选择器
 - count：运算数的个数
 - trueSel, falseSel：分别记录结果为true，false的选择器
 - cmpOp：运算符的具体实现
 - leftConst, rightConst：左右运算数哪个为常数

 逻辑结构：
 - 分支：左参数 全是NULL。结果全是false。
 - 分支：右参数 全是NULL。类似。
 - 分支：左参数是constant。
 - 分支：右参数是constant。
 - 分支：左、右参数都不是constant。
后面三种情况都是由函数`selectFlatLoopSwitch`完成。

```go
func selectFlat[T any](
	left, right *chunk.Vector,
    sel *chunk.SelectVector,
    count int,
    trueSel, falseSel *chunk.SelectVector,
    cmpOp CompareOp[T],
    leftConst, rightConst bool) int {

	if 左参数 全是NULL {
		...
	}

	if 右参数 全是NULL {
		...
	}

	if 左参数是constant {
		return selectFlatLoopSwitch[T](
			...
            chunk.GetMaskInPhyFormatFlat(right),...)
	}else if 右参数是constant {
        return selectFlatLoopSwitch[T](
	        ...
            chunk.GetMaskInPhyFormatFlat(left),...)
	}else{
		//左、右参数都不是constant
        merge := chunk.GetMaskInPhyFormatFlat(left)
        rMask := chunk.GetMaskInPhyFormatFlat(right)
        merge.Combine(rMask, count)
        return selectFlatLoopSwitch[T](
			...
            merge,...)
	}
}
```

`selectFlatLoopSwitch`的逻辑结构：
- 既保留true的结果，又保留false的结果
- 保留true的结果
- 保留false的结果

具体实现基于函数`selectFlatLoop`。
```go
func selectFlatLoopSwitch[T any](
    ldata, rdata []T,
    sel *chunk.SelectVector,
    count int,
    mask *util.Bitmap,
    trueSel, falseSel *chunk.SelectVector,
    cmpOp CompareOp[T],
    leftConst, rightConst bool) int {
    if trueSel != nil && falseSel != nil {
        return selectFlatLoop[T](
			...
			true, true)
    } else if trueSel != nil {
        return selectFlatLoop[T](
			...
            true, false)
    } else {
        return selectFlatLoop[T](
			...
            false, true)
    }
}
```

`selectFlatLoop`与`selectFlat` 不同的参数的意义：
- ldata, rdata。参数数组。
- mask。左参数的mask与右参数的mask组合后的mask，表示最终的NULL情况。
- hasTrueSel, hasFalseSel。trueSel是否为nil。falseSel是否为nil。

`selectFlatLoop`的实现思路
- 8行一组。依次处理每个组。组内结果行的情况：全不是NULL,全是NULL,部分是NULL.
- 全不是NULL
	- 取每行的两个参数值
	- 执行运算符：`res := cmpOp.operation(&ldata[lidx], &rdata[ridx])`。
	- 结果为true,记录true的行。结果为false,记录false的行。
- 全是NULL
	- 结果全是false。记录所有行为false值。
- 部分是NULL
	- 只取有效行的两个参数值。
	- 执行运算符：`res := cmpOp.operation(&ldata[lidx], &rdata[ridx])`。
	- 结果为true,记录true的行。结果为false,记录false的行。
- 返回值：
	- 如果有trueSel,返回true的行数。
	- 如果没有trueSel,返回非false的行数。

```go
func selectFlatLoop[T any](  
    ldata, rdata []T,  
    sel *chunk.SelectVector,  
    count int,  
    mask *util.Bitmap,  
    trueSel, falseSel *chunk.SelectVector,  
    cmpOp CompareOp[T],  
    leftConst, rightConst bool,  
    hasTrueSel, hasFalseSel bool,  
) int {  
    trueCount, falseCount := 0, 0  
    baseIdx := 0  
    entryCount := util.EntryCount(count)  
    //按8行分组。
    for eidx := 0; eidx < entryCount; eidx++ {  
       entry := mask.GetEntry(uint64(eidx))  
       next := min(baseIdx+8, count)  
       if 全不是NULL {  
          //all valid: perform operation  
          for ; baseIdx < next; baseIdx++ {  
             取每行的两个参数值； 
             res := cmpOp.operation(&ldata[lidx], &rdata[ridx])  
             ...
             trueSel.SetIndex(trueCount, resIdx)  
             ...
             falseSel.SetIndex(falseCount, resIdx)  
             ...
          }  
       } else if 全是NULL {  
	      ...
          falseSel.SetIndex(falseCount, resIdx)  
          ...
          continue  
       } else {  
          //部分是NULL
          start := baseIdx  
          for ; baseIdx < next; baseIdx++ {  
             只取有效行的两个参数值;
             res := util.RowIsValidInEntry(entry, uint64(baseIdx-start)) &&  
                cmpOp.operation(&ldata[lidx], &rdata[ridx])  
             ...
             trueSel.SetIndex(trueCount, resIdx)  
             ...
             falseSel.SetIndex(falseCount, resIdx)  
             ...  
          }  
       }  
    }  
	如果有trueSel,返回true的行数。
	如果没有trueSel,返回非false的行数。
}
```

##### selectGeneric

比较运算逻辑的通用处理方式。

参数意义：
 - left，right：运算数
 - sel：输入参数的选择器
 - count：运算数的个数
 - trueSel, falseSel：分别记录结果为true，false的选择器
 - cmpOp：运算符的具体实现

`selectGeneric` 依据参数类型也分为好几层。

第一层，vector转UnifiedFormat：
- left,right 转成UnifiedFormat。UnifiedFormat的重要内容：数值数组、选择器和Mask。
- 调用下一层接口：`selectGenericLoopSwitch`

```go
func selectGeneric[T any](left, right *chunk.Vector, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector, cmpOp CompareOp[T]) int {  
    var ldata, rdata chunk.UnifiedFormat  
    left.ToUnifiedFormat(count, &ldata)  
    right.ToUnifiedFormat(count, &rdata)  
    lslice := chunk.GetSliceInPhyFormatUnifiedFormat[T](&ldata)  
    rslice := chunk.GetSliceInPhyFormatUnifiedFormat[T](&rdata)  
    return selectGenericLoopSwitch[T](...)  
}
```

第二层，按mask的NULL值情况进行分类：
- 左参数部分为NULL、或右参数部分为NULL
- 全不为NULL
```go
func selectGenericLoopSwitch[T any](  
    ldata, rdata []T,  
    lsel, rsel *chunk.SelectVector,  
    resSel *chunk.SelectVector,  
    count int,  
    lmask, rmask *util.Bitmap,  
    trueSel, falseSel *chunk.SelectVector,  
    cmpOp CompareOp[T]) int {  
    if !lmask.AllValid() || !rmask.AllValid() {  
       return selectGenericLoopSelSwitch[T](..., false)  
    } else {// 全不为NULL
       return selectGenericLoopSelSwitch[T](..., true)  
    }  
}
```

第三层，按trueSel是否为nil和falseSel是否为nil再分类
- trueSel不为nil和falseSel不为nil
- trueSel不为nil
- 其它情况
```go
func selectGenericLoopSelSwitch[T any](  
    ldata, rdata []T,  
    lsel, rsel *chunk.SelectVector,  
    resSel *chunk.SelectVector,  
    count int,  
    lmask, rmask *util.Bitmap,  
    trueSel, falseSel *chunk.SelectVector,  
    cmpOp CompareOp[T],  
    noNull bool,  
) int {  
    if trueSel != nil && falseSel != nil {  // trueSel不为nil和falseSel不为nil
       return selectGenericLoop[T](  
	      ...,
          true, true,  
       )  
    } else if trueSel != nil {  
       return selectGenericLoop[T](  
          ...,  
          true, false,  
       )  
    } else {  
       return selectGenericLoop[T](  
          ...,  
          false, true,  
       )  
    }  
}
```

第四层，实际比较逻辑
- 取参数值，检查NULL情况。
- 执行运算符
- 返回值：
	- 如果有trueSel, 返回true的行数。
	- 如果没有trueSel, 返回非false的行数。
```go
func selectGenericLoop[T any](  
    ldata, rdata []T,  
    lsel, rsel *chunk.SelectVector,  
    resSel *chunk.SelectVector,  
    count int,  
    lmask, rmask *util.Bitmap,  
    trueSel, falseSel *chunk.SelectVector,  
    cmpOp CompareOp[T],  
    noNull bool,  
    hasTrueSel, hasFalseSel bool,  
) int {  
    for i := 0; i < count; i++ {  
       if (noNull || 
	       lmask.RowIsValid(uint64(lidx)) && 
	       rmask.RowIsValid(uint64(ridx))) &&  
          cmpOp.operation(&ldata[lidx], &rdata[ridx]) {  
          ...
          trueSel.SetIndex(trueCount, resIdx)  
          ...
       } else {  
		  ...
          falseSel.SetIndex(falseCount, resIdx)  
          ...
       }  
    }  
	如果有trueSel,返回true的行数。
	如果没有trueSel,返回非false的行数。
}

```

##### 比较运算符

在上面的执行框架上，对运算符和数据类型，实现相应的逻辑。

比较运算符接口：
```go
type CompareOp[T any] interface {  
    operation(left, right *T) bool  
}
```

举相等运算符为例子。
```go
//可直接比较的数值数据类型
type equalOp[T comparable] struct {  
}  
  
func (e equalOp[T]) operation(left, right *T) bool {  
    return *left == *right  
}

//字符串类型
type equalStrOp struct {  
}  
  
func (e equalStrOp) operation(left, right *common.String) bool {  
    return left.Equal(right)  
}

func (s *String) Equal(o *String) bool {  
    if s.Len != o.Len {  
       return false  
    }  
    sSlice := util.PointerToSlice[byte](s.Data, s.Len)  
    oSlice := util.PointerToSlice[byte](o.Data, o.Len)  
    return bytes.Equal(sSlice, oSlice)  
}
```


#### 逻辑运算符

##### and运算符

输入是true/false,输出也是true/false。

逻辑与有短路特性。只要碰到false,计算结果一定是false,计算过程结束。
借助true select vector实现这种短路特性。

实现方案：
- 计算第一个子表达式。计算结果有两个：`trueCount`- true的个数；`trueSel`-结果为true的行。
- 从第二个子表达式开始，计算的输入是：前一个子表达式的计算结果`trueCount`和`trueSel`。因为短路特性，只需要前一个子表达式的计算结果为true的行。
- 如此，直到结果都是false或子表达式都计算完。

```go
func (exec *ExprExec) execSelectAnd(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector) (int, error) {  
    var err error  
    curSel := sel  
    curCount := count  
    falseCount := 0  
    trueCount := 0  
	...
    for i, child := range expr.Children {  
       //计算第i个子表达式
       trueCount, err = exec.execSelectExpr(child,  
          eState._children[i],  
          curSel,  
          curCount,  
          trueSel,  
          tempFalse)  
       //结果为false的行个数
       fCount := curCount - trueCount  
       if fCount > 0 && falseSel != nil {  
          //记录结果为false的行
          ...
          falseSel.SetIndex(falseCount, tempFalse.GetIndex(j))
          ...
       }  
       //结果为true的行个数
       curCount = trueCount  
       if curCount == 0 {  
          break  
       }  
       //还有结果为true的行，进入下一轮计算
       if curCount < count {  
          curSel = trueSel  //为true的行，才需要进入下一轮计算
       }  
    }  
	//返回结果为true的行个数
    return curCount, nil  
}

```

##### or运算符

逻辑或有短路特性。只要碰到true,计算结果一定是true,计算过程结束。
借助false select vector实现这种短路特性。

实现方案：
- 计算第一个子表达式。计算结果有两个：`trueCount`- true的个数；`falseSel`-结果为false的行。
- 从第二个子表达式开始，计算的输入是：前一个子表达式的计算结果`falseCount`和`falseSel`。因为短路特性，只需要前一个子表达式的计算结果为false的行。
- 如此，直到结果都是true或子表达式都计算完。

```go
func (exec *ExprExec) execSelectOr(expr *Expr, eState *ExprState, sel *chunk.SelectVector, count int, trueSel, falseSel *chunk.SelectVector) (int, error) {  
    var err error  
    curSel := sel  
    curCount := count  
    resCount := 0  
    trueCount := 0  
    ...
    for i, child := range expr.Children {  
	   //计算第i个子表达式
       trueCount, err = exec.execSelectExpr(  
          child,  
          eState._children[i],  
          curSel,  
          curCount,  
          tempTrue,  
          falseSel)  
       //结果行为true的行个数
       if trueCount > 0 {  
          if trueSel != nil {  
             //记录结果为true的行
             ...
             trueSel.SetIndex(resCount, tempTrue.GetIndex(j))  
             ...  
          }  
          //false count
          curCount -= trueCount  
          //结果为false的数据行
          curSel = falseSel  
       }  
    }  
	//返回结果为true的行个数
    return resCount, nil  
}
```
# 算子
优先介绍最复杂的算子:agg, order, join. 详解他们的实现方案.

## agg

在深入具体实现之前，先明确一些必要的概念：
- group by表达式。select语句中提供的。
- grouping set。常规的是仅对group by分组聚合。如果要在同一个select语句中，对多个不同子分组分别聚合，用grouping set表达。

例子：
```sql
SELECT city, street_name, avg(income) 
FROM addresses 
GROUP BY GROUPING SETS ((city, street_name), (city), (street_name), ());
```

agg整体执行过程：
- 构建hash表阶段
- 取聚合结果阶段
```go
func (run *Runner) aggrExec(output *chunk.Chunk, ,,,) {
	if 构建hash表阶段 {
		for {
			//读取子节点数据
			childChunk := &chunk.Chunk{}
			res, err = run.execChild(run.children[0], childChunk, state)
			...
			//准备表达式
			err = run.state.groupbyWithParamsExec.executeExprs(
			[]*chunk.Chunk{childChunk, nil, nil}, groupChunk)
			...
			//构建hash表
			run.hAggr.Sink(groupChunk)
		}
	}else{
		//取聚合结果
		for {
			...
			//取聚合结果
			res = run.hAggr.GetData(
				run.state.haScanState, groupAndAggrChunk, childChunk)
			...
			//过滤
			count, err = state.filterExec.executeSelect(
			[]*chunk.Chunk{childChunk, nil, filterInputChunk}, state.filterSel)
			...
			//计算输出值
			err = run.state.outputExec.executeExprs(
			[]*chunk.Chunk{childChunk2, nil, aggrStatesChunk3}, output)
		}
	}
}
```

hash聚合整体实现阶段：
- 初始化。设计数据结构和数据组织方式。
- 填充数据。构建hash表阶段。存储数据。更新聚合函数中间值。
- 用数据。hash表构建完成后。上层算子获取聚合函数的值。

### 初始化

先看需要哪些内容初始化agg算子。
输入参数：
- agg算子的输出值类型。
- 聚合函数表达式。agg算子要计算的聚合函数。
- group by表达式。分组的依据
- grouping set。group by表达式的再分子分组。每个子分组的实现形式是一样的。
- 子节点的输出值表达式。


agg算子复杂，涉及多个数据结构，容易弄混。初始化过程分为多个层次。需要按层次理解关联的数据结构和初始化过程。

agg管理结构的层次：第一、二层为逻辑结构。第三层为物理结构。
- 第一层：HashAggr。agg最顶层结构。管理一个agg算子的所有内容。一个plan里面有多个agg算子，会有多个HashAggr对象。
- 第二层：相关表达式和存储方式
	- 全局GroupedAggrData。在整个aggr算子中共享。
		- group by表达式 及其类型
		- agg表达式及其参数类型，返回值类型。
		- 子节点输出表达式。
	- DistinctAggrCollectionInfo。distinct聚合函数信息。在整个aggr算子中共享。
		- distinct聚合表达式的index。
	- HashAggrGroupingData。每个grouping set关联的聚合结果
		- RadixPartitionedHashTable。聚合结果包装层。
			- GroupedAggrHashTable。存储聚合结果
		- DistinctAggrData。为每个distinct聚合函数单独存储数据。
			- GroupedAggrData。每个distinct聚合函数关联的groupby,agg,子节点表达式。
			- RadixPartitionedHashTable。每个distinct聚合函数的聚合结果
- 第三层：用行层存储聚合结果
	- GroupedAggrHashTable 存储聚合结果。
		- TupleDataCollection。列存转行存存储。
			- TupleDataLayout。行存的存储格式。
			- TupleDataSegment。行存的实际数据block地址。
		- 哈希表。存储的数据的blockid和block的位置。

关键的数据结构：
- HashAggr
- GroupedAggrData
- DistinctAggrCollectionInfo
- HashAggrGroupingData
- RadixPartitionedHashTable
- GroupedAggrHashTable
- TupleDataCollection
- TupleDataLayout
- TupleDataSegment
- DistinctAggrData

agg算子的初始化实质是构建上述三层结构的关联的数据结构对象。具体每个数据结构有哪些字段，字段的意义，在对agg算子处理数据的过程中来介绍。


### 输入数据

整体过程：
- 从子节点读取一批数据。
- 数据预处理。计算一些表达式，转化成agg算子需要的输入数据。
- 如果有distinct。数据先进入distinct的逻辑。
- 数据进入hash聚合逻辑。

第一步简单。重点介绍后面的环节。

#### 数据预处理

输入：
- 子节点的输出数据。

输出数据的组织形式，有很多列，分为几段：
- 第一段：group by表达式的值。计算每个group by表达式，得出值。
- 第二段：每个聚合函数输入参数表达式的值。计算每个聚合函数输入参数表达式，得出值。
- 第三段：子节点的输出数据。复制子节点的输出数据。

输出数据组织成一个chunk，每个vector对应上面的一列数据。
```go

group by 0,...,aggr 0 param 0,...,child ouput 0,....

```

#### hash聚合逻辑

聚合逻辑的数据输入入口：
```go
func (haggr *HashAggr) Sink(data *chunk.Chunk) {
	//准备数据
	...
	//处理distinct
	if haggr._distinctCollectionInfo != nil {
		haggr.SinkDistinct(
			data, 
			childrenOutput)
	}

	//处理各个grouping set
	for _, grouping := range haggr._groupings {	
		grouping._tableData.Sink(
			data, 
			payload, 
			childrenOutput, 
			haggr._nonDistinctFilter)
	}
}
```


内部分为三个部分：
- 准备数据。从输入chunk中分离出数据（目前的处理方式不好，有些冗余）
	- payload。聚合函数参数表达式的值。
	- childrenOutput。子节点的输出数据。
- 处理distinct。下一节再讲
- 处理每个grouping set 的聚合。数据经过几层接口进入聚合逻辑。
	- RadixPartitionedHashTable.Sink。
		- 第一次进入时，初始化哈希表和物理组织结构。
		- 分离group by 表达式的值。
		- GroupedAggrHashTable.AddChunk2。
			- 计算group by表达式的值的哈希值。一行值算一个哈希值。因为要按group by表达式的值分组。
			- GroupedAggrHashTable.AddChunk。
				- GroupedAggrHashTable.FindOrCreateGroups。按group by表达式的值进行分组。
				- UpdateStates。更新每个聚合函数的中间状态。

重点介绍数据分组和更新中间状态。
##### 数据分组
`FindOrCreateGroups`完成数据分组功能。

参数：
- groups。group by 表达式的值
- groupHashes。group by表达式的值的哈希值
- addresses。函数返回后，每个组的首地址。
- newGroupsOut。产生的新组。
- childrenOutput。子节点的输出数据。

整体过程：
- 哈希表的扩容（可能）。线性探测法解决冲突。后面会单独讲哈希表的组织方式。
- 从group by哈希值提取。
	- offset。哈希表桶号。`offset = hash % capacity`
	- salt。哈希值的前缀。用于快速判断。`salt = hash >> _hashPrefixShift`。\
- 复合输入数据：作为后续处理的输入。
	- group by 表达式的值。
	- group by hash值。
	- childrenOutput。
	- 复合数据转为UnifiedFormat。方便后续处理。
- 分组逻辑：线性探测法，可能需要多轮扫哈希表。
	- 每轮的逻辑
		- 探测哈希表。有三种结果：
			- 新组。并同时占用空闲hash表元素。
			- salt值相同。需要进一步比较分组值。仅salt值相同，不能完全断定分组值也相同。
			- 没有匹配。由于是线性探测法，不能立即说此值不存在。要移动到下一个位置继续探测。
		- 对`新组`的处理：（后面会单独讲每块内容）
			- 为新组分配存储空间
			- 初始化聚合中间状态
			- 更新哈希表
		- 对`需要再比较分组值`的处理：
			- 取出要进一步比较的存储地址
			- 执行比较逻辑
		- 对`没有匹配`的处理：
			- 指向hash表下一个位置
			- 进入下一轮测试


###### 处理`新组`

agg的聚合状态数据是以行存的形式存储。列存数据要先转行存。先介绍这一转换过程。

先说行存的数据格式。`TupleDataLayout`表示行存的格式。
放进行存的数据有：
- group by 表达式的值。可能多个。非定长字段，保留堆指针。
- 子节点的输出数据。可能多个。非定长字段，保留堆指针。
- 聚合函数中间状态。可能多个。在讲聚合函数实现时再讲。
- bitmap。放NULL值。表示group by表达式和子节点输出数据的NULL值情况。
- heap size。堆空间大小。行中有非定长字段时，需要在堆上分配空间存储溢出的数据。 

`TupleDataLayout` 除了在agg算子中会用到。也用在join算子中。有些数据是可能不存在的。
数据布局: 
- heap size。可选。全是定长字段，无此值。
- 子节点的输出数据。可选
- 聚合函数中间状态。可选
```go

bitmap | heap_size? | group by...| 子节点的输出数据? | 聚合函数中间状态? |

```


**为新组分配存储空间**

入口函数`TupleDataCollection.AppendUnified`。

整体过程：
- 如果有非定长字段，计算heap size
- 为行分配空间
- 填充group by 表达式的值
- 填充子节点的输出数据

1. **heap size计算过程**

两层循环，外层对每列遍历，内层对每列的每一行计算heap size。
因为非定长字段，每一行的数据长度都不同。也就是说，在堆上，变长字段是依次排列。目前支持的变长数据类型是`VARCHAR`。
如果此行非NULL，加上字符串长度。
```go
func (tuple *TupleDataCollection) computeHeapSizes(...) {

	//外层
	//对group by表达式值的每列
	for i := 0; i < data.ColumnCount(); i++ {
		tuple.evaluateHeapSizes(state._heapSizes, data.Data[i], &state._data[i], appendSel, cnt)
	}
	
	//对的子节点的输出数据的每列
	for i := 0; i < childrenOutput.ColumnCount(); i++ {
		colIdx := state._childrenOutputIds[i]
		tuple.evaluateHeapSizes(state._heapSizes, childrenOutput.Data[i], &state._data[colIdx], appendSel, cnt)
	}
}

//内层
func (tuple *TupleDataCollection) evaluateHeapSizes(...) {
	//only care varchar type
	switch pTyp {
	case common.VARCHAR:
		//计算列的每行
		srcSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](srcUni)
		for i := 0; i < cnt; i++ {
			srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
			if srcMask.RowIsValid(uint64(srcIdx)) {
				heapSizeSlice[i] += uint64(srcSlice[srcIdx].Length())
			} else {
				heapSizeSlice[i] += uint64(common.String{}.NullLen())
			}
		}
	}
}
```

2. **行存空间组织和内存分配**

空间组织：
- TupleDataSegment。至少1个（目前实现只有1个）。
	- TupleDataChunk。每个TupleDataSegment中至少1个。
		- 记录最多2048行。与单个vector的行数对应。
		- TupleDataChunkPart。记录部分行。每个TupleDataChunk中至少1个。
			- 记录内存block的地址。
			- block上的行数
- TupleDataAllocator。内存block分配器。记录已经分配的内存block。
	- TupleDataBlock。内存block的句柄，内存空间使用量，内存总容量。
	

内存分配过程：
- 遍历输入的每一行，为其计算空间：
	- 判断当前TupleDataChunk是否满，满了创建新的TupleDataChunk。
	- 计算当前TupleDataChunk能放的行数
	- 为这些行分配空间TupleDataChunkPart
		- 判断当前TupleDataBlock是否能放下至少一行数据。如果满了，分配新TupleDataBlock。
		- 后续逻辑，整体是确定TupleDataChunPart的关键字段的值
			- 固定行的空间。
				- `_rowBlockIdx` block索引。
				- `_rowBlockOffset` block中的地址偏移
			- 变长字段的空间。
				- `_heapBlockIdx` block索引
				- `_heapBlockOffset` block中的地址偏移
				- `_baseHeapPtr` 内存基地址
				- `_totalHeapSize` 堆空间总大小。
			- `_count` 总行数
		- 分配空间过程：
			-  `_rowBlockIdx`  = 当前TupleDataBlock的索引
			- `_rowBlockOffset` =  当前TupleDataBlock的剩余空间偏移
			- `_count` = 当前TupleDataBlock的剩余空间最多能放的行数
			- 对于不定长的字段：
				- 计算总heap size。 `_count` 行的heap size的总和
				- 如果当前的heap block空间够（也是TupleDataBlock），直接分配。确定`_heapBlockIdx`和`_heapBlockOffset`的值。 否则，分配新heap block再分配。里面有些优化细节，不细说。
				- 计算`_baseHeapPtr` 基地址。
	- 记录TupleDataChunk的索引，以及TupleDataChunkPart的索引
- 确定每一行的首地址。分配的TupleDataChunkPart记录的是一批行的首地址。要将每一行的首地址算出来。
	- 对每个分配的TupleDataChunkPart
		- 计算每行的首地址
		- 如果用到heap，计算每行的heap首地址
	  

函数调用关系：
- TupleDataCollection.Build 入口
	- TupleDataAllocator.Build 分配多个block
		- TupleDataAllocator.BuildChunkPart 分配单个block和heap空间
		- TupleDataAllocator.InitChunkStateInternal 计算每行首地址

```go
func (tuple *TupleDataCollection) Build(...
	) {
	...
	seg._allocator.Build(seg, pinState, chunkState, appendOffset, appendCount)
	...
}

func (alloc *TupleDataAllocator) Build(...
	) {


	offset := uint64(0)	
	//分配空间
	for offset != appendCount {
		...
		//当前TupleDataChunk
		chunk := util.Back(segment._chunks)
		
		//count of rows can be recorded
		next := min(
			appendCount-offset,
			util.DefaultVectorSize-chunk._count,
		)
		
		  
		//分配TupleDataChunkPart
		//allocate space for these rows
		part := alloc.BuildChunkPart(
			pinState,	
			chunkState,		
			appendOffset+offset,
			next,
		)
		
		chunk.AddPart(part, alloc._layout)
		。。。
		chunkPart := util.Back(chunk._parts)
		next = uint64(chunkPart._count)	
		segment._count += next
		offset += next
	}
	...
	//decide the base ptr of rows
	//计算每行的首地址
	alloc.InitChunkStateInternal(
		pinState,
		chunkState,
		appendOffset,
		....
		parts,
	)
}


func (alloc *TupleDataAllocator) BuildChunkPart(...
	) *TupleDataChunkPart {
	//当前的block idx
	result._rowBlockIdx = uint32(len(alloc._rowBlocks) - 1)
	rowBlock := util.Back(alloc._rowBlocks)
	//block offset
	result._rowBlockOffset = uint32(rowBlock._size)
	//block 能放的行数
	result._count = uint32(min(
	rowBlock.RemainingRows(uint64(alloc._layout.rowWidth())),appendCount))
	if !alloc._layout.allConst() {
		//分配heap 空间
		...
	}
}
```

3. **填充数据**

向行存空间中，填充group by 表达式的值和子节点的输出数据。
实质是将每个vector的每行填到对应的位置。
- 确定每行首地址
- 确定列在行的偏移。在`TupleDataLayout`中。
- 将列的值填到偏移位置处。

函数关系：
- TupleDataCollection.scatter 入口。计算非定长字段的堆内存地址
	- TupleDataCollection.scatterVector 包装器
		- TupleDataTemplatedScatterSwitch 数据类型switch
			- TupleDataTemplatedScatter 取每行的值，并填充
				- TupleDataValueStore 将值填到偏移处

```go

func (tuple *TupleDataCollection) scatter(...){
	...
	if !tuple._layout.allConst() {
		//非定长计算堆内存地址
		heapSizeOffset := tuple._layout.heapSizeOffset()
		heapSizes := chunk.GetSliceInPhyFormatFlat[uint64](state._heapSizes)
		for i := 0; i < cnt; i++ {
			util.Store[uint64](heapSizes[i], util.PointerAdd(rowLocations[i], heapSizeOffset))
		}
	}
	...
	//处理每列
	for i, coldIdx := range state._columnIds {
		tuple.scatterVector(state, data.Data[i], coldIdx, appendSel, cnt)
	}
}

func (tuple *TupleDataCollection) scatterVector(...){
	TupleDataTemplatedScatterSwitch(...)
}

func TupleDataTemplatedScatterSwitch(...){
	pTyp := src.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		TupleDataTemplatedScatter[int32](
			...		
		)
	
	case common.INT64:
	...
	case common.UINT64:
	...
	case common.VARCHAR:
	...
	case common.INT8:
	...
	case common.DECIMAL:
	...
	case common.DATE:
	...
	case common.DOUBLE:
	...
	case common.INT128:
	...
	default:
	}
}

func TupleDataTemplatedScatter[T any](...){
	...
	//填充每行
	for i := 0; i < cnt; i++ {
		srcIdx := srcSel.GetIndex(appendSel.GetIndex(i))
		//值，行首地址，行内偏移
		TupleDataValueStore[T](srcSlice[srcIdx], targetLocs[i], offsetInRow, &targetHeapLocs[i], nVal)
	}
	...
}
```


**初始化聚合函数状态**

为每个聚合函数维护状态值。在新建分组时，初始化这些状态。这里仅介绍初始化过程，聚合函数实现细节，会在后面介绍聚合函数实现方案时，再细讲。

初始化过程：
- 对每个聚合函数对象，并对每行
	- 确定每行首地址
	- 确定状态在行的偏移。在`TupleDataLayout`中。
	- 在偏移位置处，执行聚合函数对象init函数。

```go

func InitStates(...) {  
	pointers := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](addresses)
	offsets := layout.offsets()
	aggrIdx := layout.aggrIdx()
	//每个聚合函数对象
	for _, aggr := range layout._aggregates {
		//对每行
		for i := 0; i < cnt; i++ {
			rowIdx := sel.GetIndex(i)
			//行首地址
			row := pointers[rowIdx]
			//init函数
			aggr._func._init(util.PointerAdd(row, offsets[aggrIdx]))
		}
		aggrIdx++
	}
}

```


**更新哈希表**

每组的内存分配后，将block和偏移信息填到每组关联哈希表元素中。后续使用时，只需查询哈希表就能拿到block和偏移信息。

```go

//每组首地址
rowLocations := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](state._chunkState._rowLocations)

for j := 0; j < newEntryCount; j++ {
	rowLoc := rowLocations[j]
	确定rowLock的block id
	...
	idx := state._emptyVector.GetIndex(j)
	//哈希表元素
	htEntry := &htEntrySlice[htOffsetsPtr[idx]]
	//block id和偏移 回填哈希表元素
	htEntry._pageNr = uint32(blockId + 1)
	htEntry._pageOffset = uint16(util.PointerSub(rowLoc, blockPtr) / int64(aht._tupleSize))
	//记录行地址
	addresessSlice[idx] = rowLoc
}
```

###### 处理`需要再比较分组值`

hash值的salt相同，但是不能确定两组group by表达式的值相同。需要再比较它们的值。

再比较两组值：
- 第一组值：新输入的vector
- 第二组值：已经在行存中的值

计算过程：
- 确定第二组值的行首地址
- 对每一列，对每一行
	- 从vector中取出 第一组值
	- 第二组值 在行存的位置 = 行首地址 + 偏移
	- 对比 第一组值 和 第二组值

函数关系：
- Match 函数入口
	- TemplatedMatch 对每列进行比较
		- TemplatedMatchOp 按照数据类型区分。
			- TemplatedMatchType 对每行比较两组值

```go
func Match(...){
	TemplatedMatch(...)
}

func TemplatedMatch(...){
	//每列
	for i := 0; i < len(predicates); i++ {
		vec := columns.Data[i]
		col := colData[i]
		TemplatedMatchOp(...)
	}
}

func TemplatedMatchOp(...){
	colOffset := layout.offsets()[colNo]
	//区分类型
	switch predTyp {
	case ET_Equal, ET_In:
		pTyp := layout.types()[colNo].GetInternalType()
		switch pTyp {
		case common.INT32:
			TemplatedMatchType[int32](...)
		...
		}
	...
	}
}

func TemplatedMatchType(...){
	//第一组 新输入的值
	dataSlice := chunk.GetSliceInPhyFormatUnifiedFormat[T](col)
	...
	//对每行
	for i := 0; i < *cnt; i++ {
		idx := sel.GetIndex(i)
		row := util.PointerToSlice[uint8](ptrs[idx], rowWidth)
		mask := util.Bitmap{Bits: row}
		isNull := !util.RowIsValidInEntry(mask.GetEntry(entryIdx), idxInEntry)
		colIdx := col.Sel.GetIndex(idx)
		//第二组 行存中的值
		val := util.Load[T](util.PointerAdd(ptrs[idx], colOffset))
		//比较两组值
		if !isNull && cmp.operation(&dataSlice[colIdx], &val) {
			sel.SetIndex(matchCnt, idx)
			matchCnt++
		} else {
			if noMatchSel {
				noMatch.SetIndex(*noMatchCnt, idx)
				(*noMatchCnt)++
			}
		}	
	}
	...
}
```


###### 处理`没有匹配`

用线性探测法移动到下一个哈希表元素位置。进入下一轮测试。

```go

for i := 0; i < noMatchCount; i++ {
	idx := state._noMatchVector.GetIndex(i)
	htOffsetsPtr[idx]++
	if htOffsetsPtr[idx] >= uint64(aht._capacity) {
		htOffsetsPtr[idx] = 0
	}
}

```

###### 哈希表实现方案

哈希表元素结构：
- salt。哈希值前缀
- 数据所在的block和偏移
```go
type aggrHTEntry struct {
	_salt uint16
	_pageOffset uint16
	_pageNr uint32
}
```

哈希表实质是`aggrHTEntry`类型的数组。并用线性探测解决冲突。
随着元素的增加，哈希表数组会出现空间不够的情况。哈希表需要Resize。

Resize过程：
- 输入参数： new size
- 分配数组。
- 重新构建哈希表。实质是将现有的数据重新插入新哈希表。
	- 取出已经分组的每行数据的首地址。数据是在TupleDataCollection,TupleDataSegment，TupleDataChunk,TupleDataChunkPart里面。会涉及到较复杂的scan过程。会在讲取`聚合结果`的地方细节。
	- 取此行的hash值。
	- 由hash值确定元素在新hash表的位置
	- 计算salt，block id和偏移。

函数关系：
```go

func (aht *GroupedAggrHashTable) Resize(size int){
	分配新数组
	...
	for {
		rowLocs := iter.GetRowLocations()
		for i := 0; i < iter.GetCurrentChunkCount(); i++ {
			//行首地址
			rowLoc := rowLocs[i]
			...
			//行hash值
			hash := util.Load[uint64](util.PointerAdd(rowLoc, aht._hashOffset))
			...
			//插入新哈希表。线性探测确定元素位置
			entIdx := hash & aht._bitmask
			for hashesArr[entIdx]._pageNr > 0 {
				entIdx++
				if entIdx >= uint64(aht._capacity) {
					entIdx = 0
				}
			}
			
			htEnt := &hashesArr[entIdx]
			//salt, block id, block offset
			htEnt._salt = uint16(hash >> aht._hashPrefixShift)
			htEnt._pageNr = uint32(1 + blockId)
			htEnt._pageOffset = uint16(util.PointerSub(rowLoc, blockPtr) / int64(aht._tupleSize))
		}
		//下一批行
		next := iter.Next()
		if !next {
			break
		}
	}
}

```

##### 更新聚合函数状态

对数据按group by表达式的值分组后，需要将数据更新进聚合函数状态中，即实质的聚合过程。

###### 聚合函数实现方案

支持的聚合函数：
- sum
- avg
- count
- max
- min

基础的概念。
- 状态。聚合函数的中间值。
- 聚合操作。对状态的动作。
	- init。聚合过程开始时，初始化状态。
	- update。新数据补充进状态。
	- combine。并行计算时，多个状态融合进一个状态。
	- finalize。从状态中取出结果值。

聚合过程实质是将聚合操作施加到状态上的过程。

重要模块：
- 状态。
	- State。存储状态。
	- StateOp。操作状态的包装接口。
		- 具体实现：SumStateOp，AvgStateOp，CountStateOp，MaxStateOp，MinStateOp
- 操作
	- AggrOp。聚合操作接口。
		- 具体实现：SumOp，AvgOp，CountOp，MinMaxOp
		- 最终落在StateOp的接口上
- 聚合流程：聚合操作的具体实现。基于状态和操作接口，完成聚合计算过程。
	- 入口：
		- GetXXXAggr。取某种聚合函数的某种数据类型的实现
		- UnaryAggregate。拼接聚合函数对象
	- 流程接口
		- aggrStateSize。取状态内存空间大小。
		- aggrInit。更新新数据
		- aggrCombine。复合另一个状态
		- aggrFinalize。取聚合结果值
		- aggrUpdate。聚合新数据
		- simpleUpdate。聚合新数据

**State**

- `_typ` 聚合类型。不同类型，值组成不同。支持sum，avg，count，max，min。
- `_value` 泛型值。某种数据类型。
- `_count` 个数。avg,count需要。
- 基本接口。
	- Init。按种类初始化。
	- Combine。复合另一个State。按种类有不同的复合方法。
	- SetIsset/GetIsset，GetValue/SetValue

```go
type State[T any] struct {
	_typ StateType // 聚合类型
	_isset bool // 是否已设置值
	_value T // 存储的值
	_count uint64 // 计数
}
```

**StateOp**

操作状态的包装接口。依据聚合种类，对State施加不同动作。
- Init。初始化state。内部实现用State.Init
- Combine。复合另一个State。按种类有不同的复合方法。
- AddValues。辅助接口。

Combine和AddValues，按聚合种类有不同的实现方法。

```go
type StateOp[T any] interface {
	Init(*State[T])
	Combine(*State[T], *State[T], *AggrInputData, TypeOp[T])
	AddValues(*State[T], int)
}
```

**Combine**的实现方案：
- 使用State.Combine。sum，avg，count是同一个实现。
- 单独实现。max，min是类似的实现。

```go
//sum，avg，count相同的实现
func (SumStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	src.Combine(target, top)
}

//max，min类似的实现
func (as *MaxStateOp[T]) Combine(
	src *State[T],
	target *State[T],
	_ *AggrInputData,
	top TypeOp[T]) {
	if !src._isset {
		return
	}
	
	if !target._isset {
		target._isset = src._isset
		target._value = src._value
	} else if top.Less(&target._value, &src._value) {
		target._value = src._value
	}
}
```


**AddValues**的实现方案：
- 更新count。avg，count
- 空。max,min

```go
func (as *CountStateOp[T]) AddValues(s *State[T], cnt int) {
	s._count += uint64(cnt)
}

func (as *MaxStateOp[T]) AddValues(s *State[T], cnt int) {
}
```

**AggrOp**
聚合操作接口。
- Init。初始化状态。内部实现用StateOp.Init
- Combine。复合另一个状态。内部实现用StateOp.Combine。
- Operation。聚合值。新数据聚合到状态上。
- ConstantOperation。聚合值。多个相同的新数据聚合到状态上。
- Finalize。从状态中取聚合结果值。
- IgnoreNull。忽略NULL。

Init，Combin和IgnoreNull的实现容易。

Operation和ConstantOperation的实现：
- sum, avg,count的实现相同。用StateOp添加新数据。
- min,max的实现相同。比较操作。新值与状态中的值比较，依据比较结果，进行更新。

```go
//sum,avg,count
func (s SumOp[ResultT, InputT]) Operation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
		sop.AddValues(s3, 1)
		aop.AddNumber(s3, input, top)
}

  
func (s SumOp[ResultT, InputT]) ConstantOperation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	count int,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
		sop.AddValues(s3, count)
		aop.AddConstant(s3, input, count, top)
}

func (MinMaxOp[ResultT, InputT]) Operation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	if !s3._isset {//第一次赋值
		aop.Assign(s3, input)
		s3._isset = true
	} else {//比较，再更新
		aop.Execute(s3, input, top)
	}
}  

func (MinMaxOp[ResultT, InputT]) ConstantOperation(
	s3 *State[ResultT],
	input *InputT,
	data *AggrUnaryInput,
	count int,
	sop StateOp[ResultT],
	aop AddOp[ResultT, InputT],
	top TypeOp[ResultT]) {
	
	if !s3._isset {//第一次赋值
		aop.Assign(s3, input)
		s3._isset = true
	} else {//比较，再更新
		aop.Execute(s3, input, top)
	}
}
```

Finalize的实现：
- count, min,max,sum。实现相似。从聚合结果中取值
- avg。计算平均值 = 聚合值 / 值个数。

```go
func (s SumOp[ResultT, InputT]) Finalize(
	s3 *State[ResultT],
	target *ResultT,
	data *AggrFinalizeData) {
	if !s3.GetIsset() {
		data.ReturnNull()
	} else {
		*target = s3.GetValue()
	}
}

func (AvgOp[ResultT, InputT]) Finalize(
	s3 *State[ResultT],	
	target *ResultT,	
	data *AggrFinalizeData) {
	
	if s3._count == 0 {
		data.ReturnNull()	
	} else {
		//计算平均值
		var rt = s3.GetValue()
		switch v := any(rt).(type) {
		case float64:
			c := float64(s3._count)
			r := v / c
			*target = any(r).(ResultT)
		...	
		}
	}
}
```

**聚合入口**
GetXXXAggr。依据数据类型、聚合函数类型（sum,count,...)调用UnaryAggregate封装聚合函数对象。
sum,min,max,count,avg都是类似的形式。

```go
func GetSumAggr(pTyp common.PhyType) *FunctionV2 {
	switch pTyp {
	case common.INT32:
		fun := UnaryAggregate[common.Hugeint, State[common.Hugeint], int32, SumOp[common.Hugeint, int32]](
			...
		)
		return fun
	...
	}
...
}
```

**UnaryAggregate**。拼接聚合函数对象
确定聚合流程接口的具体实现。
- aggrStateSize。取State的对象
- aggrInit。应用AggrOp.Init
- aggrCombine。应用Combine函数。
- aggrFinalize。应用Finalize函数
- aggrUpdate。应用UnaryScatter函数
- simpleUpdate。应用UnaryUpdate函数

```go
func UnaryAggregate[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](	
	inputTyp common.LType,
	retTyp common.LType,
	aop AggrOp[ResultT, InputT],
	...
	) *FunctionV2 {
	var size aggrStateSize
	var init aggrInit
	var update aggrUpdate
	var combine aggrCombine
	var finalize aggrFinalize
	var simpleUpdate aggrSimpleUpdate
	
	size = func() int {
		var val State[ResultT]
		return int(unsafe.Sizeof(val))
	}
	
	init = func(pointer unsafe.Pointer) {
		aop.Init((*State[ResultT])(pointer), sop)
	}
	
	update = func(inputs []*chunk.Vector, data *AggrInputData, inputCount int, states *chunk.Vector, count int) {
		UnaryScatter[ResultT, STATE, InputT, OP](inputs[0], states, data, count,...)
	}
	
	combine = func(source *chunk.Vector, target *chunk.Vector, data *AggrInputData, count int) {
		Combine[ResultT, STATE, InputT, OP](source, target, data, count,...)
	}
	
	finalize = func(states *chunk.Vector, data *AggrInputData, result *chunk.Vector, count int, offset int) {
		Finalize[ResultT, STATE, InputT, OP](states, data, result, count, offset, ...)
	}
	
	simpleUpdate = func(inputs []*chunk.Vector, data *AggrInputData, inputCount int, state unsafe.Pointer, count int) {
		UnaryUpdate[ResultT, STATE, InputT, OP](inputs[0], data, state, count, ...)
	}
	
	return &FunctionV2{
			_funcTyp: AggregateFuncType,
			_args: []common.LType{inputTyp},
			_retType: retTyp,
			_stateSize: size,
			_init: init,
			_update: update,
			_combine: combine,
			_finalize: finalize,
			_simpleUpdate: simpleUpdate,		
		}
}
```


**Combine函数**：
对每个source state，应用AggrOp.Combine

```go
func Combine[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	source *chunk.Vector,
	target *chunk.Vector,
	data *AggrInputData,
	count int,
	aop AggrOp[ResultT, InputT],
	...
	) {
	
	sourcePtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](source)
	targetPtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](target)
	for i := 0; i < count; i++ {
		//AggrOp.Combine
		aop.Combine(
			(*State[ResultT])(sourcePtrSlice[i]),
			(*State[ResultT])(targetPtrSlice[i]),
			...
		)
	}
}
```

**Finalize函数**:
对每个state，应用AggrOp.Finalize

```go
func Finalize[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	states *chunk.Vector,
	data *AggrInputData,
	result *chunk.Vector,//agg结果
	count int,
	offset int,
	aop AggrOp[ResultT, InputT],
	...
	) {	
	statePtrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](states)
	resultSlice := chunk.GetSliceInPhyFormatFlat[ResultT](result)
	final := NewAggrFinalizeData(result, data)
	for i := 0; i < count; i++ {
		final._resultIdx = i + offset
		aop.Finalize((*State[ResultT])(
			statePtrSlice[i]), 
			&resultSlice[final._resultIdx], final)
	}
}
```

**UnaryScatter函数**：
根据新数据vector和state vector的物理表示，分情况处理。
- 都是constant表示。应用AggrOp.ConstantOperation
- 都是flat表示。应用UnaryFlatLoop
- 其他表示。统一转UnifiedFormat。应用UnaryScatterLoop

函数层次关系：
- UnaryScatter
	- AggrOp.ConstantOperation
	- UnaryFlatLoop
		- AggrOp.Operation
	- UnaryScatterLoop
		- AggrOp.Operation
```go
func UnaryScatter[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
		input *chunk.Vector,
		states *chunk.Vector,
		data *AggrInputData,
		count int,
		aop AggrOp[ResultT, InputT],
		...
	) {
	//constant
	if input.PhyFormat().IsConst() && states.PhyFormat().IsConst() {
		...
		aop.ConstantOperation((*State[ResultT])(statesPtrSlice[0]), &inputSlice[0], inputData, count,...)	
	//flat & flat
	} else if input.PhyFormat().IsFlat() && states.PhyFormat().IsFlat() {
		...
		UnaryFlatLoop[ResultT, STATE, InputT, OP](
			...
		)
	
	} else {//其它
		...
		UnaryScatterLoop[ResultT, STATE, InputT](
			...
		)
	}
}
```

**UnaryFlatLoop**
对每个新数据，应用AggrOp.Operation将新数据更新到state上.

```go
func UnaryFlatLoop[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputSlice []InputT,	
	data *AggrInputData,	
	statesPtrSlice []unsafe.Pointer,
	mask *util.Bitmap,
	count int,
	aop AggrOp[ResultT, InputT],
	...
	) {	
	...
	input := NewAggrUnaryInput(data, mask)
	i := &input._inputIdx
	for *i = 0; *i < count; *i++ {
		aop.Operation((*State[ResultT])(statesPtrSlice[*i]), &inputSlice[*i], input, sop, addOp, top)
	}
}
```

**UnaryScatterLoop**
处理UnifiedFormat。对每个新数据，应用AggrOp.Operation将新数据更新到state上.
与UnaryFlatLoop的区别，取state的方式不同。

```go
func UnaryScatterLoop[ResultT any, STATE State[ResultT], InputT any](
	inputSlice []InputT,
	data *AggrInputData,
	statesPtrSlice []unsafe.Pointer,
	isel *chunk.SelectVector,
	ssel *chunk.SelectVector,
	mask *util.Bitmap,
	count int,
	aop AggrOp[ResultT, InputT],
	...
	) {
	...	
	input := NewAggrUnaryInput(data, mask)
	for i := 0; i < count; i++ {
		input._inputIdx = isel.GetIndex(i)
		sidx := ssel.GetIndex(i)
		aop.Operation((*State[ResultT])(statesPtrSlice[sidx]), &inputSlice[input._inputIdx], input, sop, addOp, top)
	}
}
```

**UnaryUpdate函数**
将新数据更新到单个state。而UnaryScatter是更新到一批state。

根据新数据vector物理表示，分情况处理。
- constant表示。应用AggrOp.ConstantOperation
- flat表示。应用UnaryFlatUpdateLoop
- 其他表示。统一转UnifiedFormat。应用UnaryUpdateLoop

函数层次关系：
- UnaryUpdate
	- AggrOp.ConstantOperation
	- UnaryFlatUpdateLoop
		- AggrOp.Operation
	- UnaryUpdateLoop
		- AggrOp.Operation

```go
func UnaryUpdate[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	input *chunk.Vector,
	data *AggrInputData,
	statePtr unsafe.Pointer,
	count int,
	aop AggrOp[ResultT, InputT],
	...
	) {
	
	switch input.PhyFormat() {
	case chunk.PF_CONST://constant
		...
		aop.ConstantOperation((*State[ResultT])(statePtr), &inputSlice[0], inputData, count, sop, addOp, top)
	case chunk.PF_FLAT://flat
		...
		UnaryFlatUpdateLoop[ResultT, STATE, InputT, OP](
		...
		)
	
	default:
		var idata chunk.UnifiedFormat
		input.ToUnifiedFormat(count, &idata)
		UnaryUpdateLoop[ResultT, STATE, InputT, OP](
			...
		)
	}
}
```

**UnaryFlatUpdateLoop与UnaryUpdateLoop**
实现很接近。区别在于取新数据的方式不同。

```go
func UnaryFlatUpdateLoop[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputSlice []InputT,
	data *AggrInputData,
	statePtr unsafe.Pointer,
	count int,
	mask *util.Bitmap,
	aop AggrOp[ResultT, InputT],
	...
	) {
	...
	for ; *baseIdx < next; *baseIdx++ {
		aop.Operation((*State[ResultT])(statePtr), &inputSlice[*baseIdx], input, sop,..)
	}
	...

}

func UnaryUpdateLoop[ResultT any, STATE State[ResultT], InputT any, OP AggrOp[ResultT, InputT]](
	inputSlice []InputT,
	data *AggrInputData,
	statePtr unsafe.Pointer,
	count int,
	mask *util.Bitmap,
	selVec *chunk.SelectVector,
	aop AggrOp[ResultT, InputT],
	sop StateOp[ResultT],
	...
	) {
	...	
	for i := 0; i < count; i++ {
		input._inputIdx = selVec.GetIndex(i)
		aop.Operation((*State[ResultT])(statePtr), &inputSlice[input._inputIdx], input, sop,...)
	}
}
```

###### 聚合过程

在确定新数据的分组后，要将新数据更新进每个聚合函数的状态。

过程：
- 对每个聚合函数，更新状态。函数UpdateStates
```go
func (aht *GroupedAggrHashTable) AddChunk(
	state *AggrHTAppendState,
	groups *chunk.Chunk,
	groupHashes *chunk.Vector,
	payload *chunk.Chunk,
	childrenOutput *chunk.Chunk,
	filter []int,
	) int {
	
	...
	//分组
	newGroupCount := aht.FindOrCreateGroups(
		...
	)
	//跳到行存中第一个状态的位置
	AddInPlace(state._addresses, int64(aht._layout.aggrOffset()), payload.Card())
	
	//对每个聚合函数，更新状态
	payloadIdx := 0	
	for i, aggr := range aht._layout._aggregates {
		...
		UpdateStates(
			aggr,
			state._addresses,
			payload,
			payloadIdx,
			payload.Card(),
		)
		//跳到一个聚合函数状态
		payloadIdx += aggr._childCount
		AddInPlace(state._addresses, int64(aggr._payloadSize), payload.Card())
	}
	
	return newGroupCount
}
```

**UpdateStates函数**：
目前只支持一元聚合函数。
应用聚合函数的aggrUpdate 将新数据更新进状态。

```go
func UpdateStates(
	aggr *AggrObject,
	addresses *chunk.Vector,
	payload *chunk.Chunk,
	argIdx int,
	cnt int,
	) {
	...
	if aggr._childCount != 0 {
		input = []*chunk.Vector{payload.Data[argIdx]}
	}
	//aggrUpdate
	aggr._func._update(
		input,
		...
		addresses,
		cnt,	
	)
}
```


#### distinct逻辑

处理步骤：
- 去重复阶段。每个distinct函数有个单独的hash表对象。数据先进入单独的hash表对象，按参数值去重复。
- 再聚合阶段。在结束构建hash表时，将distinct函数按参数去重后的数据，再次进入distinct函数的聚合逻辑，计算结果值。


##### 去重复阶段
注意：单独的hash表对象的key是聚合函数的参数值，而不是group by表达式的值。
除了hash表达对象不同，Sink的逻辑完全相同。

函数关系：
- HashAggr.Sink
	- HashAggr.SinkDistinct
		- HashAggr.SinkDistinctGrouping
			- RadixPartitionedHashTable.Sink

```go
func (haggr *HashAggr) SinkDistinct(chunk, childrenOutput *chunk.Chunk) {
	for i := 0; i < len(haggr._groupings); i++ {
		haggr.SinkDistinctGrouping(chunk, childrenOutput, i)
	}
}

func (haggr *HashAggr) SinkDistinctGrouping(
	data, childrenOutput *chunk.Chunk,
	groupingIdx int,
	) {
	distinctInfo := haggr._distinctCollectionInfo	
	distinctData := haggr._groupings[groupingIdx]._distinctData
	var tableIdx int
	dump := &chunk.Chunk{}
	
	for _, idx := range distinctInfo._indices {		
		...
		radixTable := distinctData._radixTables[tableIdx]
		...		
		radixTable.Sink(data, dump, childrenOutput, []int{})
		
	}
}
```

##### 再聚合阶段
在结束构建hash表时，触发distinct函数的聚合逻辑。

函数关系：
- HashAggr.Finalize 结束hash表构建
	- HashAggr.FinalizeInternal 结束hash表构建
		- HashAggr.FinalizeDistinct distinct函数的参数再聚合
			- HashAggr.DistinctGrouping 

**HashAggr.FinalizeInternal**函数
结束hash表（包括distinct的hash表）构建。
先做distinct函数的再聚合。
然后，结束group by表达式的聚合。

```go

func (haggr *HashAggr) Finalize() {
	haggr.FinalizeInternal(true)
}

func (haggr *HashAggr) FinalizeInternal(checkDistinct bool) {
	//distinct函数的再聚合
	if checkDistinct && haggr._distinctCollectionInfo != nil {
		haggr.FinalizeDistinct()
	}

	//结束group by表达式的聚合
	for i := 0; i < len(haggr._groupings); i++ {
		grouping := haggr._groupings[i]
		grouping._tableData.Finalize()
	}
}
```

**HashAggr.FinalizeDistinct函数**

先结束distinct函数的hash表对象。
再将distinct函数的hash表对象中的数据取出来再按group by表达式的值再次聚合

```go
func (haggr *HashAggr) FinalizeDistinct() {
	//结束distinct的hash表对象
	for i := 0; i < len(haggr._groupings); i++ {
		grouping := haggr._groupings[i]
		distinctData := grouping._distinctData
		for tableIdx := 0; tableIdx < len(distinctData._radixTables); tableIdx++{
			if distinctData._radixTables[tableIdx] == nil {
				continue
			}
			radixTable := distinctData._radixTables[tableIdx]		
			radixTable.Finalize()
		}
	}

	//distinct函数的hash表对象 => group by的hash表对象
	for i := 0; i < len(haggr._groupings); i++ {	
		grouping := haggr._groupings[i]
		haggr.DistinctGrouping(grouping, i)
	}
}

```


**HashAggr.DistinctGrouping函数** 

过程：
- 对每个distinct函数对象，对其hash表对象
	- 读取一批数据
	- 进入group by表达式hash表


```go
func (haggr *HashAggr) DistinctGrouping(
	groupingData *HashAggrGroupingData,	
	groupingIdx int,
	) {
	aggregates := haggr._distinctCollectionInfo._aggregates
	data := groupingData._distinctData
	...
	//每个distinct 函数
	for i := 0; i < len(haggr._groupedAggrData._aggregates); i++ {	
		aggr := aggregates[i]
		...
		//skip non distinct
		if !data.IsDistinct(i) {
			continue
		}
		
		
		//groupby + distinct payload
		outputChunk := &chunk.Chunk{}
		...
		
		//children output
		childrenChunk := &chunk.Chunk{}
		...
		
		scanState := &TupleDataScanState{}

		//读出每批数据，然后进入group by表达式的hash表
		for {
			...
			res := radixTable.GetData(scanState, outputChunk, childrenChunk)
			...
			//分离group by表达式值
			groupedAggrData := data._groupedAggrData[tableIdx]
			for groupIdx := 0; groupIdx < groupbySize; groupIdx++ {
				groupChunk.Data[groupIdx].Reference(outputChunk.Data[groupIdx])
			}
			groupChunk.SetCard(outputChunk.Card())
			
			  
			//分离distinct函数的输入参数值
			for childIdx := 0; childIdx < len(groupedAggrData._groups)-groupbySize; childIdx++ {
				aggrInputChunk.Data[payloadIdx+childIdx].Reference(
				outputChunk.Data[groupbySize+childIdx])
			}
				
			aggrInputChunk.SetCard(outputChunk.Card())
			//输入到group by表达式hash表
			groupingData._tableData.Sink(groupChunk, aggrInputChunk, childrenChunk, []int{i})
		}
	}
}
```
### 取聚合结果

结果的内容：
- group by表达式值
- 聚合函数值
- 原始子节点的输出值

从多个维度理解聚合结果的组织结构：
- 上层：数据的空间组织层次。有分为多层:
	- TupleDataCollection
		- TupleDataSegment。存多个vector的数据。
			- TupleDataChunk。存1个vector，2048行数据
				- TupleDataChunkPart。1个block和溢出block。存多个行，小于2048行。
- 中层：行在block内，依次排列。
- 下层：TupleDataLayout决定行内部的数据格式。

读取聚合结果的过程：
- 扫所有的segment,chunk
- 扫chunk所有的block
- 扫block上的所有行
- 从行上取出数据

相应的，取聚合结果的函数接口也分为多层：
- 外部接口：取聚合结果API
	-  HashAggr.GetData
		- RadixPartitionedHashTable.GetData
			- GroupedAggrHashTable.Scan
				- TupleDataCollection.Scan
- 中部接口：读取一个chunk。
	-  TupleDataCollection.Scan
		- TupleDataCollection.NextScanIndex
		- TupleDataCollection.ScanAtIndex
			- TupleDataAllocator.InitChunkState
				- TupleDataAllocator.InitChunkStateInternal
			- TupleDataCollection.Gather
		- FinalizeStates
- 内部接口：从行中取数据
	- TupleDataCollection.Gather
		- TupleDataCollection.gather
			- TupleDataTemplatedGatherSwitch
				- TupleDataTemplatedGather

外部接口相对简单，不细讲
#### 中部接口

读取一个chunk的过程：
- 确定segment，chunk。函数NextScanIndex。
- 读取一个chunk。函数ScanAtIndex。
	- 确定每行的地址。函数InitChunkState
	- 读取每行中的数据。函数Gather。

TupleDataScanState记录了segment，chunk，block的信息。
在确定block后，借助TupleDataLayout确定数据在行的位置。

TupleDataScanState关键内容：
- 要读的列。
- 当前读到的segment,chunk的索引
- 被读取数据所在的block。TupleDataPinState
- 被读取数据的行首地址 以及 堆地址（可选）。TupleDataChunkState

TupleDataScanState在TupleDataCollection被取前初始化，并在读取过程中被更新。

**NextScanIndex函数**：
确定segment，chunk。当前chunk读完后，跳到下一个chunk。当前segment读完后，跳到下一个segment。

**ScanAtIndex函数：**

由InitChunkState确定行首地址。
由Gather读数据。

**InitChunkState函数：**
实质在InitChunkStateInternval中完成。

对每个TupleDataChunkPart（代表一个block）：
- 确定block首地址
- 确定行首地址= block首地址 + 行号 x 行宽。行宽由TupleDataLayout确定
- 如果有非定长字段，再确定堆地址

```go
func (alloc *TupleDataAllocator) InitChunkStateInternal(
	...
	offset uint64,
	...
	parts []*TupleDataChunkPart,
	) {
	
	rowLocSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](chunkState._rowLocations)
	heapSizesSlice := chunk.GetSliceInPhyFormatFlat[uint64](chunkState._heapSizes)
	heapLocSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](chunkState._heapLocations)
	
	rowWidth := alloc._layout.rowWidth()
	for _, part := range parts {
		next := part._count
	
		//block首地址	
		baseRowPtr := alloc.GetRowPointer(pinState, part)
		//确定行首地址	
		for i := uint32(0); i < next; i++ {
			rowLocSlice[offset+uint64(i)] =
			util.PointerAdd(baseRowPtr, int(i)*rowWidth)
		}
		
		。。。
  
		
		if initHeapPointers {
			//堆地址
			heapLocSlice[offset] = util.PointerAdd(
				part._baseHeapPtr,
				int(part._heapBlockOffset),
			)
			
			for i := uint32(1); i < next; i++ {
				idx := offset + uint64(i)
				heapLocSlice[idx] = util.PointerAdd(
					heapLocSlice[idx-1],
					int(heapSizesSlice[idx-1]),
				)
			}
		}
		offset += uint64(next)
	}
}
```

#### 内部接口

**Gather函数：**
读取行上的数据。

过程：
- 按列逐次读。Gather函数
- 按数据类型区分。TupleDataTemplatedGatherSwitch函数
- 读取行上数据。TupleDataTemplatedGather函数


```go
func (tuple *TupleDataCollection) Gather(
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	scanSel *chunk.SelectVector,
	scanCnt int,
	colIds []int,
	result *chunk.Chunk,
	targetSel *chunk.SelectVector,
	) {
	//读取列
	for i := 0; i < len(colIds); i++ {
		tuple.gather(...)
	}
}

func (tuple *TupleDataCollection) gather(...) {
	TupleDataTemplatedGatherSwitch(
	...
	)
}

func TupleDataTemplatedGatherSwitch(
...
) {

	pTyp := target.Typ().GetInternalType()
	switch pTyp {
	case common.INT32:
		TupleDataTemplatedGather[int32](
		...
		)
	...
	}
}

func TupleDataTemplatedGather[T any](
	layout *TupleDataLayout,
	rowLocs *chunk.Vector,
	colIdx int,
	scanSel *chunk.SelectVector,
	scanCnt int,
	target *chunk.Vector,
	targetSel *chunk.SelectVector,
	) {
	
	srcLocs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rowLocs)
	targetData := chunk.GetSliceInPhyFormatFlat[T](target)
	targetBitmap := chunk.GetMaskInPhyFormatFlat(target)
	entryIdx, idxInEntry := util.GetEntryIndex(uint64(colIdx))
	//行内偏移
	offsetInRow := layout.offsets()[colIdx]
	for i := 0; i < scanCnt; i++ {
		...	
		if util.RowIsValidInEntry(
			rowMask.GetEntry(entryIdx),
			idxInEntry) {
			//读取行上指定位置的数据
			targetData[targetIdx] = util.Load[T](util.PointerAdd(base, offsetInRow))
		} else {
			targetBitmap.SetInvalid(uint64(targetIdx))
		}
	}

}

```


## order

概念：
- order by表达式。对应语法中order by子句。
- payload。order算子的输出表达式的值。order算子的结果。

一批输入数据会算出order by表达式的值和输出表达式的值（payload）。
order算子实质是依据order by表达式的值，对payload进行排序。

执行过程：
- 输入数据
	- 计算order by表达式的值和payload
	- 列转行。以行格式存储
- 排序
	- 内存排序。暂时不涉及外部排序
	- reorder。按排序结果，对非定长字段和payload重排列
- 读排序结果：sort算子的payload
	- 从行存中读结果

### 初始化

参数：
- order by表达式。
- output 表达式。即payload

层次管理数据结构：从逻辑层到物理层
- 顶层：LocalSort。order算子管理结构。
- 中层：组织数据块。
	- RowDataCollection。多个block。存储输入数据，行存结构
		- RowDataBlock。单个block
	- SortedBlock 排序内存块。
		- SortedData 多个block
			- RowDataBlock
	- PayloadScanner 读排序结果
		- RowDataCollectionScanner 读多个block
			- RowDataCollection
- 物理层：
	- SortLayout。排序键的行存组织方式。
	- RowLayout。非定长字段，payload的行存组织方式。
	- RowDataBlock。单个block。entry个数，容量。

按阶段分：
- 输入数据阶段：
	- RowDataCollection。多个block。entry个数。
- 排序阶段：排序键的内存是单独的。
	- SortedBlock 排序键内存块。
		- SortedData 多个block
- 读排序结果阶段：
	- PayloadScanner 读排序结果
		- RowDataCollectionScanner 读多个block

#### 排序键行存结构
SortLayout定义order by表达式值的行存结构。规定的每行长度一定是定长的。非定长部分一定存储在堆中。

两部分构成：定长部分+非定长部分
- 定长部分。字节数固定。定长字段+非定长字段的固定长度prefix
- 非定长部分。字节数不定。单独行存结构。单独block存储。与定长部分的block分开。

从每个order by表达式中收集的信息：
- `_orderTypes`. 排序类型。asc,desc
- `_orderByNullTypes`. NULL值排序类型。默认是NULL值排前面
- `_logicalTypes`. 数据类型
- `_constantSize` . 是否是定长字段
- `_hasNull` . 是否有null值。
- `_columnSizes` . 列size。null byte（NULL字段）+字段size
- `_prefixLengths`. 前缀长度。varchar类型。在排序键中只存前缀。

内存形式：
```go
|null byte, col0|...|null byte,prefix|...|null byte,colN-1|
```

SortLayout除了上述信息外，还有整体信息：
- `_allConstant`. 是否全是定长字段。
- `_comparisonSize`。参与比较的字节数。
- `_entrySize`. entry长度。entry长度 = `_comparisonSize` + 行索引size(4字节)。
- `_blobLayout` 非定长字段的行存组织方式。由RowLayout表示。

#### 通用行存结构
RowLayout定义的行存结构服务payload，排序键中非定长字段。规定的每行长度一定是定长的。非定长部分一定存储在堆中。


内存形式：
```
|null bitmap|heap ptr offset|col 0|...|col i heap ptr|...|col n-1|
```


### 输入数据

数据列转行后，会分成三部分：
- 排序键的定长部分。来自order by表达式。
- 排序键的非定长部分。来自order by表达式。
- payload。

三部分数据都是用RowDataCollection存储。
- 排序键的定长部分。一个RowDataCollection对象。长度一定是固定size。
- 排序键的非定长部分。两个RowDataCollection对象。一个放堆指针，固定size的行。一个放变长字段值。
- payload。两个RowDataCollection对象。一个放堆指针，固定size的行。一个放变长字段值。

输入阶段有5个RowDataCollection对象。

输入过程是将数据转换成这三部分。

过程：
- 计算order by表达式的值和payload
- 分配内存块
- 列转行
	- 排序键的定长部分 
	- 排序键的非定长部分
	- payload

函数关系：
- LocalSort.SinkChunk 输入数据入口
    - RowDataCollection.Build 分配内存
    - RadixScatter 转换排序键的定长部分
    - Scatter  转换排序键的非定长部分和payload

```go
func (ls *LocalSort) SinkChunk(sort, payload *chunk.Chunk) {
    dataPtrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](ls._addresses)
    
    //为排序键的定长部分 分配空间
    ls._radixSortingData.Build(sort.Card(), dataPtrs, nil, chunk.IncrSelectVectorInPhyFormatFlat())
    
    //转换排序键的定长部分
    for sortCol := 0; sortCol < sort.ColumnCount(); sortCol++ {
        ...
        //copy data from input to the block
        //only copy prefix for varchar
        RadixScatter(
          ...
        )
    }
    
    //排序键的非定长部分
    if !ls._sortLayout._allConstant {
        ...
        //分配空间
        ls._blobSortingData.Build(blobChunk.Card(), dataPtrs, nil, chunk.IncrSelectVectorInPhyFormatFlat())
        ...
        //转换
        Scatter(
        ...    
        )
    }
    //payload
    //分配空间
    ls._payloadData.Build(payload.Card(), dataPtrs, nil, chunk.IncrSelectVectorInPhyFormatFlat())
    ...    
    //转换
    Scatter(
    ...
    )

}
```

#### 分配内存块

分配block和entry的首地址。

RowDataCollection：组织多个block。
- RowDataBlock 单个block

RowDataCollection的字段
- `_count` entry个数
- `_blockCapacity` 每个block中entry个数
- `_entrySize` entry字节数
- `_blocks` 分配的内存block

RowDataBlock的字段
- `_ptr` 内存地址
- `_capacity` block中entry个数
- `_entrySize` entry字节数
- `_count` entry个数
- `_byteOffset` 变成entry的写入位置

分配过程：
- 分配block
	- 如果当前block空间足够，分配entry空间
	- 如果当前block空间不够，分配新的block。并重复上一步。
	- 直到所有行的block都确定。
- 计算entry地址
	- 遍历分配的block。计算block上每个entry的首地址


函数关系：
- RowDataCollection.Build 入口。分配内存，拿到每个entry的地址
	- RowDataCollection.CreateBlock 分配全新block
	- RowDataCollection.AppendToBlock 在block上分配连续entry空间。

这几个函数实现不复杂，不再细说。

#### 转换排序键的定长部分 

处理过程，对order by表达式的值：
- 对每列vector
	- 对每行：函数RadixScatter
		- 定序编码： 函数EncodeData/EncodeStringDataPrefix

函数关系：
- RadixScatter 入口
	- TemplatedRadixScatter 转换定长字段
		- EncodeData 定长字段定序编码
	- RadixScatterStringVector 转换字符串
		- EncodeStringDataPrefix 字符串定序编码

```go
func RadixScatter(
	v vector,
	...
	keyLocs []pointer,
	...){
	switch v.Typ().GetInternalType() {
	case common.INT32:
		TemplatedRadixScatter[int32](
		...
		keyLocs,
		...
		int32Encoder{},
		)
	
	case common.VARCHAR:
		RadixScatterStringVector(
		...
		keyLocs,
		...
		)
	...
	}
}

```

**函数TemplatedRadixScatter**：

对定长字段的每行定序编码。

要解决的问题：
- NULL值编码和顺序。默认NULL值排在前。
- NULL值存储。1个byte，排在字段编码前。
- 字段编码。编码后的二进制串保留值的有序性。
- asc/desc。升序/降序

在字段编码前，加1个byte空间表示NULL.
如果NULL值要排在非NULL值前面，NULL值的null byte = 0，非NULL值的null byte = 1。

```text
...| null byte| encoded field |...
```

定序编码接口：
```go
type Encoder[T any] interface {
	EncodeData(unsafe.Pointer, *T)
	TypeSize() int
}
```

int32的编码实现：
例如：value1 = 0x12345678, value2 = 0x12345679. value1 < value2
BSWAP32(value1) => 0x78563412 => FlipSign =>0xF8563412
BSWAP32(value2) =>0x79563412 => FlipSign =>0xF9563412
在二进制上，value1 < value2 (0xF8563412 < 0xF9563412)

```go
func (i int32Encoder) EncodeData(ptr unsafe.Pointer, value *int32) {
	util.Store[uint32](BSWAP32(uint32(*value)), ptr)
	util.Store[uint8](FlipSign(util.Load[uint8](ptr)), ptr)
}

func BSWAP32(x uint32) uint32 {
	return 
	((x & 0xff000000) >> 24) | 
	((x & 0x00ff0000) >> 8) | 
	((x & 0x0000ff00) << 8) | 
	((x & 0x000000ff) << 24)
}

func FlipSign(b uint8) uint8 {
	return b ^ 128	
}
```

如果desc 为true，降序排列。将编码后的二进制串取反即可。
例如，上面的例子。
0xF8563412 =>取反=> 0x07A9CBED
0xF9563412 =>取反=> 0x06A9CBED
在二进制上，value1 > value2 (0x07A9CBED >0x06A9CBED)。恰好与数值顺序相反。

再看函数实现，
- 对每行
	- 确定null byte值。
	- 编码
	- 如果desc，编码值取反
```go
func TemplatedRadixScatter[T any](
	vdata *chunk.UnifiedFormat,
	...
	keyLocs []unsafe.Pointer,
	desc bool,
	hasNull bool,
	nullsFirst bool,
	...
	enc Encoder[T],
	) {
	//字段有null值
	if hasNull {
		mask := vdata.Mask
		valid := byte(0)
		if nullsFirst {
			valid = 1
		}
		invalid := 1 - valid
		//对每行
		for i := 0; i < addCount; i++ {
			idx := sel.GetIndex(i)
			srcIdx := vdata.Sel.GetIndex(idx) + offset
			if mask.RowIsValid(uint64(srcIdx)) {
				//not null
				//first byte
				util.Store[byte](valid, keyLocs[i])
				//编码
				enc.EncodeData(util.PointerAdd(keyLocs[i], 1), &srcSlice[srcIdx])
				//desc , invert bits
				if desc {
					for s := 1; s < enc.TypeSize()+1; s++ {
						util.InvertBits(keyLocs[i], s)
					}
				}
			
			} else {
				util.Store[byte](invalid, keyLocs[i])
				util.Memset(util.PointerAdd(keyLocs[i], 1), 0, enc.TypeSize())
			}
			
			keyLocs[i] = util.PointerAdd(keyLocs[i], 1+enc.TypeSize())
		}
		
	} 	
	...
}

```

**函数RadixScatterStringVector**：

对字符串的每行定序编码。

对NULL值和desc的处理与上面的相同。区别在字符串的定序编码。剩余的处理也相同，实现代码就再讲了

字符串的定序编码。
- 最多赋值prefixLen个字节的前缀。
- 不够prefixLen的部分填0

```go
func EncodeStringDataPrefix(
	dataPtr unsafe.Pointer,
	value *common.String,
	prefixLen int) {
	l := value.Length()
	util.PointerCopy(dataPtr, value.DataPtr(), min(l, prefixLen))
	if l < prefixLen {
		util.Memset(util.PointerAdd(dataPtr, l), 0, prefixLen-l)
	}
}
```

#### 转换排序键的非定长部分和payload

这两块是同一套逻辑。
每块都有两个RowDataCollection对象。一个放堆指针，固定size的行。一个放变长字段值。

两个RowDataCollection对象关系：
```text
对象1，固定size的行:

	|null bitmap|heap ptr offset|col 0|...|col i heap ptr|...|col n-1|
                    |						|
					|						|
|--------------------                       |
|                                           |
|                                  |--------|
|	对像2，变长字段值，堆空间：         |
|                                 \|/
|--->|var len field |...|var len field i|

```

过程：
- 分配变长字段的堆空间
    - 计算每行所有变长字段空间总size，即每行堆空间size
    - 分配堆空间。准备对象2
    - 记录每行堆空间首地址。将对象2每行堆空间首地址记录到对象1 heap ptr offset处。组成上图关系
- 列转行
    - 对每列vector
    	- 对每行：函数TemplatedScatter/ScatterStringVector
    		- 填充字段 

函数关系：
- Scatter 入口
    - TemplatedScatter 转换定长字段
    - ScatterStringVector 转换字符串

```go
func Scatter(
    ...
    ) {
    ...
    //compute the entry size of the variable size columns
    dataLocs := make([]unsafe.Pointer, util.DefaultVectorSize)
    if !layout.AllConstant() {
        entrySizes := make([]int, util.DefaultVectorSize)
        util.Fill(entrySizes, count, common.Int32Size)
        //计算变长字段 堆空间size
        for colNo := 0; colNo < len(types); colNo++ {
            if types[colNo].GetInternalType().IsConstant() {
                continue
            }
            ...
            ComputeStringEntrySizes(col, entrySizes, sel, count, 0)
        }
        
        //分配堆空间
        stringHeap.Build(count, dataLocs, entrySizes, chunk.IncrSelectVectorInPhyFormatFlat())
        
        //将堆空间指针存储到dataLocs中，关联对象2和对象1
        heapPointerOffset := layout.GetHeapOffset()
        for i := 0; i < count; i++ {
            rowIdx := sel.GetIndex(i)
            rowPtr := ptrs[rowIdx]
            util.Store[unsafe.Pointer](dataLocs[i], util.PointerAdd(rowPtr, heapPointerOffset))
            util.Store[uint32](uint32(entrySizes[i]), dataLocs[i])
            dataLocs[i] = util.PointerAdd(dataLocs[i], common.Int32Size)
        }
    }
    
      
    //转换每列
    for colNo := 0; colNo < len(types); colNo++ {
        col := colData[colNo]
        colOffset := offsets[colNo]
        switch types[colNo].GetInternalType() {
        
        case common.INT32:    
            TemplatedScatter[int32](
            ...
            )
        case common.VARCHAR:
            ScatterStringVector(
            ...
            )
        ...
        }
    }

}
```

**函数TemplatedScatter**:

对每行，从vector取字段值，填到对象1的行中。不会存到对象2中。
```go
  

func TemplatedScatter[T any](
...
) {
    data := chunk.GetSliceInPhyFormatUnifiedFormat[T](col)
    ptrs := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)
    ...
    //填每行
    for i := 0; i < count; i++ {
        idx := sel.GetIndex(i) 
        colIdx := col.Sel.GetIndex(idx)
        rowPtr := ptrs[idx]
        util.Store[T](data[colIdx], util.PointerAdd(rowPtr, colOffset))
    }
}
```

**函数ScatterStringVector**：

对每行，从vector取字段值，填到对象2的行中。再将其在对象2中的地址，存到对象1中。构成上面的两者关系图。

```go
func ScatterStringVector(
...
) {
    strSlice := chunk.GetSliceInPhyFormatUnifiedFormat[common.String](col) 
    ptrSlice := chunk.GetSliceInPhyFormatFlat[unsafe.Pointer](rows)

    nullStr := chunk.StringScatterOp{}.NullValue()    
    for i := 0; i < count; i++ {
        ...
        str := strSlice[colIdx]
        newStr := common.String{
            Len: str.Length(),
            Data: strLocs[i],
        }
        
        //copy varchar data from input chunk to
        //the location on the string heap
        util.PointerCopy(newStr.Data, str.DataPtr(), str.Length())
        
        //move strLocs[i] to the next position
        strLocs[i] = util.PointerAdd(strLocs[i], str.Length())

        //store new String obj to the row in the blob sort block        
        util.Store[common.String](newStr, util.PointerAdd(rowPtr, colOffset))
    }
}
```

### 排序

过程：
- 分配排序内存。排序内存是大块连续内存。不同于存储输入数据的内存。
    - 排序键的定长部分
    - 排序键的非定长部分
    - payload
- 排序
    - 多维排序方案
        - 第一维：按列分组
        - 第二维：按Tie分行
        - 第三维：排序算法
- 重排列
    - 重排列排序键的非定长部分
    - 重排列payload

#### 分配排序内存

为三部分数据，准备排序内存。不包括堆内存。
- 排序键的定长部分
- 排序键的非定长部分
- payload

三部分做法一致。函数ConcatenateBlocks
- 分配足够大的内存块。能装下全部的entry
- 从输入数据的block中将数据复制到排序内存

函数ConcatenateBlocks的实现清晰：分配内存，复制内存。不细讲。

#### 排序

排序方案的层次：
- 逻辑层：组织方式
    - 按列分组。竖切
        - 排序键的定长部分
    - 按Tie分行。横切
        - 排序键的非定长部分
- 物理层：数据搬运
    - 选择合适的排序算法
##### 逻辑层

函数SortInMemory完成逻辑层的工作。

函数关系：部分函数会在后面介绍
- SortInMemory
    - RadixSort 排序算法
    - SubSortTiedTuples 借助Tie，对排序键继续排序
    - ComputeTies 计算tie
    - AnyTies 任一个Tie为true，结果为true
    - SortTiedBlobs 借助Tie，对不定长字段继续排序。

```go
func (ls *LocalSort) SortInMemory() {

    ...
    dataPtr := lastBlock._ptr
    //locate to the addr of the row index
    idxPtr := util.PointerAdd(dataPtr, ls._sortLayout._comparisonSize)
    
    //给每行分配序号
    for i := 0; i < count; i++ {
        util.Store[uint32](uint32(i), idxPtr)
        idxPtr = util.PointerAdd(idxPtr, ls._sortLayout._entrySize)
    }

  

    //radix sort
    sortingSize := 0//组k的字节数
    colOffset := 0//组k的开始位置
    var ties []bool
    containsString := false
    for i := 0; i < ls._sortLayout._columnCount; i++ {
        //组k的字节数
        sortingSize += ls._sortLayout._columnSizes[i]
        containsString = containsString ||
        ls._sortLayout._logicalTypes[i].GetInternalType().IsVarchar()
        //确定组k
        if ls._sortLayout._constantSize[i] && i < ls._sortLayout._columnCount-1 {
            //util a var len column or the last column
            continue
        }

        if ties == nil {            
            //first sort
            RadixSort(
            ...
            )
            //初始化tie
            ties = make([]bool, count)
            util.Fill[bool](ties, count-1, true)
            ties[count-1] = false
        } else {
            //在组k上，借助Tie，继续排序
            //sort tied tuples        
            SubSortTiedTuples(
            ...
            )
        
        }
        

        containsString = false
        //所有列都排序完了，排序结束。不用再关系Tie了
        if ls._sortLayout._constantSize[i] &&
            i == ls._sortLayout._columnCount-1 {
            //all columns are sorted
            //no ties to break due to
            //last column is constant size
            break
        
        }
        
          
        //计算tie
        ComputeTies(
            ...
            ties,
            ...
            )
        //Tie值全为false
        if !AnyTies(ties, count) {
            //no ties, stop sorting
            break
        }
        
          
        //不定长字段
        if !ls._sortLayout._constantSize[i] {
            //在组k上，借助Tie，对不定长字段排序
            SortTiedBlobs(
            ...
            )
            
            if !AnyTies(ties, count) {
                //no ties, stop sorting
                break
            }
        }
        

        colOffset += sortingSize        
        sortingSize = 0
    
    }
}
```

##### 按列分组

分组形式：竖切
- 连续的多个定长字段+变长字段 
- 连续的多个定长字段 直到列结束
- 单个变长字段

```text
组形式 0: |fix len field 0|,...,|var len field i|
组形式 1: |var len field i+1|
组形式 2: |fix len field j|,...,|var len field N-1|
组形式 3: |fix len field j|,...,|fix len field N-1|
```

分组后，组内排序：
- 数据比较的大小。当前组的字段长度之和
- 没有任何tie时（第一次排序时）
    - 将当前`[组k]`作为内容，对组内所有行，执行排序算法。得出所有行的顺序。
- 在有tie时
    - 按行分Tie。见下节
##### 按Tie分行

引入Tie的原因，在某次排序结束时，全局顺序关系已经确定。
而相邻排序键entry，在当前组上（见上面）的顺序关系情况有：
- 行的排序键`entry[组k]` < i+1 行的排序键`entry[组k]`，即`sork_key_entry[i][组k] < sort_key_entry[i+1][组k]`。此时这两个entry，无需再进行后续排序。
- 行的排序键`entry[组k]` == i+1 行的排序键`entry[组k]`，即`sork_key_entry[i][组k] == sort_key_entry[i+1][组k]`。这个两个entry，还需要进行在后续组上排序。

Tie的定义：
- 与数据行数相等的布尔数组。`Tie bool[0,...,N-1]`
- 初始值：`Tie[0 ~ N-2] = True, Tie[N-1] = false`
- 更新：
    - 如果i行的排序键`entry[组k]` 与 i+1 行的排序键`entry[组k]`相等，则`Tie[i] = true`
    - 即：`Tie[i] = true if Tie[i] is True and sort_key_entry[i][组k] == sort_key_entry[i+1][组k], i in [0,...,N-2]`

结论1: Tie用来挑选出需要再继续排序的行。`Tie[i]`为true行都要继续排序。
结论2: 当`Tie[i]`全为false时，排序结束。
结论3: `Tie[N-1]`不会变为true。

函数ComputeTies按Tie的定义计算Tie值:
```go
func ComputeTies(   
    dataPtr unsafe.Pointer,
    count int,
    colOffset int,//组k开始位置
    tieSize int,//组k的字节数
    ties []bool,
    layout *SortLayout) {

    //组k的首地址
    dataPtr = util.PointerAdd(dataPtr, colOffset)    
    for i := 0; i < count-1; i++ {
        ties[i] = ties[i] &&
        util.PointerMemcmp(
            dataPtr,//i行
            util.PointerAdd(dataPtr, layout._entrySize),//i+1行
            tieSize,//组k的字节数
        ) == 0
        dataPtr = util.PointerAdd(dataPtr, layout._entrySize)
    }
}
```

按连续的`Tie[i]`为true的行再分组，组内进行排序。
如下逻辑，`Tie[i]~Tie[j]`为一组。函数SubSortTiedTuples和SortTiedBlobs 利用此逻辑。
```go
for i := 0; i < count; i++ {
    if !ties[i] {
        continue
    }

    var j int
    for j = i; j < count; j++ {
        if !ties[j] {
            break
        }
    }
    //对Tie[i]~Tie[j]组的行再排序。
    ...
}
```

##### 排序算法

多种排序算法组合使用，选择的依据：
- 包含字符串。用快速排序。
- 元素个数 <=24。用插入排序。
- `[组k]`的排序字节数 <= 4。用基数排序LSD
- 其它情况。用基数排序MSD
###### 快速排序

快排的执行框架：
- 划分为左右两部分。
    - 挑pivot
    - 将比pivot大的元素放在pivot右边
    - 将比pivot小的元素放在pivot左边
    - 将pivot放置到其正确位置。并且pivot的位置为全局有序位置。
- 对左半递归
- 对右半递归

框架层面的改进：
- 元素个数 < 24时，换插入排序。
    - 如果是最左边的分区，普通插入排序
    - 否则，用改进版的插入排序（少了与begin比较的判断）。
- pivot的挑选方法：
    - 元素个数 > 128。9中选1.
    - 其它，3中选1
- 递归剪枝
    - 如果是非最左边的分区，并且位置begin和begin-1的值相等。
        - 此时，将分区`[begin,end)`中与pivot相等的元素，放入此分区的左子分区。并且左子分区一定有序。
        - 仅对右子分区排序。
- 划分的改进
    - 先做第一轮与pivot的比较。如果已经划分，不再继续比较
    - 块交换。一次交换多个元素.
    - 与pivot相等的元素，放左边或右边
- 非平衡的处理
    - 如果左边的size < 总size/8 或 右边的size < 总size/8，定义此时为不平衡态
    - shuffle一些元素。没有采用堆排序。
    - 对第一轮比较已经划分的分区，尝试用插入排序。如果插入排序，一轮扫描的元素个数超过8个，停止用插入排序。
- 消除尾递归
    - 对左子分区，递归排序。
    - 对右子分区，begin = pivot + 1，进入下一轮迭代。

快排实现：
```go
func pdqsortLoop(
    begin, end *PDQIterator,
    constants *PDQConstants,
    ...
    leftMost bool,
    ...
    ) {
    for {
        size := pdqIterDiff(end, begin)
        if size < 24 {
            插入排序
            return
        }
        挑pivot；//3元素 或 9元素
        //递归剪枝:
        if !leftmost && begin-1的值 == begin的值{
            //划分.与pivot相等的元素，放在左子分区
            begin = partitionLeft(begin,end) + 1
            //左子分区已经排序。
            //仅需处理右子分区
            continue
        }
        //划分。与pivot相等的元素，放在右子分区
        pivotPos, alreadyPartitioned = partitionRightBranchless(begin, end, constants)
        //非平衡
        lSize := pdqIterDiff(&pivotPos, begin)
        x := pivotPos.plusCopy(1)
        rSize := pdqIterDiff(end, &x)
        if lSize < size/8 || rSize < size/8 {
            shuffle
        }else {
            //第一轮比较已经划分的分区，尝试用插入排序
            if alreadyPartitioned {
                if partialInsertionSort(begin, &pivotPos, constants) {  
                    x = pivotPos.plusCopy(1)
                    if partialInsertionSort(&x, end, constants) {
                        return
                    }
                }
                
            }
        }
        //对左子分区递归排序
        pdqsortLoop(begin, &pivotPos, constants, badAllowed, leftMost, branchLess)
        //对右子分区迭代排序
        x = pivotPos.plusCopy(1)
        begin = &x
        leftMost = false
    }
}
```

划分方法：

**函数partitionLeft**：与pivot相等的元素放置在左子分区。
**函数partitionRightBranchless**：与pivot相等的元素放置在右子分区。并且使用块交换。

###### 插入排序

**常规实现**：函数insertSort。容易理解，不细说

**ungarded实现**：函数unguardedInsertSort。
在常规实现中，判断 sift != begin，确保未越界。

但是，在确保begin-1存在且begin-1的值 < begin的值前提下，可以将判断 sift != begin去掉。并且不会越界。

**部分插入排序**：函数partialInsertionSort。
在常规实现的基础上，限制每轮移动的元素个数。如果一轮移动的元素个数超过8个，提前结束，排序失败。

###### 基数排序LSD
在排序字节数 <= 4时，用基数排序LSD。
用的是常规实现。结合了桶排序。无需递归。从字节序列尾部向头部（逆序），在每个字节位置上，先分桶，再重排。

###### 基数排序MSD
除了快排，插入排序，基数LSD的情况外，用基数MSD。

从字节序列头部到尾部（正序），在每个字节位置上，结合桶排序，先分桶，再重排。
假设，在第k-1个字节上，已经排序完成。说明在区间0～k-1上，桶之间的相对顺序是确定的。
那么，只需在第k-1个字节上，对同一个桶中的序列，再对第k个字节进行递归基数MSD排序即可。注意：一定是对同一个桶中的序列递归。不同桶之间，相对顺序一定已经确定。

并且在递归过程中，当元素个数 <=24时。改用插入排序。


##### 重排序
对排序键的非定长部分和payload。

过程：
- 为要重排的固定size部分，分配大内存块，存储重排后的entry。
- 遍历排序键的定长部分。
    - 取每个entry的原始index。
    - 确定源内存地址
    - 从源内存复制数据到大内存块
- 为变长部分，分配大内存块，存储重排后的堆内存
    - 依据已经重排的entry，确定堆内存地址。heapPointerOffset
    - 从源堆内存复制数据重排后的堆内存

### 读排序结果

遍历payload的数据块，取出每个entry上的对应列。

数据层次：
- PayloadScanner 入口
    - RowDataCollectionScanner 扫描数据集
        - ScanState 扫描位置：block idx，entry idx。

读取过程：
- 初始化。函数NewPayloadScanner
    - 确定payload的数据RowDataBlock。固定size部分和堆。
    - 初始化对象。
        - XXXScanner
        - ScanState。指向第一个block的，第一个entry
- scan过程：函数RowDataCollectionScanner.Scan
    - 计算要读的entry的首地址：entryIdx X rowWidth + block首地址
    - 读每个entry上的列。函数Gather.

函数关系：
- RowDataCollectionScanner.Scan 
    - Gather.

```go
func (scan *RowDataCollectionScanner) Scan(output *chunk.Chunk) {
    ...
    确定每个entry的首地址;
    ...
    //取出每列colIdx上的值
    for colIdx := 0; colIdx < scan._layout.CoumnCount(); colIdx++ {
        Gather(
            scan._addresses,
            ...
            output.Data[colIdx],
            ...
            count,
            scan._layout,
            colIdx,
            ...
        )
    }

    output.SetCard(count)
    scan._totalScanned += scanned
}

func Gather(
    rows *chunk.Vector,
    rowSel *chunk.SelectVector,
    col *chunk.Vector,
    colSel *chunk.SelectVector,
    count int,
    layout *RowLayout,
    colNo int,
    buildSize int,
    heapPtr unsafe.Pointer,
) {
    switch col.Typ().GetInternalType() {
    case common.INT32:
        TemplatedGatherLoop[int32](
        ...
        )
    ...
    case common.VARCHAR:
        GatherVarchar(
        ...
        )
    }
}

func TemplatedGatherLoop[T any](
    ...
    ) {
    ...
    for i := 0; i < count; i++ {
        rowIdx := rowSel.GetIndex(i)
        row := ptrs[rowIdx]
        colIdx := colSel.GetIndex(i)
        dataSlice[colIdx] = util.Load[T](util.PointerAdd(row, colOffset))
        rowMask := util.Bitmap{
        ...
        
        if !util.RowIsValidInEntry(
            rowMask.GetEntry(entryIdx), 
            idxInEntry) {
            ...
            colMask.SetInvalid(uint64(colIdx))
        }
    }
}
```

**函数Gather**：入口
- 函数TemplatedGatherLoop
    - 取出每个entry上的指定列
- 函数GatherVarchar。类似

## join

支持的join类型：cross, inner, mark, anti mark, semi, anti semi, left

### 非cross join

对于非cross join，目前支持基于hash表的实现方式。左子节点的数据用于probe。
非cross join的执行过程：
- 输入数据
    - 列转行。右子节点的数据存为行存结构。并计算hash值。
    - build hash表。数据输入完成后，再在之上构建hash表（拉链法解决冲突）。不是边输入数据，边构建hash表。
- 执行join
    - probe阶段
        - 读左子节点的数据。计算hash值。从hash表中确定hash值相等的桶。
    - 构建join结果
        - 从桶中确定与probe数据相等的记录
        - 依据join类型，组合join的结果。

#### 初始化

非cross join的层次管理结构：
- 顶层：HashJoin。
- 中层：
    - JoinHashTable。列转行。构建hash表。
    - Scan。执行join
- 物理层：行存结构与agg节点的做法一致。
    - TupleDataCollection。行存存储右子节点数据。
    - TupleDataLayout。行存结构。

按其执行阶段分：
- build阶段：
    - JoinHashTable
    - TupleDataCollection
- probe阶段：
    - Scan

hash表的key：join on条件中右子表达式的值。
- 约束：
    - 在plan阶段，确保join on右子表达式的数据都来自join的右子节点。在允许交换join左右子节点的前提下，也会保持这种对应关系。
    - NULL不进入hash表
- join on条件可能不止一个。filter下推阶段，可能会将多个filter条件推到join节点，作为join on条件，并且满足上面的约束。这些join on条件中右子表达式的值都要作为hash表的key。最终的hash值是这些右子表达式值的复合结果。

与hash表的key对应，probe的key：为join on条件中左子表达式的值


行存的内容：用TupleDataLayout。
- hash key。probe阶段，需要用join on条件中左子表达式的值来查hash表。并且判断与hash key的值是否相等。
- payload。join右子节点的数据。probe阶段，依据join类型，组合不同的结果。
- hash值。

#### 输入数据

build阶段的两个阶段：
- 读数据
- 构建hash表。不是边读边构建

读数据：
- 从join右子节点读一批数据（payload数据）
    - 过滤NULL值
    - 计算hash key。
    - 计算hash key的hash值
    - hash key+payload+hash值存入TupleDataCollection。与agg节点中的做法一致，不细讲
    - 直到join右子节点无数据

此时数据读完，数据在TupleDataCollection中不再移动。只需将数据的地址依据hash值存入hash表。

hash表：
- 确定hash的size。比`2*size`大的最小的2的n次方值。
- hash表的结构简化为一个地址数组。每个元素是一个链表首节点的地址。
- 拉链法解决冲突。hash值相同时，用头插入法插入新值。并且下一个值的地址放置在行格式中，原先放置hash值的位置。因为hash值不再需要了。
- 构建过程：
    - 读出TupleDataCollection的所有数据
    - 按每个行的hash值，插入对应的桶，并解决冲突


#### 执行

prob阶段的过程
- 从join左子节点读一批数据
- probe: 探测hash表。函数JoinHashTable.Probe
    - 计算probe key。join on条件中左子表达式的值
    - 计算probe key的hash值。函数JoinHashTable.hash
    - 由hash值确定桶，取桶连表的首节点地址。注意：hash值相等，不代表链表中的每个节点值都与probe key相等。函数JoinHashTable.ApplyBitmask2
- scan：依据join类型组合数据。函数Scan.Next，入口函数
    - inner。函数Scan.NextInnerJoin
    - mark, anti mark。函数Scan.NextMarkJoin
    - semi。函数Scan.NextSemiJoin
    - anti semi。函数Scan.NextAntiJoin
    - left。函数Scan.NextLeftJoin

probe函数关系：
- JoinHashTable.Probe
    - JoinHashTable.hash。复合hash值
    - JoinHashTable.ApplyBitmask2。取桶连表的首节点地址。

```go
func (jht *JoinHashTable) Probe(keys *chunk.Chunk) *Scan {
    ...
    hashes := chunk.NewFlatVector(common.HashType(), util.DefaultVectorSize)
    jht.hash(keys, curSel, newScan._count, hashes)
    jht.ApplyBitmask2(hashes, curSel, newScan._count, newScan._pointers)
    ...
    return newScan
}
```


scan的函数关系：
- Scan.Next
    - Scan.NextInnerJoin。执行inner join
    - Scan.NextMarkJoin。执行mark，anti mark join
    - Scan.NextSemiJoin。执行semi join
    - Scan.NextAntiJoin。执行anti semi join
    - Scan.NextLeftJoin。执行left join

##### inner join

整体结构：
- 确定与probe key相等的行。函数Scan.InnerJoin
    - 每个probe key会对应一个桶链表。
    - 遍历桶链表，寻找相等的节点。hash值相等，probe key与hash key不见得相等。
        - 对每行probe key。函数Scan.resolvePredicates
            - 其每列与链表首节点的每列比较。
            - 如果每列都相等，则此行probe key匹配了一行
            - 直到所有probe key与对应的首节点比较结束
        - 如果有匹配行，进入拼接结果阶段
        - 如果没有匹配行，说明此时链表首节点都不匹配。
            - 所有链表首节点移动到下一个节点。
            - 重复上面的过程，直到链表都结束。
    - 直到找到hash key相等的节点，或者没有相等的节点。
- 拼接inner join的结果。
    - join左子节点的数据。
    - join右子节点的数据。数据位置在TupleDataCollection中。

函数关系：
- Scan.NextInnerJoin
    - Scan.InnerJoin
        - Scan.resolvePredicates

**函数Scan.NextInnerJoin**:
函数gatherResult2从TupleDataCollection中取数据。与agg算子中的取法一致。
```go

func (scan *Scan) NextInnerJoin(keys, left, result *chunk.Chunk) {
    ...
    resCnt := scan.InnerJoin(keys, resVec)
    //有匹配的行
    if resCnt > 0 {
        //left part result
        result.Slice(left, resVec, resCnt, 0)
        //right part result
        //拼接join右子节点的数据
        for i := 0; i < len(scan._ht._buildTypes); i++ {
            vec := result.Data[left.ColumnCount()+i]
            scan.gatherResult2(
            vec,
            resVec,
            resCnt,
            i+len(scan._ht._keyTypes))
        }
        //链表移动到下一个节点
        scan.advancePointers2()
    
    }

}

```

**函数Scan.InnerJoin:**
```go
func (scan *Scan) InnerJoin(keys *chunk.Chunk, resVec *chunk.SelectVector) int {
    for {
        //匹配probe key与首节点
        resCnt := scan.resolvePredicates(
        keys,
        resVec,
        nil,
        )
        //有匹配的行
        if resCnt > 0 {
            return resCnt
        }
        //无匹配的行，所有链表移动到下一个节点
        scan.advancePointers2()        
        //所有链表的都结束。停止
        if scan._count == 0 {
            return 0
        }
    }

}

```

**函数Scan.resolvePredicates:**
实质是Match函数。在agg算子时讲过。
```go
func (scan *Scan) resolvePredicates(    
    keys *chunk.Chunk,
    matchSel *chunk.SelectVector,
    noMatchSel *chunk.SelectVector,
    ) int {
    ...
    noMatchCount := 0
    return Match(
        ...
        matchSel,
        scan._count,
        noMatchSel,
        &noMatchCount,
    )
}
```

##### mark, anti mark join
EXISTS，NOT EXISTS通常转为mark，anti mark。
结果会多出mark列，布尔值，其表示某行匹配与否。join节点的父节点采用mark列值为true的行就是mark join。采用false的行就是anti mark join。

过程：
- 确定所有行的匹配情况。函数Scan.ScanKeyMatches
    - 遍历桶链表，寻找相等的节点。hash值相等，probe key与hash key不见得相等。
        - 对每行probe key，计算匹配的行。见函数Scan.resolvePredicates
        - 如果有匹配行。`_foundMatch`。记录匹配的行。
        - 没有匹配行的链表首节点移动到下一个节点。
        - 如此直到所有链表结束
- 构建结果
    - join左子节点的数据。
    - mark列。`_foundMatch`值为true的行为true。

**函数Scan.ScanKeyMatches**
```go
func (scan *Scan) ScanKeyMatches(keys *chunk.Chunk) {
    matchSel := chunk.NewSelectVector(util.DefaultVectorSize)
    noMatchSel := chunk.NewSelectVector(util.DefaultVectorSize)
    for scan._count > 0 {
        //匹配probe key与链表首节点
        matchCount := scan.resolvePredicates(keys, matchSel, noMatchSel)
        noMatchCount := scan._count - matchCount
        //记录匹配的行
        for i := 0; i < matchCount; i++ {    
            scan._foundMatch[matchSel.GetIndex(i)] = true
        }
        //不匹配的行，继续比较。链表移动到下一个节点
        scan.advancePointers(noMatchSel, noMatchCount)
    }
}
```

##### semi, anti semi join
IN，NOT IN通常转为semi, anti semi。
结果只来自join左子节点的数据。semi保留匹配的行。anti semi保留不匹配的行。

过程：
- 确定所有行的匹配情况。函数Scan.ScanKeyMatches，见上面
- 构建结果
    - join左子节点的数据。semi需要匹配的行。anti semi保留不匹配的行。
```go
func (scan *Scan) NextSemiOrAntiJoin(keys, left, result *chunk.Chunk, Match bool) {
    sel := chunk.NewSelectVector(util.DefaultVectorSize)
    resultCount := 0
    for i := 0; i < keys.Card(); i++ {
        //Match为true。semi join
        //Match为false。anti semi join
        if scan._foundMatch[i] == Match {
            sel.SetIndex(resultCount, i)    
            resultCount++
        }
    }

    if resultCount > 0 {
        result.Slice(left, sel, resultCount, 0)
    }
}
```

##### left join
结果中，join左子节点的数据完全保留。与inner join的区别，不匹配的行，右边用NULL填充。

过程：
- 先当inner join执行一遍。函数Scan.NextInnerJoin，见上面。
- 构建结果
    - 匹配的行。inner join的逻辑
        - join左子节点的数据。
        - join右子节点的数据。
    - 不匹配行。left join加的逻辑
        - join左子节点的数据。
        - 右边补充NULL

```go

func (scan *Scan) NextLeftJoin(keys, left, result *chunk.Chunk) {
    //执行inner join
    scan.NextInnerJoin(keys, left, result)
    if result.Card() == 0 {
        //此时检测，还有多少行未匹配
        remainingCount := 0
        sel := chunk.NewSelectVector(util.DefaultVectorSize)
        for i := 0; i < left.Card(); i++ {
            if !scan._foundMatch[i] {
                sel.SetIndex(remainingCount, i)
                remainingCount++
            }
        }
        //有未匹配的行
        if remainingCount > 0 {
            //join左子节点的数据
            result.Slice(left, sel, remainingCount, 0)
            //右边补充NULL
            for i := left.ColumnCount(); i < result.ColumnCount(); i++ {
                vec := result.Data[i]
                vec.SetPhyFormat(chunk.PF_CONST)
                chunk.SetNullInPhyFormatConst(vec, true)
            }
        }
        
        scan._finished = true   
    }
}
```
### cross join

cross join的执行过程：
- 输入数据
    - 输入右子节点的数据，保持列存。
- 执行join
    - 读左子节点的数据。
    - 遍历右子节点的数据块。拼接结果。

#### 初始化

cross join的层次管理结构：
- 顶层：CrossProduct。cross join管理结构
    - ColumnDataCollection
    - CrossProductExec
- 中层：
    - CrossProductExec。 cross join执行器
- 物理层：
    - ColumnDataCollection。 存储列数据块

按其阶段划分：
- build阶段：
    - ColumnDataCollection
- probe阶段：
    - CrossProductExec
    - ColumnDataCollection


#### 输入数据

列式存储结构：ColumnDataCollection
- 存储格式
    - 列存。
    - Chunk数组。每个Chunk都是满的，除了最后一个可能装不满。
    - 每输入一份数据，尽可能填满最后一个Chunk。
- 接口
    - Append。追加新数据到集合中。
        - 尽可能填满最后一个Chunk。空间不够时，会分配新的Chunk。
    - Scan。从指定chunk的指定行，读取一个Chunk。
        - 每读完一个Chunk，切换到下一个Chunk。

#### 执行

过程：函数CrossProductExec.Execute
- 对每一块左子节点的数据（简称左边数据块）
    - 确定右子节点的Chunk（简称右边数据块）。`
        - 第一次时，从ColumnDataCollection读取第一个Chunk。
    - 对左边数据块的每一行。函数CrossProductExec.NextValue
        - 与右边数据块组合成一个输出块。
            - 左边部分是这一行的重复
            - 右边部分是右边数据块
        - 如此直到左边数据块的行用完。
            - 左边数据块回到第一行。
            - 从ColumnDataCollection读取下一个Chunk。
            - 如果还有Chunk，重复上面的逻辑
            - 如果没有Chunk，从左子节点读取下一个块
    - 直到左子节点的数据读完

```go
func (cross *CrossProductExec) Execute(input, output *chunk.Chunk) (OperatorResult, error) {
    ...
    //切换左边数据块的下一行。
    //如果左边数据块的行用完，从ColumnDataCollection读取下一个Chunk。
    if !cross.NextValue(input, output) {
        //右边数据块读完。
        //从左子节点读取下一个块
        cross._init = false
        //RHS is read over.
        //switch to the next Chunk on the LHS and reset the RHS
        return NeedMoreInput, nil
    
    }
    
    var constChunk *chunk.Chunk
    //scanning chunk. refer a single value
    var scanChunk *chunk.Chunk
    for i := 0; i < output.ColumnCount(); i++ {
        ...
        constChunk = cross._scanChunk
        ...
        scanChunk = input
        ... 
        if tblIdx == -2 {
            //拼接右边数据块
            output.Data[i].Reference(constChunk.Data[colIdx])
        } else if tblIdx == -1 {
            //拼接左边数据块的某一行。此行重复多次。
            chunk.ReferenceInPhyFormatConst(
                output.Data[i],
                scanChunk.Data[colIdx],
                cross._positionInChunk,
                scanChunk.Card(),
            )
        }
    }
    output.SetCard(constChunk.Card())
    return haveMoreOutput, nil
}
```

## 其它算子

scan、project、filter比前面介绍的算子简单，不再展开。topN算子暂时不支持。
