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



## order

## join

## scan

## project

## filter

## limit

# 小结