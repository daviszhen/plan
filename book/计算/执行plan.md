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


## join

## scan

## project

## filter
