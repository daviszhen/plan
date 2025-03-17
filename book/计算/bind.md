# 简介
整体介绍select的bind过程。bind实质是对语法树做语义分析的过程。检测语义上是否有效合法。
聚焦在tpch 22个query的语义分析过程。尽管在框架上做到了通用，但是目前还不能支持任意类型的query。

bind依次分析每个语法单元。过程有出错，整个分析提前结束。

bind涉及的组件：
- Builder。语义分析的driver。每个select对应一个Builder。驱动整个分析过程。也记录了部分分析结果。
- Binding。表示关系（表、子查询）的定义。关系的属性、名称。
- BindContext。对应一个作用域，记录这个作用域中有多少关系。BindContext之间有层次结构。
- Expr。结果表达式。除了关系外都转成表达式。

bind的结果最终存入Builder和BindContext。结果包含:Binding，各种结果表达式Expr
# bind过程
按语法单元逐个介绍分析过程。

重要的部分：
- 解析表定义。获取表的属性定义。
- 解析表达式。分析表达式是否合法。相当多语法单元的分析都可归结为分析表达式。

整体分析过程：
- 预处理Cte。
- 分析from。解析表定义、join类型、join条件等。
- 展开`*`表达式
- 预处理select list
- 分析where
- 分析group by
- 分析having
- 分析select list
- 分析order by
- 分析limit

分析where，group by，having，select list，order by都属于表达式分析的范围。区别在于它们能访问到的语义是不一样的。会优先讲表达式解析的整体过程，再介绍不同语法单元的差异点。


```go
func(Builder)buildSelect(select,ctx,depth)error{
	//cte
	buildWith(select.WithClause,ctx,depth);

	//from 
	b.fromExpr, err = b.buildTables(select.FromClause, ctx, depth)

	//展开 * 表达式
	for _, expr := range targetList {  
	    ret, err := b.expandStar(expr.GetResTarget())
    }

	//预处理select list
	for i, expr := range newSelectExprs {
		...
	}

	//分析where
	b.whereExpr, err = b.bindExpr(ctx,  IWC_WHERE,  select.WhereClause,  depth)

	//分析groupby
	for _, expr := range select.GroupClause {  
	    retExpr, err = b.bindExpr(ctx, IWC_GROUP, expr, depth)  
	}

	//分析having
	retExpr, err = b.bindExpr(ctx, IWC_HAVING, select.HavingClause, depth)

	//分析select list
	for i, expr := range newSelectExprs {  
	    retExpr, err = b.bindExpr(ctx, IWC_SELECT, expr.GetVal(), depth)
    }

	//分析order by
	for _, expr := range select.SortClause {  
	    retExpr, err = b.bindExpr(ctx, IWC_ORDER, expr, depth)
    }

	//分析limit
	b.limitCount, err = b.bindExpr(ctx, IWC_LIMIT, select.LimitCount, depth)

}
```

## 预处理cte
将cte的定义记录到BindContext中。在解析表时，如果判断表是个cte，再解析cte。

## 解析表定义
from子句的语法成分：
- 单个表
- 子查询
- 表join
- 上面的语法单元混用。情况比较复杂

from子句bind后的结果是一个表达式。

整体是个递归过程：分治向下到单个表。
- 单个表，直接解析。
- 多个表。拆分成前面N-1个，递归处理。最后1个，直接解析。最后merge两部分内容。
```go
func(Builder)buildTables(
	tables []*pg_query.Node, 
	ctx *BindContext, 
	depth int){
	if tables 空 {
		报错;
	}else if tables 单个表{
		buildTable(tables[0], ctx, depth);
	}else{
		nodeCnt := len(tables)  
		
		leftCtx := NewBindContext(ctx)  
		//left  
		left, err := b.buildTables(tables[:nodeCnt-1], leftCtx, depth)

		rightCtx := NewBindContext(ctx)  
		//right  
		right, err := b.buildTable(tables[nodeCnt-1], rightCtx, depth)
		
		//合并BindContext。生成叉乘表达式
		b.mergeTwoTable(  
		    leftCtx,  
		    rightCtx,  
		    left,  
		    right,  
		    ET_JoinTypeCross,  
		    ctx,  
		    depth)
	}
}
```

解析单个表。在语法上的单个节点，实质上也可能是多个表：
- 单个表。真实的单个表。或者cte
- 子查询
- 两表join

解析过程也是递归的。
- 单个表。
	- 如果表是cte，将cte改写为子查询递归解析。
	- 将表定义从catalog中拿出，生成Binding记录在BindContext中表示对表一次引用。
	- 如果没有报错。
- 两表join
	- 解析左表，右表。
	- 有on子句，也要解析。
	- 再merge 两表的解析结果。
- 子查询
	- 创建新的Builder和BindContext。递归构建子查询。
	- 构建结果也会形成Binding，记录在BindContext。
	
```go
func(Builder)buildTable(
	table *pg_query.Node, 
	ctx *BindContext, 
	depth int,
){
	swith table{
	case 单表:
		cte := b.findCte(tableName,ctx)
		if 是cte {
			cte 改写为 子查询;
			buildTable(子查询，ctx,depth);
		}else{
			catalog取表定义，没有报错;
			创建Binding,记录到BindContext，表示对表的引用；
			形成表引用表达式；
		}
	case 两表join:
		left = buildTable(左表,leftCtx);
		right = buildTable(右表,rightCtx);
		if on 条件{
			解析表达式;
		}
		形成join表达式;
	case 子查询：
		创建子查询需要的Builder和BindContext;
		subBuilder.buildSelect(subquery,subContext);//递归过程
		子查询也是关系;
		创建Binding,记录到BindContext，表示对子查询的引用；
		形成子查询引用表达式；
	}
}
```

merge两个BindContext，实质是将两个作用域的内容合并到外层作用域的过程。将Binding都合并到外层BindContext，并形成join表达式。
```go
func(Builder)mergeTwoTable(
	leftCtx, rightCtx *BindContext,  
	left, right *Expr,
	ctx *BindContext,
){
	leftCtx.bindings => ctx;
	rightCtx.bindings => ctx;
	形成join表达式；
}
```
## 解析表达式
表达式解析是bind的最复杂的过程。有多种原因：
- 种类多，每种的语法结构不同。
- 可以递归定义。意味着要递归处理。
- 非常灵活性。表达式可以简单也可以很复杂。
- 又有限制性。某些子句中是不能用某些表达式的。
- 类型转换。只有符合条件的类型间才能计算。表达式之间不一定能直接计算。在计算前要先转化类型。
- 函数选择。
	- 运算符也用函数实现。不同的数据类型需要不同的函数实现。
	- 并且在解析过程中确定合适函数实现。需要考虑类型和优先级。 

表达式解析的输入：表达式语法树
表达式解析的输出：Expr，表示解析完成的逻辑表达式。

表达式解析也是递归过程。依据表达式的不同类型递归拆解。
先介绍表达式分析的整体过程。再介绍每种表达式的解析方式。最后再介绍函数注册相关的内容。

表达式种类：
- 列引用。对表字段的引用。
- 常数。字符串、浮点数、整数值。
- 子查询。select list，where中是可以放子查询的。IN，ANY，EXISTS,NOT IN,NOT EXISTS。
- 函数。聚合函数和非聚合函数。
- 布尔。and，or，not。
- 二元运算符。
	- 比较。=，<>,>,>=,<,<=
	- 数值。+，-，*，/
	- 相似。LIke，NOT LIKE
	- 范围。IN,NOT IN,BETWEEN
- cast。
- 表达式list（元组）。
- Case...When。

整个函数表达式解析的结构是大switch再分类处理，再递归。

在`bindExpr`递归处理之后，加`decideResultType`和`AddCastToType`是标准过程。其目的是对齐数据类型。
其原理后续会单独讲：
- `decideResultType`。确定结果类型
- `AddCastToType`。cast过程

```go
func(Builder)bindExpr(
	ctx *BindContext,  
	iwc InWhichClause,  
	expr *pg_query.Node, depth int
){
	switch {
	case 列引用：
		从ctx中找出列名所在的Binding（即表引用）。
		生成列引用表达式;
	case 布尔：
		bindBoolExpr();
	case 子查询:
		bindSubquery();
	case 二元运算符：
		bindAExpr();
	case 常数：
		提取字符串，整数和浮点数 字面值。
	case 函数：
		bindFuncCall();
	case order by://实质是子表达式+asc/desc
		child, err := b.bindExpr(ctx, iwc, expr.Node, depth)
	case cast://cast (expr type)
		//子表达式
		retExpr, err = b.bindExpr(ctx, iwc, expr.Arg, depth)
		//cast到目标类型
		retExpr, err = AddCastToType(retExpr, resultTyp,)
	case 表达式list://元组
		//循环解析每个表达式
		for _, item := range expr.Items {  
		    itemExpr, err := b.bindExpr(ctx, iwc, item, depth)  
		}
	case "case ... when":
		bindCaseWhen();
	}
}
```

### 列引用的解析
在tpch的查询里面，列引用是相对简单。实际列引用既灵活又限制。
- where 的列引用通常只能引用关系的属性。这里的关系范围：实体表，cte，子查询。但是where中列引用通常不能引用select list中的别名。因为where通常先执行，select list后执行。
- groupby和having中的列引用。通常不能引用select list中的别名。因为aggr通常先执行，select list后执行。
- order by中的列引用。通常能引用select list中的别名。因为select list可以先执行，然后对其结果排序。

### 布尔表达式的解析
相对比较容易理解。特别的是pg语法树，布尔表达式可能不止两个子表达式。
先解析第一个表达式，再依次遍历第2，3，...个表达式。并不断的与第一个表达式对齐数据类型。最终的表达式形式：
```text
and : e1,e2,e3
=>
(and (and e1 e2) e3)

```
```go
func(Builder)bindBoolExpr(){
	确定布尔运算符类型;
	//注意：
	left = bindExpr(子表达式);
	//从子表达式1构建
	for i = 1 ... N-1 {
		cur, err = b.bindExpr(ctx, iwc, 子表达式i, depth)；
		//确定结果类型
		resultTyp = decideResultType(left.DataTyp, cur.DataTyp)  
		//cast到结果类型
		left, err = AddCastToType(left, resultTyp)  
		cur, err = AddCastToType(cur, resultTyp)  

		// 确定布尔表达式
		left, err = b.bindFunc(  
			et.String(),  
			et,  
			expr.String(),  
			[]*Expr{left, cur},  
			[]common.LType{left.DataTyp, cur.DataTyp},  
			false)
	}
	left是最终表达式。	
}
```

### 子查询表达式的解析
子查询表达式类型
- `a IN subquery`
- `a  = ANY subquery`。可以改写为第一种。
- scalar。类似：select subquery as a
- `exists subquery` ,`not exists subquery`
第1，2种是需要配对的表达式`testexpr`。结果是布尔类型。
第3种就是单独的值。与值的类型相同。
第4种是布尔类型。

解析过程：
- 如果有`testexpr`，解析`testexpr`。
- 递归解析子查询。
```go
func(Builder)bindSubquery(){
	if testexpr{
		testExpr, err = b.bindExpr(ctx, iwc, expr.Testexpr, depth)
	}
	err = subBuilder.buildSelect(expr.Subselect.GetSelectStmt(), subBuilder.rootCtx, 0)
	根据子查询类型，构建子查询表达式。
}

```

### 二元运算符的解析
语法形式:`e1 x e2`。通用解析方式是先做`e1`，再做`e2`。
`IN`和`BETWEEN`单独处理，会改写形式。

```go
func(Builder)bindAExpr(){
	switch
	{
	case IN : bindInExpr()
	case BETWEEN : bindBetweenExpr
	}

	//其它通用做法
	left, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)  
 
	right, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)  
		resultTyp = decideResultType(left.DataTyp, right.DataTyp)  
	  
	//cast  
	left, err = AddCastToType(left, resultTyp, )     
	right, err = AddCastToType(right, resultTyp,)  
	  
	bindFunc, err := b.bindFunc(
		et.String(),
		et,
		expr.String(),
		[]*Expr{left, right}, 
		[]common.LType{left.DataTyp, right.DataTyp}, false)  
	构建二元表达式。
}

```

将`IN`表达式打散。不是语法上的改写，而是结果的表达式的改写。`a IN (e1,e2,e3)`改写为`a IN e1 or a IN e2 or a IN e3`。
```go
func(Builder)bindInExpr(){
	//a
	in, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)  
	//元组(e1,e2,e3)
	listExpr, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)  
	//对齐数据类型。
	maxType := in.DataTyp  
	for i := 0; i < len(argsTypes); i++ {  
	    maxType = common.MaxLType(maxType, argsTypes[i])  
	}

	castIn, err := AddCastToType(in, maxType, false)    
	params = append(params, castIn)  
	paramTypes = append(paramTypes, castIn.DataTyp)  
	for _, child := range children {  
	    castChild, err := AddCastToType(child, maxType, false)  
	    params = append(params, castChild)  
	    paramTypes = append(paramTypes, castChild.DataTyp)  
	}

	//改写为a IN e1 or a IN e2 or a IN e3
	orChildren := make([]*Expr, 0)  
	for i, param := range params {  
	    if i == 0 {  
	       continue  
	    }  
	    equalParams := []*Expr{params[0], param}  
	    equalTypes := []common.LType{paramTypes[0], paramTypes[i]}  
	    ret0, err := b.bindFunc(et.String(), et, expr.String(), equalParams, equalTypes, false)  
	    orChildren = append(orChildren, ret0)  
	}  
	//结果的IN表达式。
	bigOrExpr := combineExprsByOr(orChildren...)
}
```

`BETWEEN`其实时三元表达式。`a BETWEEN b AND c`结果表达式在逻辑上改写为`a >= b AND a <= c`
```go
func(Builder)bindBetweenExpr(){
	//a
	betExpr, err = b.bindExpr(ctx, iwc, expr.Lexpr, depth)  
	//b,c
	listExppr, err = b.bindExpr(ctx, iwc, expr.Rexpr, depth)  
	//对齐数据类型
	resultTyp = decideResultType(betExpr.DataTyp, left.DataTyp)  
	
	resultTyp = decideResultType(resultTyp, right.DataTyp)  
	//cast  
	betExpr, err = AddCastToType(betExpr, resultTyp, false)    
	left, err = AddCastToType(left, resultTyp, false)    
	right, err = AddCastToType(right, resultTyp, false)  
	
	//改写为：a >= b AND a <= c
	//>=  
	params := []*Expr{betExpr, left}  
	paramsTypes := []common.LType{betExpr.DataTyp, left.DataTyp}  
	ret0, err := b.bindFunc(ET_GreaterEqual.String(), ET_GreaterEqual, expr.String(), params, paramsTypes, false)  
	  
	//<=  
	params = []*Expr{betExpr, right}  
	paramsTypes = []common.LType{betExpr.DataTyp, right.DataTyp}  
	ret1, err := b.bindFunc(ET_LessEqual.String(), ET_LessEqual, expr.String(), params, paramsTypes, false)  
	  
	// >= && <=  
	params = []*Expr{ret0, ret1}  
	paramsTypes = []common.LType{ret0.DataTyp, ret1.DataTyp}  
	  
	ret, err := b.bindFunc(ET_And.String(), ET_And, expr.String(), params, paramsTypes, false)  
}
```

### 函数表达式的解析
完整的函数表达式的处理其实是的比较复杂。
- 获取函数定义。由函数签名确定函数定义。在这里讲到。
- 函数注册和查询。在后面细讲，这里先跳过。

函数的使用也有些限制：
- where中通常不能用聚合函数。
- having中通常不能用非聚合函数。
- 有group by时，投影列中非分组列通常用在聚合函数中
- 聚合函数通常不能嵌套。

解析步骤：
- 解析参数表达式
- 获取函数定义。

```go
func (Builder) bindFuncCall(){
	//参数表达式
	for _, arg := range expr.Args {  
	    child, err = b.bindExpr(ctx, iwc, arg, depth)  
	    args = append(args, child)  
	    argsTypes = append(argsTypes, child.DataTyp)  
	}
	//确定函数定义
	ret, err = b.bindFunc(  
	    name,  
	    ET_SubFunc,  
	    expr.String(),  
	    args,  
	    argsTypes,  
	    expr.AggDistinct)
}
```

### Case...When的解析
在逻辑上是switch语义。

语法结构:
```sql
case 
when a1 then b1
when a2 then b2
[else] c
```

解析过程：
- 分析所有的when ... then
- 分析else

```go
func (Builder)bindCaseWhen(){
	//解析case ... then
	for i := 0; i < len(astWhen); i++ {  
	    temp, err = b.bindExpr(ctx, iwc, astWhen[i], depth)  
	    when[i*2] = temp.Children[0]  //case
	    when[i*2+1] = temp.Children[1]  //then
	}  
	  
	//解析else
	els, err = b.bindExpr(ctx, iwc, expr.Defresult, depth)  

	retTyp := els.DataTyp  
	//decide result types  
	//max type of the THEN expr  
	for i := 0; i < len(when); i += 2 {  
	    retTyp = common.MaxLType(retTyp, when[i+1].DataTyp)  
	}  
	  
	//case THEN to  
	for i := 0; i < len(when); i += 2 {  
	    when[i+1], err = AddCastToType(when[i+1], retTyp,)  
	}  
	  
	//cast ELSE to  
	els, err = AddCastToType(els, retTyp,)  
	if err != nil {  
	    return nil, err  
	}
	//构建case表达式
	ret, err := b.bindFunc(ET_Case.String(), ET_Case, expr.String(), params, paramsTypes, false)
}

```



## 数据类型

数据类型相关的几个方面：
- 数据类型的表达
- 类型转化规则。显式的，隐式的。

数据类型的表示方式：
- 逻辑类型。简单理解是sql层面的类型。
- 物理类型。物理存储方式。多个逻辑类型可能会实质对应同一个物理类型。

逻辑类型的组成部分：
- 类型id。一种编号。隐藏了一种优先级关系。
- 物理类型ID。一种编号。
- 数值宽度。因数据类型不同意义也不同。
- scale 或精度。

```go
type LType struct {  
    Id    LTypeId  
    PTyp  PhyType  
    Width int  
    Scale int  
}
```

目前只关注tpch 用到的数据类型：INT，BIGINT，VARCHAR，DECIMAL，DATE，INTERVAL。

### 决定结果类型
在不考虑运算符的情况下，由两个输入类型确定结果的类型。

决策过程由一系列的判断条件构成。大的规则：
- 两个都是数值类型，单独处理。
- 两个都是非数值类型。
	- id不同时，id大的是结果类型。date，interval单独处理。
		- 一个是date，一个是interval，结果是date
	- id相同时，第一个类型是结果类型。enum，varchar，decimal再单独处理。decimal有数值宽度和精度的问题。


两个数值类型`t1`,`t2`，判断结果类型：
- `t1`能隐式转到`t2`。结果类型时`t2`。隐式规则是预先定义好的数值类型相互转换规则。这里不细说。
- `t2`能隐式转到`t1`。结果类型时`t1`。
- 两个同类型，符号不同的。向高一级精度转化。例如：uint和int，向int64转。
- 其它情况报错。

上面介绍的规则构成了类型判断的核心逻辑：
```go
func decideResultType(left common.LType, right common.LType) common.LType
{
	resultTyp := MaxLType(left,right)
	resultTyp是decimal，再调整数值宽度和精度。
	resultTyp是varchar，也再调整。
}

func MaxLType(left, right LType) LType

```

## 函数
sql里的聚合函数，标量函数和运算符，最终都用函数的形式展示。在这里只讲函数语义方面的内容（定义，类型，查找等），函数实现在执行器会再讲。

函数的组成结构：
- 名称
- 参数个数和类型
- 返回类型
- 函数类型：标量，聚合和运算符。
- 函数体：完成功能的执行代码
	- 标量函数，运算符。只需要函数实现代码
	- 聚合函数。还包含聚合状态的初始化、更新和终结。

用结构体`FunctionV2`表示函数定义：
```go
type FunctionV2 struct {  
    _name         string  
    _args         []common.LType  
    _retType      common.LType  
    _funcTyp      FuncType
	_scalar        ScalarFunc  
	_init      aggrInit  
	_update    aggrUpdate  
	_finalize  aggrFinalize
}
```

函数有重载特性：同名，但是不同参数个数或类型会有不同的实现。`FunctionSet`表示同名重载函数。系统里面有很多不同名称的函数，这些函数都记录在`FunctionList`中。
```go
type FunctionSet struct {  
    _name      string  
    _functions []*FunctionV2  
    _funcTyp   FuncType  
}

type FunctionList map[string]*FunctionSet
```

聚合函数有单独的实现框架。标量函数和运算符是一套框架。这些都与执行器强相关，会在讲执行器时再介绍。

定义函数并注册到系统中，实质是让系统中的某个`FunctionList`记录此函数及其重载。

### 函数表达式解析
目的是确定函数的具体实现。

解析步骤：
- 确定函数类型。标量函数，聚合函数。聚合函数的解析入口不同，实质逻辑差不多。
- 确定函数的所有重载实现。确定`FunctionSet`
- 确定匹配输入参数的最佳实现。
	- 遍历重载实现，用输入参数类型 与 每个重载实现的参数类型对比，给每个重载实现计算一个分数，分数最低的为最佳的重载实现。
- 生成结果函数表达式。

函数解析关键函数：
```go
func (Builder) bindFunc(){
	if aggr {
		BindAggrFunc()
	}else{
		BindScalarFunc()
	}
}

func (FunctionBinder) BindAggrFunc(){}

func (FunctionBinder) BindScalarFunc(){}
```
### cast
`cast`函数的应用形式`cast(expr,targetTyp)`。可以显式使用，也可以隐式使用。
在表达式类型和目标类型不一致时，需要将表达式值类型转成目标类型。`cast`函数是这类转换函数的总称。不是指单个转换函数。
两两数据类型之间能否转换、以及转换方式都是事先确定好的。简单说，`cast`函数对应了转换矩阵。矩阵的两个维度分别对应两种数据类型。矩阵元素是具体的转换函数。
转换函数如何实现。在介绍执行器框架时再讲，这里不细讲


# 小结
整体介绍了bind的过程。重点介绍了表达式的解析过程。确定结果类型、函数表达式解析和`cast`还有诸多细节没讲。一方面是琐碎难以讲清楚。另外是先把表达式执行框架讲清楚了之后再讲。更容易理解。