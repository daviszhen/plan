# 简介
介绍从bind过程的结果物出发构建逻辑查询计划的过程。实质是规划查询执行的步骤，形成一个查询计划节点构成的树。而bind过程的结果物会被分散到这棵节点树上。
bind过程的结果物有：Builder,BindContext,Binding,Expr。

生产过程是按照语法单元的bind顺序，逐步构建完整的查询计划树：
- 为from生成查询节点
- 为where生成查询节点
- 为聚合函数和groupby生成查询节点
- 为having生成查询节点
- 为project生成查询节点
- 为orderby生成查询节点
- 为limit生成查询节点

查询节点结构：
- 类型。scan,filter,project,join,agg,order,limit等
- 子节点引用数组。
- 附加字段。不同的节点类型附加字段也不同。

```go

type LogicalOperator struct {  
    Typ              LOT  
    Children         []*LogicalOperator
    ...
    附加字段
}
```

# 生成查询计划
## 生成table scan节点
table scan节点表示对某种关系读取数据。

from表达式的结构：
- 单表
- 两表join
- 子查询
- values list。insert语句的values子句。
- 由这些基本结构组成的递归结构。

对from表达式进行递归处理，构建子树：
```go
func (Builder) createFrom(expr *Expr, root *LogicalOperator) *LogicalOperator{
	switch expr.Typ{
	case 单表：
		取表对象
		构建scan节点;
	case join:
		left, err = b.createFrom(expr.Children[0], root)    
		right, err = b.createFrom(expr.Children[1], root)
		join类型;
		on条件;
		构建join节点;
	case 子查询:
		_, root, err = b.createSubquery(expr, root)//后续单独讲
	case values list:
		确定列名称和类型，values list;
		构建scan节点;
	}

}
```


由单表通常生成table scan节点。
- 从catalog拿到表对象。
- 拿到表的统计信息。

scan节点形式：
```go
LogicalOperator{  
    Typ:       LOT_Scan,  
    Index:     expr.Index,  //关系序号
    Database:  expr.Database,  //schema name
    Table:     expr.Table,  //table name
    Alias:     expr.Alias,    //alias
    Stats:     stats,  //统计信息
    TableEnt:  tabEnt,  //表对象
}
```

由join通常生成join 节点。递归生成
- 生成左、右子节点的查询树
- 确定join类型。inner，left，cross
- 确定join on条件表达式。

```go
LogicalOperator{  
    Typ:      LOT_JOIN,  
    Index:    uint64(b.GetTag()),  //关系序号
    JoinTyp:  jt,  //join 类型
    OnConds:  []*Expr{onExpr.copy()},  //join ... on
    Children: []*LogicalOperator{left, right},  //子节点
}
```

为子查询生成查询树，比较复杂，会在后面单独讲，这里跳过。

由values list生成scan节点。没有表对象，数据从values list中读取。并且额外增加列名称和类型。
```go
LogicalOperator{  
    Typ:         LOT_Scan,  
    TableIndex:  int(expr.Index),  
    ScanTyp:     ScanTypeValuesList,  
    Types:       expr.Types,  
    Names:       expr.Names,  
    Values:      expr.Values,  
    ColName2Idx: expr.ColName2Idx,  
}

```

## 转换子查询

子查询可以出现在投影列、where子句和from子句中。tpch的查询中常用子查询。在为这些子句的表达式生成查询节点时，子查询展开是最复杂的环节，必须先讲清楚。

子查询又分为关联子查询和非关联子查询。关联子查询指子查询引用外层查询的列。

最基础的子查询执行方式：外层查询执行一条记录，执行一次子查询。这样执行性能低。
展开子查询的目的就是将子查询与外层查询融合起来，解决这种低效的子查询执行方式。

在展开子查询之前，子查询的完整查询计划树是已经生成的。在外层处理过程中，遇到子查询表达式时，就开始展开子查询。

不是每个表达式都是子查询，遇到表达式时，对表达式分类处理。

表达式递归处理过程：
- 子查询。先生成子查询的plan，展开子查询。
- 函数。对每个参数表达式，处理子查询。再产生新的结果表达式。对`IN`,`NOT IN`，会改写结果表达式。
- 其它。原样返回。
```go 
func (Builder) createSubquery(
	expr *Expr, 
	root *LogicalOperator) (*Expr, *LogicalOperator){
	switch expr{
	case 子查询:
		subRoot = 为子查询创建查询计划树;//递归过程
		//展开子查询
		apply(expr,root,subRoot);
	case 函数:
		//处理参数表达式
		for child := expr.Children{
			childExpr,root = createSubquery(child,root)
		}
		//对IN,NOT IN改写表达式
		生产结果表达式;
	case 其它:
		不做处理；
	}
}

```
### 展开子查询
采用论文`Orthogonal Optimization of Subqueries and Aggregation`中提到的`apply`算法展开子查询。

这里介绍算法的工程实现，与原始的算法有差别。

算法输入与输出：
- 输入1：`expr`. 子查询表达式
- 输入2：`root`. 外层查询已经形成的部分查询计划子树。
- 输入3：`subRoot`. 子查询完整的查询计划树
- 输出1: 结果表达式，替换子查询表达式。
- 输出2: 展开后的查询计划树。融合了外层和子查询的查询计划树。

算法过程：
- 确认关联列。从子查询查询计划树中，找出所有的关联列。
- 如果没有关联列，是非关联子查询。处理方式：
	- 标量子查询。转成cross join节点,`children = {root,subRoot}`。
	- `IN`子查询。转成semi join节点,`children = {root,subRoot}`。
	- `NOT IN`子查询。转成anti join节点,`children = {root,subRoot}`。
- 如果有关联列，是关联子查询。处理方式：
	- **上拉关联表达式**。关联filter`子查询.t1.a = 外层查询.t2.b`。最终是将这样的filter拉出子查询与外层查询融合。
	- **从关联表达式中，消除关联列**。存在有些关联列消除不掉的情况。
	- **改写子查询**。
		- 标量子查询。用新列引用替换子查询。新列引用去引用子查询投影列。
		- `exists`，`not exists`子查询。
			- 在上一步，已经改写为mark join和anti mark join。
			- 在mark join和anti mark join之上，加一个标志列，表示某行的值是否存在。具体mark join和anti mark join是怎么计算的，在讲join算子时，会再讲。

上拉关联表达式的逻辑：
- 存在一个问题:`子查询.t1.a`这部分是不能直接拉出子查询的，拉出来就是有问题的。但是又要保持filter的语义不变。
- 因此在上拉过程中，`子查询.t1.a`这部分会进入`project`,`aggr` 节点，并间接引用`project`,`aggr`。并将filter 改写为  `间接引用 = 外层查询.t2.b`。有点抽象，请看后面的伪代码。
- 对于`filter`节点。将filter节点中的关联表达式切出来。
```go
func (Builder) removeCorrFilters(
	subRoot *LogicalOperator,
) (*LogicalOperator, []*Expr){//返回新subRoot和关联表达式
	//对子节点递归处理
	for i, child := range subRoot.Children {  
	    subRoot.Children[i], childFilters, err = b.removeCorrFilters(child)
	}
	switch {
	case project://上拉经过project
		for filter := childFilters{
			递归处理(filter){
				if filter无关联列 {//进入project投影列。并间接引用此列
					idx := len(subRoot.Projects)  
					subRoot.Projects = append(subRoot.Projects, filter)
					间接引用此列{Column,subRoot.Index,idx}
				}else{
					递归处理filter的子节点。
				}
			}
		}
	case agg://上拉经过agg
		for filter := childFilters{
			递归处理(filter){
				if filter无关联列 {//进入project投影列。并间接引用此列
					idx := len(subRoot.GroupBys)  
					subRoot.GroupBys = append(subRoot.GroupBys, filter)
					间接引用此列{Column,subRoot.Index,idx}
				}else{
					递归处理filter的子节点。
				}
			}
		}
	case fiter://将filter节点中的关联表达式切出来
		for _, filter := range subRoot.Filters {  
		    if filter是关联表达式 {  
		       corrFilters = append(corrFilters, filter)  
		    } else {  
		       leftFilters = append(leftFilters, filter)  
		    }  
		}
	}
}

```

消除关联列。递归处理，将表达式中的深度大于0的列引用的深度都减去1。
- 列引用。深度大于0（`expr.Depth > 0`），`expr.Depth - 1`。
- 函数表达式。对参数表达式，递归处理。
经过递归消除后，有些关联列消除为非关联列，依然有些关联列没消除掉。
对递归消除后的结果进行处理：
- 非关联列（由关联列消除而得）。这些非关联列，成为join on条件。
	- 标量子查询。转为inner join节点。非关联列是on条件。
	- `exists`子查询。转为mark join节点。非关联列是on条件。
	- `not exists`子查询。转为anti mark join节点。非关联列是on条件。
- 关联列（由关联列消除后，依然是关联列）。成为filter节点。

## 生成filter节点
filter节点的内容：表达式数组。数组内元素语义上是and。

产生filter节点的场景：
- `where`,`having` 表达式
- 展开子查询。消除关联项之后。
- 优化器优化过程中。例如：filter下推

`where`,`having` 表达式有显著的区别。`where`中不能用聚合函数。`having`可以用。本文没有区分这一点。两者转filter节点的逻辑都是相同的。

`where`,`having` 表达式转filter节点的过程：
- 转换成合取范式。单个表达式=>表达式数组。用and将大表达式切分成小表达式。相对简单不细说。
- 对每个小表达式，转换子查询。展开子查询已在前面讲过。
- 对每个小表达式，处理分配律，方便后续优化。`(a and b1) or (a and b2) or (a and b3) => a and (b1 or b2 or b3)`。

```go
func (Builder) createWhere(expr *Expr, root *LogicalOperator) (*LogicalOperator){
	//按and拆分。
	filters := splitExprByAnd(expr)
	//展开子查询
	for _, filter := range filters {  
	    newFilter, root, err = b.createSubquery(filter, root)
	}
	//
	newFilters = distributeExprs(newFilters...)
}
```

表达式，分配律（类似提取公因子）处理逻辑：
- 转成析取范式。用or将大表达式切分成小表达式。
- 将每个小表达式，再转换成合取范式。
- 找出所有合取范式中，相同的表达式。抽出来。
- 再重组。

## 生成Agg节点
查询中有聚合函数和group by，要生成Agg节点。agg节点的内容：聚合函数表达式，group by表达式。
生成过程简单不细说。

## 生成Project节点
为投影列表生成project节点。project节点内容：投影表达式。
投影列是可以用子查询的。
```go
func (Builder) createProject(root *LogicalOperator) (*LogicalOperator){
	for _, expr := range b.projectExprs {  
	    newExpr, root, err = b.createSubquery(expr, root)
	}
}
```


## 生成order节点
为order by子句生order节点。order节点内容：order by表达式。
生成过程简单不细说。

## 生成limit节点
为limit子句生limit节点。limit节点内容：limit表达式。
生成过程简单不细说。

# 小结
展开子查询是生成查询计划树中最复杂的环节。涉及到表达式变换、改写、替换，过程抽象容易理解出错。实际上本文介绍的展开子查询的算法 不能 展开任意子查询。