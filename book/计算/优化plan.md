# 简介
优化逻辑查询计划以提升查询性能。

优化过程复杂，有很多原因：
- 不同的优化规则，复杂度不同。
- 优化过程中，涉及查询树和表达式的变换。基本是递归操作。
- 对关系代数有一定的要求。

本文介绍的优化规则：
- filter下推
- join定序
- 列裁剪

整体的优化过程：
- filter下推
- join定序
- filter下推
- 列裁剪
- 生成引用计数
- 生成输出表达式

# filter下推
查询计划树上，filter离scan节点越近，越能及早过滤掉不必要的数据。filter下推就是将树上层的filter尽可能的往下推。整体看，filter下推过程比较清晰。

下推输入与输出：
- 输入1: 查询计划树
- 输入2: 上个节点传下来要下推的filter
- 返回1：新的查询计划树
- 返回2：不能下推的filter

下推是递归完成的。不同节点下推filter的做法不同。
- scan节点。引用当前表的filter保留在scan节点。
- join节点。确定filter表达式，哪些只引用了左子树的表。哪些只引用了右子树的表。哪些左右子树的表都引用了。
	- 分别向左、右子树下推。
	- 下推不了的生成新的filter节点。或者形成join on条件。
- agg节点。如果filter是引用了agg节点上的表达式，是不能下推的。这些得保留。
- filter和project节点。表达式只需要做初步处理，即可下推。
```go
func (b *Builder) pushdownFilters(root *LogicalOperator, filters []*Expr) (*LogicalOperator, []*Expr){
	switch root.Typ{
	case scan:
		for _, f := range filters {
			如果filter只引用了此节点，保留。
			其它，返回。
		}
	case join:
		收集左子树的表;
		收集右子树的表;
		for i, filter := range fitlers {  
		    确定filter仅在左、右子树还是左右都占;
		} 
		
		for i, filter := range fitlers {
			switch filter{
			case 左右都不粘:
				如果是inner join，左右都下推;
				如果是left join，左边下推;
				其它，不下推，此filter返回；
			case 左:
				左边下推;
			case 右:
				右边下推;
			case 左右都粘:
				如果是inner join或 left join，可以转换成join on条件.
				其它，不下推，此filter返回；
			}
		}
		//左边下推
		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], leftNeeds)  
		if len(childLeft) > 0 {  
			 下推不了的，形成新filter节点；
		} 
		root.Children[0] = childRoot

		childRoot, childLeft, err = b.pushdownFilters(root.Children[1], rightNeeds)    
		if len(childLeft) > 0 {  
			下推不了的，形成新filter节点；
		}  
		root.Children[1] = childRoot
		
	case agg:
		//预处理
		for _, f := range filters {
			如果filter引用了agg的表达式，不能下推。保留在agg节点上。
			如果filter间接引用了group by的表达式，间接引用要先替换成实质的group by 表达式。
		}
		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)  
		if len(childLeft) > 0 {  
			 下推不了的，形成新filter节点；
		} 
		root.Children[0] = childRoot
	case project:
		//预处理
		for _, f := range filters {
			如果filter间接引用了project的表达式，间接引用要先替换成实质的project表达式。
		}
		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)  
		if len(childLeft) > 0 {  
			 下推不了的，形成新filter节点；
		} 
		root.Children[0] = childRoot
	case filter:
		合并filter节点本身的filter表达式 与 父节点传下来的filter表达式;
		childRoot, childLeft, err = b.pushdownFilters(root.Children[0], needs)  
		if len(childLeft) > 0 {  
			root.Children[0] = childRoot
			下推不了的，合并到filter节点；
		}else{
			都下推了，无需此filter节点;
		}
	case limit:
		不下推；
	case 其它:
		如果是双子节点，不下推；
		如果是单子节点，尝试下推；下推不了的，形成新filter节点；
	}
}
```

# join定序
相当复杂的处理过程。

本文介绍的join定序具有的特点：
- 递归过程
- 前期要多步准备
- 引入新的概念和数据结构
- 贪心算法
- 抽象，不易理解

## join order 优化框架
join定序优化入口。
定序由多个环节组成。
- step1 ～step 2。准备join定序的SingleJoinRelation。每个表示一个基础关系。
- step3 。基于filter构建图和边。
	- 图结点是JoinRelationSet。其是relation id的集合。与SingleJoinRelation不是同个概念
- step4～step 6。准备join定序的运算对象。有NodeOp,JoinNode构建的初始plan结点。
- step7～step8。执行join定序算法。选出最优的plan结点
- step9。基于plan结点，生成最终的plan树。

```go
//join定序优化
func (joinOrder *JoinOrderOptimizer) Optimize(root *LogicalOperator) (*LogicalOperator, error) {
	//step 1: 抽取relations
	noReorder, filterOps, err :=joinOrder.extractJoinRelations(root, nil)

	//step 2：从filterOps收集filters
	遍历filterOps，将filter整理到joinOrder.filters

	//step 3：基于filter构建无向图。
	//图结构：
	//  结点：左、右子表达式中引用到的table index数组对应的relation id数组。
	//  边：(左子表达式关联的relation id数组， 右子表达式关联的relation id数组)构成了一条边
	//
	//可以认为图中只有relation id信息了。直接看不到table index信息了。
	对joinOrder.filters的每个filter处理：
		//step 3.1 构建整个filter的信息：filterInfo
		抽取filter关联的relation id数组filterRelations:
			joinOrder.collectRelation
		借助JoinRelationSetManager将relation id数组转化为JoinRelationSet；
			joinOrder.setManager.getRelation(filterRelations)
		//step 3.2 用filter创建图和边
		joinOrder.createEdge(filter的左子表达式, filter的右子表达式, filterInfo)

	//step 4：构建join tree的叶结点（单结点的plan）和其基数。及单个relation id表示一个树结点
	//nodes_ops []NodeOp
	为每个SingleJoinRelation创建叶结点：
		node(JoinRelationSet) := joinOrder.setManager.getRelation(relation id)
		NodeOp{JoinNode{node,SingleJoinRelation.op}}

	//step 5: 为每个叶结点NodeOp初始化基数
	err = joinOrder.estimator.InitCardinalityEstimatorProps(nodesOpts, joinOrder.filterInfos)

	//step 6： 基于叶结点，初始化基础plan
	//这个plan记录在映射：JoinRelationSet => JoinNode
	joinOrder.plans.set(nodeOp.node.set, nodeOp.node)

	//step 7：对基础plan应用join order算法，确定最终的plan。
	err = joinOrder.solveJoinOrder()

	//step 8: 生成结果plan结点
	//join order优化后的理想结果是：
	//	joinOrder.plans中有JoinRelationSet完整数组组成的plan结点。此时说明图能形成一棵新的完整plan结点。
	final := joinOrder.plans.get(set)  
	if final == nil {  
		//step 8.1 无完整plan结点
		//为JoinRelationSet数组两两元素，在图中构建一条边
	    joinOrder.generateCrossProduct()  
	    //再重新执行join order算法
	    err = joinOrder.solveJoinOrder()  
	    //一定能拿到新的完整的plan结点
	    final = joinOrder.plans.get(set)  
	}

	//step 9：基于结果plan结点，生成最终的plan树。
	//注意：plan结点指 JoinNode构成的。
	// plan树指 LogicalOperator构成的。
	return joinOrder.rewritePlan(root, final)
}

```

## extractJoinRelations
准备join定序的SingleJoinRelation。收集基础关系。在join定序过程中无法被任意交换的关系。
```go 
//递归过程。搜索plan子树，寻找所有符合条件的节点，生成所有SingleJoinRelation。
//输入： plan子树
//输出：
//  nonReorder：不能做reorder操作。
//  filterOps：有过滤条件的结点。
//  relations： SingleJoinRelation结点
//  relationMapping：table index -> relation id
func (joinOrder *JoinOrderOptimizer) 
	extractJoinRelations(root, parent *LogicalOperator) 
	(nonReorder bool, filterOps []*LogicalOperator, err error) {
	//step 1：
	遍历子树，寻找第一个符合条件的节点：
	  不是project
	  不是scan
	  且有两个子节点的
	遍历过程中，遇到agg节点（符合上述条件），对agg节点子树递归优化(Optimize)。递归优化结束后，停止遍历。

	//step 2：
	跳出上述遍历过程的基本就是project,scan,join,set-op等
	如果节点刚好是join时,
		如果不是inner join又不是cross join时，不能对子树进行reorder操作，需要特殊处理。
	
	不能reorder join的特殊处理逻辑：
		对每个子节点，递归优化；
		生成SingleJoinRelation结点；
		取子树的所有table index,同时合并子节点的relation index 与 table index的映射关系。
		返回

	//step 3： 可以reorder的情况
	switch op.Typ{
	case join:
		（join结点本身不产出什么）
		对左、右子结点递归extractJoinRelations;
	case scan:
		生成SingleJoinRelation结点；
	case project:
		对子结点递归优化(Optimize);
		生成SingleJoinRelation结点；
	}
	
	
}

```

## collectRelation
从表达式中确定table index对应的所有relation id。
```go
//从表达式中抽取table index对应的relation id数组
//递归操作
//从(table index,column index)转(relation id,column index)
func (joinOrder *JoinOrderOptimizer) collectRelation(e *Expr, set map[uint64]bool){
	switch e.Typ{
	case colRef:
		relId := joinOrder.relationMapping[index];
		//(table index,column index) => (relation id,column index)
		joinOrder.estimator.AddColumnToRelationMap(relId, e.ColRef[1])
	...
	}

}
```

## createEdge
在图中，基于左，右表达式构建一条边。
生成图结点的过程：表达式=>relation ids =>JoinRelationSet =>图结点。

```go

//基于左，右表达式构建一条边。
func (joinOrder *JoinOrderOptimizer) createEdge(left, right *Expr, info *FilterInfo){
	//左表达式 的table index => leftRelations(relation id数组)
	joinOrder.collectRelation(left,leftRelations)
	//右表达式 的table index => rightRelations(relation id数组)
	joinOrder.collectRelation(right,rightRelations)
	//如果leftRelations,rightRelations都不为空时，创建图的边
	//将leftRelations(relation id数组)转化为JoinRelationSet
	info.leftSet = joinOrder.setManager.getRelation(leftRelations)  
	//将rightRelations(relation id数组)转化为JoinRelationSet
	info.rightSet = joinOrder.setManager.getRelation(rightRelations)
	//如果leftRelations(JoinRelationSet),rightRelations(JoinRelationSet)不相交，创建图里面的边。
	//结点：info.leftSet，info.rightSet
	//边：无向图两条边
	joinOrder.queryGraph.CreateEdge(info.leftSet, info.rightSet, info)  
	joinOrder.queryGraph.CreateEdge(info.rightSet, info.leftSet, info)	
	
}
```

## join order贪心算法
基于图query graph. 图以JoinRelationSet为结点，以filter构建边。
初始的plan的结点，在映射：joinOrder.plans {JoinRelationSet => JoinNode}

算法复杂度：O(N^3)

算法初始化：
- 初始结点数组。单个relation id构成的JoinRelationSet数组。

算法框架：
- 在初始JoinRelationSet数组上，进行迭代运算。
	- 枚举 i,j 分别对应两个不同元素。共有N（N-1）种情况
	- 如果图中，有i,j对应的JoinRelationSet构成的边，
		- 两个JoinRelationSet融合，并构建新的plan结点。
- 直到，数组中只有单个JoinRelationSet结点。

```go
// 贪心join order算法
func (joinOrder *JoinOrderOptimizer) solveJoinOrder() error {  
    return joinOrder.greedy()  
}

//join order贪心算法
func (joinOrder *JoinOrderOptimizer) greedy() (err error) {
	//step 1：算法初始化
	生成由单个relation id构成的JoinRelationSet数组。

	//step 2：直到数组只有一个元素为止
	for 数组元素个数 > 1 {
		//最佳的结点，以及对应的JoinRelationSet
		best = nil.
		bestLeft,bestRight = 0,0
		//step 2.1 找出cost最低的两两组合
		for i := 0...len{
			for j := i+1...len{
				if i,j对应的JoinRelationSet有边{
					node = joinOrder.emitPair(i,j的JoinRelationSet结点,边);
					更新best,bestLeft,bestRight = node,i,j；
				}
			}
		}
		//处理特殊情况：没有best,多种原因。
		//硬加一条边
		if best == nil{
			从数组中，取两个基数最低的i,j；
			在图中为它们增加边：joinOrder.queryGraph.CreateEdge(left, right, nil);
			在为i,j生成新结点：best, err = joinOrder.emitPair(left, right, conns);
		}
		//step 2.2 将最优的两两组合从数组中删掉
		//并插入组合后的JoinRelationSet
		joinRelations = util.Erase(joinRelations, bestRight)  
		joinRelations = util.Erase(joinRelations, bestLeft)  
		joinRelations = append(joinRelations, best.set)
	}
}



```

## emitPair
融合有边的两个JoinRelationSet，并生成新的plan结点

```go

//构建新的plan结点
//left,right会融合成新的JoinRelationSet
func (joinOrder *JoinOrderOptimizer) emitPair(left, right *JoinRelationSet, info []*neighborInfo) (*JoinNode, error){
	//step 1: 取left,right对应的plan结点。
	//从joinOrder.plans {JoinRelationSet => JoinNode}中取结点
	leftPlan, err := joinOrder.getPlan(left)  
	rightPlan, err := joinOrder.getPlan(right)
	//step 2：融合left,right形成新JoinRelationSet
	newSet := joinOrder.setManager.union(left, right)  
	//step 3：基于融合的JoinRelationSet和leftPlan,rightPlan，创建新的Join plan结点
	newPlan, err := joinOrder.createJoinTree(newSet, info, leftPlan, rightPlan)
	//step 4：新plan结点更新到joinOrder.plans
	joinOrder.plans.set(newSet, newPlan)
}

```
## getRelation
JoinRelationSetManager： 
	relation id序列组成的trie树
	trie树叶结点是JoinRelationSet
	
JoinRelationSet：
	relation id数组
```go
//relation id数组 => JoinRelationSet
func (jrsm *JoinRelationSetManager) getRelation(relations UnorderedSet) *JoinRelationSet{
	...
}

```

## generateCrossProduct
补充query图的边

```go

//为JoinRelationSet数组两两元素，在图中构建一条边
func (joinOrder *JoinOrderOptimizer) generateCrossProduct(){
	for i := 0...len{
		for j := 0...len{
			if i != j{
				joinOrder.queryGraph.CreateEdge(left, right, nil)  
				joinOrder.queryGraph.CreateEdge(right, left, nil)
			}
		}
	}

}

```

## rewritePlan
生成新plan数据

```go

//基于最优plan结点重新生成新plan树
func (joinOrder *JoinOrderOptimizer) rewritePlan(root *LogicalOperator, node *JoinNode) (*LogicalOperator, error) {
	//step 1:
	extractedRelations = 取出每个SingleJoinRelation关联的LogicalOperator，形成数组；
	
	//step 2:基于extractedRelations和plan结点，生成新的join 结点
	//joinTree是结果的join tree
	joinTree, err := joinOrder.generateJoins(extractedRelations, node)
	//step 3： 处理剩余的filters
	//step 4：上移结果joinTree的位置。
}

//递归生成join 结点树
func (joinOrder *JoinOrderOptimizer) generateJoins(extractedRels []*LogicalOperator, node *JoinNode) (*GenerateJoinRelation, error) {
	if node有双子结点 {
		//构建为新join结点
		//先对左，右子结点递归生成
		left, err := joinOrder.generateJoins(extractedRels, node.left)  
		right, err := joinOrder.generateJoins(extractedRels, node.right)
		//依据filter个数分为cross join和inner join
		//结果是：
		//    结果的JoinRelationSet是左，右子结点的JoinRelationSet合并的结果
		//    结果的LogicalOperator是新生成的join结点
	}else{
		//结果是：
		//    结果的JoinRelationSet是结点的JoinRelationSet
		//    结果的LogicalOperator是结点的JoinRelationSet对应的第一个
	}
	//如果join结点，还有剩余的filter，要带上，不能丢弃
	
}

```
# 列裁剪
将非根结点的输出列个数减到最少。只要某个结点的输出列没有被祖先结点所引用都应该删除掉。

从根结点往下遍历plan树，递归处理过程：
- 记录结点的表达式的所有的列引用
- 在project,scan,agg结点时，
	- 递归先序遍历时，记录要删除的列。
	- 对子结点进行处理。
	- 递归后序遍历时，彻底删除列。

记录列引用的映射：
```go
//列引用 => 表达式数组
type ReferredColumnBindMap map[ColumnBind][]*Expr
```

递归过程：
```go

func (cp *ColumnPrune) prune(root *LogicalOperator) (*LogicalOperator, error) {
	switch root.Typ{
	case order:
		记录order by表达式中的列引用;
	case project:
		处理影列表达式list：
			如果列表达式{project结点index,i}，
				没在祖先结点中被引用，此列为不再需要的；
				已经被引用，那么记录列表达式中的列引用。
				并计算旧列引用{project结点index,i}对应的新列引用{project结点index,j}；
		递归对子结点裁剪；
		删除不再需要的列；
		对记录的表达式，将旧列引用都替换为新列引用；
		返回；
	case aggr:
		记录group bys,filters表达式中的列引用；
		处理聚合表达式list：
			如果列表达式{aggr结点index2,i}，
				没在祖先结点中被引用，此列为不再需要的；
				已经被引用，那么记录列表达式中的列引用。
				并计算旧列引用{aggr结点index2,i}对应的新列引用{aggr结点index2,j}；
		递归对子结点裁剪；
		删除不再需要的列；
		对记录的表达式，将旧列引用都替换为新列引用；
	case join:
		记录on表达式中的列引用；
	case scan:
		记录filters表达式中的列引用；
		处理scan结点列表达式list：
			如果列表达式{scan结点index,i}，
				没在祖先结点中被引用，此列为不再需要的；
				已经被引用，那么记录列表达式中的列引用。
				并计算旧列引用{scan结点index,i}对应的新列引用{scan结点index,j}；
		删除不再需要的列；
		对记录的表达式，将旧列引用都替换为新列引用；
	case filter:
		记录filters表达式中的列引用；
	}


	//对子结点递归裁剪
	for i, child := range root.Children {  
	    root.Children[i], err = cp.prune(child)
	}
}

```

# 生成列引用计数
计算每个结点要输出的列引用，列引用的引用个数和列引用的输出位置序号。这些值在生成输出表达式时需要。

从根结点往下遍历plan树，递归处理过程：
- 删除祖先结点引用此结点的列引用个数
- 统计结点的表达式的所有的列引用个数
- 统计子结点的列用个数
- 在project,agg,join结点时，
	- 递归先序时，先从引用计数中，删除对此结点的引用。因为此结点的子结点中不可能再引用此结点。
	- 递归后序时，再加山对此结点的引用。如此才能保证引用个数的正确


实际记录列引用个数的函数是updateCounts。

```go
func (update *countsUpdater) generateCounts(root *LogicalOperator, upCounts 
ColumnBindCountMap) (*LogicalOperator, error) {

	updateCounts ：=func(){
		记录结点表达式的列引用个数；
		递归更新子结点的引用个数；
		resCount，upcounts恢复祖先结点对引用此结点的引用个数；
		resCount中删除祖先结点不需要的列引用；
		resCount，upcounts删除结点表达式的列引用个数；
	}

	switch root.Typ{
	case limit:
	case order:
		updateCounts（order by表达式）；
	case project:
		从upcounts中删除，对此结点的引用个数；
		updateCounts（投影表达式）;
	case aggr:
		从upcounts中删除，对此结点的引用个数；
		updateCounts（group by,aggr,filter表达式）;
	case join:
		从upcounts中删除，对此结点的引用个数；
		updateCounts（on表达式）
	case scan:
		resCounts = upCounts;
		基于resCounts生成此结点的引用个数；
	case filter:
		updateCounts（filter表达式）；
	}
}

```


# 生成输出表达式
结点的投影表达式和输出表达式。
它们的不同点：
- 存在与否。投影表达式，不是每个结点都有。输出表达式每个结点都会被补上。
- 列引用形式。投影表达式，引用实际结点index. 输出表达式，只引用直接子结点的输出列。

输出表达式 方便执行器生成数据。

输出表达式的类型：
- 祖先结点引用此结点的列引用
- 祖先结点引用此结点的子结点的列引用。子结点的列引用数据会透传给祖先结点。

生成输出表达式的整体递归过程：
- 对子结点递归生成输出表达式；
- 结点表达式的列引用替换为对子结点输出表达式的列引用；
- 基于结点的引用计数，生成输出表达式：
	- 引用此结点本身，列引用表达式引用此列位置
	- 引用子结点，引用左右子结点的输出表达式的列位置。
	- aggr结点的特殊性：其输出表达式，按情况集中输出，依次排开。
		- 引用aggr结点
		- 引用aggr 聚合函数结果
		- 其它情况。
	- join结点的特殊性：其输出表达式，先是左右子结点，再是此结点。
		- 引用左右子结点
		- 引用join结点

```go

func (update *outputsUpdater) generateOutputs(root *LogicalOperator) (*LogicalOperator, error) {
	
	switch root.Typ{
	case limit:
		递归子结点生成输出表达式；
		生成输出表达式；
	case order:
		递归子结点生成输出表达式；
		order by表达式的列引用替换为对子结点输出表达式的列引用；
		生成输出表达式；
	case project:
		递归子结点生成输出表达式；
		投影表达式的列引用替换为对子结点输出表达式的列引用；
		生成输出表达式；
	case aggr:
		递归子结点生成输出表达式；
		group by,aggrs,filters表达式的列引用替换为对子结点输出表达式的列引用；
		生成输出表达式；
	case join:
		递归子结点生成输出表达式；
		on表达式的列引用替换为对子结点输出表达式的列引用；
		为左右子结点生成输出表达式；
		生成输出表达式；
	case scan:
		生成输出表达式；
	case filer:
		递归子结点生成输出表达式；
		filters表达式的列引用替换为对子结点输出表达式的列引用；
		生成输出表达式；
	}
}

```


# 小结
介绍filter下推，join定序，列裁剪三种基础的优化算法实现方案。
