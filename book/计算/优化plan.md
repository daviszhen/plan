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



# 列裁剪


# 生成引用计数


# 生成输出表达式


# 小结

