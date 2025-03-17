# 简介
将表的列从旧值换成新值。表对象提供update接口。本文介绍update的逻辑过程。

update的特点：
- 输入：rowid、列索引、新值。
- 多个事务能同时更新一张表。更新的行列可能交叉。
- 旧值可以在表对象上 或 在事务的local storage 中。如果在表对象上，需要版本信息。如果在事务的local storage中，只有此事务可见，无需版本信息。

# update过程
- 用rowid定位旧值所在的RowGroup、ColumnData。
- 用列索引确定要更新的列
- 在列数据上加版本信息

从外层到内层涉及多个接口：

| 接口                        | 参数           | 说明                     |
| ------------------------- | ------------ | ---------------------- |
| DataTable.Update          | rowid，列索引，新值 | update的入口              |
| LocalStorage.Update       | rowid，列索引，新值 | local storage的update入口 |
| RowGroupCollection.Update | rowid，列索引，新值 | RowGroup集合的update入口    |
| RowGroup.Update           | rowid，列索引，新值 | RowGroup的update接口      |
| ColumnData.Update         | rowid，列索引，新值 | 列数据 update接口           |
| ColumnData.Fetch          | rowid        | 取旧值                    |
| UpdateSegment.Update      | rowid，列索引，新值 | 列版本 update接口           |
- DataTable.Update按rowid对数据分流。rowid属于事务local storage范围内的，进入LocalStorage.Update，在此事务内部存储中进行更新操作。rowid在表对象上，进入RowGroup集合的update接口。
- LocalStorage.Update。事务local storage的update接口。事务为每张表关联的局部存储`LocalTableStorage`，其内部也是RowGroup集合。最终也用了RowGroup集合的update接口。
- RowGroupCollection.Update。表对象和事务局部存储最终都用到此update接口。
- RowGroup.Update。更新在此RowGroup上的列。
- ColumnData.Update。用rowid取旧值。版本信息需要保存旧值。
- UpdateSegment.Update。构建版本链。

## 确定RowGroup

 RowGroupCollection.Update 完成rowid拆分。
一批rowid。某些rowid在同个RowGroup。另外一些rowid在其它RowGroup。 将同属一个RowGroup的rowid收集起来，再分为2K行一组（vector大小），一组组的交由RowGroup update。分属于多个RowGroup，就执行多次。

## 取旧值

事务undo时，恢复旧值。旧值要记录在列版本系统中。在update时，只有rowid和新值。必须取旧值。
ColumnData.Fetch 读取旧值，读取rowid所属的vector。读取过程，简要得说，确定rowid所在的ColumnSegment，再从block内存中读取vector。详细的读取过程，在讲读数据时再讲。

## 构建版本信息
多个事务可能对同一个vector中的数据行进行update。产生了数据的多个版本。事务只能读到它可见的版本。

每个RowGroup 120K行，分成60个vector（2K行）。每个vector有一个版本链，表示关联的版本信息。

UpdateSegment.Update完成版本信息，是最复杂的环节。

分为几步：
- 对rowid递增排序。
- 如果对应的vector，没有版本链，要创建版本链，并加入此事务的版本信息。
- 如果对应的vector，有版本链表。
	- 版本链表上，无此事务的版本信息。
	- 版本链表上，有此事务的版本信息。

### 创建版本链表

没有版本链表，要创建版本链表，并为此事务创建版本节点。每个vector有一个版本链表。同一个vector中的行共享同一个版本链表。一个RowGroup有60个vector，每个列就有对应的60个版本链表。

版本链表的结构：
- 头节点：永远放置新值。即使有多个事务更新同一个vector，也放置每个事务update的新值。
- 非头节点：每个事务都有一个唯一节点，记录事务update前的旧值。

```text
_______     __________    __________
|头节点| ->  |事务i版本 |-->|事务j版本 |->....
-------     ----------    ----------

```

版本节点：
- `_versionNumber`。版本号（事务id）
- `_vectorIndex`。所属的vector索引
- `_N`。更新的行数(< 2K)。
- `_tuples`。vector内部的下标 = rowid - 此vector首行rowid 。
```go
UpdateInfo{
	_versionNumber uint64 
	_vectorIndex int
	_N int
	_tuples []int
	_prev,_next *UpdateInfo
}
```

头节点初始化过程：
- 将事务update的新值填入`_tuples`
- 更新`_N`
- 更新`_versionNumber` = 特殊值。

为事务创建版本节点：
- 将旧值记录到节点中。
- `_versionNumber` = 事务id

InitUpdateData完成这一过程。
```go
func InitUpdateData(
	baseInfo *UpdateInfo,  //事务版本节点
	baseData *chunk.Vector,  //旧值
	updateInfo *UpdateInfo,  //头节点
	update *chunk.Vector,//新值
){
	update -> updateInfo._tuples //新值 -> 头节点
	baseData -> baseInfo //旧值 -> 事务版本节点
}
```

### 有版本链表
考虑几个问题：
- 是否有其它事务与此事务冲突。修改同一行同一个列。
- 链表上有没有此事务版本节点。没有得先创建版本节点。
- 更新头节点和事务版本节点。放置新值和旧值。

不同的事务对同个vector进行update时，会出现更新同一个值的情况。需要机制来检测事务WW冲突。
检测原理：遍历版本链表，每个版本号 大于 当前事务的节点（已经提交或未提交）。当前事务修改的行号如果在此节点上，说明已经有事务update此行了，出现了WW冲突。
冲突处理：当前事务abort。
```go
func CheckForConflicts(
	info *UpdateInfo,
	txn *Txn,
	rowIds []RowType){
	if info.versionNumber  == txn.id {
		//同个事务不检查
	}else if info.versionNumber > txn.id {
		for rid := rowIds {
			if rid in info.tuples {
				"ww conflicts"
			}
		}
	}
	return CheckForConflicts(info.Next,txn,rowIds);
}
```


确定事务版本节点。遍历版本链表，节点版本号 等于 此事务的节点，即为此事务版本节点。如果没有找到，需要为事务创建版本节点，并插入到头节点之后。

更新版本信息。
- 复杂性在于确定旧值的位置。既要在头节点中填新值，又要在事务版本节点中填旧值。
- 旧值有两部分：当前事务本次要update的值。当前事务之前update的值。本次与之前update的值可能不重叠。
- 准备新值。

旧值的位置：
- 在表中。第一次修改此值。
- 在事务版本节点中。当前事务已经改过此值。

确定旧值位置的方法：
- 对每个要update的rowid。先在事务版本节点中查找，如果存在的话，说明旧值在事务版本节点中。事务已经update过此值。如果不存在，再去头节点中查找。
- 如果存在于头节点，说明旧值在头节点中。别的事务已经update此值，并且已经提交。
- 如果不存在于头节点，说明旧值在表中 并且 是第一次修改此值。

收集旧值并重填入事务版本节点中，构成修改后的旧值。
- 本次update的旧值，在确定了位置后读取。
- 本次update未涉及，但是之前update过的旧值，已经在事务版本节点中。

准备新值：将新值填入头节点中。只将本次update涉及的新值填入头节点。实质是二路归并的过程。

# 小结
数据多版本是理解的难点。多版本的生成方式理解update逻辑的关键点。