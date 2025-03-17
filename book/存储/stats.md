# 简介
介绍表的统计信息的构成、更新和使用。
统计信息构成：
- 表级别，列级别
- 列级别：zonemap(最大值，最小值)。不同值个数。

在插入新数据时，要更新列级别的统计信息。
在查询优化、checkpoint等都会用到统计信息。

# 统计信息结构
前面介绍过内存层次结构。
- 表对象
- RowGroup集合
- RowGroup
- 列数据ColumnData
- ColumnSegment

统计信息的分层与内存层次结构相关，但不完全相同。

| 内存结构          | 统计信息          | 说明            |
| ------------- | ------------- | ------------- |
| 表对象           | 无             | 无             |
| RowGroup集合    | TableStats    | 表的统计信息。       |
| RowGroup      | 无             | 无             |
| 列数据ColumnData | SegmentStats  | 每个列上的统计信息     |
| ColumnSegment | SegmentStats  | 每个block上的统计信息 |
|               | BaseStats     | zonemap信息     |
|               | DistinctStats | 不同值统计信息       |
数据结构层次关系：
```go
type TableStats struct {  
    _columnStats []*ColumnStats  
}

type ColumnStats struct {  
    _stats         BaseStats  
    _distinctStats *DistinctStats  
}

type SegmentStats struct {  
    _stats BaseStats  
}

```


zonemap信息收集、不同值统计（hyperloglog）这里都不再细说。

# 更新
更新统计信息的过程是从下到上的。
- ColumnSegment。在向block内存块，追加数据时，同时更新列的统计信息（SegmentStats：zonemap信息）。
- 列数据ColumnData。更新完成ColumnSegment上的统计信息会merge到ColumnData中的统计信息。
- RowGroup集合。
	- RowGroup插入数据后，所有列的统计信息合并到RowGroup集合的表统计信息上。
	- 更新每个列的不同值个数。

# 使用

## 查询

1, filter下推，zonemap过滤。
目前filter下推到scan节点。但是不支持zonemap过滤，不能跳过不需要读的block。完整支持需要完善filter的能力。

2，join定序。
用到表的总行数 和 列的不同值个数。

## checkpoint
完整的checkpoint过程请看事务章节。简要的说，checkpoint过程是每个RowGroup以及其中每个ColumnData逐个持久化。
checkpoint过程中统计信息的收集、存储。从checkpoint的下层向上层：
- 列数据ColumnData
	- 每个ColumnSegment的统计信息都会聚合到每个列。每个列会有一个整体统计信息。
	- RowGroup在持久化每列元信息后，会持久化每个ColumnSegment的统计信息。
	- 每个列的整体统计信息的去处 会聚合到外层表的统计信息。因为表有多个RowGroup。
- 外层表级统计信息在表元信息序列化时序列化。排在RowGroup信息序列化之前。

从统计信息序列化过程看，有这些特点：
- block的级的统计信息会序列化
- RowGroup级别没有序列化统计信息
- 列统计信息最终在表级统计信息序列化中体现

# 小结
统计信息的收集和序列化比较容易理解，操作过程比较繁琐。统计信息的使用目前讲得概要。join定序相关的在优化器部分会讲。zonemap过滤等支持后再讲了。
