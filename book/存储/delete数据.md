# 简介
从表中删除数据行。

delete的特点：
- 区分数据在事务local storage 还是 在表对象中。最终都会走到RowGroup集合Delete接口。
- 处理到RowGroup层。不需到ColumnSegment层。因为行版本信息记录在RowGroup上。
- 行版本信息也按vector分组。

# delete过程

外层接口到内层接口：

| 接口                        | 参数     | 含义                   |
| ------------------------- | ------ | -------------------- |
| DataTable.Delete          | rowids | 表delete接口            |
| LocalStorage.Delete       | rowids | 事务局部存储delete接口       |
| RowGroupCollection.Delete | rowids | RowGroup集合delete接口   |
| RowGroup.Delete           | rowidx | RowGroup delete接口    |
| VersionDeleteState.Delete | rowid  | 删除某一个行               |
| VersionDeleteState.Flush  | rowids | 同一个vector的行批量更新行版本信息 |
| ChunkInfo.Delete          | rowids | 设置行版本号               |
- RowGroupCollection.Delete。先确定rowid所在的RowGroup。同一个RowGroup的row一起删除。
- RowGroup.Delete。循环删除每个行。同一个vector的行，先收集。再一次性更新版本信息。
- VersionDeleteState.Delete。收集同一个vector的行，有可能要创建对应的版本信息。
- VersionDeleteState.Flush。检测冲突：同一行是否已经被其它事务删掉。更新同一个vector的版本信息。

# 小结
delete相对插入、update容易理解。没有复杂的状态也没有版本链。