# 简介
从表中读数据。在事务框架下，又支持MVCC，读数据过程复杂。
复杂性来自多个方面：
- 涉及内存结构，从RowGroup集合到ColumnSegment。需要多个状态支撑读数据过程。
- 数据可能在事务的local storage中，也可能在表中。
- 数据可见与否。事务只能基于版本信息读取其可见的数据。用行版本和列版本过滤的处理方式不同。

# 状态

多层的内存结构，决定多层的状态。每层状态维护其内的读取位置。

层次状态：
- TableScanState。表级别
- CollectionScanState。 RowGroup集合
- ColumnScanState。 列级别
- SegmentScanState。ColumnSegment级别

```go
TableScanState{
	CollectionScanState{
		ColumnScanState{
			SegmentScanState{
				
			}
		}
	}
}
```

| 状态                  | 字段              | 含义                 | 备注  |
| ------------------- | --------------- | ------------------ | --- |
| TableScanState      | _tableState     | 读表对象的状态            |     |
| TableScanState      | _localState     | 读local storage的状态  |     |
| TableScanState      | _columnIds      | 读取的列               |     |
| CollectionScanState | _rowGroup       | 当前RowGroup         |     |
| CollectionScanState | _vectorIdx      | 当前vector下标         |     |
| CollectionScanState | _maxRowGroupRow | RowGroup最多能读的行数    |     |
| CollectionScanState | _columnScans    | 列的读状态              |     |
| CollectionScanState | _maxRow         | RowGroup集合中能读的最大行号 |     |
| ColumnScanState     | _current        | 要读的列segment        |     |
| ColumnScanState     | _rowIdx         | 列的第一个行号            |     |
| ColumnScanState     | _scanState      | block              |     |

# 读数据过程
分为几步：
- 初始化阶段。给状态赋初值。
- 读数据。

## 初始化
1，初始化TableScanState
- 记录要读的列。
- 初始化表对象的读状态
- 初始化事务局部存储的读状态

2，初始化CollectionScanState
表对象的读状态 和 事务局部存储的读状态 都需要RowGroup集合的读状态。
- `_maxRow` RowGroup集合中能读的最大行号 = 开始行号 + 总行数。
- 寻找第一个有数据的RowGroup。
	- `_vectorIdx`  = 0
	- `_maxRowGroupRow`  RowGroup能读的行数

3，初始化ColumnScanState
初始化每个列的读状态。
- `_current` 列数据的第一个ColumnSegment
- `_rowIdx` 列的第一个行号
- `_internalIdx`  = `_rowIdx`

## 读过程
通过多层接口，从外层直到最内层，读到数据。每层又依据状态进行分流。读取成功又更新状态。

| 接口                                   | 状态                  |
| ------------------------------------ | ------------------- |
| DataTable.Scan                       | TableScanState      |
| LocalStorage.Scan                    | CollectionScanState |
| CollectionScanState.Scan             | CollectionScanState |
| RowGroup.Scan,RowGroup.TemplatedScan | CollectionScanState |
RowGroup.TemplatedScan完成实质的读过程。先概要的理解此函数，再细说每个组件。
整体逻辑：
- 确定要读的vector
- 行版本信息过滤。vector中，事务读其可见的数据。（MVCC）
- 如果vector中，事务一行都读不到。要跳过这个vector。
- 对需要读的vector，要么全读，要么部分读。对每个列都读一次。
	- 全读，接口ColumnData.Scan（或ColumnData.ScanCommitted）。
	- 部分读，接口ColumnData.FilterScan（或ColumnData.FilterScanCommitted）。

```go
func (RowGroup)TemplatedScan(
		txn,state,result,scanTyp,
	){
	//读一个vector
	for{
		确定vector的开始行号和可读行数；
		行版本过滤；事务读可见的行；
		if vector全过滤了 {
			跳过此vector;
			continue
		}
		if vector全读 {
			for 列i {
				ColumnData.Scan
				（或ColumnData.ScanCommitted）
			}
		}else{//部分读
			for 列i {
				ColumnData.FilterScan
				（或ColumnData.FilterScanCommitted）
			}
		}
	}
}
```

### 行版本过滤
RowGroup上有行版本信息。分60组，每组2K行。对应vector个数，和vector中的行数。

行版本信息有两类：
- insert操作。此行被事务插入。
- delete操作。此行被事务删除。

行被事务可见的含义：
- insert操作。被事务看见，结果是看到这行数据。
- delete操作。被事务看见，结果是看**不**到这行数据。与insert操作的结果相反。

行版本过滤原理：
- 输入1：行版本信息：insert操作的事务id。delete操作的事务id。
- 输入2：读取此行的事务id和事务startTime。
- 判断逻辑：依据事务可见性含义。
	- insert操作。insertId < startTime || insertId == txnId。insert操作的事务已经提交或是读事务插入的。读事务能看到这行数据。
	- delete操作。!(deleteId < startTime || deleteId == txnId)。delete操作的事务已经提交或是读事务删除的。数据已经删除了。读事务看不到这行数据。

```go

//看到insert操作。看到insert后的数据行
func (op TxnVersionOp) UseInsertedVersion(  
    startTime, txnId, id TxnType) bool {  
    return id < startTime || id == txnId  
}  

//看到delete操作。数据已经删除了。事务看不到这行数据。
func (op TxnVersionOp) UseDeletedVersion(  
    startTime, txnId, id TxnType) bool {  
    return !op.UseInsertedVersion(startTime, txnId, id)  
}

```

### 跳过vector
vector的所有数据行都不需要读。跳过需要读的所有列的`_vectorIdx`对应的vector。
跳过逻辑的层次接口：

| 接口                           | 状态                  |
| ---------------------------- | ------------------- |
| RowGroup.NextVector          | CollectionScanState |
| ColumnData.Skip              | ColumnScanState     |
| ColumnScanState.Next         | ColumnScanState     |
| ColumnScanState.NextInternal | ColumnScanState     |

跳过逻辑：
- `_vectorIdx`++。指向下一个vector。
- 每个列跳过2K行。
	- `_rowIdx` 下一个vector的第一行的行号。
	- 如果当前ColumnSegment读完了，即`_rowIdx`超过当前ColumnSegmet的范围，需要切换到下一个ColumnSegment。
	- 

```go
func (RowGroup)NextVector(state *CollectionScanState){
	state._vectorIdx++
	for colid {
		ColumnData.Skip(scanState,count){
			ColumnScanState.Next(count){
				ColumnScanState.NextInternal(count){
					state._rowIdx += count;
					if state._rowIdx 超过当前ColumnSegment的范围 {
						切换到下一个ColumnSegment;
					}
				}
			}
		}
	}
}

```

### 读vector
读取vector内的全部行或部分行。
读取步骤：
- 从ColumnSegment的block内存中读取数据。这些数据不一定是最新的。
- 列版本过滤。列版本链表头节点是新值，但是事务可见的版本，不一定这些新值。有可能是旧值。这需要结合列版本信息来过滤。
- 筛选需要的行。（部分行的情况）

读vector的接口，从外层到内层：

| 接口                                            | 状态              |
| --------------------------------------------- | --------------- |
| ColumnData.Scan                               | ColumnScanState |
| ColumnData.ScanVector,ColumnData.ScanVector2  | ColumnScanState |
| ColumnSegment.Scan                            | ColumnScanState |
| ColumnSegment.Scan2,ColumnSegment.ScanPartial | ColumnScanState |
| UpdateSegment.FetchUpdates                    |                 |
| UpdateMergeFetch                              |                 |
| UpdatesForTransaction                         |                 |
- ColumnData.ScanVector2。从block内存读取vector数据。
	- ColumnSegment.ScanScan2（ScanPartial）。读完整vector，或vector的部分
	- 如果当前ColumnSegment的数据不够，切换到下一个ColumnSegment。
- UpdateSegment.FetchUpdates。列版本过滤。

回顾下列版本相关的内容：
- 每个vector有对应的版本链表。头节点记录vector的update后的新值。非头节点--事务版本节点，记录事务修改前的旧值。
- 列版本信息。此行此列被事务update过。
- 列版本事务可见。update操作被当前事务可见。当前事务只能读旧值。

列版本过滤逻辑：
- 输入1：从block内存读取的vector数据。
- 输入2：列版本链。
- 遍历版本链表：如果列版本事务可见，要取旧值。
	- 列版本事务可见的判断条件：列版本_versionNumber > startTime && 列版本_versionNumber != txnId。
	- 头节点一定满足此条件。一定能从头节点中取新值。
	- 当前事务版本节点一定不满足此条件。

```go

func UpdateMergeFetch(
	startTime,
	txnId,
	info,
	result,
	){
	UpdatesForTransaction(){
		for info != nil{
			if info._versionNumber > startTime && info._versionNumber != txnId {
				MergeUpdateInfo(){
					info.旧值 => result
				}
			}
			info = info.next
		}
	}
}

```

如果是读vector中的部分行，需要再筛选。筛选的依据是行版本过滤得出的选择器（SelectVector)。

### 读已经提交过程
4种读模式：
- 常规。读当前事务可见的数据。就是上面说的读过程。
- 读已经提交。读所有行，包括删除的行。并且合并已经提交的updates。
- 读已经提交但不要updates。读所有行，包括删除的行。不要未提交的updates。
- 读已经提交但不要永久删除的行。

读已经提交，与上面讲的常规读过程区别：
- 常规读过程中，当前事务可能修改了部分数据，且一定没提交。
- 读已经提交。这个过程是由特殊事务去做的，当前事务是特殊构建的，具有最小活跃事务id 和 最小活跃事务startTime，且只读。

在场景上，常规读就是一般从表中读数据。提交事务时，事务有插入数据时，用读已经提交，读取数据并写入walog。另一处，在事务回滚时，用读已经提交，读取数据并从索引中删除。

与常规读过程相比，接口有些重合。入口不同。
从外层到内层的接口：

| 接口                                           | 状态                  |
| -------------------------------------------- | ------------------- |
| DataTable.ScanTableSegment                   | TableScanState      |
| CollectionScanState.ScanCommitted            | CollectionScanState |
| RowGroup.ScanCommitted                       | CollectionScanState |
| RowGroup.TemplatedScan                       | CollectionScanState |
| ColumnData.ScanCommitted/FilterScanCommitted | ColumnScanState     |
| UpdateSegment.FetchCommitted                 |                     |
- DataTable.ScanTableSegment，从表对象中读取行号区间[rowStart, rowStart + count]中的数据。内部先确定RowGroup，再用读已提交模式读数据。
- CollectionScanState.ScanCommitted。读完一个RowGroup，切换到下一个。
- RowGroup.ScanCommitted。构建新事务：具有最小活跃事务id 和 最小活跃事务startTime。用此事务再去读数据。
- RowGroup.TemplatedScan。读列数据接口用ScanCommitted和FilterScanCommitted。
- ColumnData.ScanCommitted/FilterScanCommitted。先从ColumnSegment的block内存读数据。再合并版本链表头节点的最新已经提交的数据。
- UpdateSegment.FetchCommitted。头节点就是最新已经提交的数据。需要注意的是：在提交事务时，事务有插入数据时，用读已经提交，读取数据并写入walog。在这个场景下，新数据对应的头节点中最新值一定是当前事务插入的。

```go

func (DataTable) ScanTableSegment(
	rowStart,
	count,
){
	end := rowStart + count
	state :=TableScanState{}
	table.InitScanWithOffset(state, colIds, rowStart, rowStart+count)
	for currentRow < end {
		state._tableState.ScanCommitted(data, TableScanTypeCommittedRows){
			for rowGroup {
				_rowGroup.ScanCommitted(state, result){
					//具有最小活跃事务id 和 最小活跃事务startTime
					id, start := GTxnMgr.Lowest()  
					txn, err := GTxnMgr.NewTxn2("lowest", id, start)
					RowGroup.TemplatedScan(txn,state,result)
				}
			}
		}
		...
	}
}

func (RowGroup)TemplatedScan(
		txn,state,result,scanTyp,
	){
	//读一个vector
	for{
		确定vector的开始行号和可读行数；
		行版本过滤；事务读可见的行；
		if vector全过滤了 {
			跳过此vector;
			continue
		}
		if vector全读 {
			for 列i {
				ColumnData.ScanCommitted
			}
		}else{//部分读
			for 列i {
				ColumnData.FilterScanCommitted
			}
		}
	}
}
```

### checkpoint读

checkpoint时，对每个RowGroup中，每个列数据中，依次读取ColumnSegment的每个vector。并写入新的ColumnSegment。
读取checkpoint也分为两步：
- 从block内存读取数据
- 从版本链头节点读取最新数据。

从外层到内层接口：

| 接口                                  | 状态              |
| ----------------------------------- | --------------- |
| ColumnDataCheckpointer.WriteToDisk  |                 |
| ColumnDataCheckpointer.ScanSegments |                 |
| ColumnData.CheckpointScan           | ColumnScanState |
| ColumnSegment.Scan                  | ColumnScanState |
| ColumnSegment.Scan2/ScanPartial     | ColumnScanState |
| UpdateSegment.FetchCommittedRange   |                 |
- ColumnDataCheckpointer.WriteToDisk。checkpoint实质写数据的接口。
- ColumnDataCheckpointer.ScanSegments。读所有ColumnSegment，读出数据并存入新ColumnSegment。
- ColumnData.CheckpointScan。先读vector数据，再读版本链表头节点的最新数据。
- ColumnSegment.Scan2/ScanPartial。读vector数据。
- UpdateSegment.FetchCommittedRange。读版本链表头节点的最新数据。

版本链表头节点的最新数据一定是已经提交的数据。原因是：
- 能checkpoint的条件
	- 已经提交的事务都被GC了。
	- 活跃事务只有当前事务（当前事务正在提交，并触发checkpoint）。
# 小结
行版本和列版本过滤支持事务MVCC特性。准确理解版本可见性的含义和判断条件是理解事务读逻辑的基础，也是难点。