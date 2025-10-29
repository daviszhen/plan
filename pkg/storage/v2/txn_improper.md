# MVCC 实现逻辑不一致问题分析

## 概述

本文档分析了 `Find`、`Insert` 和版本可见性实现中的关键逻辑不一致问题。这些问题可能导致 ACID 属性违反，特别是在并发场景下可能产生数据不一致和隔离级别违反。

## 主要逻辑不一致问题

### 1. `IsActiveInSnapshot` 函数的语义错误

**问题位置**：`pkg/storage/v2/txn.go:82-84`

**问题描述**：`IsActiveInSnapshot` 函数的逻辑与名称不符：

```go
func (snapshot *Snapshot) IsActiveInSnapshot(txnId TxnId) bool {
    // case 2: txn id is greater than max txn id
    if snapshot._maxTxnId.LessEq(txnId) {
        return true  // ❌ 这里逻辑错误！
    }
}
```

**问题分析**：
- 函数名暗示应该返回 `true` 当事务在快照创建时是活跃的
- 但实际实现中，对于 ID ≥ `_maxTxnId` 的事务返回 `true`
- 这些事务实际上是**未来事务**，在快照创建时还不存在，应该是不可见的

**影响**：这会导致所有可见性判断错误，可能造成脏读。

**建议修复**：
```go
if snapshot._maxTxnId.LessEq(txnId) {
    return false  // 未来事务不可见
}
```

### 2. `TxnMgr.TxnState` 的默认假设问题

**问题位置**：`pkg/storage/v2/txn.go:340-342`

**问题描述**：未知事务被假设为已提交：

```go
func (mgr *TxnMgr) TxnState(txnId TxnId) TxnState {
    // ... 搜索逻辑 ...
    if !ok {
        //not found, it must be committed or doest not exist
        return TxnStateCommitted  // ❌ 危险假设！
    }
}
```

**问题分析**：
- 未知事务被假设为已提交，但可能是：
  - 未来事务（还未开始）
  - 已被垃圾回收的旧事务
  - 无效的事务ID

**影响**：可能导致对不存在事务的错误可见性判断。

**建议修复**：
```go
if !ok {
    return TxnStateUnknown  // 区分未知和已提交
}
```

### 3. `MVCCRead` 的版本选择逻辑问题

**问题位置**：`pkg/storage/v2/txn_version.go:186-213`

**问题描述**：`MVCCRead` 读取"最后一个不可见版本"，但这不符合快照隔离语义：

```go
func MVCCRead(mgr *TxnMgr, txn *Txn, updates *UpdateList, colData *T_COL) {
    for ptr != nil {
        visible, _ = CheckUpdateVisible(mgr, txn, ptr)
        if !visible {
            *colData = ptr._colData  // 取最后一个不可见版本
        } else {
            break
        }
        ptr = ptr._next
    }
}
```

**问题分析**：
- 在快照隔离中，应该读取**快照时间点可见的最新已提交版本**
- 当前实现读取"最后一个不可见版本"，可能包括已中止事务的数据
- 这违反了快照隔离的一致性保证

**建议修复**：
- 读取快照时间点可见的最新已提交版本
- 忽略已中止事务的数据

### 4. `Insert` 函数的覆盖逻辑不一致

**问题位置**：`pkg/storage/v2/row_group.go:160-170`

**问题描述**：`Insert` 函数对不可见行的覆盖逻辑过于宽松：

```go
if has {
    has, wwConflictsOnChange = CheckRowVisible(runCtx.TxnMgr(), txn, item._rowVersion)
    if has || wwConflictsOnChange {
        return  // 不覆盖
    }
    // 覆盖不可见行
    itemToChange = item
}
```

**问题分析**：
- 任何不可见行都被允许覆盖，包括：
  - 已提交但不在快照中的事务插入的行（应该保留）
  - 活跃事务插入的行（应该产生冲突）
- 这可能导致数据丢失和并发控制失效

**建议修复**：
- 区分不同类型的不可见行
- 只允许覆盖已中止或当前事务的数据

### 5. `ChangeOnColumns` 的版本链语义混乱

**问题位置**：`pkg/storage/v2/row_group.go:220-247`

**问题描述**：版本链存储旧值但更新当前数据：

```go
newInfo := &UpdateInfo{
    _colIdx:  colIdx,
    _colData: oldCol,  // ❌ 存储旧值
}
// ... 但然后更新当前数据 ...
item._colsData[colIdx] = newCol
```

**问题分析**：
- 版本链中存储的是**旧值**（用于回滚）
- 但当前数据被**就地更新**为新值
- 这种混合语义使得回滚和一致性检查变得复杂

**建议修复**：
- 要么存储新值（用于读取）
- 要么存储旧值（用于回滚），但需要明确文档化

### 6. `find` 函数的原子性问题

**问题位置**：`pkg/storage/v2/row_group.go:113-130`

**问题描述**：行可见性检查和 MVCC 读取之间缺乏原子性：

```go
has, _ = CheckRowVisible(runCtx.TxnMgr(), txn, item._rowVersion)
if has {
    // ... 稍后 ...
    MVCCRead(runCtx.TxnMgr(), txn, item._colUpdates[colId], colData)
}
```

**问题分析**：
- 在可见性检查和 MVCC 读取之间，事务状态可能发生变化
- 可能导致读取到不一致的数据状态

**建议修复**：
- 在单个锁保护下完成所有可见性检查
- 使用一致的快照进行所有判断

## 影响评估

### 数据一致性风险
- **脏读**：由于 `IsActiveInSnapshot` 错误，可能读取到未来事务的数据
- **数据丢失**：由于 `Insert` 覆盖逻辑过于宽松，可能覆盖不应该覆盖的数据
- **不一致读取**：由于 `find` 函数缺乏原子性，可能读取到不一致的状态

### 并发控制风险
- **隔离级别违反**：快照隔离的语义可能被违反
- **写写冲突检测失效**：某些冲突可能被错误地忽略
- **事务状态混乱**：对未知事务的错误假设可能导致错误的并发控制决策

## 修复优先级

1. **高优先级**：修复 `IsActiveInSnapshot` 函数（影响所有可见性判断）
2. **高优先级**：改进 `TxnState` 错误处理（影响事务状态判断）
3. **中优先级**：修正 `MVCCRead` 语义（影响数据读取一致性）
4. **中优先级**：加强 `Insert` 冲突检测（影响数据写入安全性）
5. **低优先级**：统一版本链语义（影响代码可维护性）
6. **低优先级**：增加原子性保证（影响并发性能）

## 测试建议

1. **并发测试**：在多个事务并发执行的情况下测试各种场景
2. **快照隔离测试**：验证快照隔离级别的一致性保证
3. **边界条件测试**：测试事务ID边界、空值、异常状态等边界条件
4. **压力测试**：在高并发负载下测试系统的稳定性

## 结论

当前的 MVCC 实现存在多个逻辑不一致问题，这些问题可能导致 ACID 属性违反。建议按照优先级逐步修复这些问题，并在修复后进行全面的测试验证。
