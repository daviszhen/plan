# 简介
catalog记录数据库中schema和表的元信息。catalog中数据的组织方式与表不同。
具有以下特点：
- 用entry表示元信息的某个版本。分为几类：schema，table等。
- 简要理解，用entry的集合表示所有元信息。
- 事务对元信息修改产生新版本。可以回滚到旧版本。创建entry时，会记录undo log。提交事务时，会写walog。
- 元信息分层：
	- catalog层：schema的entry集合
	- schema层：table的entry集合
	- table层：表相关的entry集合。
- 维护元信息之间的依赖关系。例如：table与schema关联。

# entry
entry有两层含义：
- 具体类型的元数据。schema，table等。由`CatalogEntry`表示。
- 在放入entry集合时，在`CatalogEntry`之上又再套一层。由`EntryValue`，`EntryIndex`表示。

`CatalogEntry`的关键信息：
- `_typ`。类型。schema，table等。
- `_name`。schema名称，或 table名称。
- `_timestamp`。创建时，事务的id。提交事务时，commitid。
- `_child`， `_parent`。entry被修改后产生新版本。`_child`指向旧版本，`_parent`指向新版本，组成版本链。
- 附加信息。
	- 对schema，有表的entry集合。
	- 对table，有表对象，表的属性。

`EntryValue`的关键信息：
- `_entry *CatalogEntry`。元信息。

`EntryIndex`的关键信息：
- `_index`。对entry的序号或下标。

# entry集合
`CatalogSet`表示entry集合。依据要表达的元信息类型，entry集合里面就放什么。
- catalog：schema的entry集合
- schema：table的entry集合
- table：表相关的entry集合

`CatalogSet`的关键信息：
- `_entries`：entryIdx -> `*EntryValue`。从entry idx到entry链表。`CatalogEntry`可以指向旧版本。
- `_mapping`：name -> `*MappingValue`链表。从名称到entry idx链表。`MappingValue`可以指向旧版本。

`MappingValue`的关键信息：
- `_index`：entry idx
- `_timestamp`：创建时的事务id
- `_child`，`_parent`：`_child`指向旧版本。版本链表。

`CatalogSet`需要的entry接口：
- 创建entry
- 读取entry
- entry依赖关系维护

## 创建entry
创建schema和表时，要创建entry。
分两步：
- 创建元信息的CatalogEntry。这一步相对简单，不再细说。
- 创建EntryValue

创建EntryValue的过程相对复杂
- 用名称取entry idx。如果没有，分配新的entry idx。记录映射关系：名称->entry idx。同个名称会对应多个版本的entry idx。用链表维护。
	- 需要版本可见性。
- 维护依赖关系。entry 与其它entry之间的关联。
- 记录映射关系：entry idx -> entry value。同个entryIdx 也可能对应多个entry value，用链表维护。
- 记录undo log。讲旧entry记录到undo log。旧entry的parent是新entry。

接口层次：

| 接口                     | 含义                                           |
| ---------------------- | -------------------------------------------- |
| CatalogSet.CreateEntry | 创建entry入口                                    |
| CatalogSet.GetMapping  | 用名称取entry idx                                |
| CatalogSet.PutEntry    | 记录映射关系（覆盖已有的）：entry idx -> entry value       |
| CatalogSet.PutMapping  | 记录映射关系：名称->entry idx。插入链表头部。                 |
| DependMgr.AddObject    | 维护依赖关系                                       |
| CatalogSet.PutEntry2   | 记录映射关系：entry idx -> entry value。新entry插入链表头部 |
| Txn.PushCatalogEntry   | 记录undo log                                   |
- CatalogSet.GetMapping。在取entry idx时，需要版本过滤。当前事务只能看到自己修改的或已经提交事务的 entry idx。

```go

func(CatalogSet) CreateEntry(
	txn,name,value,list,
){
	mapping :=GetMapping(txn,name){
		ent := _mapping[name];
		for ent._child != nil{
			if ent 对 txn可见 {
				返回ent;
			}
		}
	}

	if mapping 无效{
		PutEntry(new entry idx,dummy catalog entry);
		PutMapping(txn,name,new entry idx);//记录新映射关系。名称->entry idx。
	}else{
		用mapping._entryidx从_entries取最新的CatalogEntry;
		if txn 与 catalogEntry冲突{
			w-w conflicts;
		}
	}
	DependMgr.AddObject(txn,value,list)
	CatalogSet.PutEntry2(entryidx,value);//记录映射关系：entry idx -> entry value。
	Txn.PushCatalogEntry(...)
}

```

## 取CatalogEntry
从`CatalogSet`取entry 分为两类：
- 取当前事务可见的有效CatalogEntry。
	- 从名称取entryIdx。用GetMapping
	- 由entryIdx取得Entry链表。遍历链表取事务可见的entry。
- 取已经提交的有效CatalogEntry。
	- 遍历entry idx -> entry value。对每个entry 链表。遍历链表取事务可见的entry。

## 维护依赖关系
例如：schema由多个表组成，每个表只属于唯一一个schema。删除schema必须先删除所有的表。
用图来表达entry之间的依赖关系。
`DependMgr`实现依赖关系图：
- 内部记录两层关系：每个entry是要知道，我依赖了谁 和 谁依赖了我。
- 接口1: `AddObject(ent,dependList)`添加依赖关系。
- 接口2: `EraseObject(ent)` 删除ent以及与之相关的依赖关系。

## commit操作
在创建entry时，会记录undo log，并且记录的是旧entry（旧entry的parent是新entry）。
在事务提交时，catalog操作也要写walog。
- 设置新entry的提交时间戳
- 必要时，设置旧entry的提交时间戳
- 将新entry序列化写入walog。

## undo操作
创建了entry后，回归事务，需要撤销对catalog的修改。
创建entry简要逻辑：
- 记录映射关系：名称到entryidx。链表方式。
- 记录映射关系：entryidx到entry的映射。链表方式。
- 维护依赖关系

那么撤销操作，逆向操作恢复原样：
- 删除依赖关系
- 删除映射关系：entryidx到entry的映射。实质是将新entry从链表删除掉。
- 删除映射关系：名称到entryidx。实质是entryidx从链表中删掉。

# 小结
catalog维护entry集合，方式与表不同。创建entry和undo操作相对复杂。理解两层映射关系有助于理解这一点。
