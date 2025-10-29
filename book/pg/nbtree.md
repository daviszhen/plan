# 问题

tiebreaker attribute 是什么?
    heap tid

pg_index的key属性是什么？

pivot tuple 是什么?

forward scan 和 backward scan的区别？

fast_root 与 root 什么区别

heapkeyspace是啥？
```
* heapkeyspace indicates if we expect all keys in the index to be physically
* unique because heap TID is used as a tiebreaker attribute, and if index may
* have truncated key attributes in pivot tuples.


heapkeyspace indexes are
* indexes whose version is >= version 4.
```

suffix truncation是啥?
    pivot tuple中省略不必要的后缀键属性

index key attribute 是什么?
    关键属于 用于排序的。

allequalimages意义
    表示索引中的所有键属性都是"equal image"属性，即相等性可以通过字节图像比较来确定，这使得索引可以安全地使用去重（deduplication）功能。


non-truncated (key) attributes 和 truncated (key) attributes是什么？

suffix truncation
    针对key属性。leaf page分裂过程中，不需要page high key。tuple逻辑保留truncated key attributes，-inf。suffix truncation能让pivot tuple更小。

`key->scantid == NULL`的情况。
    1. 唯一性检查期间：临时设置为NULL，避免使用heap TID进行搜索
    2. 非heapkeyspace索引：不使用heap TID作为决胜属性
    3. 没有提供索引元组：调用者传递itup = NULL
    4. 索引元组没有heap TID：枢轴元组的heap TID被截断


# 数据类型
Datum 表示立即数 或 指针。通过一些转换函数 `DatumGetXXX`和 `XXXGetDatum`进行转换。

```c

src/include/postgres.h

typedef uintptr_t Datum;

```

变长数据类型头

```c

struct varlena
{
char vl_len_[4]; /* Do not touch this field directly! */

char vl_dat[FLEXIBLE_ARRAY_MEMBER]; /* Data content is here */

};

#define VARHDRSZ ((int32) sizeof(int32))


```
# 关系

```c
关系cache entry。内存中表示一个关系需要的信息。

typedef struct RelationData

{
    ....
}

src/include/utils/relcache.h

typedef struct RelationData *Relation;

```

这是个宏

```c

/*

* RelationGetDescr

* Returns tuple descriptor for a relation.

*/

#define RelationGetDescr(relation) ((relation)->rd_att)


```

tuple结构描述。
FormData_pg_attribute 复杂的结构体。
```c

typedef struct TupleDescData

{

int natts; /* number of attributes in the tuple */

Oid tdtypeid; /* composite type ID for tuple type */

int32 tdtypmod; /* typmod for tuple type */

int tdrefcount; /* reference count, or -1 if not counting */

TupleConstr *constr; /* constraints, or NULL if none */

/* attrs[N] is the description of Attribute Number N+1 */

FormData_pg_attribute attrs[FLEXIBLE_ARRAY_MEMBER];

} TupleDescData;

typedef struct TupleDescData *TupleDesc;

取第i个属性
/* Accessor for the i'th attribute of tupdesc. */

#define TupleDescAttr(tupdesc, i) (&(tupdesc)->attrs[(i)])
```
# item
磁盘page上的item
```c
src/include/storage/itemptr.h

typedef struct ItemPointerData

{

BlockIdData ip_blkid;

OffsetNumber ip_posid;

}

typedef ItemPointerData *ItemPointer;
```

# 索引

unique check选项
```c

typedef enum IndexUniqueCheck

{

UNIQUE_CHECK_NO, /* Don't do any uniqueness checking */ //允许重复值

UNIQUE_CHECK_YES, /* Enforce uniqueness at insertion time */ //重复值报错

UNIQUE_CHECK_PARTIAL, /* Test uniqueness, but no error */ //允许重复值

UNIQUE_CHECK_EXISTING, /* Check if existing tuple is unique */ //重复值报错。只检查重复值，但是不插入数据。

} IndexUniqueCheck;

```

构建index entry的信息
```c


typedef struct IndexInfo

{

...
} IndexInfo;

```

index tuple 头结构。index tuple都会有。
```c

typedef struct IndexTupleData

{

ItemPointerData t_tid; /* reference TID to heap tuple */

  

/* ---------------

* t_info is laid out in the following fashion:

*

* 15th (high) bit: has nulls

* 14th bit: has var-width attributes

* 13th bit: AM-defined meaning

* 12-0 bit: size of tuple

* ---------------

*/


unsigned short t_info; /* various info about tuple */


} IndexTupleData; /* MORE DATA FOLLOWS AT END OF STRUCT */

  

typedef IndexTupleData *IndexTuple;

//bitmap 信息
typedef struct IndexAttributeBitMapData

{

bits8 bits[(INDEX_MAX_KEYS + 8 - 1) / 8];

} IndexAttributeBitMapData;

  

typedef IndexAttributeBitMapData * IndexAttributeBitMap;

```

用数据，tuple描述符 构建index tuple
```c

/* ----------------

* index_form_tuple

*

* As index_form_tuple_context, but allocates the returned tuple in the

* CurrentMemoryContext.

* ----------------

*/

IndexTuple

index_form_tuple(TupleDesc tupleDescriptor,

const Datum *values,

const bool *isnull)

{

return index_form_tuple_context(tupleDescriptor, values, isnull,

CurrentMemoryContext);

}

```


tuple 表示
- **non-pivot / posting tuple format**
`t_tid | t_info | key values | posting list (TID array)`

内容：
```
A posting
* list is an array of ItemPointerData elements.


the content of
* the posting tuple's t_tid to store the location of the posting list
* (instead of a block number), as well as the total number of heap TIDs
* present in the tuple (instead of a real offset number).
```

成立条件：
`t_info.INDEX_ALT_TID_MASK = 1` 
`t_tid's offset number.BT_IS_POSTING = 1 

- **non-pivot / plain/non-posting variant**
`t_tid | t_info | key values | INCLUDE columns, if any`

内容：
```
* t_tid points to the heap TID, which is a tiebreaker key column as of
* BTREE_VERSION 4.

key and non-key attributes.

INCLUDE B-Tree indexes have non-key attributes. ...returned by index-only scans. 

formally, non-key attributes are not
* considered to be part of the key space

Non-key attributes are only
* present in leaf index tuples whose item pointers actually point to heap
* tuples (non-pivot tuples).
```

- **pivot** 
`t_tid | t_info | key values | [heap TID]`

内容：
```
pivot tuples never need to store a real
* offset (pivot tuples generally store a downlink in t_tid, though).


The
* offset field only stores the number of columns/attributes when the
* INDEX_ALT_TID_MASK bit is set, which doesn't count the trailing heap
* TID column sometimes stored in pivot tuples -- that's represented by
* the presence of BT_PIVOT_HEAP_TID_ATTR（此时元组中包含heap TID）.

* The 12 least significant bits from t_tid's offset number are used to
* represent the number of key columns within a pivot tuple. This leaves 4
* status bits (BT_STATUS_OFFSET_MASK bits), which are shared by all tuples
* that have the INDEX_ALT_TID_MASK bit set (set in t_info)

```

# btree

插入上下文
```c

typedef struct BTInsertStateData

{

IndexTuple itup; /* Item we're inserting */

Size itemsz; /* Size of itup -- should be MAXALIGN()'d */

BTScanInsert itup_key; /* Insertion scankey */

  

/* Buffer containing leaf page we're likely to insert itup on */

Buffer buf;

  

/*

* Cache of bounds within the current buffer. Only used for insertions

* where _bt_check_unique is called. See _bt_binsrch_insert and

* _bt_findinsertloc for details.

*/

bool bounds_valid;

OffsetNumber low;

OffsetNumber stricthigh;

  

/*

* if _bt_binsrch_insert found the location inside existing posting list,

* save the position inside the list. -1 sentinel value indicates overlap

* with an existing posting list tuple that has its LP_DEAD bit set.

*/

int postingoff;

} BTInsertStateData;

  

typedef BTInsertStateData *BTInsertState;

```


插入过程中的scan参数。包含key和控制参数
```c

typedef struct BTScanInsertData

{

bool heapkeyspace;

bool allequalimage;

bool anynullkeys;

bool nextkey;

bool backward; /* backward index scan? */

ItemPointer scantid; /* tiebreaker for scankeys */

int keysz; /* Size of scankeys array */

ScanKeyData scankeys[INDEX_MAX_KEYS]; /* Must appear last */

} BTScanInsertData;

```


表或索引列与常数之间的运算符。
```c

typedef struct ScanKeyData

{

int sk_flags; /* flags, see below */

AttrNumber sk_attno; /* table or index column number */

StrategyNumber sk_strategy; /* operator strategy number */

Oid sk_subtype; /* strategy subtype */

Oid sk_collation; /* collation to use, if needed */

FmgrInfo sk_func; /* lookup info for function to call */

Datum sk_argument; /* data to compare */

} ScanKeyData;

  

typedef ScanKeyData *ScanKey;

```

搜索栈

```c
typedef struct BTStackData

{

BlockNumber bts_blkno;

OffsetNumber bts_offset;

struct BTStackData *bts_parent;

} BTStackData;

  

typedef BTStackData *BTStack;
```

调用关系

```c
//src/backend/access/index/indexam.c

index_insert(indexRelation,values,isnull,heap_t_ctid,heapRelation,)
    indexRelation->rd_indam->aminsert(...) //对应btinsert

//src/backend/access/nbtree/nbtree.c
btinsert(rel,values,isnull,ht_ctid,heapRel,)
    //生成index tuple
    itup =index_form_tuple(tupleDesc,values,isnull)
    itup->t_tid = *ht_ctid;//TID in heap tuple

    //实际插入函数
    _bt_doinsert(rel, itup,heapRel)


//src/backend/access/nbtree/nbtinsert.c
_bt_doinsert(rel, itup,heapRel)
    itup_key = _bt_mkscankey(rel, itup)


    if checkingUnique {
        //TODO
    }

//搜素
search:
    //搜索插入位置。返回路径栈
    stack = _bt_search_insert(rel, heapRel, &insertstate);

    if (checkingunique)
    {
        //TODO
    }

    //确定插入位置
    newitemoff = _bt_findinsertloc(rel, &insertstate, checkingunique,
        stack, heapRel);
        
    _bt_insertonpg(rel, heapRel, itup_key, insertstate.buf, InvalidBuffer,
        stack, itup, insertstate.itemsz, newitemoff,
        insertstate.postingoff, false);


//src/backend/access/nbtree/nbtutils.c
_bt_mkscankey(rel, itup)
    //返回BTScanInsert。内部是ScanKey。
    //用关键属性构建scan key。不包含Truncated attributes and non-key attributes
    //
    *heapkeyspace = metad->btm_version > BTREE_NOVAC_VERSION;
    *allequalimage = metad->btm_allequalimage;

    //用key column 填充scankey


//src/backend/access/nbtree/nbtinsert.c
_bt_search_insert
    //搜素scankey 所在的leafpage
    _bt_search(rel,heaprel,itup_key,buf,BT_WRITE)

//src/backend/access/nbtree/nbtsearch.c
_bt_search(rel,heaprel,itup_key,buf,access)
    //返回：BTStack。leaf page

    //取root page
    *bufP = _bt_getroot(rel,heaprel,acces)

    循环：{//逐层向下遍历。
        //可能向右走。page 可能被分裂了。
        *bufP = _bt_moveright(rel, heaprel, key, *bufP, (access == BT_WRITE),stack_in, page_access);

        page是leaf page 退出循环。

        //在page中pivot tuple,并取downlink
        offnum = _bt_binsrch(rel, key, *bufP);//取slot
        itup = (IndexTuple) PageGetItem(page, itemid);//取indextuple
        child = BTreeTupleGetDownLink(itup);//child page的blockNumber

        //向下一层，到子节点
        *bufP = _bt_relandgetbuf(rel, *bufP, child, page_access);

        //保存搜素栈
        stack_in = new_stack;
    }

    if root page is a leaf page {
        加写锁;
        *bufP = _bt_moveright(rel, heaprel, key, *bufP, true, stack_in, BT_WRITE);
    }

//src/backend/access/nbtree/nbtsearch.c
_bt_moveright(rel,heaprel,key,buf,forupdate,stack,access)
    //到正确的page。page可能被分裂。
    
    循环：{
        如果已经到达最右边节点,break;
        
        if forupdate && page incomplete split {
            //完成split
            _bt_finish_split(rel,heaprel,buf,stack)
            //
            buf = _bt_getbuf(rel, blkno, access);
            continue;
        }

        //向右走
        //_bt_compare(rel, key, page, P_HIKEY)
        if dead page or half dead or 
            (key > high key or key >= high key) {
            //向右走
            buf = _bt_relandgetbuf(rel, buf, opaque->btpo_next, access);
            continue;
        }else break;
    }

//src/backend/access/nbtree/nbtsearch.c
_bt_compare(rel,key,page,offnum)
    //比较 scankey 与 tuple at offnum
    if non-leaf page, offnum == 第一个数据key {
        return 1;
    }
    逐个key的比较；

    if index tuple 可能被截断过 {
        被截断的key属性被认为是-inf;
        return 1;
    }

    //到此处key属性没有被截断。用heapTid 与 scantid 来比较。    
    if scantid == NULL {
        if heapTid == NULL {
            表示heap tid 被截断过。被截断的key属性被认为是-inf;
            return 1;
        }
        return 0;
    }

    if heapTid == NULL {
        表示heap tid 被截断过。被截断的key属性被认为是-inf;
        return 1;
    }

    //scantid 与 heapTid 都不为NULL。直接比较
    区分non pivot tuple posting list 的情况。
    result = ItemPointerCompare(key->scantid, heapTid);



//src/backend/access/nbtree/nbtsearch.c
_bt_binsrch(rel,key,buf)
    //在page中，二分查找key
    //slot
    low = P_FIRSTDATAKEY(opaque);//第一个实际item序号。最右边page是1，其它是2.
    high = PageGetMaxOffsetNumber(page);//item 个数
    high++;
    执行二分查找逻辑；
    //如果cmpval = 0，当high = low时，low's key > scan key
    //如果cmpval = 1,当high = low时，low's key >= scan key

    if leaf-page {
        return low;
    }

    //non-leaf-page
    return low - 1;
```