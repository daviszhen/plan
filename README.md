# plan
单机事务数据库内核。

围绕执行tpch query而设计的，有计算引擎和存储引擎。

两个bin：
plandb：psql交互执行
tester：非交互。

## 支持的语句

1. create schema
创建schema. 内置的schema是`public`。

```
create schema if not exists s1;
```    
2. create table 
创建表。

```
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
	N_NAME       VARCHAR(25) /*CHAR(25)*/ NOT NULL,
	N_REGIONKEY  INTEGER NOT NULL,
	N_COMMENT    VARCHAR(152),
	PRIMARY KEY (N_NATIONKEY)
);
```

3. copy
从parquet,csv文件中导入数据到表中。

```
copy nation
from '/home/pengzhen/Documents/tpch-parquet/nation.parquet'
with (FORMAT 'parquet');
```

4. insert ... values


5. tpch 22条query

tpch query 1
```
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '112 day'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus
;
```

## plandb

使用psql连接交互执行。

```
make plandb
./plandb

psql -h 127.0.0.1

```

## tester

`make tester`

### 配置

配置文件`tester.toml`

配置文件搜索路径。以下路径都没有配置文件，报错退出。
```text
./
./etc/tpch/1g/
```

### 执行tpch1g
```shell
tester help tpch1g

//测试tpch query
tester tpch1g
  --query_id int 
    运行指定序号的query。范围[1,22]。为0时，按顺序执行22条query。
  --data_path string
    tpch1g 数据位置
  --data_format string
    tpch1g 数据格式。csv,parquet
  --result_path string
    query结果位置
  --need_headline bool
    query结果第一行为headline行

```

tpch1g测试结果

**注意：在忽略精度和结果标题前提下，进行对比

| tpch 1g qX | status                                                 | 与duckdb相同                  | 与mo相同                         |
|------------|--------------------------------------------------------|----------------------------|-------------------------------|
| q1         | right                                                  | y                          | y                             |
| q2         | right                                                  | n (s_ddress,s_comment 不同)  | y                             |
| q3         | right                                                  | y                          | y                             |
| q4         | right                                                  | y                          | y                             |
| q5         | right                                                  | y                          | y                             |
| q6         | right                                                  | y                          | y                             |
| q7         | right                                                  | y                          | y                             |
| q8         | right                                                  | y                          | y                             |
| q9         | right                                                  | y                          | y                             |
| q10        | right (use topN further)                               | n (c_address,c_comment 不同) | y                             |
| q11        | right                                                  | y                          | y                             |
| q12        | right                                                  | y                          | y                             |
| q13        | almost right. (duckdb convert left join to inner join) | n                          | almost y. count(NULL) is diff |
| q14        | right                                                  | y                          | y                             |
| q15        | right                                                  | n (s_address 不同)           | y                             |
| q16        | right                                                  | y                          | y                             |
| q17        | right                                                  | y                          | y                             |
| q18        | right                                                  | y                          | y                             |
| q19        | right                                                  | y                          | y                             |
| q20        | right                                                  | n (s_address 不同)           | y                             |
| q21        | right                                                  | y                          | y                             |
| q22        | right                                                  | y                          | y                             |

**


### 执行sql

执行支持的语句。不限于tpch的query.

```shell
tpch1gddl --ddl 'sql'
```