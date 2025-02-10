# plan
单机事务数据库内核。

围绕执行tpch query而设计的，有计算引擎和存储引擎。

文档：[快速实现数据库内核](book/快速实现数据库内核.md)


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


### 执行sql

执行支持的语句。不限于tpch的query.

```shell
tester tpch1gddl --ddl 'sql'
```

### 导入tpch1g的数据

创建tpch表并从parquet中导入数据。

```sql

tester tpch1gddl --ddl "
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
	N_NAME       VARCHAR(25) /*CHAR(25)*/ NOT NULL,
	N_REGIONKEY  INTEGER NOT NULL,
	N_COMMENT    VARCHAR(152),
	PRIMARY KEY (N_NATIONKEY)
);

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
	R_NAME       VARCHAR(25) /*CHAR(25)*/ NOT NULL,
	R_COMMENT    VARCHAR(152),
	PRIMARY KEY (R_REGIONKEY)
);

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
	  P_NAME        VARCHAR(55) NOT NULL,
	  P_MFGR        VARCHAR(25) /*CHAR(25)*/ NOT NULL,
	  P_BRAND       VARCHAR(10) /*CHAR(10)*/ NOT NULL,
	  P_TYPE        VARCHAR(25) NOT NULL,
	  P_SIZE        INTEGER NOT NULL,
	  P_CONTAINER   VARCHAR(10) /*CHAR(10)*/ NOT NULL,
	  P_RETAILPRICE DECIMAL(15,2) NOT NULL,
	  P_COMMENT     VARCHAR(23) NOT NULL,
	PRIMARY KEY (P_PARTKEY)
);

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
		 S_NAME        VARCHAR(25) /*CHAR(25)*/ NOT NULL,
		 S_ADDRESS     VARCHAR(40) NOT NULL,
		 S_NATIONKEY   INTEGER NOT NULL,
		 S_PHONE       VARCHAR(15) /*CHAR(15)*/ NOT NULL,
		 S_ACCTBAL     DECIMAL(15,2) NOT NULL,
		 S_COMMENT     VARCHAR(101) NOT NULL,
	PRIMARY KEY (S_SUPPKEY)
	);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
		 PS_SUPPKEY     INTEGER NOT NULL,
		 PS_AVAILQTY    INTEGER NOT NULL,
		 PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
		 PS_COMMENT     VARCHAR(199) NOT NULL,
	 PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
	);

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
		 C_NAME        VARCHAR(25) NOT NULL,
		 C_ADDRESS     VARCHAR(40) NOT NULL,
		 C_NATIONKEY   INTEGER NOT NULL,
		 C_PHONE       VARCHAR(15) /*CHAR(15)*/ NOT NULL,
		 C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
		 C_MKTSEGMENT  VARCHAR(10) /*CHAR(10)*/ NOT NULL,
		 C_COMMENT     VARCHAR(117) NOT NULL,
	PRIMARY KEY (C_CUSTKEY)
	);

CREATE TABLE ORDERS  ( O_ORDERKEY       BIGINT NOT NULL,
	   O_CUSTKEY        INTEGER NOT NULL,
	   O_ORDERSTATUS    VARCHAR(1)/*CHAR(1)*/ NOT NULL,
	   O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
	   O_ORDERDATE      DATE NOT NULL,
	   O_ORDERPRIORITY  VARCHAR(15) /*CHAR(15)*/ NOT NULL,  
	   O_CLERK          VARCHAR(15) /*CHAR(15)*/ NOT NULL, 
	   O_SHIPPRIORITY   INTEGER NOT NULL,
	   O_COMMENT        VARCHAR(79) NOT NULL,
	PRIMARY KEY (O_ORDERKEY)
	);

CREATE TABLE LINEITEM ( L_ORDERKEY    BIGINT NOT NULL,
		 L_PARTKEY     INTEGER NOT NULL,
		 L_SUPPKEY     INTEGER NOT NULL,
		 L_LINENUMBER  INTEGER NOT NULL,
		 L_QUANTITY    INTEGER /*DECIMAL(15,2)*/ NOT NULL,
		 L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
		 L_DISCOUNT    DECIMAL(15,2) NOT NULL,
		 L_TAX         DECIMAL(15,2) NOT NULL,
		 L_RETURNFLAG  VARCHAR(1) NOT NULL,
		 L_LINESTATUS  VARCHAR(1) NOT NULL,
		 L_SHIPDATE    DATE NOT NULL,
		 L_COMMITDATE  DATE NOT NULL,
		 L_RECEIPTDATE DATE NOT NULL,
		 L_SHIPINSTRUCT VARCHAR(25) /*CHAR(25)*/ NOT NULL,
		 L_SHIPMODE     VARCHAR(10) /*CHAR(10)*/ NOT NULL,
		 L_COMMENT      VARCHAR(44) NOT NULL,
	 PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
	);
copy nation
from '/home/pengzhen/Documents/tpch-parquet/nation.parquet'
with (FORMAT 'parquet');
copy region
from '/home/pengzhen/Documents/tpch-parquet/region.parquet'
with (FORMAT 'parquet');
copy part
from '/home/pengzhen/Documents/tpch-parquet/part.parquet'
with (FORMAT 'parquet');
copy supplier
from '/home/pengzhen/Documents/tpch-parquet/supplier.parquet'
with (FORMAT 'parquet');
copy partsupp
from '/home/pengzhen/Documents/tpch-parquet/partsupp.parquet'
with (FORMAT 'parquet');
copy customer
from '/home/pengzhen/Documents/tpch-parquet/customer.parquet'
with (FORMAT 'parquet');
copy orders
from '/home/pengzhen/Documents/tpch-parquet/orders.parquet'
with (FORMAT 'parquet');
copy lineitem
from '/home/pengzhen/Documents/tpch-parquet/lineitem.parquet'
with (FORMAT 'parquet');
"

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
