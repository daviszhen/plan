# plan

| tpch 1g qX | status                                                                                  |
|------------|-----------------------------------------------------------------------------------------|
| q1         | right                                                                                   |
| q2         | right                                                                                   |
| q3         | right                                                                                   |
| q4         | right (result same as Duckdb)                                                           |
| q5         | right                                                                                   |
| q6         | right                                                                                   |
| q7         | right                                                                                   |
| q8         | right                                                                                   |
| q9         | right                                                                                   |
| q10        | right (use topN further)                                                                |
| q11        | right                                                                                   |
| q12        | right                                                                                   |
| q13        | almost right (value of COUNT(NULL) is different. duckdb convert left join to inner join |
| q14        | right                                                                                   |
| q15        | right                                                                                   |
| q16        | right(even without distinct);                                                           |
| q17        | right                                                                                   |
| q18        | right                                                                                   |
| q19        | right                                                                                   |
| q20        | right                                                                                   |
| q21        | right                                                                                   |
| q22        | right                                                                                   |


# tester

配置文件`tester.toml`

配置文件搜索路径。以下路径都没有配置文件，报错退出。
```text
./
./etc/tpch/1g/
```

## tpch1g



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