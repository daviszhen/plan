# plan

注意：在忽略精度和结果标题前提下，进行对比

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
| q16        | almost right. (no distinct)                            | n                          | n (与duckdb,mo都不同。duckdb与mo相同) |
| q17        | right                                                  | y                          | y                             |
| q18        | right                                                  | y                          | y                             |
| q19        | right                                                  | y                          | y                             |
| q20        | right                                                  | n (s_address 不同)           | y                             |
| q21        | right                                                  | y                          | y                             |
| q22        | right                                                  | y                          | y                             |


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