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