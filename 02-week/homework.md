# Homework Week 2

## Bigquery

```SQL
-- Yellow rows for 2020
select count(*) from `de-zoomcamp-397401.zoomcamp.yellow_tripdata` y where left(right(y.filename, 11),4) = '2020'
-- Green rows for 2020
select count(*) from `de-zoomcamp-397401.zoomcamp.green_tripdata` g where left(right(g.filename, 11),4) = '2020'
-- Yellow rows for 2021-03
select count(*) from `de-zoomcamp-397401.zoomcamp.yellow_tripdata` y where left(right(y.filename, 11),7) = '2021-03'
```
