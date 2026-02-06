-- Create the external table
CREATE OR REPLACE EXTERNAL TABLE
  `de-zoomcamp-397401.zoomcamp.hw3`
  WITH CONNECTION DEFAULT
  OPTIONS(
    format ="PARQUET",
    uris = ['gs://dezoomcamp_hw3_2025_rp/yellow_tripdata_2024-*.parquet' ]
    );

-- Create the materialized table from the external table
CREATE OR REPLACE TABLE `de-zoomcamp-397401.zoomcamp.hw3-mt` AS
SELECT * FROM `de-zoomcamp-397401.zoomcamp.hw3`;

-- Question 1. Counting records
-- -- What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(*) FROM `de-zoomcamp-397401.zoomcamp.hw3-mt`;

--Question 2. Data read estimation
-- 0.00 MB and 0.00 MB

/*
Question 3. Why are the estimated number of Bytes different? (1 point)

A) BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed. 
*/

-- Question 4. How many records have a fare_amount of 0? (1 point)
SELECT COUNT(*) FROM `de-zoomcamp-397401.zoomcamp.hw3-mt` t where t.fare_amount = 0;
--8333

/*
Question 5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy) (1 point)

A) Partition by tpep_dropoff_datetime and Cluster on VendorID
*/

CREATE OR REPLACE TABLE `de-zoomcamp-397401.zoomcamp.hw3-partitoned-v2`
PARTITION BY 
  DATE(tpep_dropoff_datetime),
  VendorId
CLUSTER BY
  VendorId
  AS
SELECT * FROM `de-zoomcamp-397401.zoomcamp.hw3-mt`;

/*
Question 6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values? (1 point)


12.47 MB for non-partitioned table and 326.42 MB for the partitioned table

310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

5.87 MB for non-partitioned table and 0 MB for the partitioned table

310.31 MB for non-partitioned table and 285.64 MB for the partitioned table
*/
-- Partitioned
SELECT t.VendorID FROM `de-zoomcamp-397401.zoomcamp.hw3-mt` t where t.tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15' GROUP BY t.VendorID;
-- A) 310.24MB

-- Non partitioned
SELECT t.VendorID FROM `de-zoomcamp-397401.zoomcamp.hw3-partitoned` t where t.tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15' GROUP BY t.VendorID;
-- A) 26.84MB

/*
Question 7. Where is the data stored in the External Table you created? (1 point)

A) GCP Bucket
*/

/*
Question 8. It is best practice in Big Query to always cluster your data: (1 point)

A) False
*/


--Question 9. Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why? (not graded)


SELECT count(*) FROM `de-zoomcamp-397401.zoomcamp.hw3-mt` ;
