--Question 3. Q3: Count of records in fct_monthly_zone_revenue? (1 point)
select count(*) from `de-zoomcamp-397401.zoomcamp.fct_monthly_zone_revenue`
--15,421


-- Question 4. Q4: Zone with highest revenue for Green taxis in 2020? (1 point)
select * from `de-zoomcamp-397401.zoomcamp.fct_monthly_zone_revenue` m where EXTRACT(YEAR FROM m.revenue_month) = 2020 and m.service_type = 'Green'
order by m.revenue_monthly_total_amount desc
limit 1
--East Harlem North

--Question 5. Q5: Total trips for Green taxis in October 2019? (1 point)
select count(*) from `de-zoomcamp-397401.zoomcamp.fct_trips` m where EXTRACT(YEAR FROM m.pickup_datetime) = 2019 and EXTRACT(MONTH FROM m.pickup_datetime) = 10 and m.service_type = 'Green'
--384,624

--Question 6. Q6: Count of records in stg_fhv_tripdata (filter dispatching_base_num IS NULL)? (1 point)
select count(*) from `de-zoomcamp-397401.zoomcamp.stg_fh
--43,244,693

