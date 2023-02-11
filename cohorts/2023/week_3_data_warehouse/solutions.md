1. Created using the UI - pointed to bucket location with wildcard 2019-*, as external table. 

```
SELECT count(*) FROM `dtc-de-course-374903.trips_data_all.fhv_tripdata` returns 43244696 
```

2. 

```
SELECT count(distinct Affiliated_base_number) FROM `dtc-de-course-374903.trips_data_all.fhv_tripdata`  
SELECT count(distinct Affiliated_base_number) FROM `dtc-de-course-374903.trips_data_all.fhv_tripdata_bq`
```

- 0 MB for the External Table and 317.94MB for the BQ Table 

3.

```
SELECT count(*) FROM `dtc-de-course-374903.trips_data_all.fhv_tripdata_bq` 
where PUlocationID is null and DOlocationID is null 
```
717748
4. - Partition by pickup_datetime Cluster on affiliated_base_number
5. 

```
CREATE OR REPLACE TABLE `dtc-de-course-374903.trips_data_all.fhv_tripdata_bq_partitioned` 
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS (
  SELECT * FROM `dtc-de-course-374903.trips_data_all.fhv_tripdata_bq` 
);
```

```
select count(distinct Affiliated_base_number)
from `dtc-de-course-374903.trips_data_all.fhv_tripdata_bq_partitioned` 
where pickup_datetime between '2019-03-01 00:00:00' and  '2019-03-31 00:00:00' 
```

- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table


6. GCP bucket
7. No