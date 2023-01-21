# --iidfile string
# 3

Command to ingest data
```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz


```


```sql
3.
SELECT * FROM public.green_taxi_trips
where lpep_pickup_datetime between '2019-01-15 00:00:00'::timestamp 
AND '2019-01-16 00:00:00'::timestamp;

4.
select max(trip_distance), date_trunc('day', lpep_pickup_datetime)  from public.green_taxi_trips group by date_trunc('day', lpep_pickup_datetime) order by max(trip_distance) desc;

5.
select count(*), passenger_count from public.green_taxi_trips where 
(passenger_count = 2 or passenger_count = 3) 
and date_trunc('day', lpep_pickup_datetime) = date '2019-01-01'
group by passenger_count

6.
select * from public.taxi_zone_lookup where "Zone" = "Astoria";
select max(tip_amount),"DOLocationID"  from public.green_taxi_trips where "PULocationID" = 7
group by "DOLocationID"
order by max(tip_amount) desc;
select * from public.taxi_zone_lookup where "LocationID" = 146;
```