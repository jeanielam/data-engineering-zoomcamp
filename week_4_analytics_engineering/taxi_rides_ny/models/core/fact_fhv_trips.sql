{{ config(materialized='table') }}

with fhv_data as (
    select *,
        'Fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
    where pickup_locationid is not null and dropoff_locationid is not null
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_data.tripid,
    fhv_data.dispatching_base_num,
    fhv_data.pickup_datetime,
    fhv_data.dropoff_datetime,
    fhv_data.SR_Flag,
    fhv_data.Affiliated_base_number,
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.zone as dropoff_zone
from
fhv_data
inner join dim_zones as pickup_zone
on fhv_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.dropoff_locationid = dropoff_zone.locationid
