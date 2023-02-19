1. 61541183 closest answer is 61648442
2. 89.8/10.2 closest answer is 89.9/10.1
3. 43244696

```
{{ config(materialized='view') }}

with fhv_data as (
    select * from {{ source('staging', 'fhv_tripdata') }}
    where pickup_datetime <= "2020-01-01"
) 
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    dispatching_base_num,
    SR_Flag,
    Affiliated_base_number
from fhv_data

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}

```

4. 22998722

```
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

```

5. January
