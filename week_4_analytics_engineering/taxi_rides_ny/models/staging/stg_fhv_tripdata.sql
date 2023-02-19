{{ config(materialized='view') }}

with fhv_data as (
    select * from {{ source('staging', 'fhv_tripdata') }}
    where pickup_datetime < "2020-01-01"
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
