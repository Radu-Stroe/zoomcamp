with raw_fhv_data as (
    -- Extract data for 2019 and filter out rows where dispatching_base_num is not null
    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        PUlocationID as pickup_location_id,
        DOlocationID as dropoff_location_id,
        SR_Flag,
        Affiliated_base_number
    from `dbt-sandbox-450506`.`trips_data_all`.`fhv_tripdata`
    where dispatching_base_num is not null  -- Only include records where dispatching_base_num is null
)

select 
    *
from raw_fhv_data


