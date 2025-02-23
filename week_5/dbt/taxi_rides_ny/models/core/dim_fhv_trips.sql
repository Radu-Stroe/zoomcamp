{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select 
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,  
        dropoff_location_id, 
        SR_Flag,
        Affiliated_base_number,
        EXTRACT(YEAR FROM pickup_datetime) AS trip_year,  -- ✅ Extract year
        EXTRACT(MONTH FROM pickup_datetime) AS trip_month, -- ✅ Extract month
        'FHV' as service_type  
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv_tripdata.dispatching_base_num,
    fhv_tripdata.service_type,
    fhv_tripdata.pickup_location_id, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv_tripdata.dropoff_location_id,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhv_tripdata.pickup_datetime, 
    fhv_tripdata.dropoff_datetime, 
    fhv_tripdata.trip_year,  -- ✅ Added trip_year
    fhv_tripdata.trip_month, -- ✅ Added trip_month
    fhv_tripdata.SR_Flag,
    fhv_tripdata.Affiliated_base_number
    from fhv_tripdata
    inner join dim_zones as pickup_zone
    on fhv_tripdata.pickup_location_id = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_location_id = dropoff_zone.locationid