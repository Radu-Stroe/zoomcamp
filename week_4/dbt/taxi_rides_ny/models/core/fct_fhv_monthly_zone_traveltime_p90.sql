{{
    config(
        materialized='table'
    )
}}

with trips as (
    select 
        pickup_location_id, 
        dropoff_location_id, 
        trip_year, 
        trip_month,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration
    from {{ ref('dim_fhv_trips') }}
), 

p90_trip_duration as (
    select
        distinct trip_year,
        trip_month,
        pickup_location_id,
        dropoff_location_id,
        PERCENTILE_CONT(trip_duration, 0.9) OVER (
            PARTITION BY trip_year, trip_month, pickup_location_id, dropoff_location_id
        ) as p90_trip_duration  
    from trips
),

dim_zones as (
    select 
        locationid,
        borough,
        zone
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    p90_trip_duration.trip_year,
    p90_trip_duration.trip_month,
    p90_trip_duration.p90_trip_duration,
    pickup_zone.zone as pickup_zone, 
    pickup_zone.borough as pickup_borough, 
    dropoff_zone.zone as dropoff_zone,
    dropoff_zone.borough as dropoff_borough
from p90_trip_duration
inner join dim_zones as pickup_zone
    on p90_trip_duration.pickup_location_id = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on p90_trip_duration.dropoff_location_id = dropoff_zone.locationid
