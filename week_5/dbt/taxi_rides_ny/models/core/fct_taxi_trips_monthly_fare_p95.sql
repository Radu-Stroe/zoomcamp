{{
    config(
        materialized='table'
    )
}}

with filtered_trips as (
    -- Filter out invalid trips
    select 
        fare_amount,
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month
    from {{ ref('fact_trips') }}
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit card')
), percentiles as (
    -- Calculate continuous percentiles (97, 95, 90) for fare_amount by service_type, year, and month
    select 
        distinct service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) AS p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) AS p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) AS p90
    from filtered_trips
)
select 
    service_type,
    year,
    month,
    p97,
    p95,
    p90
from percentiles
order by year, month, service_type
