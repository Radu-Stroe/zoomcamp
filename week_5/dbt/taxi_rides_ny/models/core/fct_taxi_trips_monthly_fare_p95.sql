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
        service_type,
        year,
        month,
        -- Using APPROX_QUANTILES to calculate percentiles
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] as p97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] as p95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] as p90
    from filtered_trips
    group by service_type, year, month
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
