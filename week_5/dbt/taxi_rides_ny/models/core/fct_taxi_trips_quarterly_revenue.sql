{{ config(materialized='table') }}

with filtered_trips as (
    -- Keep only data for 2019 and 2020
    select *
    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) in (2019, 2020)
),

quarterly_revenue as (
    -- Compute total revenue per service type, year, and quarter
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        sum(total_amount) as quarterly_total_amount
    from filtered_trips
    group by service_type, year, quarter
),

yoy_growth as (
    -- Compute Year-over-Year (YoY) revenue growth
    select
        curr.service_type,
        curr.year,
        curr.quarter,
        curr.quarterly_total_amount,
        prev.quarterly_total_amount as prev_year_quarterly_total_amount,
        case 
            when prev.quarterly_total_amount is not null 
            then round((curr.quarterly_total_amount - prev.quarterly_total_amount) / prev.quarterly_total_amount * 100, 2)
            else null
        end as quarterly_revenue_yoy_growth
    from quarterly_revenue curr
    left join quarterly_revenue prev
        on curr.service_type = prev.service_type
        and curr.year = prev.year + 1
        and curr.quarter = prev.quarter
    where curr.year = 2020 -- Only keep 2020 data for YoY calculations
),

best_worst_quarters as (
    -- Identify best & worst quarterly growth for each service type in 2020
    select
        service_type,
        max_by(struct(quarter, quarterly_revenue_yoy_growth), quarterly_revenue_yoy_growth).quarter as best_quarter,
        min_by(struct(quarter, quarterly_revenue_yoy_growth), quarterly_revenue_yoy_growth).quarter as worst_quarter
    from yoy_growth
    group by service_type
)

select * from yoy_growth