{{ config(materialized='view') }}

-- Staging model for traffic forecast data
-- Cleans and standardizes the raw traffic data

with source_data as (
    select 
        date,
        account_name,
        product,
        account_id,
        customer_id,
        traffic,
        -- Add some derived fields
        extract(year from date) as year,
        extract(month from date) as month,
        extract(dayofweek from date) as day_of_week,
        extract(dayofyear from date) as day_of_year
    from {{ source('traffic_forecast', 'stg_traffic_forecast') }}
),

final as (
    select 
        *,
        -- Add business logic
        case 
            when day_of_week in (1, 7) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        -- Calculate rolling metrics if needed
        lag(traffic, 1) over (
            partition by account_id, product 
            order by date
        ) as previous_day_traffic,
        
        -- Add row number for each account/product combination
        row_number() over (
            partition by account_id, product 
            order by date
        ) as day_sequence
        
    from source_data
)

select * from final