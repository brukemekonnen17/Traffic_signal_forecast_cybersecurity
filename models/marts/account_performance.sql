{{ config(materialized='table') }}

-- Account performance analysis combining traffic data with MAPE signals
-- This model identifies accounts with poor forecast performance

with traffic_summary as (
    select 
        account_id,
        account_name,
        count(distinct product) as product_count,
        count(distinct date) as days_tracked,
        sum(traffic) as total_traffic,
        avg(traffic) as avg_daily_traffic,
        max(traffic) as max_traffic,
        min(traffic) as min_traffic,
        stddev(traffic) as traffic_stddev
    from {{ ref('stg_traffic_data') }}
    group by account_id, account_name
),

signal_data as (
    select 
        account_id,
        account_name,
        snapshot_datetime,
        "MEAN ABSOLUTE PERCENTAGE ERROR":POST_OPTIMIZATION::float as post_optimization_mape
    from {{ source('traffic_forecast', 'traffic_forecast_signal') }}
),

final as (
    select 
        t.account_id,
        t.account_name,
        t.product_count,
        t.days_tracked,
        t.total_traffic,
        t.avg_daily_traffic,
        t.max_traffic,
        t.min_traffic,
        t.traffic_stddev,
        
        -- MAPE performance metrics
        s.post_optimization_mape,
        s.snapshot_datetime,
        
        -- Performance categorization
        case 
            when s.post_optimization_mape > 0.4 then 'Poor'
            when s.post_optimization_mape > 0.25 then 'Fair'
            when s.post_optimization_mape > 0.15 then 'Good'
            else 'Excellent'
        end as performance_category,
        
        -- Traffic variability indicator
        case 
            when t.traffic_stddev / nullif(t.avg_daily_traffic, 0) > 0.3 then 'High Variability'
            when t.traffic_stddev / nullif(t.avg_daily_traffic, 0) > 0.15 then 'Medium Variability'
            else 'Low Variability'
        end as traffic_variability
        
    from traffic_summary t
    left join signal_data s 
        on t.account_id = s.account_id
)

select * from final
order by post_optimization_mape desc