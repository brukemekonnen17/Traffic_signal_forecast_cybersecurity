{{ config(materialized='view') }}

-- Identify accounts that need model retraining based on MAPE performance
-- Excludes accounts that have already been processed this month

with account_performance as (
    select *
    from {{ ref('account_performance') }}
),

recent_runs as (
    select 
        account_id,
        max(run_date) as last_run_date
    from {{ source('traffic_forecast', 'forecast_rerun_flags') }}
    where run_date >= date_trunc('month', current_date())
    group by account_id
),

eligible_accounts as (
    select 
        ap.*,
        rr.last_run_date,
        
        -- Check if account was already processed this month
        case 
            when rr.last_run_date is not null then false
            else true
        end as eligible_for_retraining,
        
        -- Priority scoring (higher = worse performance, needs attention)
        row_number() over (
            order by ap.post_optimization_mape desc
        ) as retraining_priority
        
    from account_performance ap
    left join recent_runs rr 
        on ap.account_id = rr.account_id
),

final as (
    select 
        account_id,
        account_name,
        post_optimization_mape,
        performance_category,
        traffic_variability,
        total_traffic,
        avg_daily_traffic,
        eligible_for_retraining,
        retraining_priority,
        last_run_date,
        
        -- Business rules for retraining eligibility
        case 
            when not eligible_for_retraining then 'Already processed this month'
            when post_optimization_mape > 0.4 then 'High priority - Poor performance'
            when post_optimization_mape > 0.25 then 'Medium priority - Fair performance'
            else 'Low priority - Good performance'
        end as retraining_reason
        
    from eligible_accounts
    where eligible_for_retraining = true
      and post_optimization_mape is not null
)

select * from final