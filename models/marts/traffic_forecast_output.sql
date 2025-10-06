{{ config(materialized='table') }}

-- Final output model for traffic forecast pipeline
-- This model takes the identified accounts for retraining and creates forecast output records
-- In a real scenario, this would integrate with your ML forecasting model

with accounts_needing_retraining as (
    select *
    from {{ ref('accounts_for_retraining') }}
    where retraining_priority <= 3  -- Top 3 priorities
),

-- Get recent traffic data for these accounts
recent_traffic as (
    select 
        account_id,
        account_name,
        product,
        date,
        traffic,
        -- Create simple forecast using trend (in real scenario, use your ML model)
        traffic * (1.05 + (random() * 0.1 - 0.05)) as forecasted_traffic
    from {{ ref('stg_traffic_data') }}
    where account_id in (
        select account_id 
        from accounts_needing_retraining
    )
),

-- Generate forecast output for next 30 days
forecast_output as (
    select 
        rt.account_id,
        rt.account_name,
        rt.product,
        dateadd('day', seq.seq_num, current_date()) as ds,  -- forecast date
        rt.forecasted_traffic as yhat,
        'v2.0_retraining_priority_' || anr.retraining_priority::string as model_version,
        'automated_retraining_pipeline' as param_source,
        current_date() as snapshot_date
    from recent_traffic rt
    join accounts_needing_retraining anr 
        on rt.account_id = anr.account_id
    cross join (
        select row_number() over (order by null) - 1 as seq_num
        from table(generator(rowcount => 30))  -- Generate 30 days of forecasts
    ) seq
    -- Use latest traffic data as baseline
    qualify row_number() over (
        partition by rt.account_id, rt.product 
        order by rt.date desc
    ) = 1
)

select 
    account_id,
    account_name,
    product,
    ds,
    round(yhat, 2) as yhat,
    model_version,
    param_source,
    snapshot_date
from forecast_output
order by account_id, product, ds