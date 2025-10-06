{{ config(
    contract={"enforced": true}
) }}

-- Daily selection of worst performers for the Python automation script
-- This view is consumed by forecast_runner.py with enforced data contracts

SELECT 
    account_id::varchar as account_id,
    account_name::varchar as account_name, 
    post_optimization_mape::float as post_optimization_mape,
    retraining_priority::int as retraining_priority,
    CURRENT_TIMESTAMP()::timestamp_ntz as updated_at
FROM {{ ref('accounts_for_retraining') }}
WHERE retraining_priority <= 5  -- Top 5 worst performers
ORDER BY retraining_priority