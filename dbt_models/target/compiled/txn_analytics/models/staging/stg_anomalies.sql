-- stg_anomalies.sql
-- Staging model: clean, typed anomaly events from Parquet export.

with raw as (
    select * from read_parquet('seeds/raw/anomalies.parquet')
),

deduplicated as (
    select
        *,
        row_number() over (partition by txn_id order by timestamp desc) as _rn
    from raw
)

select
    txn_id,
    user_id,
    cast(amount as double)                         as amount,
    currency,
    timestamp,
    merchant_id,
    merchant_category,
    payment_method,
    cast(z_score as double)                        as z_score,
    cast(user_running_mean as double)              as user_running_mean,
    cast(user_txn_count as integer)                as user_txn_count,

    -- Derived
    date_trunc('day', timestamp)                   as anomaly_date,
    abs(cast(z_score as double))                   as abs_z_score

from deduplicated
where _rn = 1