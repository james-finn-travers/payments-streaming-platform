-- stg_transactions.sql
-- Staging model: clean, typed, deduplicated transactions from Parquet export.

with raw as (
    select * from read_parquet('seeds/raw/transactions.parquet')
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
    status,
    location_lat,
    location_lon,
    cast(user_txn_count as integer)                as user_txn_count,
    cast(user_running_mean as double)              as user_running_mean,
    cast(z_score as double)                        as z_score,
    cast(is_anomaly as boolean)                    as is_anomaly,

    -- Derived fields
    date_trunc('day', timestamp)                   as txn_date,
    date_trunc('hour', timestamp)                  as txn_hour,
    extract(dow from timestamp)                    as day_of_week,
    extract(hour from timestamp)                   as hour_of_day

from deduplicated
where _rn = 1