-- dim_merchants.sql
-- Merchant dimension table with lifetime stats.

with txns as (
    select * from {{ ref('stg_transactions') }}
    where status = 'approved'
),

merchant_stats as (
    select
        merchant_id,
        merchant_category,

        count(*)                            as lifetime_txn_count,
        sum(amount)                         as lifetime_revenue,
        avg(amount)                         as avg_txn_amount,
        min(amount)                         as min_txn_amount,
        max(amount)                         as max_txn_amount,
        count(distinct user_id)             as unique_customers,
        count(distinct currency)            as currency_count,
        min(timestamp)                      as first_txn_at,
        max(timestamp)                      as last_txn_at

    from txns
    group by merchant_id, merchant_category
),

anomaly_rates as (
    select
        merchant_id,
        count(*)                            as anomaly_count
    from {{ ref('stg_anomalies') }}
    group by merchant_id
)

select
    m.*,
    coalesce(a.anomaly_count, 0)            as anomaly_count,
    case
        when m.lifetime_txn_count > 0
        then round(coalesce(a.anomaly_count, 0) * 100.0 / m.lifetime_txn_count, 4)
        else 0
    end                                     as anomaly_rate_pct

from merchant_stats m
left join anomaly_rates a using (merchant_id)
order by lifetime_revenue desc
