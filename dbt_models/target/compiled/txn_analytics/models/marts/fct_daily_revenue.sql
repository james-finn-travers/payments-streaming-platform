-- fct_daily_revenue.sql
-- Daily revenue aggregated by merchant category and currency.

with txns as (
    select * from "txn_analytics"."main"."stg_transactions"
    where status = 'approved'
),

daily as (
    select
        txn_date,
        merchant_category,
        currency,

        count(*)                                        as txn_count,
        sum(amount)                                     as total_revenue,
        avg(amount)                                     as avg_txn_amount,
        min(amount)                                     as min_txn_amount,
        max(amount)                                     as max_txn_amount,
        count(distinct user_id)                         as unique_users,
        count(distinct merchant_id)                     as unique_merchants

    from txns
    group by txn_date, merchant_category, currency
),

with_rolling as (
    select
        *,
        sum(total_revenue) over (
            partition by merchant_category, currency
            order by txn_date
            rows between 6 preceding and current row
        )                                               as rolling_7d_revenue,

        avg(total_revenue) over (
            partition by merchant_category, currency
            order by txn_date
            rows between 29 preceding and current row
        )                                               as rolling_30d_avg_revenue,

        -- Day-over-day growth
        lag(total_revenue) over (
            partition by merchant_category, currency
            order by txn_date
        )                                               as prev_day_revenue

    from daily
)

select
    *,
    case
        when prev_day_revenue > 0
        then round((total_revenue - prev_day_revenue) / prev_day_revenue * 100, 2)
        else null
    end                                                 as dod_growth_pct

from with_rolling
order by txn_date desc, total_revenue desc