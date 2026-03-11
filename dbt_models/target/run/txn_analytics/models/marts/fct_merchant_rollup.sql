
  
    
    

    create  table
      "txn_analytics"."main"."fct_merchant_rollup__dbt_tmp"
  
    as (
      -- fct_merchant_rollup.sql
-- Rolling merchant performance: recent window vs lifetime.

with txns as (
    select * from "txn_analytics"."main"."stg_transactions"
    where status = 'approved'
),

-- Last-30-day window
recent as (
    select
        merchant_id,
        merchant_category,
        count(*)                            as recent_txn_count,
        sum(amount)                         as recent_revenue,
        avg(amount)                         as recent_avg_amount,
        count(distinct user_id)             as recent_unique_users
    from txns
    where txn_date >= current_date - interval '30' day
    group by merchant_id, merchant_category
),

-- Lifetime
lifetime as (
    select
        merchant_id,
        count(*)                            as lifetime_txn_count,
        sum(amount)                         as lifetime_revenue
    from txns
    group by merchant_id
)

select
    r.merchant_id,
    r.merchant_category,

    -- Recent metrics
    r.recent_txn_count,
    round(r.recent_revenue, 2)              as recent_revenue,
    round(r.recent_avg_amount, 2)           as recent_avg_amount,
    r.recent_unique_users,

    -- Lifetime context
    l.lifetime_txn_count,
    round(l.lifetime_revenue, 2)            as lifetime_revenue,

    -- Share of lifetime happening recently
    case
        when l.lifetime_revenue > 0
        then round(r.recent_revenue / l.lifetime_revenue * 100, 2)
        else 0
    end                                     as pct_revenue_last_30d

from recent r
join lifetime l using (merchant_id)
order by recent_revenue desc
    );
  
  