
  
    
    

    create  table
      "txn_analytics"."main"."fct_anomaly_summary__dbt_tmp"
  
    as (
      -- fct_anomaly_summary.sql
-- Daily anomaly summary with severity breakdown.

with anomalies as (
    select * from "txn_analytics"."main"."stg_anomalies"
),

daily as (
    select
        anomaly_date,
        merchant_category,

        count(*)                                             as anomaly_count,
        count(distinct user_id)                              as affected_users,
        count(distinct merchant_id)                          as affected_merchants,
        avg(abs_z_score)                                     as avg_z_score,
        max(abs_z_score)                                     as max_z_score,
        sum(amount)                                          as flagged_amount,

        -- Severity buckets
        count(*) filter (where abs_z_score between 3 and 4)  as mild_count,
        count(*) filter (where abs_z_score between 4 and 6)  as moderate_count,
        count(*) filter (where abs_z_score > 6)              as severe_count

    from anomalies
    group by anomaly_date, merchant_category
)

select
    *,
    -- 7-day moving average of anomaly count
    avg(anomaly_count) over (
        partition by merchant_category
        order by anomaly_date
        rows between 6 preceding and current row
    )                                                        as rolling_7d_avg_anomalies

from daily
order by anomaly_date desc, anomaly_count desc
    );
  
  