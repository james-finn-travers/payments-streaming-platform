
    
    

select
    merchant_id as unique_field,
    count(*) as n_records

from "txn_analytics"."main"."fct_merchant_rollup"
where merchant_id is not null
group by merchant_id
having count(*) > 1


