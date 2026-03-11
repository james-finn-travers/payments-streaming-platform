
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select txn_count
from "txn_analytics"."main"."fct_daily_revenue"
where txn_count is null



  
  
      
    ) dbt_internal_test