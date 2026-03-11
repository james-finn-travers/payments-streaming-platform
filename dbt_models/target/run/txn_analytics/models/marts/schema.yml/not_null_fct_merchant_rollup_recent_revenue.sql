
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select recent_revenue
from "txn_analytics"."main"."fct_merchant_rollup"
where recent_revenue is null



  
  
      
    ) dbt_internal_test