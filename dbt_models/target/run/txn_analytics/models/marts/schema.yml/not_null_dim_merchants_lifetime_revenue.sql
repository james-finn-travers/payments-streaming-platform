
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select lifetime_revenue
from "txn_analytics"."main"."dim_merchants"
where lifetime_revenue is null



  
  
      
    ) dbt_internal_test