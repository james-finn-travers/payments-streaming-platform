
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select status
from "txn_analytics"."main"."stg_transactions"
where status is null



  
  
      
    ) dbt_internal_test