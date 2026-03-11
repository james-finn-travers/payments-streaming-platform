
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp
from "txn_analytics"."main"."stg_transactions"
where timestamp is null



  
  
      
    ) dbt_internal_test