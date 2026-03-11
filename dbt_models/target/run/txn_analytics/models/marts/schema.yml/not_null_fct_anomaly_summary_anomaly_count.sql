
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select anomaly_count
from "txn_analytics"."main"."fct_anomaly_summary"
where anomaly_count is null



  
  
      
    ) dbt_internal_test