
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select anomaly_date
from "txn_analytics"."main"."fct_anomaly_summary"
where anomaly_date is null



  
  
      
    ) dbt_internal_test