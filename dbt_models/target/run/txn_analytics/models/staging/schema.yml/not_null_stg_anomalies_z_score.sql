
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select z_score
from "txn_analytics"."main"."stg_anomalies"
where z_score is null



  
  
      
    ) dbt_internal_test