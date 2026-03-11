
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from "txn_analytics"."main"."stg_transactions"
    group by status

)

select *
from all_values
where value_field not in (
    'approved','declined','pending'
)



  
  
      
    ) dbt_internal_test