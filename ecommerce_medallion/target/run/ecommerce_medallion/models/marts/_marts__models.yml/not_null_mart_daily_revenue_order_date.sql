
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_date
from DE_ECOMMERCE_DB.gold.mart_daily_revenue
where order_date is null



  
  
      
    ) dbt_internal_test