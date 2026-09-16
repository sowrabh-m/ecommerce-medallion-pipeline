
  
    

create or replace transient table DE_ECOMMERCE_DB.gold.dim_products
    
    
    
    
    

    as (select * from DE_ECOMMERCE_DB.silver.stg_products
    )
;


  