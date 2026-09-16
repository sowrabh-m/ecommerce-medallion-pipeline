
  
    

create or replace transient table DE_ECOMMERCE_DB.gold.mart_daily_revenue
    
    
    
    
    

    as (select
    order_date,
    count(distinct order_id) as order_count,
    sum(line_amount) as total_revenue
from DE_ECOMMERCE_DB.gold.fct_order_items
where status != 'cancelled'
group by order_date
order by order_date
    )
;


  