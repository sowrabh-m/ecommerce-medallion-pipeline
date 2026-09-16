
  create or replace   view DE_ECOMMERCE_DB.silver.stg_order_items
  
  
  
  
  as (
    select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as line_amount
from DE_ECOMMERCE_DB.BRONZE.order_items
  );

