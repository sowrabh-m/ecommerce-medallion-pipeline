select
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    o.status,
    oi.quantity,
    oi.unit_price,
    oi.line_amount
from DE_ECOMMERCE_DB.silver.stg_order_items oi
join DE_ECOMMERCE_DB.silver.stg_orders o on oi.order_id = o.order_id