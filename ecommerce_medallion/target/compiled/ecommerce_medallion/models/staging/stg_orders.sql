select
    order_id,
    customer_id,
    order_date,
    lower(trim(status)) as status
from DE_ECOMMERCE_DB.BRONZE.orders