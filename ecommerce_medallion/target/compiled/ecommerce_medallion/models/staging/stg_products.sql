select
    product_id,
    trim(product_name) as product_name,
    trim(category) as category,
    unit_price
from DE_ECOMMERCE_DB.BRONZE.products