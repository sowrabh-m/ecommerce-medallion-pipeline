select
    order_id,
    customer_id,
    order_date,
    lower(trim(status)) as status
from {{ source('bronze', 'orders') }}
