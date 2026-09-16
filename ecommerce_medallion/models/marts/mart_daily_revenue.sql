select
    order_date,
    count(distinct order_id) as order_count,
    sum(line_amount) as total_revenue
from {{ ref('fct_order_items') }}
where status != 'cancelled'
group by order_date
order by order_date
