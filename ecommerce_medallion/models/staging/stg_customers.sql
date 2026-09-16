select
    customer_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    lower(trim(email)) as email,
    signup_date,
    trim(city) as city,
    trim(country) as country
from {{ source('bronze', 'customers') }}
