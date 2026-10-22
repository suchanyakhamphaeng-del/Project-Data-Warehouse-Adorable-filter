select
    customer_id,
    trim(city) as customer_city,
    signup_date
from {{ source('raw', 'customers') }}