select
    order_id,
    customer_id,
    store_id,
    order_date,
    promotion_id
from {{ source('raw', 'orders') }}