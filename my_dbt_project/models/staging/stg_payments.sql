select
    payment_id,
    order_id,
    amount as payment_amount
from {{ source('raw', 'payments') }}