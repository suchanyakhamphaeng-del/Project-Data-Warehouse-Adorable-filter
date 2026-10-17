select
    return_id,
    order_item_id,
    refund as refund_amount
from {{ source('raw', 'returns') }}