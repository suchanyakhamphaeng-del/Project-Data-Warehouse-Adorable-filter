select
    order_item_id,
    order_id,
    product_id,
    qty   as quantity,
    price as unit_price
from {{ source('raw', 'order_items') }}