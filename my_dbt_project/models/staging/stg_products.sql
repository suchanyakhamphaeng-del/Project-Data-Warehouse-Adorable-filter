select
    product_id,
    category_id,
    supplier_id,
    price as unit_price
from {{ source('raw', 'products') }}