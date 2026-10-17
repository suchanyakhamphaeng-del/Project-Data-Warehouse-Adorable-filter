select
    shipment_id,
    order_id,
    lower(trim(status)) as shipment_status
from {{ source('raw', 'shipments') }}