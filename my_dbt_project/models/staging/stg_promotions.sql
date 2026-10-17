select
    promotion_id,
    discount as discount_pct
from {{ ref('promotions') }}