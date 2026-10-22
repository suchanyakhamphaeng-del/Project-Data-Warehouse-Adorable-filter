select *
from {{ ref('promotions') }}
where discount < 0 or discount > 100