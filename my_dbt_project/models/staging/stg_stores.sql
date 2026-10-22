select
    store_id,
    trim(city) as store_city
from {{ ref('stores') }}