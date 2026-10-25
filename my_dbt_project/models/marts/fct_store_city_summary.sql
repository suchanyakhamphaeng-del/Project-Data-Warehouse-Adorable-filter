select
    store_city as city,
    count(*) as store_count,
    min(store_id) as first_store_id,
    max(store_id) as last_store_id,
    current_timestamp as loaded_at
from {{ ref('stg_stores') }}
group by store_city
order by store_count desc, city
