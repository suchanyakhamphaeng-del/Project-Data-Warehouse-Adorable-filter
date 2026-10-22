select
    category_id,
    trim(category_name) as category_name
from {{ ref('categories') }}