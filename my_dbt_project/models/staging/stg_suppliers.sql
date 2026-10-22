select
    supplier_id,
    trim(country) as supplier_country
from {{ ref('suppliers') }}