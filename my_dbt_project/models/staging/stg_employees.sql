-- note: salary is sensitive, so restrict access in marts if needed
select
    employee_id,
    store_id,
    salary
from {{ source('raw', 'employees') }}