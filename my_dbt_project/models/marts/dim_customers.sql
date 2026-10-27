SELECT 
    customer_id,
    customer_city,
    signup_date
FROM {{ ref('stg_customers') }}
