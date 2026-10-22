SELECT 
    customer_id,
    city AS customer_city,
    signup_date
FROM {{ ref('customers') }}