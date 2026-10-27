WITH stores AS (
    SELECT * FROM {{ ref('stg_stores') }}
),
employees AS (
    SELECT 
        store_id,
        COUNT(employee_id) AS employee_count,
        AVG(salary) AS avg_employee_salary
    FROM {{ ref('stg_employees') }}
    GROUP BY store_id
)
SELECT 
    s.store_id,
    s.store_city,
    COALESCE(e.employee_count, 0) AS employee_count,
    ROUND(COALESCE(e.avg_employee_salary, 0), 2) AS avg_employee_salary
FROM stores s
LEFT JOIN employees e ON s.store_id = e.store_id
