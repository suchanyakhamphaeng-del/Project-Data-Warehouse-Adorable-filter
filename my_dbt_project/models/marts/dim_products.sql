WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),
categories AS (
    SELECT * FROM {{ ref('stg_categories') }}
),
suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
)
SELECT 
    p.product_id,
    p.unit_price AS default_price,
    c.category_id,
    c.category_name,
    s.supplier_id,
    s.supplier_country
FROM products p
LEFT JOIN categories c ON p.category_id = c.category_id
LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id
