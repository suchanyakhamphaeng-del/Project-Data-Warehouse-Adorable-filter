WITH products AS (
    SELECT * FROM {{ ref('products') }}
),
categories AS (
    SELECT * FROM {{ ref('categories') }}
),
suppliers AS (
    SELECT * FROM {{ ref('suppliers') }}
)
SELECT 
    p.product_id,
    p.price AS default_price,
    c.category_id,
    c.category_name,
    s.supplier_id,
    s.country AS supplier_country
FROM products p
LEFT JOIN categories c ON p.category_id = c.category_id
LEFT JOIN suppliers s ON p.supplier_id = s.supplier_id