SELECT 
    promotion_id,
    discount_pct
FROM {{ ref('stg_promotions') }}
UNION ALL
SELECT
    0 AS promotion_id,
    0 AS discount_pct
