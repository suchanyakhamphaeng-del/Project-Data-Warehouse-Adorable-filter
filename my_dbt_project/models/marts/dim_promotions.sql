SELECT 
    promotion_id,
    discount AS discount_amount
FROM {{ ref('promotions') }}