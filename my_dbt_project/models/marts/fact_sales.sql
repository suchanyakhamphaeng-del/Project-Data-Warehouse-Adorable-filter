WITH orders AS (
    SELECT * FROM {{ ref('orders') }}
),
order_items AS (
    SELECT * FROM {{ ref('order_items') }}
),
shipments AS (
    SELECT * FROM {{ ref('shipments') }}
),
returns AS (
    SELECT * FROM {{ ref('returns') }}
),
payments AS (
    SELECT * FROM {{ ref('payments') }}
)
SELECT 
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    o.store_id,
    oi.product_id,
    o.promotion_id,
    o.order_date,
    sh.status AS shipment_status,
    oi.qty,
    oi.price AS unit_price,
    (oi.qty * oi.price) AS subtotal_amount,
    COALESCE(r.refund, 0) AS refund_amount,
    p.amount AS order_payment_amount
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN shipments sh ON o.order_id = sh.order_id
LEFT JOIN returns r ON oi.order_item_id = r.order_item_id
LEFT JOIN payments p ON o.order_id = p.order_id