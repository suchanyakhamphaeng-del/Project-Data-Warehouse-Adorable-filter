WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),
returns AS (
    SELECT 
        order_item_id,
        SUM(refund_amount) AS refund_amount
    FROM {{ ref('stg_returns') }}
    GROUP BY order_item_id
),
payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),
order_item_subtotals AS (
    SELECT 
        order_id,
        SUM(quantity * unit_price) AS order_subtotal
    FROM order_items
    GROUP BY order_id
)
SELECT 
    oi.order_item_id,
    oi.order_id,
    o.customer_id,
    o.store_id,
    oi.product_id,
    COALESCE(o.promotion_id, 0) AS promotion_id,
    o.order_date,
    sh.shipment_status,
    oi.quantity AS qty,
    oi.unit_price,
    (oi.quantity * oi.unit_price) AS subtotal_amount,
    COALESCE(r.refund_amount, 0) AS refund_amount,
    p.payment_amount AS order_payment_amount,
    ROUND(
        (oi.quantity * oi.unit_price) / NULLIF(ots.order_subtotal, 0) * p.payment_amount, 
        2
    ) AS allocated_payment_amount
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN order_item_subtotals ots ON o.order_id = ots.order_id
LEFT JOIN shipments sh ON o.order_id = sh.order_id
LEFT JOIN returns r ON oi.order_item_id = r.order_item_id
LEFT JOIN payments p ON o.order_id = p.order_id
