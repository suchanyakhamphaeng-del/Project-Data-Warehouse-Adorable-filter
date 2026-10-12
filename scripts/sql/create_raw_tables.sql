-- 1. Create Schema
CREATE SCHEMA IF NOT EXISTS raw;

COMMENT ON SCHEMA raw IS 'ชั้น raw (landing zone) - ข้อมูลดิบที่โหลดเข้ามาตรง ๆ จากไฟล์ CSV';

-- -------------------------------------------------------------------
-- Seeds / Lookups (Person 1 Format)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.stores (
    store_id    INTEGER         NOT NULL,
    city        VARCHAR(100)    NOT NULL,
    loaded_at   TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_stores PRIMARY KEY (store_id)
);

CREATE TABLE IF NOT EXISTS raw.promotions (
    promotion_id    INTEGER     NOT NULL,
    discount        INTEGER     NOT NULL,
    loaded_at       TIMESTAMP   NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_promotions PRIMARY KEY (promotion_id)
);

CREATE TABLE IF NOT EXISTS raw.categories (
    category_id     INTEGER         NOT NULL,
    category_name   VARCHAR(100)    NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_categories PRIMARY KEY (category_id)
);

-- -------------------------------------------------------------------
-- Transactional Tables (With loaded_at + Primary Keys)
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        INTEGER         NOT NULL,
    customer_id     INTEGER         NOT NULL,
    store_id        INTEGER         NOT NULL,
    order_date      DATE            NOT NULL,
    promotion_id    INTEGER,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_orders PRIMARY KEY (order_id)
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id   INTEGER         NOT NULL,
    order_id        INTEGER         NOT NULL,
    product_id      INTEGER         NOT NULL,
    qty             INTEGER         NOT NULL,
    price           NUMERIC(10,2)   NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_order_items PRIMARY KEY (order_item_id)
);

CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id      INTEGER         NOT NULL,
    order_id        INTEGER         NOT NULL,
    amount          NUMERIC(10,2)   NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_payments PRIMARY KEY (payment_id)
);

CREATE TABLE IF NOT EXISTS raw.shipments (
    shipment_id     INTEGER         NOT NULL,
    order_id        INTEGER         NOT NULL,
    status          VARCHAR(100)    NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_shipments PRIMARY KEY (shipment_id)
);

CREATE TABLE IF NOT EXISTS raw.returns (
    return_id       INTEGER         NOT NULL,
    order_item_id   INTEGER         NOT NULL,
    refund          NUMERIC(10,2)   NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_returns PRIMARY KEY (return_id)
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id     INTEGER         NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    signup_date     DATE            NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_customers PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id      INTEGER         NOT NULL,
    category_id     INTEGER         NOT NULL,
    supplier_id     INTEGER         NOT NULL,
    price           NUMERIC(10,2)   NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_products PRIMARY KEY (product_id)
);

CREATE TABLE IF NOT EXISTS raw.employees (
    employee_id     INTEGER         NOT NULL,
    store_id        INTEGER         NOT NULL,
    salary          INTEGER         NOT NULL,
    loaded_at       TIMESTAMP       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_employees PRIMARY KEY (employee_id)
);

-- -------------------------------------------------------------------
-- 3. Truncate for Idempotency
-- -------------------------------------------------------------------
TRUNCATE TABLE 
    raw.stores,
    raw.promotions,
    raw.categories,
    raw.orders,
    raw.order_items,
    raw.payments,
    raw.shipments,
    raw.returns,
    raw.customers,
    raw.products,
    raw.employees CASCADE;