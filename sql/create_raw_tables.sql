-- ===================================================================
--  create_raw_tables.sql
--  สร้างตารางชั้น raw (landing zone) ของ warehouse `dw`
--
--  ชนิดข้อมูลตรงกับที่ประกาศไว้ใน my_dbt_project/seeds/_seeds.yml
--  (integer / varchar(100)) บวกคอลัมน์ loaded_at สำหรับ audit
--
--  ไฟล์นี้รันซ้ำได้ (idempotent) — ใช้ CREATE ... IF NOT EXISTS ทั้งหมด
--  รันอัตโนมัติ 2 ทาง
--    1. ตอน Postgres init ครั้งแรก (mount เป็น
--       /docker-entrypoint-initdb.d/03_create_raw_tables.sql)
--    2. ทุกครั้งที่ DAG dw_sample_pipeline ทำงาน (task create_raw_tables)
--
--  รันมือ:
--    docker compose exec -T dw_postgres \
--      psql -U dw_user -d dw -v ON_ERROR_STOP=1 -f /opt/sql/create_raw_tables.sql
-- ===================================================================

CREATE SCHEMA IF NOT EXISTS raw;

COMMENT ON SCHEMA raw IS
    'ชั้น raw (landing zone) - ข้อมูลดิบที่โหลดเข้ามาตรง ๆ จากไฟล์ CSV ยังไม่แปลงค่า';

-- -------------------------------------------------------------------
--  raw.stores
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.stores (
    store_id    integer         NOT NULL,
    city        varchar(100)    NOT NULL,
    loaded_at   timestamp       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_stores PRIMARY KEY (store_id)
);

COMMENT ON TABLE  raw.stores             IS 'ข้อมูลดิบรายสาขา โหลดจาก seeds/stores.csv';
COMMENT ON COLUMN raw.stores.store_id    IS 'รหัสสาขา - primary key มาจากไฟล์ต้นทาง';
COMMENT ON COLUMN raw.stores.city        IS 'ชื่อเมืองที่สาขาตั้งอยู่';
COMMENT ON COLUMN raw.stores.loaded_at   IS 'เวลาที่แถวนี้ถูกโหลดเข้า warehouse (audit)';

-- -------------------------------------------------------------------
--  raw.promotions
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.promotions (
    promotion_id    integer     NOT NULL,
    discount        integer     NOT NULL,
    loaded_at       timestamp   NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_promotions PRIMARY KEY (promotion_id)
);

COMMENT ON TABLE  raw.promotions              IS 'ข้อมูลดิบรายโปรโมชัน โหลดจาก seeds/promotions.csv';
COMMENT ON COLUMN raw.promotions.promotion_id IS 'รหัสโปรโมชัน - primary key มาจากไฟล์ต้นทาง';
COMMENT ON COLUMN raw.promotions.discount     IS 'ส่วนลดเป็นเปอร์เซ็นต์ (จำนวนเต็ม เช่น 24 = 24%)';
COMMENT ON COLUMN raw.promotions.loaded_at    IS 'เวลาที่แถวนี้ถูกโหลดเข้า warehouse (audit)';

-- -------------------------------------------------------------------
--  raw.categories
-- -------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.categories (
    category_id     integer         NOT NULL,
    category_name   varchar(100)    NOT NULL,
    loaded_at       timestamp       NOT NULL DEFAULT now(),
    CONSTRAINT pk_raw_categories PRIMARY KEY (category_id)
);

COMMENT ON TABLE  raw.categories               IS 'ข้อมูลดิบรายหมวดสินค้า โหลดจาก seeds/categories.csv';
COMMENT ON COLUMN raw.categories.category_id   IS 'รหัสหมวดสินค้า - primary key มาจากไฟล์ต้นทาง';
COMMENT ON COLUMN raw.categories.category_name IS 'ชื่อหมวดสินค้า';
COMMENT ON COLUMN raw.categories.loaded_at     IS 'เวลาที่แถวนี้ถูกโหลดเข้า warehouse (audit)';
