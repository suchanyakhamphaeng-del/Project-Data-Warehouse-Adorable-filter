#!/bin/bash
# ===================================================================
#  สร้างตารางชั้น raw ตอน Postgres init ครั้งแรก
#
#  ตัว DDL จริงอยู่ที่ scripts/sql/create_raw_tables.sql (single source of truth)
#  ซึ่ง docker-compose.yml mount ไว้ที่ /opt/sql ในคอนเทนเนอร์
#  สคริปต์นี้เป็นแค่ตัวเรียกให้ Postgres รันไฟล์นั้นตามลำดับ (หลัง 01 และ 02)
#
#  ไฟล์ DDL เป็น idempotent (CREATE ... IF NOT EXISTS) และถูกรันซ้ำทุกครั้ง
#  ที่ DAG dw_pipeline ทำงาน (task create_raw_tables) ด้วย
# ===================================================================
set -eo pipefail

RAW_DDL="/opt/sql/create_raw_tables.sql"

if [ ! -f "$RAW_DDL" ]; then
    echo "[init] ไม่พบไฟล์ $RAW_DDL - ข้ามการสร้างตารางชั้น raw" >&2
    echo "[init] ตรวจว่า docker-compose.yml mount ./scripts/sql:/opt/sql:ro ไว้แล้วหรือยัง" >&2
    exit 1
fi

echo "[init] รัน DDL ชั้น raw จาก $RAW_DDL"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -f "$RAW_DDL"

echo "[init] 03_create_raw_tables.sh เสร็จแล้ว"
