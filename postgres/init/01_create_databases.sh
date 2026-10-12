#!/bin/bash
# ===================================================================
#  สร้าง database เพิ่มเติมสำหรับ metadata ของ Airflow และ Metabase
#  สคริปต์นี้รันเฉพาะตอน initialize data volume ครั้งแรกเท่านั้น
#  (ถ้าอยากให้รันใหม่ ต้องลบ volume: docker compose down -v)
# ===================================================================
set -eo pipefail

AIRFLOW_DB_NAME="${AIRFLOW_DB:-airflow}"
METABASE_DB_NAME="${METABASE_DB:-metabase}"

create_database_if_missing() {
    db_name="$1"

    exists=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
        -tAc "SELECT 1 FROM pg_database WHERE datname = '${db_name}'")

    if [ "$exists" = "1" ]; then
        echo "[init] database '${db_name}' มีอยู่แล้ว - ข้าม"
    else
        echo "[init] creating database '${db_name}'"
        psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
            -c "CREATE DATABASE \"${db_name}\" OWNER \"${POSTGRES_USER}\" ENCODING 'UTF8';"
    fi
}

create_database_if_missing "$AIRFLOW_DB_NAME"
create_database_if_missing "$METABASE_DB_NAME"

echo "[init] 01_create_databases.sh เสร็จแล้ว"
