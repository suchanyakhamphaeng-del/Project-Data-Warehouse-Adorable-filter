#!/bin/bash
# ===================================================================
#  สร้าง schema ของ warehouse 3 ชั้น (ค่า default: raw, staging, marts)
#  ปรับรายชื่อได้จากตัวแปร DW_SCHEMAS ในไฟล์ .env
#  สคริปต์นี้รันเฉพาะตอน initialize data volume ครั้งแรกเท่านั้น
# ===================================================================
set -eo pipefail

SCHEMAS="${DW_SCHEMAS:-raw staging marts}"

for schema_name in $SCHEMAS; do
    echo "[init] creating schema '${schema_name}' in database '${POSTGRES_DB}'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
        -c "CREATE SCHEMA IF NOT EXISTS \"${schema_name}\" AUTHORIZATION \"${POSTGRES_USER}\";"
done

# ให้ user หลักมีสิทธิ์เต็มบน schema ที่สร้างไว้
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<SQL
GRANT ALL ON SCHEMA public TO "${POSTGRES_USER}";
SQL

echo "[init] 02_create_schemas.sh เสร็จแล้ว"
