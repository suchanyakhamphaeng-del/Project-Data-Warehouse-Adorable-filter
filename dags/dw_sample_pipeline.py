"""DAG ตัวอย่างของ DW stack — เดินข้อมูลครบทั้ง 3 ชั้น raw -> staging -> marts.

ลำดับการทำงาน::

    check_warehouse
        -> create_raw_tables
        -> load_raw_from_csv
        -> dbt_seed
        -> dbt_test
        -> build_fct_store_city_summary

* check_warehouse                 ทดสอบว่าต่อ warehouse (connection `dw_postgres`) ได้จริง
                                  และสร้าง schema ที่หายไปกลับมาให้
* create_raw_tables               รัน sql/create_raw_tables.sql (idempotent) สร้าง `raw.*`
* load_raw_from_csv               TRUNCATE แล้ว COPY ไฟล์ CSV ใน seeds/ เข้า `raw.*`
* dbt_seed                        โหลด seed เข้า schema `staging` พร้อม cast ชนิดข้อมูล
* dbt_test                        รัน test ที่ประกาศไว้ใน seeds/_seeds.yml (unique / not_null)
* build_fct_store_city_summary    สร้างตาราง `marts.fct_store_city_summary`
                                  = จำนวนสาขาต่อเมือง จาก `staging.stores`

ดูรายละเอียดโครงสร้างข้อมูลที่ ``docs/database_design.md`` และมาตรฐานชื่อที่
``docs/naming_conventions.md``
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# ค่าคงที่ - path และ connection อ่านจาก env ที่ตั้งไว้ใน docker-compose.yml
# ---------------------------------------------------------------------------
DW_CONN_ID = "dw_postgres"
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/my_dbt_project")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
SQL_DIR = os.environ.get("SQL_DIR", "/opt/airflow/sql")

RAW_SCHEMA = "raw"
STAGING_SCHEMA = os.environ.get("DBT_SCHEMA", "staging")
MARTS_SCHEMA = "marts"

RAW_DDL_FILE = Path(SQL_DIR) / "create_raw_tables.sql"
SEEDS_DIR = Path(DBT_PROJECT_DIR) / "seeds"

# ตาราง raw -> (ไฟล์ CSV ต้นทาง, คอลัมน์ที่ COPY เข้าไป)
# ไม่ใส่ loaded_at เพื่อให้ DEFAULT now() ทำงาน
RAW_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "stores": ("stores.csv", ("store_id", "city")),
    "promotions": ("promotions.csv", ("promotion_id", "discount")),
    "categories": ("categories.csv", ("category_id", "category_name")),
}

log = logging.getLogger(__name__)

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


def check_warehouse(**_context) -> str:
    """ตรวจว่าเชื่อมต่อ warehouse ได้ และ schema ทั้ง 3 ชั้นมีอยู่จริง."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    version = hook.get_first("SELECT version();")[0]
    current_db = hook.get_first("SELECT current_database();")[0]
    schemas = [
        row[0]
        for row in hook.get_records(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema' "
            "ORDER BY schema_name;"
        )
    ]

    log.info("เชื่อมต่อ warehouse สำเร็จ")
    log.info("  database : %s", current_db)
    log.info("  version  : %s", version)
    log.info("  schemas  : %s", ", ".join(schemas))

    # ถ้า schema หายไป (เช่นถูกลบ) ให้สร้างกลับมาเพื่อให้ขั้นถัดไปทำงานต่อได้
    for required in (RAW_SCHEMA, STAGING_SCHEMA, MARTS_SCHEMA):
        if required not in schemas:
            log.warning("ไม่พบ schema %s - กำลังสร้างให้ใหม่", required)
            hook.run(f'CREATE SCHEMA IF NOT EXISTS "{required}";')

    return f"{current_db} ok"


def create_raw_tables(**_context) -> str:
    """รัน DDL ของชั้น raw จาก sql/create_raw_tables.sql (รันซ้ำได้)."""
    if not RAW_DDL_FILE.is_file():
        raise FileNotFoundError(f"ไม่พบไฟล์ DDL: {RAW_DDL_FILE}")

    ddl = RAW_DDL_FILE.read_text(encoding="utf-8")
    log.info("รัน DDL จาก %s (%d ตัวอักษร)", RAW_DDL_FILE, len(ddl))

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    hook.run(ddl, autocommit=True)

    tables = [
        row[0]
        for row in hook.get_records(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{RAW_SCHEMA}' ORDER BY table_name;"
        )
    ]
    log.info("ตารางใน schema %s : %s", RAW_SCHEMA, ", ".join(tables))
    return ", ".join(tables)


def load_raw_from_csv(**_context) -> str:
    """TRUNCATE แล้ว COPY ไฟล์ CSV จาก seeds/ เข้าตาราง raw.* ในทรานแซกชันเดียว."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    results: list[str] = []

    try:
        with conn.cursor() as cur:
            for table, (csv_name, columns) in RAW_TABLES.items():
                csv_path = SEEDS_DIR / csv_name
                if not csv_path.is_file():
                    raise FileNotFoundError(f"ไม่พบไฟล์ CSV: {csv_path}")

                target = f'"{RAW_SCHEMA}"."{table}"'
                column_list = ", ".join(f'"{c}"' for c in columns)

                log.info("TRUNCATE %s", target)
                cur.execute(f"TRUNCATE TABLE {target};")

                log.info("COPY %s -> %s", csv_path, target)
                # เปิดไฟล์แบบอ่านอย่างเดียว เพื่อไม่ให้ไฟล์ seed ต้นทางถูกแก้ไข
                with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
                    cur.copy_expert(
                        f"COPY {target} ({column_list}) "
                        "FROM STDIN WITH (FORMAT csv, HEADER true)",
                        csv_file,
                    )

                cur.execute(f"SELECT COUNT(*) FROM {target};")
                row_count = cur.fetchone()[0]
                log.info("  -> %s มี %s แถว", target, row_count)
                results.append(f"{RAW_SCHEMA}.{table}={row_count}")

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("โหลด CSV เข้า raw ไม่สำเร็จ - rollback แล้ว")
        raise
    finally:
        conn.close()

    return ", ".join(results)


with DAG(
    dag_id="dw_sample_pipeline",
    description=(
        "ตัวอย่าง pipeline 3 ชั้น: ตรวจ warehouse -> สร้าง/โหลด raw -> "
        "dbt seed -> dbt test -> สร้าง marts"
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    schedule=None,  # trigger เอง (เปลี่ยนเป็น "@daily" ได้ถ้าต้องการ)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["dw", "dbt", "example"],
) as dag:

    check_warehouse_task = PythonOperator(
        task_id="check_warehouse",
        python_callable=check_warehouse,
    )

    create_raw_tables_task = PythonOperator(
        task_id="create_raw_tables",
        python_callable=create_raw_tables,
    )

    load_raw_from_csv_task = PythonOperator(
        task_id="load_raw_from_csv",
        python_callable=load_raw_from_csv,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} seed "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--full-refresh"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"{DBT_BIN} test "
            f"--profiles-dir {DBT_PROFILES_DIR} "
            f"--project-dir {DBT_PROJECT_DIR}"
        ),
    )

    build_fct_store_city_summary = SQLExecuteQueryOperator(
        task_id="build_fct_store_city_summary",
        conn_id=DW_CONN_ID,
        autocommit=True,
        sql=[
            f'CREATE SCHEMA IF NOT EXISTS "{MARTS_SCHEMA}";',
            f'DROP TABLE IF EXISTS "{MARTS_SCHEMA}".fct_store_city_summary;',
            f"""
            CREATE TABLE "{MARTS_SCHEMA}".fct_store_city_summary AS
            SELECT
                city                                   AS city,
                COUNT(*)                               AS store_count,
                MIN(store_id)                          AS first_store_id,
                MAX(store_id)                          AS last_store_id,
                CURRENT_TIMESTAMP                      AS loaded_at
            FROM "{STAGING_SCHEMA}".stores
            GROUP BY city
            ORDER BY store_count DESC, city;
            """,
            f"""
            COMMENT ON TABLE "{MARTS_SCHEMA}".fct_store_city_summary IS
                'จำนวนสาขาต่อเมือง สร้างจาก staging.stores โดย DAG dw_sample_pipeline';
            """,
            f'ANALYZE "{MARTS_SCHEMA}".fct_store_city_summary;',
        ],
    )

    (
        check_warehouse_task
        >> create_raw_tables_task
        >> load_raw_from_csv_task
        >> dbt_seed
        >> dbt_test
        >> build_fct_store_city_summary
    )
