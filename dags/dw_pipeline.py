"""DAG หลักของโปรเจกต์ DW — โหลด raw แล้วให้ Cosmos รัน dbt ทั้ง project.

ลำดับการทำงาน::

    check_warehouse
        -> create_raw_tables
        -> load_raw_from_csv
        -> audit_raw_row_counts
        -> dbt (DbtTaskGroup ของ Cosmos)
               seeds  : stores, promotions, categories, suppliers
               staging: stg_* (แต่ละ model มี run + test ของตัวเอง)
               marts  : fct_* / dim_* (โผล่เป็น task อัตโนมัติเมื่อเพิ่ม model)

* check_warehouse        ต่อ warehouse (connection `dw_postgres`) ได้จริง + สร้าง schema ที่หาย
* create_raw_tables      รัน scripts/sql/create_raw_tables.sql (idempotent)
* load_raw_from_csv      TRUNCATE + COPY CSV ใน raw_data/ เข้า raw.* ในทรานแซกชันเดียว
* audit_raw_row_counts   เทียบจำนวนแถวกับไฟล์ CSV — ไม่ตรงให้ fail ก่อนเข้า dbt
* dbt                    Cosmos อ่าน dbt project แล้วแตก seed/model/test เป็น task
                         ตาม dependency ใน ref()/source() ให้เอง ไม่ต้องเขียนลำดับมือ
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

# ---------------------------------------------------------------------------
# ค่าคงที่ - path และ connection อ่านจาก env ที่ตั้งไว้ใน docker-compose.yml
# ---------------------------------------------------------------------------
DW_CONN_ID = "dw_postgres"
DBT_BIN = os.environ.get("DBT_BIN", "/opt/dbt_venv/bin/dbt")
DBT_PROJECT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/my_dbt_project"))
DBT_PROFILES_DIR = Path(os.environ.get("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR)))
SQL_DIR = Path(os.environ.get("SQL_DIR", "/opt/airflow/sql"))
RAW_DATA_DIR = Path(os.environ.get("RAW_DATA_DIR", "/opt/airflow/raw_data"))

RAW_SCHEMA = "raw"
STAGING_SCHEMA = os.environ.get("DBT_SCHEMA", "staging")
MARTS_SCHEMA = "marts"

RAW_DDL_FILE = SQL_DIR / "create_raw_tables.sql"

# ตาราง raw -> (ไฟล์ CSV ใน raw_data/, คอลัมน์ที่ COPY)
# ไม่ใส่ loaded_at เพื่อให้ DEFAULT now() ทำงาน
# ลำดับ/คอลัมน์ตรงกับ scripts/powershell/ingest_data.ps1
RAW_TABLES: dict[str, tuple[str, tuple[str, ...]]] = {
    "orders": ("orders.csv", ("order_id", "customer_id", "store_id", "order_date", "promotion_id")),
    "order_items": ("order_items.csv", ("order_item_id", "order_id", "product_id", "qty", "price")),
    "payments": ("payments.csv", ("payment_id", "order_id", "amount")),
    "shipments": ("shipments.csv", ("shipment_id", "order_id", "status")),
    "returns": ("returns.csv", ("return_id", "order_item_id", "refund")),
    "customers": ("customers.csv", ("customer_id", "city", "signup_date")),
    "products": ("products.csv", ("product_id", "category_id", "supplier_id", "price")),
    "employees": ("employees.csv", ("employee_id", "store_id", "salary")),
}

log = logging.getLogger(__name__)

default_args = {
    "owner": "data_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}


# ---------------------------------------------------------------------------
# Python callables ของชั้น raw
# ---------------------------------------------------------------------------
def check_warehouse(**_context) -> str:
    """ตรวจว่าเชื่อมต่อ warehouse ได้ และ schema ทั้ง 3 ชั้นมีอยู่จริง."""
    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)

    version = hook.get_first("SELECT version();")[0]
    current_db = hook.get_first("SELECT current_database();")[0]
    schemas = {
        row[0]
        for row in hook.get_records(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema';"
        )
    }

    log.info("เชื่อมต่อ warehouse สำเร็จ: db=%s", current_db)
    log.info("version: %s", version)
    log.info("schemas: %s", ", ".join(sorted(schemas)))

    for required in (RAW_SCHEMA, STAGING_SCHEMA, MARTS_SCHEMA):
        if required not in schemas:
            log.warning("ไม่พบ schema %s - กำลังสร้างให้ใหม่", required)
            hook.run(f'CREATE SCHEMA IF NOT EXISTS "{required}";', autocommit=True)

    return f"{current_db} ok"


def create_raw_tables(**_context) -> str:
    """รัน DDL ของชั้น raw (CREATE ... IF NOT EXISTS - รันซ้ำได้)."""
    if not RAW_DDL_FILE.is_file():
        raise FileNotFoundError(
            f"ไม่พบไฟล์ DDL: {RAW_DDL_FILE} "
            "(ตรวจ volume ./scripts/sql:/opt/airflow/sql ใน docker-compose.yml)"
        )

    ddl = RAW_DDL_FILE.read_text(encoding="utf-8")
    log.info("รัน DDL จาก %s", RAW_DDL_FILE)

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    hook.run(ddl, autocommit=True)

    tables = [
        row[0]
        for row in hook.get_records(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name;",
            parameters=(RAW_SCHEMA,),
        )
    ]
    log.info("ตารางใน schema %s : %s", RAW_SCHEMA, ", ".join(tables))
    return ", ".join(tables)


def load_raw_from_csv(**_context) -> dict[str, int]:
    """TRUNCATE แล้ว COPY ทุกไฟล์เข้า raw.* ในทรานแซกชันเดียว (พังตัวไหน rollback หมด)."""
    missing = [f for f, _ in RAW_TABLES.values() if not (RAW_DATA_DIR / f).is_file()]
    if missing:
        raise FileNotFoundError(f"ไม่พบไฟล์ CSV ใน {RAW_DATA_DIR}: {', '.join(missing)}")

    hook = PostgresHook(postgres_conn_id=DW_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False
    counts: dict[str, int] = {}

    try:
        with conn.cursor() as cur:
            targets = ", ".join(f'"{RAW_SCHEMA}"."{t}"' for t in RAW_TABLES)
            cur.execute(f"TRUNCATE TABLE {targets};")

            for table, (csv_name, columns) in RAW_TABLES.items():
                target = f'"{RAW_SCHEMA}"."{table}"'
                column_list = ", ".join(f'"{c}"' for c in columns)
                csv_path = RAW_DATA_DIR / csv_name

                log.info("COPY %s -> %s", csv_path, target)
                with csv_path.open("r", encoding="utf-8", newline="") as csv_file:
                    cur.copy_expert(
                        f"COPY {target} ({column_list}) "
                        "FROM STDIN WITH (FORMAT csv, HEADER true)",
                        csv_file,
                    )

                cur.execute(f"SELECT COUNT(*) FROM {target};")
                counts[table] = cur.fetchone()[0]
                log.info("  -> %s = %s แถว", target, counts[table])

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("โหลด CSV เข้า raw ไม่สำเร็จ - rollback แล้ว")
        raise
    finally:
        conn.close()

    return counts  # ส่งผ่าน XCom ให้ task audit


def _csv_data_rows(path: Path) -> int:
    """นับจำนวนแถวข้อมูลในไฟล์ CSV (ไม่นับ header / บรรทัดว่างท้ายไฟล์)."""
    with path.open("r", encoding="utf-8", newline="") as f:
        return max(sum(1 for line in f if line.strip()) - 1, 0)


def audit_raw_row_counts(ti, **_context) -> str:
    """เทียบจำนวนแถวใน raw.* กับไฟล์ CSV ต้นทาง - ไม่ตรงให้ fail ก่อนเข้า dbt."""
    loaded: dict[str, int] = ti.xcom_pull(task_ids="load_raw_from_csv") or {}
    mismatches = []

    for table, (csv_name, _) in RAW_TABLES.items():
        expected = _csv_data_rows(RAW_DATA_DIR / csv_name)
        actual = loaded.get(table, -1)
        status = "OK  " if actual == expected else "FAIL"
        log.info("%s raw.%-12s loaded=%9s expected=%9s", status, table, actual, expected)
        if actual != expected:
            mismatches.append(f"{table} ({actual} != {expected})")

    if mismatches:
        raise ValueError("จำนวนแถวไม่ตรง: " + "; ".join(mismatches))
    return f"{len(RAW_TABLES)} tables ok"


# ---------------------------------------------------------------------------
# Cosmos config - ใช้ profiles.yml เดิมของ dbt project (อ่านค่าจาก env var)
# ---------------------------------------------------------------------------
project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_DIR)

profile_config = ProfileConfig(
    profile_name="my_dbt_project",
    target_name=os.environ.get("DBT_TARGET", "dev"),
    profiles_yml_filepath=DBT_PROFILES_DIR / "profiles.yml",
)

# dbt อยู่ใน venv แยก (/opt/dbt_venv) ไม่ปนกับ Airflow
execution_config = ExecutionConfig(dbt_executable_path=DBT_BIN)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,          # ใช้ `dbt ls` อ่าน dependency จริงของ project
    dbt_deps=True,                        # ติดตั้ง packages จาก packages.yml อัตโนมัติ
    test_behavior=TestBehavior.AFTER_EACH,  # แต่ละ model รัน test ของตัวเองต่อท้าย
    # test ที่อ้างหลาย model (เช่น relationships stg_orders -> stg_stores)
    # แยกเป็น task ของตัวเอง รอให้ parent ทุกตัวสร้างเสร็จก่อน
    # ถ้าไม่เปิด test จะรันก่อน stg_stores มีอยู่จริง แล้ว fail แบบสุ่ม
    should_detach_multiple_parents_tests=True,
)


with DAG(
    dag_id="dw_pipeline",
    description="raw (CSV) -> dbt seed/staging/marts ผ่าน Cosmos",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    schedule=None,  # trigger เอง (เปลี่ยนเป็น "@daily" ได้)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["dw", "dbt", "cosmos"],
) as dag:

    check_warehouse_task = PythonOperator(
        task_id="check_warehouse",
        python_callable=check_warehouse,
    )

    create_raw_tables_task = PythonOperator(
        task_id="create_raw_tables",
        python_callable=create_raw_tables,
    )

    load_raw_task = PythonOperator(
        task_id="load_raw_from_csv",
        python_callable=load_raw_from_csv,
    )

    audit_raw_task = PythonOperator(
        task_id="audit_raw_row_counts",
        python_callable=audit_raw_row_counts,
    )

    dbt_tg = DbtTaskGroup(
        group_id="dbt",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "install_deps": True,  # ติดตั้ง dependencies จาก packages.yml อัตโนมัติ (ตรงกับ dbt_deps ใน RenderConfig)
            # ไม่ใช้ Airflow Datasets (ปิด alias ด้วย env
            # AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS ใน docker-compose.yml ด้วย)
            "emit_datasets": False,
        },
        default_args={"retries": 0},  # dbt fail ให้เห็นทันที ไม่ต้อง retry
    )

    (
        check_warehouse_task
        >> create_raw_tables_task
        >> load_raw_task
        >> audit_raw_task
        >> dbt_tg
    )
