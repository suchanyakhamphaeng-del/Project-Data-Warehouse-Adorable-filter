# Airflow + Cosmos Orchestration (คนที่ 5)

DAG หลัก: **`dw_pipeline`** (`dags/dw_pipeline.py`) — รันทั้ง pipeline ตั้งแต่ CSV จนถึง dbt
โดยใช้ [astronomer-cosmos](https://github.com/astronomer/astronomer-cosmos) แปลง dbt project เป็น task ของ Airflow

## ลำดับ task

```
check_warehouse
  -> create_raw_tables        รัน scripts/sql/create_raw_tables.sql (idempotent)
  -> load_raw_from_csv        TRUNCATE + COPY raw_data/*.csv เข้า raw.* (ทรานแซกชันเดียว)
  -> audit_raw_row_counts     เทียบจำนวนแถว raw.* กับไฟล์ CSV ไม่ตรง = fail
  -> dbt (DbtTaskGroup)
       seeds   : stores, promotions, categories, suppliers  (+ test)
       staging : stg_* 12 ตัว  (แต่ละตัว = run + test)
       tests   : relationships_* แยกเป็น task รอ parent ครบก่อน
       marts   : fct_* / dim_* โผล่เป็น task อัตโนมัติเมื่อเพิ่ม model ใน models/marts/
```

ไม่ต้องเขียนลำดับ dbt เอง — Cosmos อ่าน `ref()` / `source()` แล้วต่อ dependency ให้

## ไฟล์ที่เกี่ยวข้อง

| ไฟล์ | สิ่งที่ตั้งค่า |
| --- | --- |
| `airflow/Dockerfile` | ติดตั้ง `astronomer-cosmos==1.15.1` ใน env ของ Airflow (dbt ยังอยู่ใน `/opt/dbt_venv`) |
| `docker-compose.yml` | mount `./scripts/sql` และ `./raw_data`, env `RAW_DATA_DIR`, `AIRFLOW__COSMOS__ENABLE_DATASET_ALIAS=False` |
| `dags/dw_pipeline.py` | DAG + config ของ Cosmos |
| `my_dbt_project/profiles.yml` | Cosmos ใช้ไฟล์นี้ตรง ๆ (`profiles_yml_filepath`) |

### Config ของ Cosmos ที่เลือกใช้

| ค่า | เหตุผล |
| --- | --- |
| `LoadMode.DBT_LS` | ใช้ `dbt ls` อ่าน dependency จริงของ project |
| `TestBehavior.AFTER_EACH` | test ของแต่ละ model รันทันทีหลัง model นั้น |
| `should_detach_multiple_parents_tests=True` | test ที่อ้างหลาย model (relationships) รอ parent ครบก่อน — ถ้าไม่เปิดจะ fail แบบสุ่มเพราะ test รันก่อน `stg_stores` ถูกสร้าง |
| `emit_datasets=False` + `ENABLE_DATASET_ALIAS=False` | ไม่ได้ใช้ Airflow Datasets และ alias ของ Cosmos ทำให้ `airflow dags test` บน Airflow 2.10 พัง (`FlushError: DatasetModel.aliases`) |

## วิธีรัน

```bash
docker compose build          # ต้อง build ใหม่ครั้งแรก เพราะเพิ่ม Cosmos ใน image
docker compose up -d
```

- UI: http://localhost:8080 → เปิด DAG `dw_pipeline` (unpause) → Trigger
- หรือรันจาก CLI:

```bash
docker compose exec airflow-scheduler airflow dags test dw_pipeline
```

> ถ้า volume ของ Postgres ถูกสร้างไว้ก่อนแก้ path `scripts/sql` ตาราง raw อาจไม่ถูกสร้างตอน init
> ไม่เป็นไร task `create_raw_tables` สร้างให้อยู่แล้ว หรือ reset ด้วย `docker compose down -v`

## ผลการทดสอบ

ทดสอบด้วย Airflow 2.10.5 + Cosmos 1.15.1 + dbt-postgres + PostgreSQL 16

- `airflow dags test dw_pipeline` → **47/47 task success** ทั้งรันแรก (warehouse ว่าง) และรันซ้ำ (idempotent)
- raw: orders 300,000 · order_items 600,000 · payments 300,000 · shipments 300,000 · returns 30,000 · customers 50,000 · products 10,000 · employees 1,000 (ตรงกับ CSV ทุกตาราง)
- staging: seed 4 ตาราง + view `stg_*` 12 ตัว, dbt test ผ่านทั้งหมด

## หมายเหตุ

- `dags/dw_sample_pipeline.py` เดิมถูกแทนที่ด้วย `dw_pipeline.py` (ตัวเดิมโหลด stores/promotions/categories เข้า raw ซึ่งไม่มีแล้ว จึง fail)
- `scripts/powershell/ingest_data.ps1` ยังใช้โหลดมือได้เหมือนเดิม — DAG ทำงานเดียวกันจากใน container
