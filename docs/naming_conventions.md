# มาตรฐานการตั้งชื่อ (Naming Conventions)

กฎการตั้งชื่อของโปรเจกต์นี้ ใช้กับทั้งฐานข้อมูล ไฟล์ และ pipeline
โครงสร้างข้อมูลจริงดูที่ [`database_design.md`](database_design.md)

---

## 1. หลักการรวม

| หลักการ | รายละเอียด |
| --- | --- |
| **snake_case ตัวพิมพ์เล็กทั้งหมด** | `store_city_summary` ไม่ใช่ `StoreCitySummary` หรือ `store-city-summary` |
| **ไม่ต้องใส่ double quote** | ชื่อที่เป็น lowercase snake_case ใช้ใน SQL ได้โดยไม่ต้อง `"quote"` — PostgreSQL พับชื่อที่ไม่ quote เป็นตัวพิมพ์เล็กอยู่แล้ว |
| **ภาษาอังกฤษ** | ชื่อ object ทุกอย่างเป็นภาษาอังกฤษ ส่วนคำอธิบาย (`COMMENT`, docstring, README) เป็นภาษาไทยได้ |
| **ไม่ใช้คำย่อที่เดาไม่ออก** | `category_name` ไม่ใช่ `cat_nm` — ยกเว้นคำย่อที่เป็นมาตรฐานแล้ว เช่น `id`, `fct`, `dim`, `stg` |
| **เอกพจน์สำหรับคอลัมน์ พหูพจน์สำหรับตาราง** | ตาราง `stores` มีคอลัมน์ `store_id` |

---

## 2. ฐานข้อมูล

### 2.1 Schema

ชื่อ schema = ชื่อชั้นของข้อมูล คำเดียว ตัวพิมพ์เล็ก

| schema | ความหมาย |
| --- | --- |
| `raw` | ข้อมูลดิบจากต้นทาง ยังไม่แปลงค่า |
| `staging` | ทำความสะอาด / cast ชนิดข้อมูลแล้ว |
| `marts` | ตารางสำหรับวิเคราะห์ |

ตั้งค่ารายชื่อได้จากตัวแปร `DW_SCHEMAS` ใน `.env` (ค่าเริ่มต้น `raw staging marts`)

### 2.2 ตาราง

| ชั้น | รูปแบบชื่อ | ตัวอย่าง |
| --- | --- | --- |
| `raw` | `{entity}` — ชื่อเดียวกับไฟล์ต้นทาง ไม่ใส่ prefix | `raw.stores`, `raw.promotions`, `raw.categories` |
| `staging` | `{entity}` (ตารางที่มาจาก seed) | `staging.stores` |
| `staging` | `stg_{source}__{entity}` (dbt model) | `staging.stg_seed__stores` |
| `marts` | `fct_{business_process}` สำหรับตารางข้อเท็จจริง / ตัวเลข | `marts.fct_store_city_summary` |
| `marts` | `dim_{entity}` สำหรับตารางมิติ / lookup | `marts.dim_store`, `marts.dim_category` |

กฎย่อยของ `marts`

* **ทุกตารางใน `marts` ต้องขึ้นต้นด้วย `fct_` หรือ `dim_`** ไม่มีข้อยกเว้น
* `fct_` = มีตัวเลขที่เอาไป aggregate ได้ (count, sum, avg) และมี grain ชัดเจน
* `dim_` = ใช้สำหรับ filter / group by เป็นคุณลักษณะของ entity
* `dim_` ใช้**เอกพจน์** (`dim_store`) เพราะหนึ่งแถวแทนหนึ่ง entity

### 2.3 คอลัมน์

| ประเภท | รูปแบบ | ตัวอย่าง |
| --- | --- | --- |
| Primary key | `{entity}_id` | `store_id`, `promotion_id`, `category_id` |
| Foreign key | `{referenced_entity}_id` — ชื่อตรงกับ PK ที่อ้างถึง | `store_id` ใน `fct_sales` อ้าง `dim_store.store_id` |
| ตัวนับ / ปริมาณ | `{entity}_count`, `{measure}_amount`, `{measure}_qty` | `store_count`, `sales_amount` |
| วันเวลา | `{event}_at` สำหรับ timestamp, `{event}_date` สำหรับ date | `loaded_at`, `created_at`, `order_date` |
| Boolean | `is_{adjective}` หรือ `has_{noun}` | `is_active`, `has_discount` |
| เปอร์เซ็นต์ | `{measure}_pct` | `discount_pct` |

กฎย่อย

* **ทุกตารางใน `raw` ต้องมีคอลัมน์ `loaded_at timestamp NOT NULL DEFAULT now()`**
  เพื่อตรวจย้อนหลังว่าแถวนั้นเข้ามาเมื่อไหร่
* ห้ามตั้งชื่อคอลัมน์ว่า `id` เฉย ๆ — ต้องมีชื่อ entity นำหน้า เพื่อให้ join อ่านง่าย
  และไม่ชนกันเวลา `SELECT *`

### 2.4 Constraint และ index

| ประเภท | รูปแบบ | ตัวอย่าง |
| --- | --- | --- |
| Primary key | `pk_{schema}_{table}` | `pk_raw_stores` |
| Foreign key | `fk_{schema}_{table}__{referenced_table}` | `fk_marts_fct_sales__dim_store` |
| Unique | `uq_{schema}_{table}__{columns}` | `uq_raw_stores__city_store_id` |
| Check | `ck_{schema}_{table}__{rule}` | `ck_raw_promotions__discount_range` |
| Index | `ix_{schema}_{table}__{columns}` | `ix_raw_stores__city` |

---

## 3. ไฟล์

### 3.1 ไฟล์ SQL — `sql/`

```
create_{layer}_tables.sql
```

| ไฟล์ | หน้าที่ |
| --- | --- |
| `sql/create_raw_tables.sql` | DDL ของทุกตารางในชั้น `raw` |

* หนึ่งไฟล์ต่อหนึ่งชั้น ไม่ต้องแยกรายตาราง
* ต้อง **idempotent** — ใช้ `CREATE SCHEMA IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS`
  เพื่อให้รันซ้ำได้ทั้งตอน init และตอน DAG ทำงาน
* ไฟล์ที่เป็น DML/transform ใช้ `{verb}_{target}.sql` เช่น `load_raw_stores.sql`

### 3.2 สคริปต์ init ของ Postgres — `postgres/init/`

```
{NN}_{verb}_{object}.{sh|sql}
```

`{NN}` คือเลขสองหลักกำหนดลำดับการรัน (Postgres รันไฟล์ใน
`/docker-entrypoint-initdb.d` เรียงตามชื่อ)

| ลำดับ | ไฟล์ | หน้าที่ |
| --- | --- | --- |
| 01 | `01_create_databases.sh` | สร้าง database `airflow`, `metabase` |
| 02 | `02_create_schemas.sh` | สร้าง schema `raw`, `staging`, `marts` |
| 03 | `03_create_raw_tables.sh` | เรียก `psql -f /opt/sql/create_raw_tables.sql` — DDL ต้นฉบับอยู่ที่ `sql/` ไม่ได้ก๊อปไฟล์ซ้ำ |

* ลำดับสำคัญ: database → schema → table
* ไฟล์ `.sh` และ `.sql` ในโฟลเดอร์นี้ **ต้องใช้ line ending แบบ LF** ไม่ใช่ CRLF
  (`.gitattributes` บังคับไว้แล้ว)

### 3.3 DAG — `dags/`

```
{domain}_{purpose}.py
```

ชื่อไฟล์ต้อง**ตรงกับ `dag_id`** ที่อยู่ข้างใน เพื่อให้หาไฟล์จาก UI ได้ทันที

| ไฟล์ | `dag_id` | domain | purpose |
| --- | --- | --- | --- |
| `dags/dw_sample_pipeline.py` | `dw_sample_pipeline` | `dw` | `sample_pipeline` |

ตัวอย่างชื่อที่ถูกตามมาตรฐาน: `dw_daily_refresh.py`, `sales_hourly_ingest.py`

### 3.4 ชื่อ task ใน DAG

```
{verb}_{object}
```

| task | ความหมาย |
| --- | --- |
| `check_warehouse` | ตรวจการเชื่อมต่อ warehouse |
| `create_raw_tables` | สร้างตารางชั้น raw |
| `load_raw_from_csv` | โหลด CSV เข้า raw |
| `dbt_seed` / `dbt_test` | คำสั่ง dbt — ใช้ `dbt_{command}` |
| `build_fct_store_city_summary` | สร้างตาราง mart (ชื่อ task = `build_` + ชื่อตารางปลายทาง) |

### 3.5 dbt model — `my_dbt_project/models/`

| ชั้น | รูปแบบไฟล์ | ตัวอย่าง |
| --- | --- | --- |
| staging | `stg_{source}__{entity}.sql` | `stg_seed__stores.sql`, `stg_seed__promotions.sql` |
| marts (fact) | `fct_{business_process}.sql` | `fct_store_city_summary.sql` |
| marts (dimension) | `dim_{entity}.sql` | `dim_store.sql`, `dim_category.sql` |

* **ชื่อไฟล์ model = ชื่อตาราง/view ที่ dbt สร้าง** เสมอ
* `{source}` คือชื่อ source ใน dbt ไม่ใช่ชื่อ schema — ใช้ `__` (ขีดล่างสองตัว) คั่น
  ระหว่าง source กับ entity เพื่อแยกจาก `_` ที่อยู่ในชื่อ entity เอง
* จัดโฟลเดอร์ตามชั้น: `models/staging/`, `models/marts/`
* ไฟล์ YAML ของเอกสาร/test ขึ้นต้นด้วย `_` เพื่อให้ลอยอยู่บนสุดของโฟลเดอร์:
  `_models.yml`, `_sources.yml`

### 3.6 Seeds — `my_dbt_project/seeds/`

```
{entity}.csv          # พหูพจน์ ตรงกับชื่อตารางปลายทาง
_seeds.yml            # ไฟล์ประกาศ schema/test ของ seed ทั้งหมด
```

| ไฟล์ | ตารางที่ได้ |
| --- | --- |
| `seeds/stores.csv` | `staging.stores` |
| `seeds/promotions.csv` | `staging.promotions` |
| `seeds/categories.csv` | `staging.categories` |

* header ของ CSV ต้องเป็น snake_case ตรงกับชื่อคอลัมน์ในฐานข้อมูล
* ชนิดข้อมูลประกาศไว้ใน `_seeds.yml` ที่ `config.column_types`

### 3.7 เอกสาร — `docs/`

```
{topic}.md            # snake_case ตัวพิมพ์เล็ก
```

| ไฟล์ | เนื้อหา |
| --- | --- |
| `docs/database_design.md` | โครงสร้าง schema / ตาราง / ERD |
| `docs/naming_conventions.md` | เอกสารฉบับนี้ |

`README.md` เป็นข้อยกเว้นเดียวที่ใช้ตัวพิมพ์ใหญ่ (ขนบของ GitHub) เช่นเดียวกับ
ไฟล์ตั้งค่าที่เครื่องมือกำหนดชื่อมาให้แล้ว (`docker-compose.yml`, `dbt_project.yml`,
`profiles.yml`, `servers.json`, `Dockerfile`, `.env`)

---

## 4. ตัวแปร environment

```
{COMPONENT}_{SETTING}
```

ตัวพิมพ์ใหญ่ทั้งหมด คั่นด้วย `_` และขึ้นต้นด้วยชื่อ component

| prefix | ใช้กับ | ตัวอย่าง |
| --- | --- | --- |
| `POSTGRES_` | PostgreSQL | `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` |
| `AIRFLOW_` | Airflow (ตัวแปรของเราเอง) | `AIRFLOW_ADMIN_USERNAME`, `AIRFLOW_FERNET_KEY` |
| `AIRFLOW__` | config ของ Airflow เอง — `AIRFLOW__{SECTION}__{KEY}` | `AIRFLOW__CORE__EXECUTOR` |
| `AIRFLOW_CONN_` | Airflow connection — `AIRFLOW_CONN_{CONN_ID ตัวพิมพ์ใหญ่}` | `AIRFLOW_CONN_DW_POSTGRES` → conn_id `dw_postgres` |
| `DBT_` | dbt | `DBT_HOST`, `DBT_SCHEMA`, `DBT_TARGET_PATH` |
| `METABASE_` / `MB_` | Metabase (`MB_` คือของ Metabase เอง) | `METABASE_PORT`, `MB_DB_HOST` |
| `PGADMIN_` | pgAdmin | `PGADMIN_DEFAULT_EMAIL` |
| `DW_` | ตัวแปรของ warehouse เอง | `DW_SCHEMAS` |

---

## 5. Docker

| สิ่งของ | รูปแบบ | ตัวอย่าง |
| --- | --- | --- |
| compose project | `{project}_stack` | `dw_stack` |
| service | `{component}` หรือ `dw_{component}` | `dw_postgres`, `airflow-webserver`, `dw_metabase` |
| container | `{project}_stack_{component}` | `dw_stack_postgres`, `dw_stack_airflow_webserver` |
| network | `{project}_stack_net` | `dw_stack_net` |
| volume | `{project}_stack_{component}_data` | `dw_stack_postgres_data` |

**ชื่อ service สำคัญกว่าชื่อ container** เพราะเป็น hostname ที่ใช้คุยกันใน Docker network
(เช่น Metabase ต่อ Postgres ที่ host `dw_postgres`) ส่วนชื่อ container ตั้งให้มี prefix
`dw_stack_` เพื่อไม่ให้ชนกับ stack อื่นในเครื่องเดียวกัน

> service ของ Airflow ใช้ `-` (`airflow-webserver`) ตามขนบของ compose file ตัวอย่าง
> จาก Apache Airflow เพื่อให้คนที่เคยใช้คุ้นมือ ส่วน service อื่นใช้ `_`

---

## 6. ตรวจสอบว่าของที่มีอยู่ตรงมาตรฐาน

| สิ่งของ | ชื่อปัจจุบัน | ตรงมาตรฐาน? |
| --- | --- | --- |
| schema | `raw`, `staging`, `marts` | ✔ |
| ตาราง raw | `raw.stores`, `raw.promotions`, `raw.categories` | ✔ |
| ตาราง staging | `staging.stores`, `staging.promotions`, `staging.categories` | ✔ seed-based |
| ตาราง marts | `marts.fct_store_city_summary` | ✔ (เปลี่ยนจาก `store_city_summary` ที่ไม่มี prefix) |
| คอลัมน์ PK | `store_id`, `promotion_id`, `category_id` | ✔ `{entity}_id` |
| คอลัมน์ audit | `loaded_at` | ✔ `{event}_at` |
| ไฟล์ SQL | `sql/create_raw_tables.sql` | ✔ `create_{layer}_tables.sql` |
| init script | `01_create_databases.sh`, `02_create_schemas.sh`, `03_create_raw_tables.sh` | ✔ `{NN}_{verb}_{object}` |
| DAG | `dags/dw_sample_pipeline.py` → `dag_id` `dw_sample_pipeline` | ✔ `{domain}_{purpose}` |
| seeds | `stores.csv`, `promotions.csv`, `categories.csv`, `_seeds.yml` | ✔ |
| docs | `database_design.md`, `naming_conventions.md` | ✔ |
