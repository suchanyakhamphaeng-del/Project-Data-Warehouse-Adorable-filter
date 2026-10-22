# DW — Data Warehouse บน Docker

Data Warehouse ครบชุดที่รันด้วย Docker Compose ตัวเดียว ประกอบด้วย

| ชั้นงาน | เครื่องมือ |
| --- | --- |
| Warehouse | PostgreSQL 16 (database `dw`, schema `raw` / `staging` / `marts`) |
| Orchestration | Apache Airflow 2.10 (LocalExecutor) |
| Transformation | dbt (dbt-postgres) ติดตั้งใน virtualenv แยกใน image ของ Airflow |
| BI / Dashboard | Metabase |
| Database admin | pgAdmin 4 |

ทุกบริการใช้ PostgreSQL ตัวเดียวกัน แต่แยก database กัน — `dw` เป็น warehouse, `airflow`
เก็บ metadata ของ Airflow, `metabase` เก็บ application data ของ Metabase

---

## 1. โครงสร้างไฟล์

```
DW/
├── docker-compose.yml              # นิยาม service ทั้งหมด (อ่านค่าจาก .env)
├── .env.example                    # ตัวอย่าง environment — คัดลอกเป็น .env
├── .gitignore / .gitattributes     # กัน .env หลุด + บังคับ .sh/.sql เป็น LF
├── README.md
│
├── docs/
│   ├── database_design.md          # schema 3 ชั้น + ทุกตาราง + ERD (Mermaid)
│   └── naming_conventions.md       # มาตรฐานชื่อ schema/ตาราง/คอลัมน์/ไฟล์
│
├── scripts/sql/
│   └── create_raw_tables.sql       # DDL ชั้น raw (idempotent, มี COMMENT)
│                                   #   single source of truth — เรียกใช้ทั้งจาก
│                                   #   init script 03 และจาก DAG
│
├── airflow/
│   └── Dockerfile                  # apache/airflow + dbt-postgres (venv แยก)
│                                   #   + apache-airflow-providers-postgres
│
├── postgres/
│   └── init/                       # รันครั้งเดียวตอน init volume ของ Postgres
│       ├── 01_create_databases.sh  # สร้าง database `airflow`, `metabase`
│       ├── 02_create_schemas.sh    # สร้าง schema `raw`, `staging`, `marts`
│       └── 03_create_raw_tables.sh # เรียก psql -f /opt/sql/create_raw_tables.sql
│
├── pgadmin/
│   └── servers.json                # ลงทะเบียน server ของ warehouse ให้อัตโนมัติ
│
├── dags/
│   └── dw_pipeline.py              # DAG หลัก: raw → dbt ผ่าน Cosmos (ดู README_Airflow.md)
│
├── logs/                           # log ของ Airflow (mount จาก host)
├── plugins/                        # plugin ของ Airflow (mount จาก host)
│
└── my_dbt_project/                 # dbt project (mount เข้า container ที่
    ├── dbt_project.yml             #   /opt/airflow/my_dbt_project)
    ├── profiles.yml                # อ่าน host/user/password จาก env var
    ├── seeds/
    │   ├── _seeds.yml              # schema + test ของ seed
    │   ├── stores.csv              # 100 สาขา (store_id, city)
    │   ├── promotions.csv          # 50 โปรโมชัน (promotion_id, discount)
    │   └── categories.csv          # 30 หมวดสินค้า (category_id, category_name)
    ├── models/                     # (ว่าง — ใส่ stg_*/fct_*/dim_* ของตัวเองได้)
    ├── macros/ · tests/ · analyses/ · snapshots/
```

> **ไฟล์ DDL มีต้นฉบับเดียว** — `scripts/sql/create_raw_tables.sql` ถูก mount เข้าคอนเทนเนอร์
> Postgres ที่ `/opt/sql/` แล้วสคริปต์ `postgres/init/03_create_raw_tables.sh` เป็นตัว
> เรียก `psql -f` ให้ตอน init ครั้งแรก (ลำดับ 03 = หลังสร้าง database และ schema)
> ส่วน Airflow mount ไฟล์เดียวกันที่ `/opt/airflow/sql/` ให้ task `create_raw_tables`
> อ่านไปรัน จึงไม่ต้องก๊อป DDL ไว้หลายที่

---

## 2. โครงสร้างข้อมูล

warehouse คือ database **`dw`** แบ่งเป็น **3 ชั้น** ตามหน้าที่ของข้อมูล

| ชั้น | schema | หน้าที่ | ใครเขียน |
| --- | --- | --- | --- |
| 1 | **`raw`** | **ข้อมูลดิบ** — รับจากไฟล์ CSV ตรง ๆ ไม่แปลงค่า มี `loaded_at` ไว้ตรวจย้อนหลัง | Airflow task `load_raw_from_csv` (TRUNCATE + COPY) |
| 2 | **`staging`** | **ทำความสะอาด** — cast ชนิดข้อมูลให้ถูก บังคับความถูกต้องด้วย dbt test | dbt (`dbt seed`) |
| 3 | **`marts`** | **ตารางสำหรับวิเคราะห์** — aggregate แล้ว พร้อมต่อ Metabase | Airflow task `build_fct_store_city_summary` |

```
seeds/*.csv ──COPY──────────> raw.*     (ดิบ + loaded_at)
            └─dbt seed─────> staging.*  (typed + tested)
                                  └─GROUP BY──> marts.fct_*  ──> Metabase
```

### ตารางทั้งหมด

| Schema | ตาราง | จำนวนแถว | คอลัมน์ |
| --- | --- | --- | --- |
| `raw` | `stores` | 100 | `store_id` int PK · `city` varchar(100) · `loaded_at` timestamp |
| `raw` | `promotions` | 50 | `promotion_id` int PK · `discount` int · `loaded_at` timestamp |
| `raw` | `categories` | 30 | `category_id` int PK · `category_name` varchar(100) · `loaded_at` timestamp |
| `staging` | `stores` | 100 | `store_id` int · `city` varchar(100) |
| `staging` | `promotions` | 50 | `promotion_id` int · `discount` int |
| `staging` | `categories` | 30 | `category_id` int · `category_name` varchar(100) |
| `marts` | `fct_store_city_summary` | 4 | `city` · `store_count` · `first_store_id` · `last_store_id` · `loaded_at` |

ตารางในชั้น `raw` มี PRIMARY KEY และ NOT NULL จริงในระดับฐานข้อมูล
ส่วนชั้น `staging` บังคับด้วย **dbt test** (`unique`, `not_null`) ตามที่ประกาศใน `_seeds.yml`

### เอกสารเพิ่มเติม

| เอกสาร | เนื้อหา |
| --- | --- |
| **[`docs/database_design.md`](docs/database_design.md)** | รายละเอียดทุกตาราง (ชนิดข้อมูล, PK, nullable, default, คำอธิบายรายคอลัมน์) + **ERD** แบบ Mermaid + คำสั่งตรวจโครงสร้าง |
| **[`docs/naming_conventions.md`](docs/naming_conventions.md)** | มาตรฐานชื่อ schema / ตาราง / คอลัมน์ / constraint / ไฟล์ SQL / DAG / dbt model / seeds / env var / Docker |

ดูโครงสร้างจริงในฐานข้อมูลได้ด้วย

```powershell
docker compose exec dw_postgres psql -U dw_user -d dw -c "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('raw','staging','marts') ORDER BY 1,2;"
```

---

## 3. ขั้นตอนการรัน

เปิด PowerShell ที่โฟลเดอร์ `DW` แล้ว

```powershell
# 1) สร้างไฟล์ .env จากตัวอย่าง
copy .env.example .env

# 2) แก้ .env — เปลี่ยนรหัสผ่านและคีย์ทุกตัวที่มีคำว่า change_me
notepad .env

# 3) build + start ทั้ง stack
docker compose up -d --build
```

ตรวจสถานะ

```powershell
docker compose ps                       # ทุก service ควรเป็น running / healthy
docker compose logs -f airflow-scheduler
```

ครั้งแรก Airflow จะใช้เวลาประมาณ 1–2 นาที (migrate DB + สร้าง admin user) และ
Metabase ประมาณ 2–3 นาทีกว่าจะ healthy

หยุด / ลบ

```powershell
docker compose stop        # หยุดแต่เก็บข้อมูลไว้
docker compose down        # ลบ container แต่เก็บ volume (ข้อมูลยังอยู่)
docker compose down -v     # ลบ volume ด้วย = ล้างข้อมูลทั้งหมด เริ่มใหม่หมด
```

---

## 4. URL และข้อมูลล็อกอิน

| บริการ | URL | ผู้ใช้ / รหัสผ่าน |
| --- | --- | --- |
| Airflow | <http://localhost:8080> | `AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env` (ค่าเริ่มต้น `admin`) |
| Metabase | <http://localhost:3000> | สร้างบัญชีแอดมินเองตอนเปิดครั้งแรก |
| pgAdmin | <http://localhost:5050> | `PGADMIN_DEFAULT_EMAIL` / `PGADMIN_DEFAULT_PASSWORD` ใน `.env` |
| PostgreSQL (warehouse) | `localhost:5432` → database `dw` | `POSTGRES_USER` / `POSTGRES_PASSWORD` ใน `.env` |

พอร์ตทุกตัวเปลี่ยนได้จาก `.env` (`AIRFLOW_WEBSERVER_PORT`, `METABASE_PORT`,
`PGADMIN_PORT`, `POSTGRES_PORT`)

### เชื่อม DBeaver / psql จากเครื่อง host

```
host=localhost  port=5432  database=dw  user=<POSTGRES_USER>  password=<POSTGRES_PASSWORD>
```

หรือเข้าทาง container ตรง ๆ

```powershell
docker compose exec dw_postgres psql -U dw_user -d dw -c "\dn"
```

---

## 5. DAG `dw_pipeline` (Airflow + Cosmos)

```
check_warehouse → create_raw_tables → load_raw_from_csv → audit_raw_row_counts
                → dbt (Cosmos DbtTaskGroup: seed → stg_* → tests → marts)
```

รายละเอียด config ของ Cosmos และผลการทดสอบอยู่ที่ **`README_Airflow.md`**

สั่งรัน — จาก UI ให้เปิด <http://localhost:8080> → เปิดสวิตช์ของ DAG → กด ▶ Trigger
หรือจาก command line

```powershell
docker compose exec airflow-scheduler airflow dags unpause dw_pipeline
docker compose exec airflow-scheduler airflow dags trigger dw_pipeline
```

ดูผลลัพธ์

```powershell
docker compose exec dw_postgres psql -U dw_user -d dw -c "SELECT city, store_count, first_store_id, last_store_id FROM marts.fct_store_city_summary ORDER BY store_count DESC;"
```

```
   city    | store_count | first_store_id | last_store_id
-----------+-------------+----------------+---------------
 Mumbai    |          31 |              4 |            98
 Pune      |          27 |              1 |            96
 Bangalore |          21 |              8 |            99
 Delhi     |          21 |              3 |           100
(4 rows)
```

และ seed ทั้งสามตารางใน `staging` จะมี 100 / 50 / 30 แถว (`stores`, `promotions`, `categories`)
เท่ากับชั้น `raw` — ตรวจทีเดียวทั้งสองชั้นได้ด้วย

```powershell
docker compose exec dw_postgres psql -U dw_user -d dw -c "SELECT 'raw.stores' t, COUNT(*) FROM raw.stores UNION ALL SELECT 'raw.promotions', COUNT(*) FROM raw.promotions UNION ALL SELECT 'raw.categories', COUNT(*) FROM raw.categories UNION ALL SELECT 'staging.stores', COUNT(*) FROM staging.stores ORDER BY 1;"
```

### รัน DDL ของชั้น raw ด้วยมือ

```powershell
docker compose exec -T dw_postgres psql -U dw_user -d dw -v ON_ERROR_STOP=1 -f /opt/sql/create_raw_tables.sql
```

ไฟล์เป็น idempotent รันซ้ำกี่ครั้งก็ได้ ข้อมูลเดิมไม่หาย

### Airflow connection

connection `dw_postgres` ถูกตั้งผ่าน environment variable
`AIRFLOW_CONN_DW_POSTGRES` ใน `docker-compose.yml` ไม่ต้องไปเพิ่มใน UI เอง
(connection แบบนี้จะไม่โชว์ในหน้า Admin → Connections แต่ใช้งานได้ปกติ)

ตรวจได้ด้วย

```powershell
docker compose exec airflow-scheduler airflow connections get dw_postgres
```

### รัน dbt ด้วยมือ

dbt อยู่ใน venv แยกที่ `/opt/dbt_venv/bin/dbt` (ไม่ได้ใส่ไว้ใน `PATH` เพื่อไม่ให้ไปทับ
python ของ Airflow)

```powershell
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt debug --project-dir /opt/airflow/my_dbt_project
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt seed  --project-dir /opt/airflow/my_dbt_project
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt run   --project-dir /opt/airflow/my_dbt_project
```

`profiles.yml` อ่านค่าทั้งหมดจาก env var (`DBT_HOST`, `DBT_USER`, `DBT_PASSWORD`,
`DBT_DBNAME`, `DBT_SCHEMA`) ที่ส่งเข้ามาจาก `.env` — ไม่มีรหัสผ่าน hard-code ในไฟล์

---

## 6. เชื่อม Metabase เข้ากับ warehouse

1. เปิด <http://localhost:3000> → ครั้งแรกให้ตั้งบัญชีแอดมิน (ภาษา, ชื่อ, อีเมล, รหัสผ่าน)
2. ตอนถามว่าจะเพิ่ม database ไหม เลือก **"I'll add my data later"** ก็ได้ แล้วไปเพิ่มทีหลังที่
   ไอคอนเฟือง ⚙ → **Admin settings** → **Databases** → **Add database**
3. กรอกค่าตามนี้

   | ช่อง | ค่า |
   | --- | --- |
   | Database type | **PostgreSQL** |
   | Display name | `DW Warehouse` |
   | Host | **`dw_postgres`** |
   | Port | `5432` |
   | Database name | `dw` |
   | Username | ค่า `POSTGRES_USER` ใน `.env` |
   | Password | ค่า `POSTGRES_PASSWORD` ใน `.env` |
   | Schemas | ปล่อยว่าง (ทั้งหมด) หรือระบุ `staging,marts` |

4. กด **Save** → รอ Metabase sync สักครู่ จะเห็นตารางใน `staging` และ `marts`
   เอาไปสร้าง question / dashboard ได้เลย

> **สำคัญ:** ช่อง Host ต้องใส่ `dw_postgres` ซึ่งเป็นชื่อ service ใน Docker network
> **ห้ามใส่** `localhost` เพราะ Metabase มองจากในคอนเทนเนอร์ตัวเอง `localhost`
> จะหมายถึงตัว Metabase ไม่ใช่ Postgres

### pgAdmin

server `DW Warehouse (dw)` ถูกลงทะเบียนไว้ให้แล้วจาก `pgadmin/servers.json`
พอล็อกอินเข้าไปจะเห็นในกลุ่ม *Data Warehouse* — ตอนกดเข้าครั้งแรกจะถามรหัสผ่าน
(ไฟล์ `servers.json` ไม่เก็บรหัสผ่านไว้) ใส่ค่า `POSTGRES_PASSWORD` แล้วติ๊ก
*Save password* ไว้ได้

---

## 7. ข้อควรระวัง

**สคริปต์ใน `postgres/init/` รันแค่ครั้งแรก**
Postgres จะรันไฟล์ใน `docker-entrypoint-initdb.d` เฉพาะตอนที่ data directory ยังว่าง
ถ้าแก้สคริปต์แล้วอยากให้มีผล ต้องล้าง volume ก่อน

```powershell
docker compose down -v
docker compose up -d --build
```

**แต่ DDL ของชั้น `raw` ไม่ต้อง `down -v`** — task `create_raw_tables` ใน DAG รัน
`scripts/sql/create_raw_tables.sql` ให้ทุกรอบและไฟล์เป็น idempotent ฉะนั้นถ้าแก้ DDL แค่
trigger DAG ใหม่ก็พอ (หรือรัน `psql -f /opt/sql/create_raw_tables.sql` ด้วยมือ)
ส่วนที่ต้อง `down -v` จริง ๆ คือการเปลี่ยนรายชื่อ `DW_SCHEMAS`, ชื่อ/รหัสผ่าน Postgres
หรือสคริปต์ `01_`/`02_`

> ระวัง: `CREATE TABLE IF NOT EXISTS` จะ**ไม่**แก้โครงสร้างตารางที่มีอยู่แล้ว ถ้าเพิ่ม/เปลี่ยน
> คอลัมน์ใน DDL ต้อง `DROP TABLE` ตารางนั้นเองก่อน หรือเขียน `ALTER TABLE` เพิ่ม

**ล็อกอิน pgAdmin ต้องใส่อีเมลเต็ม ไม่ใช่ `admin`**
ช่อง Email Address ของ pgAdmin บังคับให้เป็นรูปแบบอีเมล — ต้องใส่ `admin@example.com`
(ค่า `PGADMIN_DEFAULT_EMAIL`) ถ้าใส่แค่ `admin` จะขึ้น *An email address must have an @-sign*
และนับเป็นการล็อกอินผิด 1 ครั้ง ตรงนี้ต่างจาก Airflow ที่ใช้ username `admin` เฉยๆ

ค่าเริ่มต้นของ pgAdmin จะ **ล็อกบัญชีเมื่อใส่รหัสผิดครบ 3 ครั้ง** แล้วขึ้นว่า
*Your account is locked. Please contact the Administrator* — ในไฟล์ compose ตั้ง
`PGADMIN_CONFIG_MAX_LOGIN_ATTEMPTS` จากตัวแปร `PGADMIN_MAX_LOGIN_ATTEMPTS` ใน `.env`
ไว้เป็น `0` = ปิดการล็อกบัญชี (เหมาะกับ dev บนเครื่องตัวเอง) ถ้าอยากเปิดกลับ ให้ตั้งเป็น 3

ถ้าบัญชีถูกล็อกไปแล้ว **ปลดล็อกได้โดยไม่ต้องลบ volume**

```powershell
docker compose exec dw_pgadmin python -c "import sqlite3;c=sqlite3.connect('/var/lib/pgadmin/pgadmin4.db');c.execute('update user set locked=0, login_attempts=0');c.commit();print(c.execute('select email,locked,login_attempts from user').fetchall())"
```

ถ้าลืมรหัสผ่าน pgAdmin ไปเลย ให้แก้ `PGADMIN_DEFAULT_PASSWORD` ใน `.env` แล้ว

```powershell
docker compose down dw_pgadmin
docker volume rm dw_stack_pgadmin_data     # ลบแค่ config ของ pgAdmin ข้อมูล warehouse ไม่หาย
docker compose up -d dw_pgadmin            # servers.json จะถูก import ใหม่ให้เอง
```

**ชื่อ service กับชื่อ container ไม่เหมือนกัน**
compose project นี้ชื่อ `dw_stack` (ตั้งไว้ด้วย `name:` ในไฟล์ compose) และตั้งชื่อ
container เป็น `dw_stack_postgres`, `dw_stack_airflow_webserver` ฯลฯ เพื่อไม่ให้ชนกับ
stack อื่นในเครื่องที่ใช้ชื่อ `dw_postgres` / `dw_metabase` / `dw_pgadmin` อยู่แล้ว
(เช่น `dbt_project_temp` และ `lab-week01`)

ส่วน **ชื่อ service** ยังเป็น `dw_postgres`, `dw_metabase`, `dw_pgadmin` ตามเดิม —
และชื่อ service นี่แหละคือ hostname ที่ใช้คุยกันใน Docker network เพราะฉะนั้น

* ช่อง Host ใน Metabase / pgAdmin ใส่ `dw_postgres` (ไม่ใช่ `dw_stack_postgres`)
* คำสั่ง `docker compose ...` ใช้ชื่อ service เช่น `docker compose logs dw_postgres`
* คำสั่ง `docker ...` ตรง ๆ ใช้ชื่อ container เช่น `docker logs dw_stack_postgres`

**`.env` ต้องไม่เข้า git**
`.gitignore` กันไว้แล้ว ให้ commit แค่ `.env.example` และเปลี่ยนรหัสผ่าน/คีย์ทุกตัวก่อนใช้งานจริง
(`AIRFLOW_FERNET_KEY` ที่ให้มาเป็นตัวอย่าง — สร้างใหม่ก่อนใช้)

**รหัสผ่านที่มีอักขระพิเศษ**
`AIRFLOW_CONN_DW_POSTGRES` และ `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` เป็น URI
ถ้ารหัสผ่านมี `@ : / ? # % &` ต้อง URL-encode (เช่น `@` → `%40`) ไม่งั้นต่อ DB ไม่ได้
ทางที่ง่ายกว่าคือใช้รหัสผ่านที่มีแค่ตัวอักษร ตัวเลข `-` และ `_`

**เปลี่ยน `POSTGRES_USER` แล้วต้องแก้ `servers.json` ด้วย**
ไฟล์ `pgadmin/servers.json` ใส่ username เป็น `dw_user` ไว้แบบตรง ๆ (รูปแบบไฟล์นี้
ไม่รองรับ env var) ถ้าเปลี่ยน user ใน `.env` ให้แก้ฟิลด์ `Username` ในไฟล์นี้ตามไปด้วย

**ไฟล์ `.sh` ต้องเป็น LF**
สคริปต์ใน `postgres/init/` ถ้ากลายเป็น CRLF จะรันไม่ได้
(`exec format error` / `$'\r': command not found`) — `.gitattributes` บังคับ `eol=lf` ไว้แล้ว
ถ้าเปิดแก้ด้วย Notepad ให้เช็กว่า status bar ขวาล่างขึ้น **LF** ไม่ใช่ CRLF

**ทรัพยากรของ Docker Desktop**
stack นี้ (Airflow 2 process + Metabase ซึ่งเป็น JVM + Postgres) ควรให้ Docker Desktop
อย่างน้อย **4 GB RAM** แนะนำ 6 GB — ตั้งที่ Settings → Resources
ถ้า Metabase ถูก kill หรือ restart วน มักเป็นเพราะ RAM ไม่พอ

**โฟลเดอร์อยู่ใน OneDrive**
การ sync ของ OneDrive อาจชนกับไฟล์ log ที่ Airflow เขียนรัว ๆ ใน `logs/`
ถ้าเจออาการไฟล์ล็อกหรือ sync ค้าง ให้ย้ายโปรเจกต์ออกจาก OneDrive
(เช่น `C:\projects\DW`) หรือสั่ง OneDrive ข้ามโฟลเดอร์ `logs/`

**พอร์ตชนกับของเดิม**
ถ้าเครื่องมี PostgreSQL ติดตั้งอยู่แล้ว พอร์ต 5432 จะชน — แก้ `POSTGRES_PORT` ใน `.env`
เป็นค่าอื่น (เช่น `5433`) ซึ่งกระทบแค่การต่อจาก host เพราะภายใน Docker network
ยังคุยกันที่ 5432 ตามปกติ

**`target/` และ log ของ dbt**
ตั้งให้เขียนลง `/tmp/dbt/` ในคอนเทนเนอร์ (`DBT_TARGET_PATH`, `DBT_LOG_PATH`)
เพื่อเลี่ยงปัญหา permission ของ bind mount บน Windows — ไฟล์ compiled SQL จึงไม่โผล่
มาในโฟลเดอร์ host ถ้าต้องการให้โผล่ ให้ลบสองตัวแปรนั้นออกจาก `docker-compose.yml`

**LocalExecutor ไม่ได้ทำมาเพื่อ production**
เหมาะกับงาน dev/ทดลองบนเครื่องเดียว ถ้าจะขึ้น production ควรย้ายไป CeleryExecutor
หรือ KubernetesExecutor และแยก Postgres ของ warehouse ออกจาก metadata DB

---

## 8. คำสั่งที่ใช้บ่อย

```powershell
docker compose ps                                   # สถานะทุก service
docker compose logs -f airflow-scheduler            # ตาม log scheduler
docker compose logs --tail=50 dw_metabase           # log Metabase
docker compose restart airflow-scheduler            # รีสตาร์ท service เดียว
docker compose exec airflow-scheduler bash          # เข้า shell ของ Airflow
docker compose exec dw_postgres psql -U dw_user -d dw   # เข้า psql

# Airflow CLI
docker compose exec airflow-scheduler airflow dags list
docker compose exec airflow-scheduler airflow dags trigger dw_pipeline
docker compose exec airflow-scheduler airflow tasks list dw_pipeline
docker compose exec airflow-scheduler airflow connections get dw_postgres
```
