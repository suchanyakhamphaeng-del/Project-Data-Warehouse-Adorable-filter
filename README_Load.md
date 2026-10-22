# Data Warehouse Project: Ingestion & dbt Setup (README 2)

This file explains the latest changes to the ingestion layer and how to run the project, either **with Docker** or **with a local PostgreSQL install** (no Docker/WSL needed).

---

## 1. What changed

| Change | Why |
|---|---|
| Lookup tables (`stores`, `promotions`, `categories`, `suppliers`) are now **dbt seeds**, not raw tables | They are small, static reference data, so they are version-controlled in git and loaded with `dbt seed` |
| `raw` schema holds only the **8 transactional tables** | Large / frequently changing data is bulk-loaded with `\copy`, which is much faster than `dbt seed` |
| Removed duplicate lookup CSVs from `raw_data/` | One source of truth: `my_dbt_project/seeds/` |
| `create_raw_tables.sql`: `TRUNCATE` now lists only the 8 raw tables (no `CASCADE`) | The old version referenced tables that no longer exist and failed |
| Ingestion script: added `-v ON_ERROR_STOP=1`, exit-code checks and **row-count audit** | The old script printed `SUCCESS` even when a load failed |
| Fixed `foreach ($t in$tables)` typo (needs a space: `$t in $tables`) | PowerShell syntax error |
| Added `ingest_data_local.ps1` for local PostgreSQL | For machines without Docker |
| Added `suppliers` to `_seeds.yml` with tests | It was the missing 4th seed; `products.supplier_id` needs it |

---

## 2. What goes where

**Seeds** (`my_dbt_project/seeds/`, loaded with `dbt seed`)

| File | Rows |
|---|---|
| `categories.csv` | 30 |
| `promotions.csv` | 50 |
| `stores.csv` | 100 |
| `suppliers.csv` | 200 |

**Raw** (`raw_data/`, loaded into schema `raw` with `\copy`)

| File | Rows |
|---|---|
| `orders.csv` | 300,000 |
| `order_items.csv` | 600,000 |
| `payments.csv` | 300,000 |
| `shipments.csv` | 300,000 |
| `returns.csv` | 30,000 |
| `customers.csv` | 50,000 |
| `products.csv` | 10,000 |
| `employees.csv` | 1,000 |

> Every raw table also has a `loaded_at` column (default `now()`), so the `\copy` commands list their columns explicitly.

---

## 3. Project structure

```
Project-Data-Warehouse-Adorable-filter/
├── my_dbt_project/
│   ├── models/              # staging models + sources.yml (raw sources)
│   ├── seeds/               # 4 lookup CSVs + _seeds.yml
│   ├── dbt_project.yml
│   └── profiles.yml         # DO NOT commit real passwords (see section 8)
├── raw_data/                # 8 transactional CSVs
├── scripts/
│   ├── sql/create_raw_tables.sql
│   └── powershell/
│       ├── ingest_data.ps1        # Docker version
│       └── ingest_data_local.ps1  # Local PostgreSQL version
├── .env.example
└── README_2.md
```

---

## 4. Run order (the whole pipeline)

```
create raw tables  ->  load raw CSVs  ->  dbt seed  ->  dbt run  ->  dbt test
```

---

## 5A. Option A: Docker

Requires Docker Desktop (and WSL 2 on Windows). The container is named `dw_postgres`, with user `dw_user` and database `dw`.

### 1. Start the containers
From the project root (where `docker-compose.yml` is):

```powershell
docker compose up -d
docker ps
```
`docker ps` should list `dw_postgres` (and `pgadmin` if it is in your compose file) as **Up**.

###SKIP THIS FOR NOW IDK IF IT WILL BE PROBLEM IN THE FUTURE V###
### 2. Check the volume mount
The ingestion script reads files from inside the container at `/home/raw_data/`. Your `docker-compose.yml` must mount the data and SQL there, for example:

```yaml
services:
  postgres:
    container_name: dw_postgres
    image: postgres:16
    environment:
      POSTGRES_USER: dw_user
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: dw
    ports:
      - "5432:5432"
    volumes:
      - ./postgres:/var/lib/postgresql/data
      - ./raw_data:/home/raw_data
      - ./scripts/sql/create_raw_tables.sql:/home/raw_data/create_raw_tables.sql
```
###^ SKIP THIS FOR NOW IDK IF IT WILL BE PROBLEM IN THE FUTURE ###

Adjust the names and paths to match your real file. Quick check that the files are visible inside the container:

```powershell
docker exec -it dw_postgres ls /home/raw_data
```
You should see the 8 CSVs and `create_raw_tables.sql`. If the SQL file is missing, fix the mount (the script expects it at `/home/raw_data/create_raw_tables.sql`).

###^ SKIP THIS FOR NOW IDK IF IT WILL BE PROBLEM IN THE FUTURE ###

### 3. Load the raw data
```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\powershell\ingest_data.ps1
```
You should see 8 green `OK` lines and `[SUCCESS]` (same expected counts as in Option B).

### 4. Look inside the database (optional)
```powershell
docker exec -it dw_postgres psql -U dw_user -d dw
```
Then, for example:
```sql
\dt raw.*
select count(*) from raw.orders;
\q
```
Or use pgAdmin in the browser (usually `http://localhost:5050`, check your compose file for the port and login). Register a server with host `dw_postgres` (the container name; use `localhost` only if pgAdmin runs outside Docker), port `5432`, database `dw`, user `dw_user`.

### 5. Run dbt against the container
dbt runs on your computer and connects through the published port, so in `profiles.yml` use `host: localhost` and the **left** side of the port mapping (`5432` in the example above). Use the same profile as in section 6.

### Useful Docker commands

| Task | Command |
|---|---|
| Stop containers (keep data) | `docker compose stop` |
| Stop and remove containers (data in `./postgres` is kept) | `docker compose down` |
| View logs | `docker logs dw_postgres` |
| Start again | `docker compose up -d` |
| Wipe the database completely | `docker compose down`, then delete the `postgres/` folder |

> Docker users: after the load, continue with **section 6 (dbt setup)**. It is the same for Docker and local installs.

---

