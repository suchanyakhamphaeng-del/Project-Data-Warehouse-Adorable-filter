# Docker Guide: Data Warehouse Project

This guide is for people who run the project **with Docker**. It covers starting PostgreSQL, loading the raw data, and running dbt (seeds, staging models, tests).

> No Docker? See `README_2.md` for the local PostgreSQL setup.

---

## 1. What you get

```
raw (8 tables, loaded with \copy)  ->  seeds (4 lookups)  ->  staging (12 views)  ->  marts (next)
```

| Layer | What | How it is created |
|---|---|---|
| `raw` schema | orders, order_items, payments, shipments, returns, customers, products, employees | `ingest_data.ps1` (`\copy`) |
| seeds | stores, promotions, categories, suppliers | `dbt seed` |
| staging | 12 `stg_*` views (one per table, light cleanup) | `dbt run --select staging` |

---

## 2. Prerequisites

- Docker Desktop (on Windows it needs WSL 2)
- Git
- PowerShell (for the ingestion script)

Check Docker works:

```powershell
docker --version
docker compose version
```

---

## 3. Setup

### 3.1 Clone and create your `.env`

```powershell
git clone <your-repo-url>
cd Project-Data-Warehouse-Adorable-filter
copy .env.example .env
```

Open `.env` and fill in the values (database user, password, and so on). **Never commit `.env`.** It must be in `.gitignore`.

The dbt profile (`profiles.yml`) reads these variables, so nothing is hard-coded:

| Variable | Meaning | Default |
|---|---|---|
| `DBT_USER` | database user (required) | none |
| `DBT_PASSWORD` | database password (required) | none |
| `DBT_HOST` | database host | `dw_postgres` (the container name) |
| `DBT_PORT` | database port | `5432` |
| `DBT_DBNAME` | database name | `dw` |
| `DBT_SCHEMA` | target schema for seeds and models | `staging` |
| `DBT_THREADS` | parallel threads | `4` |

### 3.2 Start the containers

```powershell
docker compose up -d
docker ps
```

`dw_postgres` should be **Up** (and pgAdmin, if it is in your compose file).

### 3.3 Check the volume mount

The scripts read files from `/home/raw_data/` **inside** the container. Check that the CSVs and the SQL file are visible:

```powershell
docker exec -it dw_postgres ls /home/raw_data
```

You should see the 8 raw CSVs and `create_raw_tables.sql`. If not, fix the `volumes:` section of `docker-compose.yml` (for example `./raw_data:/home/raw_data` and a mount for `scripts/sql/create_raw_tables.sql`).

---

## 4. Load the raw data

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\powershell\ingest_data.ps1
```

The script creates the `raw` schema and tables, truncates them, loads the 8 CSVs, and checks the row counts. Expected result:

```
 OK   orders        300000
 OK   order_items   600000
 OK   payments      300000
 OK   shipments     300000
 OK   returns        30000
 OK   customers      50000
 OK   products       10000
 OK   employees       1000
[SUCCESS] Ingestion pipeline complete!
```

The script stops with `[FAILED]` if any step fails. It is safe to re-run, because it truncates before loading.

---

## 5. Run dbt

Pick the option that matches your setup.

### Option A: dbt runs inside a Docker container
If your `docker-compose.yml` has a dbt service (the example below uses the service name `dbt`; use yours):

```powershell
docker compose run --rm dbt dbt debug
docker compose run --rm dbt dbt seed
docker compose run --rm dbt dbt run --select staging
docker compose run --rm dbt dbt test
```

Inside the container, the host is `dw_postgres`, which is the default, so you don't need to set `DBT_HOST`.

### Option B: dbt runs on your computer (virtual environment)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install dbt-postgres

cd my_dbt_project
$env:DBT_USER     = "your_db_user"
$env:DBT_PASSWORD = "your_db_password"
$env:DBT_HOST     = "localhost"      # not dw_postgres: dbt is outside Docker
dbt debug
dbt seed
dbt run --select staging
dbt test
```

`localhost` works because the compose file publishes port 5432 to your computer. If you mapped another port (for example `5433:5432`), also set `$env:DBT_PORT = "5433"`.

### Expected dbt results

| Command | Expected |
|---|---|
| `dbt debug` | All checks passed |
| `dbt seed` | 4 seeds: stores 100, promotions 50, categories 30, suppliers 200 |
| `dbt run --select staging` | 12 view models, `PASS=12` |
| `dbt test` | all tests `PASS`, `ERROR=0` (58 at the time of writing) |

---

## 7. Project structure

```
.
├── docker-compose.yml
├── .env.example               # copy to .env (not committed)
├── raw_data/                  # 8 transactional CSVs
├── scripts/
│   ├── sql/create_raw_tables.sql
│   └── powershell/ingest_data.ps1
└── my_dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml           # reads env vars, no passwords inside
    ├── seeds/                 # 4 lookup CSVs + _seeds.yml
    ├── tests/                 # singular SQL tests (for example discount 0-100)
    └── models/staging/        # sources.yml, staging.yml, 12 stg_*.sql
```

Rules to remember:
- A SQL **test** goes in `tests/`, not in `models/`, or dbt treats it as a model.
- In dbt, the file name is the model name, so `stg_orders.sql` creates `stg_orders`.

---

## 9. Troubleshooting

| Problem | Fix |
|---|---|
| `docker is not recognized` / cannot connect to the Docker daemon | Start Docker Desktop and wait until it is running |
| `No such container: dw_postgres` | Run `docker compose up -d`, then check the name with `docker ps` |
| `No such file or directory: /home/raw_data/...` | The volume mount is wrong. Check section 3.3 |
| `port is already allocated` (5432) | Another PostgreSQL uses the port. Stop it, or change the left side of the mapping (for example `5433:5432`) and set `DBT_PORT` |
| `Env var required but not provided: 'DBT_USER'` | Fill in `.env` (Option A) or set `$env:DBT_USER` / `$env:DBT_PASSWORD` (Option B) |
| dbt cannot connect to host `dw_postgres` from your computer | Set `$env:DBT_HOST = "localhost"` (Option B) |
| `password authentication failed` | The password differs from the one the database was created with. If the data volume already exists, changing `.env` alone won't change it, so reset the database (section 8) |
| `relation "raw.orders" does not exist` | Run the ingestion script (section 4) before dbt |
| `source 'raw.orders' not found` | `sources.yml` must be inside `my_dbt_project/models/` |
| Model count is one too high | A SQL test file is inside `models/`. Move it to `tests/` |
| Red files / "Connect to MSSQL" in VS Code | A false alarm: VS Code reads the files as SQL Server. Change the language mode to PostgreSQL or Jinja SQL |

---

## 10. Git hygiene

`.gitignore` should contain:

```
.env
postgres/
pgadmin/
my_dbt_project/target/
my_dbt_project/dbt_packages/
my_dbt_project/logs/
.venv/
```

- Commit `.env.example` (placeholders only), never `.env`.
- `employees.csv` contains salary data, so check that it is okay to share before making the repository public.

---
