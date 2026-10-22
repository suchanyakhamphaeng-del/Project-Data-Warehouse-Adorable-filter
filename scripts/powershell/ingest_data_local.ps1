$ErrorActionPreference = "Stop"

# --- Connection (local Postgres) ---
$PgHost = "localhost"
$PgPort = "5432"
$PgUser = "dw_user"
$PgDb   = "dw"

# Make sure psql is found (your install folder)
if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
    $env:Path += ";E:\PGSQL\bin"
}

# Ask for the password (typing is visible-free, then Enter)
$secure = Read-Host "Password for $PgUser" -AsSecureString
$env:PGPASSWORD = [System.Net.NetworkCredential]::new("", $secure).Password

# --- Paths (script lives in scripts/powershell) ---
$RawDir  = (Resolve-Path "$PSScriptRoot\..\..\raw_data").Path
$SqlFile = (Resolve-Path "$PSScriptRoot\..\sql\create_raw_tables.sql").Path

function Invoke-Psql {
    param([string[]]$PsqlArgs)
    psql -h $PgHost -p $PgPort -U $PgUser -d $PgDb -v ON_ERROR_STOP=1 @PsqlArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`n[FAILED] psql exited with code $LASTEXITCODE" -ForegroundColor Red
        exit 1
    }
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Running Data Ingestion Pipeline (local)" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

Write-Host "`n1. Creating schema, raw tables, and truncating..." -ForegroundColor Yellow
Invoke-Psql -PsqlArgs @("-f", $SqlFile)

Write-Host "`n2. Bulk loading CSVs..." -ForegroundColor Yellow
$tables = @(
    @{ Name = "orders";      Cols = "(order_id, customer_id, store_id, order_date, promotion_id)"; File = "orders.csv";      Expected = 300000 },
    @{ Name = "order_items"; Cols = "(order_item_id, order_id, product_id, qty, price)";          File = "order_items.csv"; Expected = 600000 },
    @{ Name = "payments";    Cols = "(payment_id, order_id, amount)";                             File = "payments.csv";    Expected = 300000 },
    @{ Name = "shipments";   Cols = "(shipment_id, order_id, status)";                            File = "shipments.csv";   Expected = 300000 },
    @{ Name = "returns";     Cols = "(return_id, order_item_id, refund)";                         File = "returns.csv";     Expected = 30000 },
    @{ Name = "customers";   Cols = "(customer_id, city, signup_date)";                           File = "customers.csv";   Expected = 50000 },
    @{ Name = "products";    Cols = "(product_id, category_id, supplier_id, price)";              File = "products.csv";    Expected = 10000 },
    @{ Name = "employees";   Cols = "(employee_id, store_id, salary)";                            File = "employees.csv";   Expected = 1000 }
)

foreach ($t in $tables) {
    Write-Host " Loading raw.$($t.Name)..." -ForegroundColor Gray
    $path = ($RawDir + "\" + $t.File) -replace '\\','/'
    $cmd = "\copy raw.$($t.Name) $($t.Cols) FROM '$path' WITH (FORMAT csv, HEADER true)"
    Invoke-Psql -PsqlArgs @("-c", $cmd)
}

Write-Host "`n3. Row count audit..." -ForegroundColor Yellow
$failed = $false
foreach ($t in $tables) {
    $actual = [int](psql -h $PgHost -p $PgPort -U $PgUser -d $PgDb -t -A -c "SELECT COUNT(*) FROM raw.$($t.Name);")
    if ($actual -eq $t.Expected) {
        Write-Host (" OK   {0,-12} {1,9}" -f $t.Name, $actual) -ForegroundColor Green
    } else {
        Write-Host (" FAIL {0,-12} got {1}, expected {2}" -f $t.Name, $actual, $t.Expected) -ForegroundColor Red
        $failed = $true
    }
}
if ($failed) { Write-Host "`n[FAILED] Row count mismatch." -ForegroundColor Red; exit 1 }
Write-Host "`n[SUCCESS] Ingestion complete!" -ForegroundColor Green