$ErrorActionPreference = "Stop"

function Invoke-Psql {
    param([string[]]$PsqlArgs)
    docker exec -i dw_postgres psql -U dw_user -d dw -v ON_ERROR_STOP=1 @PsqlArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`n[FAILED] psql exited with code $LASTEXITCODE" -ForegroundColor Red
        exit 1
    }
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Running Data Ingestion Pipeline" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Execute DDL & Truncate
Write-Host "`n1. Creating Schemas, Raw Tables, and Truncating..." -ForegroundColor Yellow
Invoke-Psql -PsqlArgs @("-f", "/home/raw_data/create_raw_tables.sql")

# 2. Bulk Load Transactional CSVs
Write-Host "`n2. Bulk Loading Transactional Data via \copy..." -ForegroundColor Yellow

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
    $cmd = "\copy raw.$($t.Name) $($t.Cols) FROM '/home/raw_data/$($t.File)' WITH (FORMAT csv, HEADER true)"
    Invoke-Psql -PsqlArgs @("-c", $cmd)
}

# 3. Audit Check (compare against expected row counts)
Write-Host "`n3. Running Row Count Audit..." -ForegroundColor Yellow
$failed = $false
foreach ($t in $tables) {
    $actual = [int](docker exec -i dw_postgres psql -U dw_user -d dw -t -A -c "SELECT COUNT(*) FROM raw.$($t.Name);")
    if ($actual -eq $t.Expected) {
        Write-Host (" OK   {0,-12} {1,9}" -f $t.Name, $actual) -ForegroundColor Green
    } else {
        Write-Host (" FAIL {0,-12} got {1}, expected {2}" -f $t.Name, $actual, $t.Expected) -ForegroundColor Red
        $failed = $true
    }
}

if ($failed) { Write-Host "`n[FAILED] Row count mismatch." -ForegroundColor Red; exit 1 }
Write-Host "`n[SUCCESS] Ingestion pipeline complete!" -ForegroundColor Green