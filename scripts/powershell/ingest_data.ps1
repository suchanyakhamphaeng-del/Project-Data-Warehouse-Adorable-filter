Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " Running Data Ingestion Pipeline" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan

# 1. Execute DDL & Truncate
Write-Host "`n1. Creating Schemas, Raw Tables, and Truncating..." -ForegroundColor Yellow
docker exec -i dw_postgres psql -U dw_user -d dw -f /home/raw_data/create_raw_tables.sql

# 2. Bulk Load Transactional CSVs
Write-Host "`n2. Bulk Loading Transactional Data via \copy..." -ForegroundColor Yellow

$tables = @(
    @{ Name = "orders";      Cols = "(order_id, customer_id, store_id, order_date, promotion_id)"; File = "orders.csv" },
    @{ Name = "order_items"; Cols = "(order_item_id, order_id, product_id, qty, price)";          File = "order_items.csv" },
    @{ Name = "payments";    Cols = "(payment_id, order_id, amount)";                             File = "payments.csv" },
    @{ Name = "shipments";   Cols = "(shipment_id, order_id, status)";                            File = "shipments.csv" },
    @{ Name = "returns";     Cols = "(return_id, order_item_id, refund)";                         File = "returns.csv" },
    @{ Name = "customers";   Cols = "(customer_id, city, signup_date)";                           File = "customers.csv" },
    @{ Name = "products";    Cols = "(product_id, category_id, supplier_id, price)";              File = "products.csv" },
    @{ Name = "employees";   Cols = "(employee_id, store_id, salary)";                            File = "employees.csv" }
)

foreach ($t in$tables) {
    Write-Host " Loading raw.$($t.Name)..." -ForegroundColor Gray
    docker exec -i dw_postgres psql -U dw_user -d dw -c "\copy raw.$($t.Name)$($t.Cols) FROM '/home/raw_data/$($t.File)' WITH (FORMAT csv, HEADER true);"
}

# 3. Audit Check
Write-Host "`n3. Running Row Count Audit..." -ForegroundColor Yellow
$auditQuery = "SELECT 'orders' AS table_name, COUNT(*) FROM raw.orders UNION ALL SELECT 'order_items', COUNT(*) FROM raw.order_items UNION ALL SELECT 'payments', COUNT(*) FROM raw.payments UNION ALL SELECT 'shipments', COUNT(*) FROM raw.shipments UNION ALL SELECT 'returns', COUNT(*) FROM raw.returns UNION ALL SELECT 'customers', COUNT(*) FROM raw.customers UNION ALL SELECT 'products', COUNT(*) FROM raw.products UNION ALL SELECT 'employees', COUNT(*) FROM raw.employees;"
docker exec -i dw_postgres psql -U dw_user -d dw -c "$auditQuery"

Write-Host "`n[SUCCESS] Ingestion pipeline complete!" -ForegroundColor Green