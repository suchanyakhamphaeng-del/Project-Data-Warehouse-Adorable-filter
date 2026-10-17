if you don't want to copy-paste into power shell(should work i don't know)

forgot about seed import

```
docker exec -i dw_dbt dbt seed --project-dir my_dbt_project
```


```
docker exec -i dw_postgres psql -U dw_user -d dw -c "SELECT 'stores' AS table_name, COUNT(*) FROM raw.stores UNION ALL SELECT 'promotions', COUNT(*) FROM raw.promotions UNION ALL SELECT 'categories', COUNT(*) FROM raw.categories;"
```
