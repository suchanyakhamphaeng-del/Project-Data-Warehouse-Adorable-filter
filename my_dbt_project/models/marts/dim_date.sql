WITH date_series AS (
    SELECT generate_series(
        '2019-01-01'::date,
        '2025-12-31'::date,
        '1 day'::interval
    )::date AS date_day
)
SELECT
    date_day,
    TO_CHAR(date_day, 'YYYYMMDD')::integer AS date_key,
    EXTRACT(YEAR FROM date_day)::integer AS year,
    EXTRACT(QUARTER FROM date_day)::integer AS quarter,
    EXTRACT(MONTH FROM date_day)::integer AS month,
    TRIM(TO_CHAR(date_day, 'Month')) AS month_name,
    EXTRACT(DAY FROM date_day)::integer AS day_of_month,
    EXTRACT(DOW FROM date_day)::integer AS day_of_week,
    TRIM(TO_CHAR(date_day, 'Day')) AS day_name,
    EXTRACT(WEEK FROM date_day)::integer AS week_of_year,
    CASE WHEN EXTRACT(ISODOW FROM date_day) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_series
