{{ config(materialized='view') }}

WITH daily_inventory AS (
    SELECT
        product_id,
        product_name,
        category,
        snapshot_date,
        quantity_available,
        reorder_threshold,
        needs_reorder,
        warehouse_id,
        warehouse_city
    FROM {{ ref('fct_inventory') }}
)

SELECT
    product_id,
    product_name,
    category,
    warehouse_id,
    warehouse_city,
    COUNT(*) AS total_days_tracked,
    SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) AS days_below_reorder,
    ROUND(
        SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) AS stockout_risk_pct,
    MIN(quantity_available) AS min_stock_recorded,
    ROUND(AVG(quantity_available), 2) AS avg_stock_level
FROM daily_inventory
GROUP BY 1, 2, 3, 4, 5
ORDER BY stockout_risk_pct DESC