{{ config(materialized='view') }}

WITH warehouse_inventory AS (
    SELECT
        warehouse_id,
        warehouse_city,
        warehouse_state,
        snapshot_date,
        COUNT(DISTINCT product_id) AS unique_products_stored,
        SUM(quantity_available) AS total_units_stored,
        SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) AS products_needing_reorder
    FROM {{ ref('fct_inventory') }}
    GROUP BY 1, 2, 3, 4
),

warehouse_shipments AS (
    SELECT
        warehouse_id,
        COUNT(*) AS total_shipments,
        SUM(quantity_shipped) AS total_units_shipped,
        ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
        SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) AS on_time_shipments
    FROM {{ ref('fct_shipments') }}
    GROUP BY 1
)

SELECT
    i.warehouse_id,
    i.warehouse_city,
    i.warehouse_state,
    ROUND(AVG(i.unique_products_stored), 2) AS avg_products_stored,
    ROUND(AVG(i.total_units_stored), 2) AS avg_units_stored,
    ROUND(AVG(i.products_needing_reorder), 2) AS avg_products_needing_reorder,
    MAX(s.total_shipments) AS total_shipments,
    MAX(s.total_units_shipped) AS total_units_shipped,
    MAX(s.avg_delay_days) AS avg_delay_days,
    ROUND(
        MAX(s.on_time_shipments) * 100.0 / NULLIF(MAX(s.total_shipments), 0), 2
    ) AS warehouse_on_time_rate_pct
FROM warehouse_inventory i
LEFT JOIN warehouse_shipments s
    ON i.warehouse_id = s.warehouse_id
GROUP BY 1, 2, 3
ORDER BY warehouse_on_time_rate_pct DESC