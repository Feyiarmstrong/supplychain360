{{ config(
    materialized='incremental',
    unique_key=['product_id', 'warehouse_id', 'snapshot_date'],
    on_schema_change='fail'
) }}

WITH stg_inventory AS (
    SELECT * FROM {{ ref('stg_inventory') }}
    {% if is_incremental() %}
        WHERE ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

dim_warehouses AS (
    SELECT * FROM {{ ref('dim_warehouses') }}
)

SELECT
    i.product_id,
    i.warehouse_id,
    i.snapshot_date,
    i.quantity_available,
    i.reorder_threshold,
    CASE
        WHEN i.quantity_available <= i.reorder_threshold THEN TRUE
        ELSE FALSE
    END AS needs_reorder,
    p.product_name,
    p.category,
    p.brand,
    w.city AS warehouse_city,
    w.state AS warehouse_state
FROM stg_inventory i
LEFT JOIN dim_products p ON i.product_id = p.product_id
LEFT JOIN dim_warehouses w ON i.warehouse_id = w.warehouse_id