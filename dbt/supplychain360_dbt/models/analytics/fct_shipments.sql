WITH stg_shipments AS (
    SELECT * FROM {{ ref('stg_shipments') }}
),

dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

dim_warehouses AS (
    SELECT * FROM {{ ref('dim_warehouses') }}
)

SELECT
    s.shipment_id,
    s.product_id,
    s.store_id,
    s.warehouse_id,
    s.carrier,
    s.quantity_shipped,
    s.shipment_date,
    s.expected_delivery_date,
    s.actual_delivery_date,
    s.delivery_delay_days,
    CASE
        WHEN s.delivery_delay_days <= 0 THEN 'On Time'
        WHEN s.delivery_delay_days <= 3 THEN 'Slightly Late'
        ELSE 'Late'
    END AS delivery_status,
    p.product_name,
    p.category,
    w.city AS warehouse_city,
    w.state AS warehouse_state
FROM stg_shipments s
LEFT JOIN dim_products p ON s.product_id = p.product_id
LEFT JOIN dim_warehouses w ON s.warehouse_id = w.warehouse_id