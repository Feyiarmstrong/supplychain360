{{ config(materialized='table') }}

WITH stg_products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

stg_suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.unit_price,
    p.supplier_id,
    s.supplier_name,
    s.country AS supplier_country
FROM stg_products p
LEFT JOIN stg_suppliers s ON p.supplier_id = s.supplier_id