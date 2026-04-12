{{ config(
    materialized='incremental',
    unique_key='product_id',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}

    {% if is_incremental() %}
            WHERE _ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}


),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        product_id,
        COALESCE(product_name, 'UNKNOWN') AS product_name,
        COALESCE(category, 'UNCATEGORISED') AS category,
        COALESCE(brand, 'UNKNOWN') AS brand,
        COALESCE(unit_price, 0.0)::FLOAT AS unit_price,
        COALESCE(supplier_id, 'UNKNOWN') AS supplier_id,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned