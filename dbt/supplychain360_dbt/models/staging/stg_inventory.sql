{{ config(
    materialized='incremental',
    unique_key=['product_id', 'warehouse_id', 'snapshot_date'],
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'inventory') }}

    {% if is_incremental() %}
            WHERE _ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, warehouse_id, snapshot_date
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        COALESCE(product_id, 'UNKNOWN') AS product_id,
        COALESCE(warehouse_id, 'UNKNOWN') AS warehouse_id,
        COALESCE(quantity_available, 0)::INT AS quantity_available,
        COALESCE(reorder_threshold, 0)::INT AS reorder_threshold,
        snapshot_date::DATE AS snapshot_date,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned