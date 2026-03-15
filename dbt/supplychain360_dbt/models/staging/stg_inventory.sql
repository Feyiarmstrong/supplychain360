WITH source AS (
    SELECT * FROM {{ source('raw', 'inventory') }}
),

renamed AS (
    SELECT
        product_id,
        warehouse_id,
        quantity_available::INT AS quantity_available,
        reorder_threshold::INT AS reorder_threshold,
        snapshot_date::DATE AS snapshot_date,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed