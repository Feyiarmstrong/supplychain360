WITH source AS (
    SELECT * FROM {{ source('raw', 'warehouses') }}
),

renamed AS (
    SELECT
        warehouse_id,
        city,
        state,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed