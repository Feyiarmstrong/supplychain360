WITH source AS (
    SELECT * FROM {{ source('raw', 'warehouses') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        warehouse_id,
        COALESCE(city, 'UNKNOWN') AS city,
        COALESCE(state, 'UNKNOWN') AS state,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned