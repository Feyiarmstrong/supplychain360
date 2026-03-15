WITH source AS (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

renamed AS (
    SELECT
        supplier_id,
        supplier_name,
        category,
        country,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed