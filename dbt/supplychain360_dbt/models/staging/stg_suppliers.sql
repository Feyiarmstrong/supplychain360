WITH source AS (
    SELECT * FROM {{ source('raw', 'suppliers') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY supplier_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        supplier_id,
        COALESCE(supplier_name, 'UNKNOWN') AS supplier_name,
        COALESCE(category, 'UNCATEGORISED') AS category,
        COALESCE(country, 'UNKNOWN') AS country,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned