WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

renamed AS (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        unit_price::FLOAT AS unit_price,
        supplier_id,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed