WITH source AS (
    SELECT * FROM {{ source('raw', 'store_locations') }}
),

renamed AS (
    SELECT
        store_id,
        store_name,
        city,
        state,
        region,
        TRY_TO_DATE(store_open_date, 'DD/MM/YYYY') AS store_open_date,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed