{{ config(
    materialized='incremental',
    unique_key='store_id',
    on_schema_change='fail'
) }}


WITH source AS (
    SELECT * FROM {{ source('raw', 'store_locations') }}

    {% if is_incremental() %}
            WHERE _ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        store_id,
        COALESCE(store_name, 'UNKNOWN') AS store_name,
        COALESCE(city, 'UNKNOWN') AS city,
        COALESCE(state, 'UNKNOWN') AS state,
        COALESCE(region, 'UNKNOWN') AS region,
        TRY_TO_DATE(store_open_date, 'DD/MM/YYYY') AS store_open_date,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned