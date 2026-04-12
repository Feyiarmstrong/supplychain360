{{ config(
    materialized='incremental',
    unique_key='supplier_id',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'suppliers') }}

    {% if is_incremental() %}
            WHERE _ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
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