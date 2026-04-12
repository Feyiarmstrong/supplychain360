{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

WITH source AS (
    SELECT * FROM {{ source('raw', 'store_sales') }}

    {% if is_incremental() %}
            WHERE _ingested_at > (SELECT MAX(ingested_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        transaction_id,
        COALESCE(product_id, 'UNKNOWN') AS product_id,
        COALESCE(store_id, 'UNKNOWN') AS store_id,
        COALESCE(quantity_sold, 0)::INT AS quantity_sold,
        COALESCE(unit_price, 0.0)::FLOAT AS unit_price,
        COALESCE(sale_amount, 0.0)::FLOAT AS sale_amount,
        COALESCE(discount_pct, 0.0)::FLOAT AS discount_pct,
        TO_TIMESTAMP(transaction_timestamp::BIGINT / 1000000) AS transaction_timestamp,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned