WITH source AS (
    SELECT * FROM {{ source('raw', 'store_sales') }}
),

renamed AS (
    SELECT
        transaction_id,
        product_id,
        store_id,
        quantity_sold::INT AS quantity_sold,
        unit_price::FLOAT AS unit_price,
        sale_amount::FLOAT AS sale_amount,
        discount_pct::FLOAT AS discount_pct,
        TO_TIMESTAMP(transaction_timestamp::BIGINT / 1000000) AS transaction_timestamp,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed