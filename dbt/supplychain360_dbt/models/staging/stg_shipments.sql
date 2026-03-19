WITH source AS (
    SELECT * FROM {{ source('raw', 'shipments') }}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM source
),

cleaned AS (
    SELECT
        shipment_id,
        COALESCE(product_id, 'UNKNOWN') AS product_id,
        COALESCE(store_id, 'UNKNOWN') AS store_id,
        COALESCE(warehouse_id, 'UNKNOWN') AS warehouse_id,
        COALESCE(carrier, 'UNKNOWN') AS carrier,
        COALESCE(quantity_shipped, 0)::INT AS quantity_shipped,
        shipment_date::TIMESTAMP AS shipment_date,
        expected_delivery_date::TIMESTAMP AS expected_delivery_date,
        actual_delivery_date::TIMESTAMP AS actual_delivery_date,
        DATEDIFF('day',
            expected_delivery_date::TIMESTAMP,
            actual_delivery_date::TIMESTAMP
        ) AS delivery_delay_days,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM deduplicated
    WHERE row_num = 1
)

SELECT * FROM cleaned