WITH source AS (
    SELECT * FROM {{ source('raw', 'shipments') }}
),

renamed AS (
    SELECT
        shipment_id,
        product_id,
        store_id,
        warehouse_id,
        carrier,
        quantity_shipped::INT AS quantity_shipped,
        shipment_date::TIMESTAMP AS shipment_date,
        expected_delivery_date::TIMESTAMP AS expected_delivery_date,
        actual_delivery_date::TIMESTAMP AS actual_delivery_date,
        DATEDIFF('day', 
            expected_delivery_date::TIMESTAMP, 
            actual_delivery_date::TIMESTAMP
        ) AS delivery_delay_days,
        _ingested_at::TIMESTAMP AS ingested_at,
        _source AS source_system
    FROM source
)

SELECT * FROM renamed