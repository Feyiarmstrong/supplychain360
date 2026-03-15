WITH stg_warehouses AS (
    SELECT * FROM {{ ref('stg_warehouses') }}
)

SELECT
    warehouse_id,
    city,
    state
FROM stg_warehouses