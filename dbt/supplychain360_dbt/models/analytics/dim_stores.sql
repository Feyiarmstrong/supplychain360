WITH stg_store_locations AS (
    SELECT * FROM {{ ref('stg_store_locations') }}
)

SELECT
    store_id,
    store_name,
    city,
    state,
    region,
    store_open_date
FROM stg_store_locations