WITH stg_suppliers AS (
    SELECT * FROM {{ ref('stg_suppliers') }}
)

SELECT
    supplier_id,
    supplier_name,
    category,
    country
FROM stg_suppliers