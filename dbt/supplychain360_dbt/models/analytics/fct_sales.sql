WITH stg_store_sales AS (
    SELECT * FROM {{ ref('stg_store_sales') }}
),

dim_stores AS (
    SELECT * FROM {{ ref('dim_stores') }}
),

dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
)

SELECT
    s.transaction_id,
    s.product_id,
    s.store_id,
    s.quantity_sold,
    s.unit_price,
    s.sale_amount,
    s.discount_pct,
    s.transaction_timestamp,
    DATE(s.transaction_timestamp) AS sale_date,
    p.product_name,
    p.category,
    p.brand,
    st.store_name,
    st.city AS store_city,
    st.state AS store_state,
    st.region AS store_region
FROM stg_store_sales s
LEFT JOIN dim_products p ON s.product_id = p.product_id
LEFT JOIN dim_stores st ON s.store_id = st.store_id