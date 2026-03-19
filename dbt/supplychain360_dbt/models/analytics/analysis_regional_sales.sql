WITH sales_data AS (
    SELECT
        store_id,
        store_name,
        store_city,
        store_state,
        store_region,
        product_id,
        product_name,
        category,
        brand,
        DATE(transaction_timestamp) AS sale_date,
        quantity_sold,
        sale_amount,
        discount_pct
    FROM {{ ref('fct_sales') }}
)

SELECT
    store_region,
    store_state,
    DATE_TRUNC('month', sale_date) AS sale_month,
    COUNT(DISTINCT store_id) AS active_stores,
    COUNT(*) AS total_transactions,
    SUM(quantity_sold) AS total_units_sold,
    ROUND(SUM(sale_amount), 2) AS total_revenue,
    ROUND(AVG(sale_amount), 2) AS avg_transaction_value,
    ROUND(AVG(discount_pct) * 100, 2) AS avg_discount_pct,
    COUNT(DISTINCT product_id) AS unique_products_sold
FROM sales_data
GROUP BY 1, 2, 3
ORDER BY store_region, sale_month