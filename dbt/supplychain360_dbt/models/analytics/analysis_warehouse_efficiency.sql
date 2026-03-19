WITH shipment_data AS (
    SELECT
        s.shipment_id,
        s.product_id,
        s.carrier,
        s.delivery_delay_days,
        s.delivery_status,
        s.quantity_shipped,
        p.supplier_id,
        p.supplier_name,
        p.supplier_country
    FROM {{ ref('fct_shipments') }} s
    LEFT JOIN {{ ref('dim_products') }} p
        ON s.product_id = p.product_id
)

SELECT
    supplier_id,
    supplier_name,
    supplier_country,
    COUNT(*) AS total_shipments,
    SUM(quantity_shipped) AS total_units_shipped,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay_days,
    SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) AS on_time_count,
    SUM(CASE WHEN delivery_status = 'Slightly Late' THEN 1 ELSE 0 END) AS slightly_late_count,
    SUM(CASE WHEN delivery_status = 'Late' THEN 1 ELSE 0 END) AS late_count,
    ROUND(
        SUM(CASE WHEN delivery_status = 'On Time' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) AS on_time_rate_pct
FROM shipment_data
GROUP BY 1, 2, 3
ORDER BY on_time_rate_pct ASC