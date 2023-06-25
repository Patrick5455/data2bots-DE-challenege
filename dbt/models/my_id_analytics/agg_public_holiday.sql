CREATE OR REPLACE TABLE {your_id}_analytics.agg_public_holiday AS (
    SELECT
        EXTRACT(MONTH FROM order_date) AS month,
        COUNT(*) AS total_orders
    FROM
        {your_id}_staging.orders
    WHERE
        EXTRACT(DOW FROM order_date) BETWEEN 1 AND 5
        AND working_day = false
        AND order_date >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY
        month
);
