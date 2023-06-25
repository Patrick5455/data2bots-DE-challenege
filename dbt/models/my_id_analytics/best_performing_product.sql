CREATE OR REPLACE TABLE {your_id}_analytics.best_performing_product AS (
    WITH product_reviews AS (
        SELECT
            p.product_id,
            p.product_name,
            r.review_date,
            r.review_points
        FROM
            {your_id}_staging.products p
        INNER JOIN
            {your_id}_staging.reviews r ON p.product_id = r.product_id
    )
    SELECT
        p.product_id,
        p.product_name,
        MAX(r.review_date) AS most_ordered_date,
        EXTRACT(DOW FROM MAX(r.review_date)) BETWEEN 1 AND 5 AS is_public_holiday,
        SUM(r.review_points) AS total_review_points,
        (SUM(r.review_points) * 100) / (SELECT SUM(review_points) FROM product_reviews) AS review_points_percentage,
        (COUNT(*) FILTER (WHERE s.shipment_date < s.order_date + INTERVAL '6 days') * 100) / COUNT(*) FILTER (WHERE s.shipment_date >= s.order_date + INTERVAL '6 days') AS early_shipments_percentage
    FROM
        product_reviews p
    INNER JOIN
        {your_id}_staging.orders o ON p.product_id = o.product_id
    INNER JOIN
        {your_id}_staging.shipments_deliveries s ON o.order_id = s.order_id
    GROUP BY

