DROP TABLE IF EXISTS patrojun6040_analytics.best_performing_product;
CREATE TABLE patrojun6040_analytics.best_performing_product AS (
    WITH ordered_products AS (
        SELECT dim_products.product_name,
               orders.order_date                                     AS most_ordered_day,
               dim_dates.day_of_the_week_num,
               dim_dates.working_day,
               SUM(reviews.review)                                   AS tt_review_points,
               COUNT(*) FILTER (WHERE reviews.review = 1)            AS count_one_star,
               COUNT(*) FILTER (WHERE reviews.review = 2)            AS count_two_star,
               COUNT(*) FILTER (WHERE reviews.review = 3)            AS count_three_star,
               COUNT(*) FILTER (WHERE reviews.review = 4)            AS count_four_star,
               COUNT(*) FILTER (WHERE reviews.review = 5)            AS count_five_star,
               COUNT(*)                                              AS total_orders,
               COUNT(shipment_deliveries.shipment_id) FILTER (
                   WHERE shipment_deliveries.shipment_date < orders.order_date + INTERVAL '6 days'
                       AND shipment_deliveries.delivery_date IS NOT NULL
                   )                                                 AS count_early_shipments,
               COUNT(shipment_deliveries.shipment_id) FILTER (
                   WHERE shipment_deliveries.shipment_date > orders.order_date + INTERVAL '6 days'
                       AND shipment_deliveries.delivery_date IS NULL
                   )                                                 AS count_late_shipments,
               ROW_NUMBER() OVER (ORDER BY SUM(reviews.review) DESC) AS rn
        FROM patrojun6040_staging.orders
                 LEFT JOIN if_common.dim_products ON orders.product_id = dim_products.product_id
                 LEFT JOIN patrojun6040_staging.reviews ON orders.product_id = reviews.product_id
                 LEFT JOIN if_common.dim_dates ON orders.order_date = dim_dates.calendar_dt
                 LEFT JOIN patrojun6040_staging.shipment_deliveries ON orders.order_id = shipment_deliveries.order_id
        WHERE reviews.review IS NOT NULL
        GROUP BY dim_products.product_name,
                 dim_products.product_id,
                 orders.order_date,
                 dim_dates.day_of_the_week_num,
                 dim_dates.working_day
    )
    SELECT current_date                                        AS ingestion_date,
           product_name,
           most_ordered_day,
           CASE
               WHEN day_of_the_week_num IN (1, 2, 3, 4, 5) AND working_day = false THEN true
               ELSE false
               END                                             AS is_public_holiday,
           tt_review_points,
           (count_one_star::float / total_orders) * 100        AS pct_one_star_review,
           (count_two_star::float / total_orders) * 100        AS pct_two_star_review,
           (count_three_star::float / total_orders) * 100      AS pct_three_star_review,
           (count_four_star::float / total_orders) * 100       AS pct_four_star_review,
           (count_five_star::float / total_orders) * 100       AS pct_five_star_review,
           (count_early_shipments::float / total_orders) * 100 AS pct_early_shipments,
           (count_late_shipments::float / total_orders) * 100  AS pct_late_shipments
    FROM
        ordered_products
    WHERE
            rn = 1
);
