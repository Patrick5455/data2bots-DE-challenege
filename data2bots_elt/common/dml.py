def crate_analytics_tables(staging_schema_name: str,
                           analysis_schema_name: str,
                           analysis_name: str) -> str:
    print(f"selecting query to create {analysis_name} analysis")

    if analysis_name == "agg_public_holiday":
        return f"""
DROP TABLE IF EXISTS {analysis_schema_name}.{analysis_name};
CREATE TABLE {analysis_schema_name}.{analysis_name} AS (
    SELECT current_date AS ingestion_date,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 1
               )        AS tt_holiday_jan,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 2
               )        AS tt_holiday_feb,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 3
               )        AS tt_holiday_mar,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 4
               )        AS tt_holiday_apr,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 5
               )        AS tt_holiday_may,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 6
               )        AS tt_holiday_jun,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 7
               )        AS tt_holiday_jul,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 8
               )        AS tt_holiday_aug,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 9
               )        AS tt_holiday_sep,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 10
               )        AS tt_holiday_oct,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 11
               )        AS tt_holiday_nov,
           count(order_id) FILTER (
               WHERE dim_dates.day_of_the_week_num IN (1, 2, 3, 4, 5)
                   AND dim_dates.working_day <> true
                   AND extract('month' from order_date) = 12
               )        AS tt_holiday_dec
    FROM {staging_schema_name}.orders
             LEFT JOIN if_common.dim_dates
                       ON orders.order_date = dim_dates.calendar_dt
    WHERE dim_dates.year_num <= date_part('year', CURRENT_DATE) - 1 -- past year
    GROUP BY 1
)        
        """

    if analysis_name == "agg_shipments":
        return f"""
DROP TABLE IF EXISTS {analysis_schema_name}.{analysis_name};
CREATE TABLE {analysis_schema_name}.{analysis_name} AS (
    SELECT current_date AS ingestion_date,
           count(shipment_id)
               FILTER (
                   WHERE shipment_date >= order_date + interval '6 days'
                             AND delivery_date IS NULL
                   ) AS tt_late_shipments,
            count(shipment_id)
                FILTER (
                    WHERE delivery_date IS NULL
                              AND shipment_date IS NULL
                              AND '2022-09-05' = order_date + interval '15 days'
                    ) AS tt_undelievered_shipments
    FROM {staging_schema_name}.shipment_deliveries
        LEFT JOIN {staging_schema_name}.orders
            ON orders.order_id = shipment_deliveries.order_id
);
        """

    if analysis_name == "best_performing_product":
        return f"""
DROP TABLE IF EXISTS {analysis_schema_name}.{analysis_name};
CREATE TABLE {analysis_schema_name}.{analysis_name} AS (
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
        FROM {staging_schema_name}.orders
                 LEFT JOIN if_common.dim_products ON orders.product_id = dim_products.product_id
                 LEFT JOIN {staging_schema_name}.reviews ON orders.product_id = reviews.product_id
                 LEFT JOIN if_common.dim_dates ON orders.order_date = dim_dates.calendar_dt
                 LEFT JOIN {staging_schema_name}.shipment_deliveries ON orders.order_id = shipment_deliveries.order_id
        WHERE reviews.review IS NOT NULL
        GROUP BY dim_products.product_name,
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

        """

