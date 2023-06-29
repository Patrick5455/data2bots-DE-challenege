CREATE TABLE IF NOT EXISTS patrojun6040_analytics.agg_public_holiday AS (
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
    FROM patrojun6040_staging.orders
             LEFT JOIN if_common.dim_dates
                       ON orders.order_date = dim_dates.calendar_dt
    WHERE dim_dates.year_num >= date_part('year', CURRENT_DATE) - 1 -- past year
    GROUP BY 1
)
