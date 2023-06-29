DROP TABLE IF EXISTS patrojun6040_analytics.agg_shipments;
CREATE TABLE patrojun6040_analytics.agg_shipments AS (
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
    FROM patrojun6040_staging.shipment_deliveries
        LEFT JOIN patrojun6040_staging.orders
            ON orders.order_id = shipment_deliveries.order_id
);