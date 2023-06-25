CREATE OR REPLACE TABLE {your_id}_analytics.agg_shipments AS (
    SELECT
        COUNT(*) FILTER (WHERE shipment_date >= order_date + INTERVAL '6 days' AND delivery_date IS NULL) AS late_shipments,
        COUNT(*) FILTER (WHERE shipment_date IS NULL AND delivery_date IS NULL AND order_date + INTERVAL '15 days' <= CURRENT_DATE) AS undelivered_shipments
    FROM
        {your_id}_staging.shipments_deliveries
);
