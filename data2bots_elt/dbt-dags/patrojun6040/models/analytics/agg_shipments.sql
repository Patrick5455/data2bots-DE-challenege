-- Create or replace the table for late shipments
{% set schema_name = 'patrojun6040_analytics' %}

CREATE TABLE IF NOT EXISTS {{ schema_name }}.late_shipments (
                                                                ingestion_date DATE NOT NULL,
                                                                tt_late_shipments INT NOT NULL
);

-- Insert data into the table
INSERT INTO {{ schema_name }}.late_shipments (ingestion_date, tt_late_shipments)
SELECT
    date_trunc('day', current_date) AS ingestion_date,
    COUNT(*) FILTER (WHERE shipment_date >= order_date + interval '6 days' AND delivery_date IS NULL) AS tt_late_shipments
FROM
    {{ ref('shipment_deliveries') }}
WHERE
        order_date >= current_date - interval '1 year'
GROUP BY
    1;
