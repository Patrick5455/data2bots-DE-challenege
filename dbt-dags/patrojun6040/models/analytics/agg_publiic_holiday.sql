-- Create or replace the table for aggregated public holiday orders
{% set schema_name = 'patrojun6040_analytics' %}

CREATE TABLE IF NOT EXISTS {{ schema_name }}.agg_public_holiday (
                                                                    ingestion_date DATE NOT NULL,
                                                                    tt_order_hol_jan INT NOT NULL,
    -- Add similar columns for each month (tt_order_hol_feb, tt_order_hol_mar, etc.)
);

-- Insert data into the table
INSERT INTO {{ schema_name }}.agg_public_holiday (ingestion_date, tt_order_hol_jan)
SELECT
    date_trunc('month', order_date) AS ingestion_date,
    COUNT(*) FILTER (WHERE extract('dow' from order_date) BETWEEN 1 AND 5 AND working_day = false AND extract('year' from order_date) = extract('year' from current_date)) AS tt_order_hol_jan
FROM
    {{ ref('staging_orders') }}
WHERE
        order_date >= current_date - interval '1 year'
GROUP BY
    1;
