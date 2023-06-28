{% set schema_name = 'patrojun6040_staging' %}
    {% set table_name = 'shipment_deliveries' %}
    {% set full_table_name = schema_name ~ '.' ~ table_name %}

-- Create the staging table for shipment deliveries
CREATE TABLE IF NOT EXISTS {{ full_table_name }} (
     shipment_id INT NOT NULL PRIMARY KEY,
     order_id INT NOT NULL,
     shipment_date DATE NOT NULL,
     delivery_date DATE NOT NULL
    );
