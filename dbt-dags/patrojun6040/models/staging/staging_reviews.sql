{% set schema_name = 'patrojun6040_staging' %}
{% set table_name = 'reviews' %}
{% set s3_data = 's3://d2b-internal-assessment-bucket/orders_data/orders.csv' %}
{% set full_table_name = schema_name ~ '.' ~ table_name %}

-- Create the staging table for reviews
CREATE TABLE IF NOT EXISTS {{ full_table_name }} (
     review INT NOT NULL,
     product_id INT NOT NULL,
     shipment_id INT NOT NULL,
     PRIMARY KEY (review, product_id, shipment_id)
    );