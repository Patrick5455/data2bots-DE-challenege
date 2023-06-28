{% set schema_name = 'patrojun6040_staging' %}
{% set table_name = 'orders' %}
{% set s3_data = 's3://d2b-internal-assessment-bucket/orders_data/orders.csv' %}
{% set full_table_name = schema_name ~ '.' ~ table_name %}
{% set data_source = 'data/orders_data/orders.csv' %}
{{ config(materialized='table') }}

-- Create the staging table for orders
CREATE TABLE IF NOT EXISTS {{ full_table_name }} (
    order_id INT NOT NULL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    product_id VARCHAR(32) NOT NULL,
    unit_price INT NOT NULL,
    quantity INT NOT NULL,
    total_price INT NOT NULL
    );


