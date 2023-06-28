{% macro create_schema_if_not_exists(schema_name='patrojun6040_staging') %}
CREATE SCHEMA {{ schema_name }};
{% endmacro %}