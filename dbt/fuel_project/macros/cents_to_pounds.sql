{% macro cents_to_pounds(column_name) %}
    ({{ column_name }} / 100)::numeric(10,2)
{% endmacro %}
