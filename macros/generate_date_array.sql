{% macro generate_date_array(start_date, end_date) %}
{% if execute %}
    (
        SELECT
            ARRAY_AGG(date) AS date_array
        FROM
            UNNEST(GENERATE_DATE_ARRAY(
                DATE('{{ start_date }}'),
                DATE('{{ end_date }}'),
                INTERVAL 1 DAY
            )) AS date
    )
    {% endif %}
{% endmacro %}
