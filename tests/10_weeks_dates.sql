{% test ten_weeks_dates(model, column_name) %}


    {% set environment = env_var('DBT_DEPLOYMENT_ENV', 'dev') %}
    {% set backfill = var('backfill', 'false') %}
    {% set start_date = var('start_date') %}
    {% set end_date = var('end_date') %}

    {% if environment == 'prod' and backfill != 'true' %}
        {{ config(severity = 'error') }}
    {% else %}
        {{ config(severity = 'warn') }}
    {% endif %}


    {% if execute %}
        {{ log("Running 10_weeks_dates test against environment: " ~ environment, info=True) }}
        {{ log("Test start_date: " ~ start_date, info=True) }}
        {{ log("Test end_date: " ~ end_date, info=True) }}
    {% endif %}






with validation as (
    select
        {{ column_name }} as date_column
    from {{ model }}
),

missing_dates AS (
    SELECT
        date AS expected_date
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
    {%- if start_date is not none and end_date is not none -%}
        DATE_SUB(PARSE_DATE("%Y%m%d", cast({{ start_date }} as string)), INTERVAL 10 WEEK)
            , DATE_SUB(PARSE_DATE("%Y%m%d", cast({{ start_date }} as string)), INTERVAL 1 DAY)
    {%- else -%}
            DATE_SUB(CURRENT_DATE(), INTERVAL 10 WEEK)
            , DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    {%- endif -%}
        )
            ) AS date
    EXCEPT DISTINCT
    SELECT date_column FROM validation
)

select
    expected_date
from missing_dates

{% endtest %}
