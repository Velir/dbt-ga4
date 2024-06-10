
{% test ten_weeks_dates(model, column_name) %}


{% set environment = env_var('DBT_ENVIRONMENT', 'elmyra-dev') %}
{% set backfill = var('backfill', 'false') %}

{% if environment == 'elmyra-prod' and backfill != 'true' %}
    {{ config(severity = 'error') }}
{% else %}
    {{ config(severity = 'warn') }}
{% endif %}


{% if execute %}
{{ log("Running 10_weeks_dates test against environment: " ~ environment, info=True) }}
{% endif %}



with validation as (
    select
        {{ column_name }} as date_column
    from {{ model }}
),

missing_dates AS (
    SELECT
        date AS expected_date
    FROM UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE(), INTERVAL 10 WEEK), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))) AS date
    EXCEPT DISTINCT
    SELECT date_column FROM validation
)

select
    expected_date
from missing_dates

{% endtest %}