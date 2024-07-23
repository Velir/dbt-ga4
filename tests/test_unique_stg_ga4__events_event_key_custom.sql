{% test test_test_unique_stg_ga4__events_event_key_custom(model, column_name) %}
{% set start_date = var('start_date') %}
{% set end_date = var('end_date') %}

select *
from (

    select
        {{ column_name }}

    from {{ model }}
    where {{ column_name }} is not null
    {% if start_date is not none and start_date != '' and end_date is not none and end_date != '' %}
        and event_date_dt between '{{ start_date }}' and '{{ end_date }}'
    {% elif start_date is not none and start_date != '' %}
        and event_date_dt >= '{{ start_date }}'
    {% endif %}

    group by {{ column_name }}
    having count(*) > 1

) validation_errors

{% endtest %}