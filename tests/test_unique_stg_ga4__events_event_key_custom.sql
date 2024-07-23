{% test ten_weeks_dates(model, column_name) %}

select *
from (

    select
        {{ column_name }}

    from {{ model }}
    where {{ column_name }} is not null
    and {{ select_date_range_ga4_package_custom(var("start_date"), var("end_date"), "event_date_dt") }}
    group by {{ column_name }}
    having count(*) > 1

) validation_errors

{% endtest %}