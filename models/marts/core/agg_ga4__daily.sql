-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key='event_date_dt'
)
}}
with ses as (
    select
        session_start_date as date_day,
        count(distinct session_key) as sessions,
        count(distinct user_key) as users,
        sum(purchase_count) as purchases,
        sum(award_application_count) as award_applications,
        sum(event_registration_count) as event_registrations
    from {{ref('fct_ga4__sessions')}}
    where session_start_date in partitions_to_replace
    group by date_day
), 
pg as (
    select
        event_date_dt as date_day,
        sum(page_views) as page_views,
        sum(us_organic_page_views) as us_organic_page_views
    from {{ref('fct_ga4__pages')}}
    where event_date_dt in partitions_to_replace
    group by date_day
)
select
    *
from ses
left join pg using(date_day)