-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key=['event_date_dt', 'mv_region']
)
}}
with ses as (
    select
        session_start_date as event_date_dt,
        mv_region,
        count(distinct session_key) as sessions,
        countif( mv_author_session_status = 'Organic' ) as organic_sessions,
        countif( ga_session_number > 1  ) as returning_sessions,
        countif( mv_author_session_status = 'Organic' and ga_session_number > 1) as organic_returning_sessions,
        count(distinct user_key) as users,
        count( distinct  (case when mv_author_session_status = 'Organic' then user_key end ) as organic_users,
        sum(purchase_count) as purchases,
        sum(award_application_count) as award_applications,
        sum(event_registration_count) as event_registrations,
        sum(sum_engagement_time_msec) as total_engagement_time_msec,
        countif(count_page_views = 1) as one_page_view_sessions,
        sum(session_engaged) as engaged_sessions,
        sum( case when mv_author_session_status = 'Organic' then session_engaged end ) as organic_engaged_sessions,
    from {{ref('dim_ga4__sessions')}}
    where session_start_date in ({{ partitions_to_replace | join(',') }})
    group by event_date_dt, mv_region
), 
pg as (
    select
        event_date_dt,
        mv_region,
        count(mv_author_session_status) as page_views,
        countif(mv_author_session_status = 'Organic') as organic_page_views,
        sum(load_time) as total_load_time_msec,
        countif(load_time is not null) as avg_load_time_denominator
    from {{ref('fct_ga4__event_page_view')}}
    where event_date_dt in ({{ partitions_to_replace | join(',') }})
    group by event_date_dt, mv_region
)
select
    *
from ses
left join pg using(event_date_dt, mv_region)