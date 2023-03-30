-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key=['event_date_dt', 'mv_region', 'session_page_view_counts']
)
}}
with pv as (
    select
        event_date_dt,
        mv_region,
        case
            when count_page_views = 1 then 'one pv session'
            when count_page_views = 2 then 'two pv session'
            when count_page_views = 3 then 'three pv session'
            when count_page_views = 4 then 'four pv session'
            when count_page_views = 5 then 'five pv session'
            when count_page_views > 5 then 'six+ pv session'
            else null
        end as session_page_view_counts,
        count(mv_author_session_status) as page_views,
        countif(mv_author_session_status = 'Organic') as organic_page_views
    from {{ref('fct_ga4__event_page_view')}}
    where event_date_dt in ({{ partitions_to_replace | join(',') }})
    group by event_date_dt, mv_region, session_page_view_counts
)
select * from pv