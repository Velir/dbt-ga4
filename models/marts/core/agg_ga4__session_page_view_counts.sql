-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key=['event_date_dt', 'mv_region', 'session_page_view_counts'],
    cluster_by=['event_date_dt']
)
}}
with ses as (
    select
        session_start_date as event_date_dt,
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
        sum(count_page_views) as page_views,
        sum( case when mv_author_session_status = 'Organic' then count_page_views else 0 end) as organic_page_views
    from {{ref('dim_ga4__sessions')}}
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            where session_start_date in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by event_date_dt, mv_region, session_page_view_counts
)
select * from ses where session_page_view_counts is not null and event_date_dt is not null