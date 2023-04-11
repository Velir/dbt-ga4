-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key=['event_date_dt', 'mv_region', 'session_channel'],
    cluster_by= ['event_date_dt']
)
}}
with ses as (
    select
        session_start_date as event_date_dt,
        mv_region,
        session_channel,
        count(mv_author_session_status) as page_views,
        countif(mv_author_session_status = 'Organic') as organic_page_views
    from {{ref('dim_ga4__sessions')}}
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            where session_start_date in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by event_date_dt, mv_region, session_channel
)
select * from ses where event_date_dt is not null