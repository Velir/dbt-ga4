-- not a partitioned model, but using partitions to replace for query pruning
{% set partitions_to_replace = ["current_date"] %}
{% for i in range(var("static_incremental_days", 1)) %}
{% set partitions_to_replace = partitions_to_replace.append(
    "date_sub(current_date, interval " + (i + 1) | string + " day)"
) %}
{% endfor %}

{{config(
    materialized='incremental',
    unique_key=['event_date_dt', 'mv_region', 'content_topic'],
    cluster_by= ['event_date_dt']
)
}}
with pv as (
    select
        event_date_dt,
        mv_region,
        content_topic,
        count(mv_author_session_status) as page_views,
        countif(mv_author_session_status = 'Organic') as organic_page_views
    from {{ref('fct_ga4__event_page_view')}}
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by event_date_dt, mv_region, content_topic
)
select distinct * from pv where event_date_dt is not null