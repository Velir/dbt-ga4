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
with normalize as (
    select
        page_location,
        content_topic,
    from {{ref('int_ga4__normalize_page_data')}}
),
pv as (
    select
        event_date_dt,
        normalize.content_topic,
        mv_region,
        count(*) as page_views,
        countif(mv_author_session_status = 'Organic') as organic_page_views,
    from {{ref('fct_ga4__event_page_view')}}
    left join normalize using (page_location)
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by 1,2,3
)
select distinct * from pv where event_date_dt is not null