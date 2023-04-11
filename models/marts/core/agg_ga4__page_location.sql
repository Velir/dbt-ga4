{% if is_incremental %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
            partitions = partitions_to_replace,
        )
    }}
{% else %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}
with normalize as (
    select
        page_title,
        page_location,
        author,
        content_topic,
        content_group,
        article_id,
        article_pubdate
    from {{ref('int_ga4__normalize_page_data')}}
),
pv as (
    select
        event_date_dt,
        page_location,
        mv_region,
        count(page_location) as page_views,
        countif(mv_author_session_status = 'Organic') as organic_page_views,
        count(distinct user_key) as users,
        sum( if(ga_session_number = 1,1,0)) as new_users,
        sum(entrances) as entrances,
        sum(load_time) as total_load_time,
        count(load_time) as avg_load_time_denominator,
        sum(total_engagement_time_msec) as total_engagement_time_msec,
        sum(avg_engagement_time_denominator) as avg_engagement_time_denominator
    from {{ref('fct_ga4__event_page_view')}}
    {% if is_incremental() %} -- 
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by 1,2,3
)
select distinct
    *
from pv
left join normalize using (page_location)
