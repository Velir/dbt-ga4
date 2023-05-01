{% if is_incremental %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days', 1)) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            schema = 'analytics',
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
            schema = 'analytics',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}
with page_engagement as (
    select
        page_engagement_key,
        sum(page_engagement_time_msec) as total_engagement_time_msec,
        sum(page_engagement_denominator) as avg_engagement_time_denominator
    from {{ ref('stg_ga4__page_engaged_time') }}
    group by 1
)
select
    event_key,
    user_key,
    session_key,
    page_key,
    article_id,
    event_date_dt,
    event_timestamp,
    event_name,
    case 
        when geo_country = 'United States' then 'US'
        else 'Global'
    end as mv_region,
    mv_author_session_status,
    geo_country,
    device_category,
    page_title,
    page_location,
    original_page_location,
    page_referrer,
    original_page_referrer,
    pagepath_level_1,
    pagepath_level_2,
    content_type,
    author,
    scroll_position,
    content_group,
    content_topic,
    entrances,
    exits,
    load_time,
    article_pubdate,
    ga_session_number,
    page_engagement_key,
    page_engagement.total_engagement_time_msec,
    page_engagement.avg_engagement_time_denominator
from {{ ref('stg_ga4__event_page_view') }}
left join page_engagement using (page_engagement_key)
{% if is_incremental() %}
    {% if var('static_incremental_days', 1 ) %}
        where event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
{% endif %}