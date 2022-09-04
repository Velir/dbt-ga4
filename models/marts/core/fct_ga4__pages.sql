{% if var('static_incremental_days', false ) %}
    {% set partitions_to_replace = [] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            tags=["incremental"],
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
            tags=["incremental"],
            incremental_strategy = 'insert_overwrite',
            partition_by={
                "field": "event_date_dt",
                "data_type": "date",
            },
        )
    }}
{% endif %}

with page_view as (
    select
        page_key,
        event_date_dt,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        count(event_name) as page_views,
        count(distinct user_key ) as users,
        sum( if(ga_session_number = 1,1,0)) as new_users,
        sum(entrances) as entrances,
        sum(engagement_time_msec) as total_time_on_page 
        {% if var("fct_ga4__pages_custom_parameters", "none") != "none" %}
            {{ ga4.mart_custom_parameters( var("fct_ga4__pages_custom_parameters") )}}
        {% endif %}
    from {{ref('stg_ga4__event_page_view')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            where event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
    group by 1,2,3 
    {% if var("fct_ga4__pages_custom_parameters", "none") != "none" %}  
        {{ ga4.mart_group_by_custom_parameters( var("fct_ga4__pages_custom_parameters") )}} 
    {% endif %}
)
, scroll_events_cte as (
    select
        page_key,
        count(event_name) as scroll_events
    from {{ref('stg_ga4__event_scroll')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            where event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
    group by 1
)

{% if var('conversion_events',false) %}
,
conversions_cte as (
    select 
        *
    from {{ ref('stg_ga4__page_conversions') }} conversions 
    {% if is_incremental() %}
        {% if var('static_incremental_days', false ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% else %}
            where event_date_dt >= _dbt_max_partition
        {% endif %}
    {% endif %}
)

select
    page_view.* EXCEPT (page_key,event_date_dt),
    conversions_cte.*,
    ifnull(scroll_events_cte.scroll_events, 0) as scroll_events
from page_view
left join conversions_cte using (page_key)
left join scroll_events_cte using (page_key)

{% else %}
-- Else if no conversions to join
select
    page_view.*,
    ifnull(scroll_events_cte.scroll_events, 0) as scroll_events
from page_view
left join scroll_events_cte using (page_key)
{% endif %}
