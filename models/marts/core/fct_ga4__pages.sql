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
with page_view as (
    select
        page_key,
        event_date_dt,
        page_location,  -- includes query string parameters not listed in query_parameter_exclusions variable
        count(event_name) as page_views,
        count(distinct user_key ) as users,
        sum( if(ga_session_number = 1,1,0)) as new_users,
        sum(entrances) as entrances,
        sum(exits) as exits,
        sum(engagement_time_msec) as total_time_on_page 
        {% if var("fct_ga4__pages_custom_parameters", "none") != "none" %}
            {{ ga4.mart_custom_parameters( var("fct_ga4__pages_custom_parameters") )}}
        {% endif %}
    from {{ref('stg_ga4__event_page_view')}}
    {% if is_incremental() %}
    {% if var('static_incremental_days', 1 ) %}
        where event_date_dt in ({{ partitions_to_replace | join(',') }})
    {% endif %}
{% endif %}
    group by 1,2,3 
    {% if var("fct_ga4__pages_custom_parameters", "none") != "none" %}  
        {{ ga4.mart_group_by_custom_parameters( var("fct_ga4__pages_custom_parameters") )}} 
    {% endif %}
), scroll as (
    select
        page_key,
        count(event_name) as scroll_events
    from {{ref('stg_ga4__event_scroll')}}
    group by 1
)
{% if var('conversion_events',false)  == false %}
select
    page_view.*,
    ifnull(scroll.scroll_events, 0) as scroll_events
from page_view
left join scroll using (page_key)
{% else %}
,
join_conversions as (
    select 
        *
    from page_view
    left join {{ ref('stg_ga4__page_conversions') }} using (page_key)
)
select
    join_conversions.*,
    ifnull(scroll.scroll_events, 0) as scroll_events
from join_conversions
left join scroll using (page_key)

{% endif %}