-- Fact table for sessions. Join on session_key
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
                "field": "session_start_date",
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
                "field": "session_start_date",
                "data_type": "date",
            },
        )
    }}
{% endif %}

with session_metrics as 
(
    select 
        session_key,
        user_key,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp,
        countif(event_name = 'page_view') as count_page_views,
        sum(event_value_in_usd) as sum_event_value_in_usd,
        ifnull(max(session_engaged), 0) as session_engaged,
        sum(engagement_time_msec) as sum_engagement_time_msec
        {% if var("fct_ga4__sessions_custom_parameters", "none") != "none" %}
            {{ ga4.mart_custom_parameters( var("fct_ga4__sessions_custom_parameters") )}}
        {% endif %}
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by 1,2    
    {% if var("fct_ga4__sessions_custom_parameters", "none") != "none" %}  
        {{ ga4.mart_group_by_custom_parameters( var("fct_ga4__sessions_custom_parameters") )}} 
    {% endif %}
),

include_session_properties as (
    select * from session_metrics
    {% if var('derived_session_properties', false) %}
    -- If derived session properties have been assigned as variables, join them on the session_key
    left join {{ref('stg_ga4__derived_session_properties')}} using (session_key)
    {% endif %}
)

{% if var('conversion_events',false) %}
,
join_conversions as (
    select 
        *
    from include_session_properties
    left join {{ref('stg_ga4__session_conversions')}} using (session_key)
)
select * from join_conversions
{% else %}
select * from include_session_properties
{% endif %}

