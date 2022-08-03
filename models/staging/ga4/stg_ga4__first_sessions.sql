{% if var('static_incremental_days', false ) %}
    {% set partition_filter = [] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partition_filter = partition_filter.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
{% endif %}

{{ config(
    materialized= 'incremental',
    unique_key='session_key',
)
}}
with events as (
    select 
        session_key,
        user_key,
        min(event_date_dt) as session_start_date,
        min(event_timestamp) as session_start_timestamp,
    from {{ref('stg_ga4__events')}}
    where ga_session_number = 1
    group by 1,2
), session_starts as (
    select
        session_key,
        geo as first_geo,
        device as first_device,
        traffic_source as first_traffic_source,
        page_referrer as first_page_referrer
    from {{ref('stg_ga4__events')}}
    where event_name = "session_start" 
    and ga_session_number = 1
    left join events using (session_key)
)

select * from session_starts