with events_by_user_key as (
    select distinct
        user_key,
        last_event_key
    from
    (
        select 
            user_key, 
            last_event_key, 
            row_number() over (partition by user_key order by first_event_timestamp desc) as rn 
        from {{ref('stg_ga4__sessions_first_last_events')}} 
    ) 
    where rn = 1
),
events_joined as (
    select
        events_by_user_key.*,
        geo_country as last_geo_country,
        geo_region as last_geo_region,
        traffic_source_name as last_traffic_source_name,
        traffic_source_medium as last_traffic_source_medium,
        traffic_source_source as last_traffic_source_source,
        events_last.event_timestamp as last_seen_timestamp,
        events_last.event_date_dt as last_seen_dt,
        events_last.ga_session_number as num_sessions
        {% if var("stg_ga4__user_last_events_custom_parameters", "none") != "none" %}
            {{ ga4.mart_custom_parameters( var("stg_ga4__user_last_events_custom_parameters"), 'last_' )}}
        {% endif %}
    from events_by_user_key
    left join {{ref('stg_ga4__events')}} events_last
        on events_by_user_key.last_event_key = events_last.event_key
), pageview as (  -- merge pageview table to prevent query complexity error in dim_ga4__users
    select
        *
    from {{ref('stg_ga4__users_last_pageviews')}}
    right join events_joined using (user_key)
)
select distinct * from pageview