{{
    config(materialized='table')
}}

with events_with_user_id as (
    select 
        user_id,
        client_key,
        event_timestamp,
        event_timestamp_dt
    from {{ref('stg_ga4__events')}}
    where user_id is not null
        and client_key is not null
    {% if is_incremental() %}
       and event_date_dt >= CURRENT_DATE() - 7
    {% endif %}
),
include_last_seen_timestamp as (
    select 
        user_id,
        client_key,
        max(event_timestamp) as last_seen_user_id_timestamp,
        max(event_timestamp_dt) as last_seen_user_id_timestamp_dt
    from events_with_user_id
    group by 1,2
),
pick_latest_timestamp as (
    select
        user_id as last_seen_user_id,
        client_key,
        last_seen_user_id_timestamp,
        last_seen_user_id_timestamp_dt
    from include_last_seen_timestamp
    -- Find the latest mapping between client_key and user_id
    qualify row_number() over(partition by client_key order by last_seen_user_id_timestamp desc) = 1

)

select * from pick_latest_timestamp
