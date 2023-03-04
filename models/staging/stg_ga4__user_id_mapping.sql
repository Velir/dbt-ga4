with events_with_user_id as (
    select 
        user_id,
        user_pseudo_id,
        event_timestamp 
    from {{ref('stg_ga4__events')}}
    where user_id is not null
        and user_pseudo_id is not null
),
include_last_seen_timestamp as (
    select 
        user_id,
        user_pseudo_id,
        max(event_timestamp) as last_seen_user_id_timestamp
    from events_with_user_id
    group by 1,2
),
pick_latest_timestamp as (
    select
        user_id as last_seen_user_id,
        user_pseudo_id,
        last_seen_user_id_timestamp
    from include_last_seen_timestamp
    -- Find the latest mapping between user_pseudo_id and user_id
    qualify row_number() over(partition by user_pseudo_id order by last_seen_user_id_timestamp desc) = 1

)

select * from pick_latest_timestamp
