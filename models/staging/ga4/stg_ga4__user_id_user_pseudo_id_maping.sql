with user_mappings as (
    select 
        user_id,
        user_pseudo_id,
        event_timestamp 
    from {{ref('stg_ga4__events')}}
),
distinct_user_pseudo_id as (
    select distinct
        user_pseudo_id
    from user_mappings
),
most_recent_user_id_mapping as (
    select
        user_pseudo_id,
        -- Find the most recent user_id
        LAST_VALUE(user_id)
            OVER (PARTITION BY user_pseudo_id ORDER BY event_timestap ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seen_user_id
    from user_mappings
    where user_id is not null
)

select
    user_pseudo_id,
    most_recent_user_id_mapping.last_seen_user_id
from distinct_user_pseudo_id
    left join most_recent_user_id_mapping using (user_pseudo_id)