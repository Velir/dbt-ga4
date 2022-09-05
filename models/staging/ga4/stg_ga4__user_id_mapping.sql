{{
    config(
        materialized = 'incremental',
        tags = ["incremental"],
        unique_key='user_pseudo_id'
    )
}}

with user_mappings as (
    select 
        user_id,
        user_pseudo_id,
        event_timestamp 
    from {{ref('stg_ga4__events')}}
    {% if is_incremental() %}
        -- Process mappings starting the day before the most recent mapping
        where event_date_dt >= DATE_SUB((SELECT max(DATE(TIMESTAMP_MICROS(last_seen_timestamp))) from {{this}}), INTERVAL 1 DAY)
    {% endif %}
),
user_pseudo_id_cte as (
    select 
        user_pseudo_id,
        -- last_seen_timestamp is included so we know which dates to process in an incremental run
        max(event_timestamp) as last_seen_timestamp
    from user_mappings
    group by 1
),
most_recent_user_id_mapping as (
    select
        user_pseudo_id,
        -- Find the most recent user_id
        LAST_VALUE(user_id)
            OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seen_user_id
    from user_mappings
    where user_id is not null
)

select
    user_pseudo_id,
    last_seen_timestamp,
    most_recent_user_id_mapping.last_seen_user_id
from user_pseudo_id_cte
    left join most_recent_user_id_mapping using (user_pseudo_id)