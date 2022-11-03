-- User dimensions: first geo, first device, last geo, last device, first seen, last seen
{{ config(
    materialized= 'incremental',
    unique_key='user_key'
)
}}

with first_session as (
    select
        *
    from {{ref('stg_ga4__user_first_sessions')}}
)
select 
    user_events.*,
    first_session.* except(user_key)
from user_events
left join first_session using (user_key)
where user_key is not null -- Remove users with privacy settings enabled