-- simplifying the execution of the user model
{{  
    config(
        materialized='view'
    )
}}
with last_event_values as (  -- get the last session event for each user
    select
        *
    from {{ref('stg_ga4__users_last_events')}} 
)
select
    *
from {{ref('stg_ga4__first_sessions')}}
right join last_event_values using (user_key) -- limit to users with sessions in the window of time that we are working on
{% if var('derived_user_properties', false) %}
    -- If derived user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__derived_user_properties')}} using (user_key)
{% endif %}
{% if var('user_properties', false) %}
    -- If user properties have been assigned as variables, join them on the user_key
    left join {{ref('stg_ga4__user_properties')}} using (user_key)
{% endif %}
