{{ config(
  enabled = true if var('derived_user_properties', false) else false,
  materialized = "table"
) }}

-- Remove null client_key (users with privacy enabled)
with events_from_valid_users as (
    select * from {{ref('stg_ga4__events')}}
    where client_key is not null
),
unnest_user_properties as
(
    select 
        client_key,
        event_timestamp
        {% for up in var('derived_user_properties', []) %}
            ,{{ ga4.unnest_key('event_params',  up.event_parameter ,  up.value_type ) }}
        {% endfor %}
    from events_from_valid_users
)

SELECT DISTINCT
    client_key
    {% for up in var('derived_user_properties', []) %}
        , LAST_VALUE({{ up.event_parameter }} IGNORE NULLS) OVER (user_window) AS {{ up.user_property_name }}
    {% endfor %}
FROM unnest_user_properties
WINDOW user_window AS (PARTITION BY client_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
