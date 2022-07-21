{{ config(
  enabled = true if var('derived_user_properties', false) else false,
  materialized = "table"
) }}

-- Remove null user_keys (users with privacy enabled)
with events_from_valid_users as (
    select * from {{ref('stg_ga4__events')}}
    where user_key is not null
),
unnest_user_properties as
(
    select 
        user_key,
        event_timestamp
        {% for up in var('derived_user_properties', []) %}
            ,{{ ga4.unnest_key('event_params',  up.event_parameter ,  up.value_type ) }}
        {% endfor %}
    from events_from_valid_users
)
-- create 1 CTE per user property that pulls only events with non-null values for that event parameters. 
-- Find the most recent property for that user and join later
{% for up in var('derived_user_properties', []) %}
,non_null_{{up.event_parameter}} as
(
    select
        user_key,
        event_timestamp,
        {{up.event_parameter}}
    from unnest_user_properties
    where
        {{up.event_parameter}} is not null
),
last_value_{{up.event_parameter}} as 
(
    select
        user_key,
        LAST_VALUE({{ up.event_parameter }}) OVER (PARTITION BY user_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{up.user_property_name}}
    from non_null_{{up.event_parameter}}
),
last_value_{{up.event_parameter}}_grouped as 
(
    select
        user_key,
        {{up.user_property_name}}
    from last_value_{{up.event_parameter}}
    group by user_key, {{up.user_property_name}}
)
{% endfor %}
,
user_keys as 
(
    select distinct
        user_key
    from events_from_valid_users
),
join_properties as 
(
    select
        user_key
        {% for up in var('derived_user_properties', []) %}
        ,last_value_{{up.event_parameter}}_grouped.{{up.user_property_name}}
        {% endfor %}
    from user_keys
    {% for up in var('derived_user_properties', []) %}
    left join last_value_{{up.event_parameter}}_grouped using (user_key)
    {% endfor %}
)


select distinct * from join_properties
