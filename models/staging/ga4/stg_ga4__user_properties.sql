{{ config(
  enabled = true if var('user_properties', false) else false,
  materialized = "table"
) }}

-- Remove null user_pseudo_id (users with privacy enabled)
with events_from_valid_users as (
    select * from {{ref('stg_ga4__events')}}
    where user_pseudo_id is not null
),
unnest_user_properties as
(
    select 
        user_pseudo_id,
        event_timestamp
        {% for up in var('user_properties', []) %}
            ,{{ ga4.unnest_key('user_properties',  up.user_property_name ,  up.value_type ) }}
        {% endfor %}
    from events_from_valid_users
)
-- create 1 CTE per user property 
{% for up in var('user_properties', []) %}
,non_null_{{up.user_property_name}} as
(
    select
        user_pseudo_id,
        event_timestamp,
        {{up.user_property_name}}
    from unnest_user_properties
    where
        {{up.user_property_name}} is not null
),
last_value_{{up.user_property_name}} as 
(
    select
        user_pseudo_id,
        LAST_VALUE({{ up.user_property_name }}) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{up.user_property_name}}
    from non_null_{{up.user_property_name}}
),
last_value_{{up.user_property_name}}_grouped as 
(
    select
        user_pseudo_id,
        {{up.user_property_name}}
    from last_value_{{up.user_property_name}}
    group by user_pseudo_id, {{up.user_property_name}}
)
{% endfor %}
,
user_pseudo_ids as 
(
    select distinct
        user_pseudo_id
    from events_from_valid_users
),
join_properties as 
(
    select
        user_pseudo_id
        {% for up in var('user_properties', []) %}
        ,last_value_{{up.user_property_name}}_grouped.{{up.user_property_name}}
        {% endfor %}
    from user_pseudo_ids
    {% for up in var('user_properties', []) %}
    left join last_value_{{up.user_property_name}}_grouped using (user_pseudo_id)
    {% endfor %}
)


select distinct * from join_properties
