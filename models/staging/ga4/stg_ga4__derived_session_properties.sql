{{ config(
  enabled = true if var('derived_session_properties', false) else false,
  materialized = "table"
) }}

-- Remove null user_keys (users with privacy enabled)
with events_from_valid_users as (
    select * from {{ref('stg_ga4__events')}}
    where session_key is not null
),
unnest_user_properties as
(
    select 
        session_key,
        event_timestamp
        {% for sp in var('derived_session_properties', []) %}
            ,{{ ga4.unnest_key('event_params',  sp.event_parameter ,  sp.value_type ) }}
        {% endfor %}
    from events_from_valid_users
)
-- create 1 CTE per user property that pulls only events with non-null values for that event parameters. 
-- Find the most recent property for that user and join later
{% for sp in var('derived_session_properties', []) %}
,non_null_{{sp.event_parameter}} as
(
    select
        session_key,
        event_timestamp,
        {{sp.event_parameter}}
    from unnest_user_properties
    where
        {{sp.event_parameter}} is not null
),
last_value_{{sp.event_parameter}} as 
(
    select
        session_key,
        LAST_VALUE({{ sp.event_parameter }}) OVER (PARTITION BY session_key ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{sp.session_property_name}}
    from non_null_{{sp.event_parameter}}
),
last_value_{{sp.event_parameter}}_grouped as 
(
    select
        session_key,
        {{sp.session_property_name}}
    from last_value_{{sp.event_parameter}}
    group by session_key, {{sp.session_property_name}}
)
{% endfor %}
,
user_keys as 
(
    select distinct
        session_key
    from events_from_valid_users
),
join_properties as 
(
    select
        session_key
        {% for sp in var('derived_session_properties', []) %}
        ,last_value_{{sp.event_parameter}}_grouped.{{sp.session_property_name}}
        {% endfor %}
    from user_keys
    {% for sp in var('derived_session_properties', []) %}
    left join last_value_{{sp.event_parameter}}_grouped using (session_key)
    {% endfor %}
)


select distinct * from join_properties
