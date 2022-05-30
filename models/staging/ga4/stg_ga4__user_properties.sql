{{ config(
  enabled = true if var('user_properties', false) != false else false
) }}


with unnest_user_properties as
(
    select 
        client_id,
        event_timestamp
        {% for up in var('user_properties', []) %}
            ,{{ unnest_key('event_params',  up.event_parameter ,  up.value_type ) }}
        {% endfor %}
    from {{ref('stg_ga4__events')}}
)
-- create 1 CTE per user property that pulls only events with non-null values for that event parameters. 
-- Find the most recent property for that user and join later
{% for up in var('user_properties', []) %}
,non_null_{{up.event_parameter}} as
(
    select
        client_id,
        event_timestamp,
        {{up.event_parameter}}
    from unnest_user_properties
    where
        {{up.event_parameter}} is not null
),
last_value_{{up.event_parameter}} as 
(
    select
        client_id,
        LAST_VALUE({{ up.event_parameter }}) OVER (PARTITION BY client_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{up.user_property_name}}
    from non_null_{{up.event_parameter}}
),
last_value_{{up.event_parameter}}_grouped as 
(
    select
        client_id,
        {{up.user_property_name}}
    from last_value_{{up.event_parameter}}
    group by client_id, {{up.user_property_name}}
)
{% endfor %}
,
client_ids as 
(
    select distinct
        client_id
    from unnest_user_properties
),
join_properties as 
(
    select
        client_id
        {% for up in var('user_properties', []) %}
        ,last_value_{{up.event_parameter}}_grouped.{{up.user_property_name}}
        {% endfor %}
    from client_ids
    {% for up in var('user_properties', []) %}
    left join last_value_{{up.event_parameter}}_grouped using (client_id)
    {% endfor %}
)


select distinct * from join_properties
