{{ config(
  enabled = true if var('user_properties') else false 
) }}


with unnest_user_properties as
(
    select 
        client_id,
        event_timestamp
        {% for up in var('user_properties') %}
            ,{{ unnest_key('event_params',  up.event_parameter ,  up.value_type ) }}
        {% endfor %}
    from {{ref('stg_ga4__events')}}
),
find_last_value as
(
    select
        client_id
        {% for up in var('user_properties') %}
            ,LAST_VALUE({{ up.event_parameter }}) OVER (PARTITION BY client_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {{up.user_property_name}}
        {% endfor %}
    from unnest_user_properties
)

select distinct * from find_last_value
