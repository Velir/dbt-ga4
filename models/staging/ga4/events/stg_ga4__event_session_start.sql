 with session_start_with_params as (
   select *,
      {{ unnest_key('event_params', 'entrances',  'int_value') }},
      {{ unnest_key('event_params', 'value', 'float_value') }}
      {% if var("session_start") %}
        {{ stage_custom_parameters( var("session_start") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'session_start'
)

select * from session_start_with_params