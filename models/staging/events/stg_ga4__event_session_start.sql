 with session_start_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("session_start_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("session_start_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'session_start'
)

select * from session_start_with_params