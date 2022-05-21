 -- reference here: https://support.google.com/analytics/answer/9216061?hl=en&ref_topic=9756175
 
 with event_with_params as (
   select *,
      {{ unnest_key('event_params', 'entrances',  'int_value') }},
      {{ unnest_key('event_params', 'value', 'float_value') }},
      {{ unnest_key('event_params', 'file_extension') }},
      {{ unnest_key('event_params', 'file_name') }},
      {{ unnest_key('event_params', 'link_classes') }},
      {{ unnest_key('event_params', 'link_domain') }},
      {{ unnest_key('event_params', 'link_id') }},
      {{ unnest_key('event_params', 'link_text') }},
      {{ unnest_key('event_params', 'link_url') }}
      {% if var("file_download_custom_parameters", "none") != "none" %}
        {{ stage_custom_parameters( var("file_download_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'file_download'
)

select * from event_with_params