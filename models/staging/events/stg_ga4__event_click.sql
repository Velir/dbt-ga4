-- reference here: https://support.google.com/analytics/answer/9216061?hl=en 
 
 with click_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'outbound') }},
      {{ ga4.unnest_key('event_params', 'link_classes') }},
      {{ ga4.unnest_key('event_params', 'link_domain') }},
      {{ ga4.unnest_key('event_params', 'link_url') }},
      {{ ga4.unnest_key('event_params', 'click_element') }},
      {{ ga4.unnest_key('event_params', 'link_id') }},
      {{ ga4.unnest_key('event_params', 'click_region') }},
      {{ ga4.unnest_key('event_params', 'click_tag_name') }},
      {{ ga4.unnest_key('event_params', 'click_url') }},
      {{ ga4.unnest_key('event_params', 'file_name') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("click_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("click_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'click'
)

select * from click_with_params