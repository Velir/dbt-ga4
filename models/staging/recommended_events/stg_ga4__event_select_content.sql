{{
  config(
      enabled = false,
  )
}}

 with select_content_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'content_type') }},
      {{ ga4.unnest_key('event_params', 'item_id') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("select_content_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("select_content_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'select_content'
)

select * from select_content_with_params
