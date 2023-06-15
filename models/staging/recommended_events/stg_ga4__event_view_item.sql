{{
  config(
      enabled = false,
  )
}}
with view_item_with_params as (
  select * except (items)
  , {{ ga4.unnest_key('event_params', 'currency') }}
  , {{ ga4.unnest_key('event_params', 'value', 'double_value') }}
  , (select items from unnest(items) items limit 1) as items
  {% if var("default_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
  {% endif %}
  {% if var("view_item_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("view_item_custom_parameters") )}}
  {% endif %}
 from {{ref('stg_ga4__events')}}, 
  unnest(items)
 where event_name = 'view_item'
)

select * from view_item_with_params