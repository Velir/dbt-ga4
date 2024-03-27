{{
  config(
      enabled = false,
  )
}}
with select_item_with_params as (
  select * except (items),
  (select items from unnest(items) items limit 1) as items
  {% if var("default_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
  {% endif %}
  {% if var("select_item_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("select_item_custom_parameters") )}}
  {% endif %}
 from {{ref('stg_ga4__events')}},
  unnest(items)   
 where event_name = 'select_item'
)

select * from select_item_with_params