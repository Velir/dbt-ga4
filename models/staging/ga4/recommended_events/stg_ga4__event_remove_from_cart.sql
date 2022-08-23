{{
  config(
      enabled = false,
  )
}}
with remove_from_cart_with_params as (
  select * except (items),
  (select items from unnest(items) items limit 1) as items
  {% if var("default_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
  {% endif %}
  {% if var("remove_from_cart_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("remove_from_cart_custom_parameters") )}}
  {% endif %}
from {{ref('stg_ga4__events')}}, 
unnest(items)
 where event_name = 'remove_from_cart'
)

select * from remove_from_cart_with_params