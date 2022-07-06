{% if var('ecommerce', false ) ==  false %}
    {{
      config(
          enabled = false,
      )
  }}
{% endif %}
with remove_from_cart_with_params as (
  select * except (items),
  (select items FROM UNNEST(items) items LIMIT 1) as items
  {% if var("remove_from_cart_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("remove_from_cart_custom_parameters") )}}
  {% endif %}
from {{ref('stg_ga4__events')}}, 
unnest(items)
 where event_name = 'remove_from_cart'
)

select * from remove_from_cart_with_params