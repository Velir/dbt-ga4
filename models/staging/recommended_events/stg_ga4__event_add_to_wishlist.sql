{{
  config(
      enabled = false,
  )
}}
 with add_to_wishlist_with_params as (
   select * except (items),
   (select items FROM unnest(items) items LIMIT 1) as items,
      {{ ga4.unnest_key('event_params', 'currency') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("add_to_wishlist_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("add_to_wishlist_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}, 
  unnest(items)
 where event_name = 'add_to_wishlist'
)

select * from add_to_wishlist_with_params