{{
  config(
      enabled = false,
  )
}}
 with add_to_cart_with_params as (
   select *except (items),
   (select items FROM UNNEST(items) items LIMIT 1) as items
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
    {% if var("add_to_cart_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("add_to_cart_custom_parameters") )}}
    {% endif %}
 from {{ref('stg_ga4__events')}}, 
  unnest(items)
 where event_name = 'add_to_cart'
)

select * from add_to_cart_with_params