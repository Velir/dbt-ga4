{% if var('ecommerce', false ) ==  false %}
    {{
      config(
          enabled = false,
      )
  }}
{% endif %}
with select_item_with_params as (
  select * except (items),
  (select items FROM UNNEST(items) items LIMIT 1) as items
  {% if var("select_item_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("select_item_custom_parameters") )}}
  {% endif %}
 from {{ref('stg_ga4__events')}},
  unnest(items)   
 where event_name = 'select_item'
)

select * from select_item_with_params