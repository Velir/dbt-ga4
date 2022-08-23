{{
  config(
      enabled = false,
  )
}}
with select_promotion_with_params as (
  select * except (items),
  (select items from unnest(items) items limit 1) as items
  {% if var("default_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
  {% endif %}
  {% if var("select_promotion_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("select_promotion_custom_parameters") )}}
  {% endif %}
 from {{ref('stg_ga4__events')}}, 
  unnest(items)
 where event_name = 'select_promotion'
)

select * from select_promotion_with_params