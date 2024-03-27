{{
  config(
      enabled = false,
  )
}}
with view_promotion_with_params as (
  select * except (items),
  (select items from unnest(items) items limit 1) as items
  {% if var("default_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
  {% endif %}
  {% if var("view_promotion_custom_parameters", "none") != "none" %}
    {{ ga4.stage_custom_parameters( var("view_promotion_custom_parameters") )}}
  {% endif %}
 from {{ref('stg_ga4__events')}}, 
  unnest(items)
 where event_name = 'view_promotion'
)

select * from view_promotion_with_params