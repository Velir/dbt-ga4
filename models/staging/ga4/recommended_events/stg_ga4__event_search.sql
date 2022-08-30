{{
  config(
      enabled = false,
  )
}}

 with search_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'search_term') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("search_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("search_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'search'
)

select * from search_with_params