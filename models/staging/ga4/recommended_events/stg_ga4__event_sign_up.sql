{{
  config(
      enabled = false,
  )
}}

 with sign_up_with_params as (
   select *
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("sign_up_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("sign_up_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'sign_up'
)

select * from sign_up_with_params