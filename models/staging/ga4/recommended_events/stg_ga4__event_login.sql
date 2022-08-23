{{
  config(
      enabled = false,
  )
}}

 with login_with_params as (
   select *
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("login_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("login_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}
 where event_name = 'login'
)

select * from login_with_params