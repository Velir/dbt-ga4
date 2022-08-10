-- stg_ga4__event_generate_lead.sql

{{
  config(
      enabled = true,
  )
}}

with generate_lead_with_params as (
    select *,
    {{ ga4.unnest_key('event_params', 'currency') }},
    {{ ga4.unnest_key('event_params', 'value') }},
    {% if var("share_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("generate_lead_custom_parameters") )}}
    {% endif %}
    from {{ref('stg_ga4__events')}}
    where event_name = 'generate_lead'
)

select * from generate_lead_with_params