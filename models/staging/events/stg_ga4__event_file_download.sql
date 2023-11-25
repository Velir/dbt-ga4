 -- reference here: https://support.google.com/analytics/answer/9216061?hl=en&ref_topic=9756175

{{
  config(
        materialized='incremental',
        
        incremental_strategy='insert_overwrite',
        partition_by={
                        "field": "event_date_dt",
                        "data_type": "date",
                        "granularity": "day"
                    },
  )
}}

 with event_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
      {{ ga4.unnest_key('event_params', 'file_extension') }},
      {{ ga4.unnest_key('event_params', 'file_name') }},
      {{ ga4.unnest_key('event_params', 'link_classes') }},
      {{ ga4.unnest_key('event_params', 'link_domain') }},
      {{ ga4.unnest_key('event_params', 'link_id') }},
      {{ ga4.unnest_key('event_params', 'link_text') }},
      {{ ga4.unnest_key('event_params', 'link_url') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("file_download_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("file_download_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'file_download'
 {% if is_incremental() %}
  and event_date_dt >= CURRENT_DATE() - 7
 {% endif %}
)

select * from event_with_params