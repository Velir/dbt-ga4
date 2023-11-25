-- Defined as when the video starts playing. For embedded YouTube videos that have JS API support enabled. Collected by default via enhanced measurement.
-- More info: https://support.google.com/firebase/answer/9234069?hl=en
 
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

 with video_start_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'video_current_time', 'int_value') }},
      {{ ga4.unnest_key('event_params', 'video_duration', 'int_value') }},
      {{ ga4.unnest_key('event_params', 'video_percent', 'int_value') }},
      {{ ga4.unnest_key('event_params', 'video_url') }},
      {{ ga4.unnest_key('event_params', 'video_provider') }},
      {{ ga4.unnest_key('event_params', 'vide_title') }},
      {{ ga4.unnest_key('event_params', 'visible') }}
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("video_start_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("video_start_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'video_start'
 {% if is_incremental() %}
  and event_date_dt >= CURRENT_DATE() - 7
 {% endif %}
)

select * from video_start_with_params