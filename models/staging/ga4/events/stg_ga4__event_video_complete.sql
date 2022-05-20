-- Defined as when the video ends. For embedded YouTube videos that have JS API support enabled. Collected by default via enhanced measurement.
-- More info: https://support.google.com/firebase/answer/9234069?hl=en
 
 with video_complete_with_params as (
   select *,
      {{ unnest_key('event_params', 'video_current_time', 'int_value') }},
      {{ unnest_key('event_params', 'video_duration', 'int_value') }},
      {{ unnest_key('event_params', 'video_percent', 'int_value') }},
      {{ unnest_key('event_params', 'video_url') }},
      {{ unnest_key('event_params', 'video_provider') }},
      {{ unnest_key('event_params', 'vide_title') }},
      {{ unnest_key('event_params', 'visible') }}
 from {{ ref('stg_ga4__events') }}    
 where event_name = 'video_complete'
)

select * from video_complete_with_params