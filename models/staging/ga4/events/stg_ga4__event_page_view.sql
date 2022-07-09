 with page_view_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
      lag(page_location, 1) over (partition by (session_key) order by event_timestamp asc) as previous_page_location_in_session,
      case when split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
      case when split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
      case when split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
      case when split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)]) end as pagepath_level_4,

      {% if var("page_view_custom_parameters", "none") != "none" %}
        {{ stage_custom_parameters( var("page_view_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'page_view'
),
first_last_pageview_session as (
  select * from {{ref('stg_ga4__sessions_first_last_pageviews')}}
),
-- Determine the session's first pageview based on event_timestamp. This is redundant with the 'entrances' int value, but calculated in the warehouse so a bit more transparent in how it operates. 
first_pageview_joined as (
  select 
    page_view_with_params.*,
    case
      when first_last_pageview_session.first_page_view_event_key is null then FALSE
      else TRUE
    end as is_entrance
  from page_view_with_params
    left join first_last_pageview_session
      on page_view_with_params.event_key = first_last_pageview_session.first_page_view_event_key
),
last_pageview_joined as (
  select 
    first_pageview_joined.*,
    case
      when first_last_pageview_session.last_page_view_event_key is null then FALSE
      else TRUE
    end as is_exit
  from first_pageview_joined
    left join first_last_pageview_session
      on first_pageview_joined.event_key = first_last_pageview_session.last_page_view_event_key
)

select * from last_pageview_joined