 with page_view_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
      lag(page_location, 1) over (partition by (session_key) order by event_timestamp asc) as session_previous_page,
      case when split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
      case when split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
      case when split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
      case when split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)]) end as pagepath_level_4,
      {% if var("page_view_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("page_view_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'page_view'
),
last_pageview_joined as (
  select 
    page_view_with_params.*,
    case
      when first_last_pageview_session.last_page_view_event_key is null then null
      else 1
    end as exits
  from page_view_with_params
    left join {{ref('stg_ga4__sessions_first_last_pageviews')}} first_last_pageview_session
      on page_view_with_params.event_key = first_last_pageview_session.last_page_view_event_key
)

select * from last_pageview_joined