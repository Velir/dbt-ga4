 with page_view_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
      lag(page_location, 1) over (partition by (session_key) order by event_timestamp asc) as session_previous_page,
      case when split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(4)],'?')[safe_ordinal(1)]) end as pagepath_level_1,
      case when split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(5)],'?')[safe_ordinal(1)]) end as pagepath_level_2,
      case when split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(6)],'?')[safe_ordinal(1)]) end as pagepath_level_3,
      case when split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)] = '' then null else concat('/',split(split(page_location,'/')[safe_ordinal(7)],'?')[safe_ordinal(1)]) end as pagepath_level_4
      {% if var("default_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
      {% endif %}
      {% if var("page_view_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("page_view_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'page_view'
),
last_pageview_joined as (
  select 
    page_view_with_params.* except(load_time),
    case
        when load_time < 0 then null
        when load_time > 100000 then null
        else load_time
    end as load_time,
    case
      when first_last_pageview_session.last_page_view_event_key is null then null
      else 1
    end as exits
  from page_view_with_params
    left join {{ref('stg_ga4__sessions_first_last_pageviews')}} first_last_pageview_session
      on page_view_with_params.event_key = first_last_pageview_session.last_page_view_event_key
),
session_params as (
  select
    last_pageview_joined.* except(source,medium, campaign),
    last_non_direct_source,
    last_non_direct_medium,
    last_non_direct_campaign,
    last_non_direct_channel,
    source,
    medium,
    campaign,
    default_channel_grouping,
    mv_author_session_status
  from {{ref('stg_ga4__sessions_traffic_sources')}}
  right join last_pageview_joined using(session_key)
)

select * from session_params