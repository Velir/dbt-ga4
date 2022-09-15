 with page_view_with_params as (
   select *,
      {{ ga4.unnest_key('event_params', 'entrances',  'int_value') }},
      {{ ga4.unnest_key('event_params', 'value', 'float_value') }},
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
)

select * from page_view_with_params