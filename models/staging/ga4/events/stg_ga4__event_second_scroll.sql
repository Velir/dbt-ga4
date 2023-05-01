with second_scroll_with_params as (
 select 
    *
    {% if var("default_custom_parameters", "none") != "none" %}
      {{ ga4.stage_custom_parameters( var("default_custom_parameters") )}}
    {% endif %}
      {% if var("second_scroll_custom_parameters", "none") != "none" %}
        {{ ga4.stage_custom_parameters( var("second_scroll_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'second_scroll'
)

select * from second_scroll_with_params