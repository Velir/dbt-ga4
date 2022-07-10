-- Event defined as "when the app is in the foreground or webpage is in focus for at least one second."
 
 with user_engagement_with_params as (
   select *,
      {% if var("user_engagement_custom_parameters", "none") != "none" %}
        {{ stage_custom_parameters( var("user_engagement_custom_parameters") )}}
      {% endif %}
 from {{ref('stg_ga4__events')}}    
 where event_name = 'user_engagement'
)

select * from user_engagement_with_params