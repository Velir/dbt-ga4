{{
    config(
        materialized='view',
        enabled=false
    )
}}
select
    *
    {% for up in var('user_export_user_properties', []) %}
        , (select value.string_value from unnest(user_properties) where value.user_property_name = '{{up}}') as {{up | lower | replace(" ", "_")}}_string_value 
        , (select value.set_timestamp_micros from unnest(user_properties) where value.user_property_name = '{{up}}') as {{up | lower | replace(" ", "_")}}_set_timestamp_micros
        , (select value.user_property_name from unnest(user_properties) where value.user_property_name = '{{up}}') as {{up | lower | replace(" ", "_")}}_user_property_name 
    {% endfor %}
    {% for aud in var('audiences', []) %} 
        , (select id from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_id
        , (select name from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_name 
        , (select membership_start_timestamp_micros from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_membership_start_timestamp_micros
        , (select membership_expiry_timestamp_micros from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_membership_expiry_timestamp_micros
        , (select npa from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_npa
    {% endfor %}
from {{ref('base_ga4__users')}}
