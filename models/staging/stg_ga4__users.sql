{{
    config(
        materialized='view',
        enabled=false
    )
}}
select
    *
    {% for up in var('user_properties', []) %} -- don't have sample data; need to verify
        , (select value.string_value from unnest(user_properties) where key = '{{up}}') as {{up | lower | replace(" ", "_")}}_string_value 
        , (select value.set_timestamp_micros from unnest(user_properties) where key = '{{up}}') as {{up | lower | replace(" ", "_")}}_set_timestamp_micros
        , (select value.user_property_name from unnest(user_properties) where key = '{{up}}') as {{up | lower | replace(" ", "_")}}_user_property_name 
    {% endfor %}
    {% for aud in var('audiences', []) %} -- this should be good, though
        , (select id from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_id
        , (select name from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_name 
        , (select membership_start_timestamp_micros from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_membership_start_timestamp_micros
        , (select membership_expiry_timestamp_micros from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_membership_expiry_timestamp_micros
        , (select npa from unnest(audiences) where name = '{{aud}}') as audience_{{aud | lower | replace(" ", "_")}}_npa
    {% endfor %}
from {{ref('base_ga4__users')}}
