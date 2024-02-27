{%- set property_id = var('property_ids')[15] -%}

{{
    config(
        enabled = var('property_ids')|length >= 15 + 1,
        materialized = 'execution',
        alias = "int_ga4__combine_property_" ~ property_id
    )
}}

{{ ga4.combine_property_data(property_id) }}
