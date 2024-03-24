{%- macro combine_property_data(property_ids=var('property_ids'), start_date=none, end_date=none) -%}
    {{ return(adapter.dispatch('combine_property_data', 'ga4')(property_ids, start_date, end_date)) }}
{%- endmacro -%}

{% macro default__combine_property_data(property_ids=var('property_ids'), start_date=none, end_date=none) %}
    {% for property_id in property_ids %}
        {{ ga4.combine_specified_property_data(property_id, start_date, end_date) }}
    {% endfor %}
{% endmacro %}
