{% macro extract_hostname_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')
{% endmacro %}

{% macro extract_query_string_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '\\?(.+)')
{% endmacro %}

{% macro remove_query_parameters(url) %}

    {% for p in var('query_parameter_exclusions', []) %}
        REGEXP_REPLACE({{url}}, '{{p.parameter}}', '')
    {% endfor %}

{% endmacro %}