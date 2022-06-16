{% macro extract_hostname_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')
{% endmacro %}

{% macro extract_query_string_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '\\?(.+)')
{% endmacro %}