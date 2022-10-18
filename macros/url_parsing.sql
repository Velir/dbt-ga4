{% macro extract_hostname_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')
{% endmacro %}

{% macro extract_query_string_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '\\?(.+)')
{% endmacro %}

{% macro remove_query_parameters(url, parameters)%}
{% if parameters == "*all*" %}
    regexp_replace({{url}}, r'(\?|&|#).*', '')
{% else %}
REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE({{url}}, '(\\?|&)({{ parameters|join("|") }})=[^&]*', '\\1'), '\\?&+', '?'), '&+', '&'), '\\?$|&$', '')
{% endif %}
{% endmacro %}