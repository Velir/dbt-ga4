{% macro extract_hostname_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '(?:http[s]?://)?(?:www\\.)?(.*?)(?:(?:/|:)(?:.)*|$)')
{% endmacro %}

{% macro extract_query_string_from_url(url) %}
    REGEXP_EXTRACT({{ url }}, '\\?(.+)')
{% endmacro %}

{% macro remove_query_parameters(url, parameters) %}
  {{ return(adapter.dispatch('remove_query_parameters', 'ga4')(url, parameters)) }}
{% endmacro %}

{% macro default__remove_query_parameters(url, parameters)%}
{% if "*all*" in parameters %}
    regexp_replace({{url}}, r'(\?|&|#).*', '')
{% else %}
    regexp_replace({{ url }}, r'([&|#|\?]{1}({{ parameters|join("|") }})=[^\?&#]*)', '')
{% endif %}
{% endmacro %}

{% macro extract_page_path(url) %}
  {{ return(adapter.dispatch('extract_page_path', 'ga4')(url)) }}
{% endmacro %}

{% macro default__extract_page_path(url) %}
   REGEXP_EXTRACT({{url}}, '(?:\\w+:)?\\/\\/[^\\/]+([^?#]+)')
{% endmacro %}

{% macro extract_query_parameter_value(url, param) %}
    REGEXP_EXTRACT( {{url}}, r'{{param}}=([^&|\?|#]*)'  )
{% endmacro %}