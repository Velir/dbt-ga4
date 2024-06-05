{% macro valid_column_name(column_name) %}
  {% set re = modules.re %}
  {% set pattern = '[^a-zA-Z0-9_]' %}
  {# a column name can't contain a non alphanumeric or _ character #}
  {% set cleaned_name = re.sub(pattern, '_', column_name|string) %}

  {% if re.match('^\\d', cleaned_name) %}
    {# a column name can't start by a number #}
    {{ return("_" ~ cleaned_name) }}
  {% else %}
    {{ return(cleaned_name) }}
  {% endif %}

{% endmacro %}