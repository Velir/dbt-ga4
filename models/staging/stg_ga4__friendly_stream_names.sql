{% if var('ga4.stream_names', false) %}
    {{ config('enabled = true') }}
{% else %}
{{ config('enabled = false') }}

{% for name in var('ga4.stream_names') %}
    select name[0] as stream_id, name[1] as stream_name
    {% if !loop.last %} union all {% endif %}
{% endfor %}