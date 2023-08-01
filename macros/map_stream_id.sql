{% macro map_stream_id(stream_id, stream_properties) -%}
CASE
{% for sp in stream_properties %}
    WHEN {{ stream_id }} = "{{ sp.stream_id }}" THEN "{{ sp.stream_name }}"
{% endfor %}
	ELSE "new_stream"
END
{% endmacro -%}