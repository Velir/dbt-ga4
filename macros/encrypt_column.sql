-- macros/encrypt_column.sql

{% macro encrypt_column(column_name, method='sha256') %}
    {%- if method == 'md5' -%}
        TO_HEX(MD5(CAST({{ column_name }} AS STRING)))
    {%- elif method == 'sha1' -%}
        TO_HEX(SHA1(CAST({{ column_name }} AS STRING)))
    {%- elif method == 'sha256' -%}
        TO_HEX(SHA256(CAST({{ column_name }} AS STRING)))
    {%- else -%}
        -- fallback: default to sha256
        TO_HEX(SHA256(CAST({{ column_name }} AS STRING)))
    {%- endif -%}
{% endmacro %}
