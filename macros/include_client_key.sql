

-- Add key that captures a combination of stream_id and user_pseudo_id to uniquely identify a 'client' (aka. a device) within a single stream
{% macro include_client_key(source_cte) %}
    include_client_key as (
        select *,
        to_base64(md5(concat(user_pseudo_id, stream_id))) as client_key
        from {{ source_cte }}
    )
{% endmacro %}