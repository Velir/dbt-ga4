{% macro incremental_config(bq_partition_field, bq_partition_field_type) %}
    
    {% if target.type == 'bigquery' %}
    
        {{
            config(
                pre_hook = "{{ ga4.combine_property_data() }}" if var('combined_dataset', false) else "",
                materialized = 'incremental',
                incremental_strategy = 'insert_overwrite',
                partition_by = {
                    "field": "{{ bq_partition_field }}",
                    "data_type": "{{ bq_partition_field_type }}",
                },
                partitions = partitions_to_replace,
                cluster_by = ['event_name']
            )
        }}
    
    {% elif target.type == 'snowflake' %}
    
        {{
            config(
                pre_hook = "{{ ga4.combine_property_data() }}" if var('combined_dataset', false) else "",
                materialized = 'incremental',
                incremental_strategy = 'delete+insert',
            )
        }}

    {% endif %}

{% endmacro %}