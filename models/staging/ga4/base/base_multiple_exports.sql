{% set relations = [] %}
{% for schema in var('union_schemas') %}

    {% set relation=adapter.get_relation(
        database=target.database,
        schema=schema,
        identifier='events_*'
    ) -%}

    {% set relation_exists=relation is not none %}

    {% if relation_exists %}

        {% do relations.append(relation) %}

    {% endif %}

{% endfor %}

{{ dbt_utils.union_relations(relations) }}