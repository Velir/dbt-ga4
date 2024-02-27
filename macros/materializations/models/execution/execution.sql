{%- materialization execution, default %}
    {%- set identifier = model['alias'] -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                  schema=schema,
                                                  database=database,
                                                  type='view') -%}

    {% call statement('main') -%}
        {# Execute query #}
        {{ sql }}

        {# Create a view to be called by ref(target_relation) #}
        {{ get_create_view_as_sql(target_relation, 'select true as executed') }}
    {%- endcall %}
    {{ adapter.commit() }}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
