{{ config(materialized = 'view') }}

{% if var(datasets) is not none  %}
{% for ds in datasets %}
    select
        *
    {%  if var('frequency', 'daily') == 'streaming' %}
        from {{ source( ds , 'events_intraday') }}
        where cast( _table_suffix as int64) >= {{var('start_date')}}
    {% else %}
        from {{ source( ds , 'events') }}
        where _table_suffix not like '%intraday%'
        and cast( _table_suffix as int64) >= {{var('start_date')}}
    {% endif %}
    {% if not loop.last -%} union all {%- endif %}
{% endfor %}
{% else %}
    {%  if var('frequency', 'daily') == 'streaming' %}
        from {{ source( 'ga4' , 'events_intraday') }}
        where cast( _table_suffix as int64) >= {{var('start_date')}}
    {% else %}
        from {{ source( 'ga4' , 'events') }}
        where _table_suffix not like '%intraday%'
        and cast( _table_suffix as int64) >= {{var('start_date')}}
    {% endif %}
{% endif %}