{% if var('frequency', 'daily') == 'daily+streaming' %}
    {{ config(
        enabled = true,
        materialized = 'view' 
    ) }}
{% else %}
    {{ config(
        enabled = false
    ) }}
{% endif %}

{% if var(datasets) is not none  %}
{% for ds in datasets %}
    select
        *
    from {{ source( ds , 'events_intraday') }}
    where cast( _table_suffix as int64) >= {{var('start_date')}}
    {% if not loop.last -%} union all {%- endif %}
{% endfor %}
{% else %}
select
    *
from {{ source('ga4', 'events_intraday') }}
{% endif %}