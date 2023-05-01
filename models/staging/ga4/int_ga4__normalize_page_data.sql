{% set partitions_to_replace = ['current_date'] %}
{% for i in range(var('static_incremental_days', 1)) %}
    {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
{% endfor %}
select
    page_location,
    page_title,
    author,
    content_topic,
    content_group,
    article_id,
    article_pubdate
from (
    select
        page_location,
        page_title,
        author,
        content_topic,
        content_group,
        article_id,
        article_pubdate,
        row_number() over (partition by page_location order by count(author) desc) rn
    from {{ref('fct_ga4__event_page_view')}}
    {% if is_incremental() %}
        {% if var('static_incremental_days', 1 ) %}
            where event_date_dt in ({{ partitions_to_replace | join(',') }})
        {% endif %}
    {% endif %}
    group by 1,2,3,4,5,6,7
)
where rn = 1