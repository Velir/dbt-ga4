{% if var('static_incremental_days', false ) %}
    {% set partitions_to_replace = ['current_date'] %}
    {% for i in range(var('static_incremental_days')) %}
        {% set partitions_to_replace = partitions_to_replace.append('date_sub(current_date, interval ' + (i+1)|string + ' day)') %}
    {% endfor %}
    {{
        config(
            materialized = 'incremental',
            incremental_strategy = 'insert_overwrite',
            tags = ["incremental"],
            partition_by={
                "field": "session_partition_date",
                "data_type": "date",
                "granularity": "day"
            },
            partitions = partitions_to_replace
        )
    }}
{% endif %}

with last_non_direct_session_partition_key as (
  select
    client_key
    ,session_partition_key
    ,session_partition_date
    ,session_source
    ,session_medium
    ,session_source_category
    ,session_campaign
    ,session_content
    ,session_term
    ,session_default_channel_grouping
    ,non_direct_session_partition_key
    ,CASE
      WHEN non_direct_session_partition_key is null
      THEN 
          last_value(non_direct_session_partition_key ignore nulls) over(
          partition by client_key
          order by
              session_partition_timestamp range between 2592000000000 preceding
              and current row -- 30 day lookback
          )
      ELSE non_direct_session_partition_key
    END as session_partition_key_last_non_direct,
  from
  {{ref('stg_ga4__sessions_traffic_sources_daily')}}
  {% if is_incremental() %}
      -- Add 30 to static_incremental_days to include the session attribution lookback window
      where session_partition_date >= date_sub(current_date, interval ({{var('static_incremental_days')}}+30) day)
  {% endif %}
)
,join_last_non_direct_session_source as (
  select
    last_non_direct_session_partition_key.client_key
    ,last_non_direct_session_partition_key.session_partition_key
    ,last_non_direct_session_partition_key.session_partition_date
    ,last_non_direct_session_partition_key.session_source
    ,last_non_direct_session_partition_key.session_medium
    ,last_non_direct_session_partition_key.session_source_category
    ,last_non_direct_session_partition_key.session_campaign
    ,last_non_direct_session_partition_key.session_content
    ,last_non_direct_session_partition_key.session_term
    ,last_non_direct_session_partition_key.session_default_channel_grouping
    ,last_non_direct_session_partition_key.session_partition_key_last_non_direct
    ,coalesce(last_non_direct_source.session_source, '(direct)') as session_source_last_non_direct -- Value will be null if only direct sessions are within the lookback window
    ,coalesce(last_non_direct_source.session_medium, '(none)') as session_medium_last_non_direct
    ,coalesce(last_non_direct_source.session_source_category, '(none)') as session_source_category_last_non_direct
    ,coalesce(last_non_direct_source.session_campaign, '(none)') as session_campaign_last_non_direct
    ,coalesce(last_non_direct_source.session_content, '(none)') as session_content_last_non_direct
    ,coalesce(last_non_direct_source.session_term, '(none)') as session_term_last_non_direct
    ,coalesce(last_non_direct_source.session_default_channel_grouping, '(none)') as session_default_channel_grouping_last_non_direct
  from last_non_direct_session_partition_key
  left join {{ref('stg_ga4__sessions_traffic_sources_daily')}} last_non_direct_source on
    last_non_direct_session_partition_key.session_partition_key_last_non_direct = last_non_direct_source.session_partition_key
  {% if is_incremental() %}
      -- Only keep the records in the partitions we wish to replace (as opposed to the whole 30 day lookback window)
      where last_non_direct_session_partition_key.session_partition_date in ({{ partitions_to_replace | join(',') }})
  {% endif %}
)

select * from join_last_non_direct_session_source

