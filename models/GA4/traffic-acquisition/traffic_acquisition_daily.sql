{{
    config(
        materialized="incremental",
        unique_key=["snapshot_date", "session_source_medium"],
    )
}}

with
    prep_traffic as (
        select
            format_date(
                '%Y-%m-%d', parse_date('%Y%m%d', table_suffix)
            ) as snapshot_date,
            user_pseudo_id,
            (
                select value.int_value
                from unnest(event_params)
                where key = 'ga_session_id'
            ) as session_id,
            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'medium'
                )
            ) as medium,
            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'source'
                )
            ) as source,
            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'campaign'
                )
            ) as campaign,
            max(
                (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'session_engaged'
                )
            ) as session_engaged,
            max(
                (
                    select value.int_value
                    from unnest(event_params)
                    where key = 'engagement_time_msec'
                )
            ) as engagement_time_msec,
            -- change event_name to the event(s) you want to count
            countif(event_name = 'click') as event_count,
            -- change event_name to the conversion event(s) you want to count
            countif(event_name = 'purchase') as conversions,
            sum(ecommerce.purchase_revenue) as total_revenue
        from {{ ref('stg_events') }}

        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            where
                table_suffix >= (
                    select
                        format_date(
                            '%Y%m%d',
                            date_sub(date(max(snapshot_date)), interval 10 day)
                        )
                    from {{ this }}
                )
                and table_suffix < (
                    select
                        format_date(
                            '%Y%m%d', date_sub(date(max(snapshot_date)), interval 1 day)
                        )
                    from {{ this }}
                )

        {% endif %}

        group by user_pseudo_id, session_id, snapshot_date
    )

-- main query
select
    snapshot_date,
    concat(
        ifnull(source, '(direct)'), ' / ', ifnull(medium, '(none)')
    ) as session_source_medium,
    -- ifnull(medium,'(none)') as session_medium,
    -- ifnull(source,'(direct)') as session_source,
    -- ifnull(campaign,'(direct)') as session_campaign,
    /* -- definitions of the channel grouping based on the source / medium of every session
    case
        when source is null and (medium = '(not set)' or medium is null) then 'Direct'
        when medium = 'organic' then 'Organic Search'
        when regexp_contains(medium, r'^(social|social-network|social-media|sm|social network|social media)$') then 'Social'
        when medium = 'email' then 'Email'
        when medium = 'affiliate' then 'Affiliates'
        when medium = 'referral' then 'Referral'
        when regexp_contains(medium, r'^(cpc|ppc|paidsearch)$') then 'Paid Search'
        when regexp_contains(medium, r' ^(cpv|cpa|cpp|content-text)$') then 'Other Advertising'
        when regexp_contains(medium, r'^(display|cpm|banner)$') then 'Display'
        else '(Other)' end as session_default_channel_grouping,
    */
    count(distinct user_pseudo_id) as users,
    count(distinct concat(user_pseudo_id, session_id)) as sessions,
    count(
        distinct case
            when session_engaged = '1' then concat(user_pseudo_id, session_id)
        end
    ) as engaged_sessions,
    safe_divide(
        sum(engagement_time_msec / 1000),
        count(
            distinct case
                when session_engaged = '1' then concat(user_pseudo_id, session_id)
            end
        )
    ) as average_engagement_time_per_session_seconds,
    safe_divide(
        count(
            distinct case
                when session_engaged = '1' then concat(user_pseudo_id, session_id)
            end
        ),
        count(distinct user_pseudo_id)
    ) as engaged_sessions_per_user,
    safe_divide(
        sum(event_count), count(distinct concat(user_pseudo_id, session_id))
    ) as events_per_session,
    safe_divide(
        count(
            distinct case
                when session_engaged = '1' then concat(user_pseudo_id, session_id)
            end
        ),
        count(distinct concat(user_pseudo_id, session_id))
    ) as engagement_rate,
    sum(event_count) as event_count,
    sum(conversions) as conversions,
    ifnull(sum(total_revenue), 0) as total_revenue
from prep_traffic
group by
    session_source_medium,
    snapshot_date
    -- ,session_medium
    -- ,session_source
    -- ,session_campaign
    -- ,session_default_channel_grouping
    
