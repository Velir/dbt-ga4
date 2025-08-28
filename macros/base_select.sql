{% macro base_select_source() %}
    {{ return(adapter.dispatch('base_select_source', 'ga4')()) }}
{% endmacro %}

{% macro default__base_select_source() %}
    parse_date('%Y%m%d',event_date) as event_date_dt
    , event_timestamp
    , event_name
    , event_params
    , event_previous_timestamp
    , event_value_in_usd
    , event_bundle_sequence_id
    , event_server_timestamp_offset
    , user_id
    , user_pseudo_id
    , privacy_info
    , user_properties
    , user_first_touch_timestamp
    , user_ltv
    , device
    , geo
    , app_info
    , traffic_source
    , stream_id
    , platform
    , ecommerce.total_item_quantity
    , ecommerce.purchase_revenue_in_usd
    , ecommerce.purchase_revenue
    , ecommerce.refund_value_in_usd
    , ecommerce.refund_value
    , ecommerce.shipping_value_in_usd
    , ecommerce.shipping_value
    , ecommerce.tax_value_in_usd
    , ecommerce.tax_value
    , ecommerce.unique_items
    , ecommerce.transaction_id
    , items
    , session_traffic_source_last_click
    , collected_traffic_source
    , {%- if  var('combined_dataset', false) != false %} cast(left(regexp_replace(_table_suffix, r'^(intraday_)?\d{8}', ''), 100) as int64)
        {%- else %} {{ var('property_ids')[0] }}
        {%- endif %} as property_id
{% endmacro %}

{% macro base_select_renamed() %}
    {{ return(adapter.dispatch('base_select_renamed', 'ga4')()) }}
{% endmacro %}

{% macro default__base_select_renamed() %}
    event_date_dt
    , event_timestamp
    , lower(replace(trim(event_name), " ", "_")) as event_name -- Clean up all event names to be snake cased
    , event_params
    , event_previous_timestamp
    , event_value_in_usd
    , event_bundle_sequence_id
    , event_server_timestamp_offset
    , user_id
    , user_pseudo_id
    , privacy_info.analytics_storage as privacy_info_analytics_storage
    , privacy_info.ads_storage as privacy_info_ads_storage
    , privacy_info.uses_transient_token as privacy_info_uses_transient_token
    , user_properties
    , user_first_touch_timestamp
    , user_ltv.revenue as user_ltv_revenue
    , user_ltv.currency as user_ltv_currency
    , device.category as device_category
    , device.mobile_brand_name as device_mobile_brand_name
    , device.mobile_model_name as device_mobile_model_name
    , device.mobile_marketing_name as device_mobile_marketing_name
    , device.mobile_os_hardware_model as device_mobile_os_hardware_model
    , device.operating_system as device_operating_system
    , device.operating_system_version as device_operating_system_version
    , device.vendor_id as device_vendor_id
    , device.advertising_id as device_advertising_id
    , device.language as device_language
    , device.is_limited_ad_tracking as device_is_limited_ad_tracking
    , device.time_zone_offset_seconds as device_time_zone_offset_seconds
    , device.browser as device_browser
    , device.browser_version as device_browser_version
    , device.web_info.browser as device_web_info_browser
    , device.web_info.browser_version as device_web_info_browser_version
    , device.web_info.hostname as device_web_info_hostname
    , geo.continent as geo_continent
    , geo.country as geo_country
    , geo.region as geo_region
    , geo.city as geo_city
    , geo.sub_continent as geo_sub_continent
    , geo.metro as geo_metro
    , app_info.id as app_info_id
    , app_info.version as app_info_version
    , app_info.install_store as app_info_install_store
    , app_info.firebase_app_id as app_info_firebase_app_id
    , app_info.install_source as app_info_install_source
    , traffic_source.name as user_campaign
    , traffic_source.medium as user_medium
    , traffic_source.source as user_source
    , session_traffic_source_last_click.manual_campaign.campaign_id as session_traffic_source_last_click_manual_campaign_id
    , session_traffic_source_last_click.manual_campaign.campaign_name as session_traffic_source_last_click_manual_campaign_name
    , session_traffic_source_last_click.manual_campaign.source as session_traffic_source_last_click_manual_source
    , session_traffic_source_last_click.manual_campaign.medium as session_traffic_source_last_click_manual_medium
    , session_traffic_source_last_click.manual_campaign.term as session_traffic_source_last_click_manual_term
    , session_traffic_source_last_click.manual_campaign.content as session_traffic_source_last_click_manual_content
    , session_traffic_source_last_click.manual_campaign.source_platform as session_traffic_source_last_click_manual_source_polatform
    , session_traffic_source_last_click.manual_campaign.creative_format as session_traffic_source_last_click_creative_format
    , session_traffic_source_last_click.manual_campaign.marketing_tactic as session_traffic_source_last_click_manual_marketing_tactic
    , collected_traffic_source.manual_campaign_id as collected_traffic_source_manual_campaign_id
    , collected_traffic_source.manual_campaign_name as collected_traffic_source_manual_campaign_name
    , collected_traffic_source.manual_source as collected_traffic_source_manual_source
    , collected_traffic_source.manual_medium as collected_traffic_source_manual_medium
    , collected_traffic_source.manual_term as collected_traffic_source_manual_term
    , collected_traffic_source.manual_content as collected_traffic_source_manual_content
    , collected_traffic_source.manual_source_platform as collected_traffic_source_manual_source_platform
    , collected_traffic_source.manual_creative_format as collected_traffic_source_manual_creative_format
    , collected_traffic_source.manual_marketing_tactic as collected_traffic_source_manual_marketing_tactic
    , collected_traffic_source.gclid as collected_traffic_source_gclid
    , collected_traffic_source.dclid as collected_traffic_source_dclid
    , collected_traffic_source.srsltid as collected_traffic_source_srsltid
    , stream_id
    , platform
    , struct(
        total_item_quantity
        , purchase_revenue_in_usd
        , purchase_revenue
        , refund_value_in_usd
        , refund_value
        , shipping_value_in_usd
        , shipping_value
        , tax_value_in_usd
        , tax_value
        , unique_items
        , transaction_id        
    ) as ecommerce
    , (select 
        array_agg(struct(
            unnested_items.item_id
            , unnested_items.item_name
            , unnested_items.item_brand
            , unnested_items.item_variant
            , unnested_items.item_category
            , unnested_items.item_category2
            , unnested_items.item_category3
            , unnested_items.item_category4
            , unnested_items.item_category5
            , unnested_items.price_in_usd
            , unnested_items.price
            , unnested_items.quantity
            , unnested_items.item_revenue_in_usd
            , unnested_items.item_revenue
            , unnested_items.item_refund_in_usd
            , unnested_items.item_refund
            , unnested_items.coupon
            , unnested_items.affiliation
            , unnested_items.location_id
            , unnested_items.item_list_id
            , unnested_items.item_list_name
            , unnested_items.item_list_index
            , unnested_items.promotion_id
            , unnested_items.promotion_name
            , unnested_items.creative_name
            , unnested_items.creative_slot
            , unnested_items.item_params
        )) from unnest(items) as unnested_items 
    ) items
    , property_id
    , {{ ga4.unnest_key('event_params', 'ga_session_id', 'int_value', 'session_id') }}
    , {{ ga4.unnest_key('event_params', 'page_location') }}
    , {{ ga4.unnest_key('event_params', 'ga_session_number',  'int_value', 'session_number') }}
    , COALESCE(
        (SELECT value.int_value FROM unnest(event_params) WHERE key = "session_engaged"),
        (CASE WHEN (SELECT value.string_value FROM unnest(event_params) WHERE key = "session_engaged") = "1" THEN 1 END)
    ) as session_engaged
    , {{ ga4.unnest_key('event_params', 'engagement_time_msec', 'int_value') }}
    , {{ ga4.unnest_key('event_params', 'page_title') }}
    , {{ ga4.unnest_key('event_params', 'page_referrer') }}
    , {{ ga4.unnest_key('event_params', 'source', 'lower_string_value', 'event_source') }}
    , {{ ga4.unnest_key('event_params', 'medium', 'lower_string_value', 'event_medium') }}
    , {{ ga4.unnest_key('event_params', 'campaign', 'lower_string_value', 'event_campaign') }}
    , {{ ga4.unnest_key('event_params', 'content', 'lower_string_value', 'event_content') }}
    , {{ ga4.unnest_key('event_params', 'term', 'lower_string_value', 'event_term') }}
    , CASE 
        WHEN event_name = 'page_view' THEN 1
        ELSE 0
    END AS is_page_view
    , CASE 
        WHEN event_name = 'purchase' THEN 1
        ELSE 0
    END AS is_purchase
{% endmacro %}


{% macro base_select_usr_source() %}
    {{ return(adapter.dispatch('base_select_usr_source', 'ga4')()) }}
{% endmacro %}

{% macro default__base_select_usr_source() %}
    , user_info.last_active_timestamp_micros as user_info_last_active_timestamp_micros
    , user_info.user_first_touch_timestamp_micros as user_info_user_first_touch_timestamp_micros
    , user_info.first_purchase_date as user_info_first_purchase_date
    , device.operating_system as device_operating_system
    , device.category as device_category
    , device.mobile_brand_name as device_mobile_brand_name
    , device.mobile_model_name as device_mobile_model_name
    , device.unified_screen_name as device_unified_sceen_name
    , geo.city as geo_city 
    , geo.country as geo_country 
    , geo.continent as geo_continent 
    , geo.region as geo_region 
    , user_ltv.revenue_in_usd as user_ltv_revenue_in_usd
    , user_ltv.sessions  as user_ltv_sessions 
    , user_ltv.engagement_time_millis  as user_ltv_engagement_time_millis
    , user_ltv.purchases  as user_ltv_purchases
    , user_ltv.engaged_sessions  as user_ltv_engaged_sessions
    , user_ltv.session_duration_micros as user_ltv_session_duration_micros
    , predictions.in_app_purchase_score_7d as predictions_in_app_purchase_score_7d
    , predictions.purchase_score_7d as predictions_purchase_score_7d
    , predictions.churn_score_7d as predictions_churn_score_7d 
    , predictions.revenue_28d_in_usd as predictions_revenue_28d_in_usd
    , privacy_info.is_limited_ad_tracking as privacy_info_is_limited_ad_tracking
    , privacy_info.is_ads_personalization_allowed as privacy_info_is_ads_personalization_allowed
    , parse_date('%Y%m%d' , occurrence_date) as occurrence_date
    , parse_date('%Y%m%d' , last_updated_date) as last_updated_date 
    , user_properties
    , audiences
{% endmacro %}
