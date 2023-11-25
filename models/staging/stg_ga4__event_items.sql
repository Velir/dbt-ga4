{{
    config(
        materialized='incremental',

        incremental_strategy='insert_overwrite',
        partition_by={
                        "field": "event_date_dt",
                        "data_type": "date",
                        "granularity": "day"
                    }

    )
}}

with items_with_params as (
    select
        event_key,
        event_name,
        event_date_dt,
        stream_id,
        stream_name,
        {{ ga4.unnest_key('event_params', 'mq_id', 'int_value') }},
        {{ ga4.unnest_key('event_params', 'transaction_id', 'string_value') }},
        {{ ga4.unnest_key('event_params', 'affiliation', 'string_value', rename_column = "mq_brand") }},
        i.item_id,
        i.item_name,
        i.item_brand,
        i.item_variant,
        i.item_category,
        i.item_category2,
        i.item_category3,
        i.item_category4,
        i.item_category5,
        i.price_in_usd,
        i.price,
        i.quantity,
        i.item_revenue_in_usd,
        i.item_refund,
        i.coupon,
        i.location_id,
        i.item_list_id,
        i.item_list_name,
        i.promotion_id,
        i.promotion_name,
        i.creative_name,
        i.creative_slot
    from {{ref('stg_ga4__events')}},
        unnest(items) as i
    where event_name in ('add_payment_info', 'add_shipping_info', 'add_to_cart','add_to_wishlist','begin_checkout' ,'purchase','refund', 'remove_from_cart','select_item', 'select_promotion','view_item_list','view_promotion', 'view_item')
    {% if is_incremental() %}
        and event_date_dt >= CURRENT_DATE() - 7
    {% endif %}
)

select * from items_with_params