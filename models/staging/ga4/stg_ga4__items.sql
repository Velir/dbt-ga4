select 
    event_timestamp,
    event_name,
    client_id,
    items
from {{ref('base_ga4__events')}},
    unnest(items) as items
where
    event_name in ( 
        "view_item_list", 
        "view_item", 
        "select_item", 
        "view_promotion", 
        "select_promotion", 
        "add_to_cart",
        "add_to_wishlist",
        "remove_from_cart",
        "view_cart",
        "begin_checkout",
        "add_payment_info",
        "add_shipping_info",
        "purchase",
        "refund" 
    )