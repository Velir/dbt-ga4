select  
    ecommerce.total_item_quantity as total_item_quantity,
    ecommerce.purchase_revenue_in_usd as purchase_revenue_in_usd,
    ecommerce.refund_value_in_usd as refund_value_in_usd,
    ecommerce.refund_value as refund_value,
    ecommerce.shipping_value_in_usd as shipping_value_in_usd,
    ecommerce.shipping_value as shipping_value,
    ecommerce.tax_value_in_usd as tax_value_in_usd,
    ecommerce.tax_value as tax_value,
    ecommerce.unique_items as unique_items,
    trim(ecommerce.transaction_id) as transaction_id
from {{ref('base_ga4__events')}}
where 
    ecommerce.transaction_id is not null and
    ecommerce.transaction_id != "(not set)"