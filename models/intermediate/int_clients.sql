WITH clean_sessions AS (
    SELECT *
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY session_at_ts DESC) as rnk  FROM {{ref ('base_snowflake_sessions')}}) s
    WHERE rnk = 1
),
clean_orders AS (
    SELECT *
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_at_ts DESC) as rnk FROM  {{ref ('base_snowflake__ORDERS')}}) o
    WHERE rnk = 1
),
clean_returns AS (
    SELECT *
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY RETURNED_DATE DESC) as rnk FROM  {{ref ('base_google_drive__RETURNS')}}) r
    WHERE rnk = 1
), 
raw_client AS (
    SELECT 
        --basic info
        s.client_id, o.client_name, o.phone, o.shipping_address, o.state, o.payment_info, 

        --transcation/order data
        --RFM Model
        MIN(o.order_at_ts) OVER (PARTITION BY s.client_id) as first_order_ts, 
        MAX(o.order_at_ts) OVER (PARTITION BY s.client_id) as last_order_ts, 
        MIN(s.session_at_ts) OVER (PARTITION BY s.client_id) as first_session_ts, 
        MAX(s.session_at_ts) OVER (PARTITION BY s.client_id) as last_session_ts,
        COUNT(distinct s.session_id) OVER (PARTITION BY s.client_id) as session_count,
        COUNT(distinct item_name) OVER (PARTITION BY s.client_id) as viewed_item_count,
        COUNT(ITEM_VIEW_AT_TS) OVER (PARTITION BY s.client_id) as item_views,
        COUNT(distinct o.order_id) OVER (PARTITION BY s.client_id) as order_count,
        COUNT(distinct CASE WHEN r.is_refunded IS NOT NULL THEN o.order_id ELSE NULL END) OVER (PARTITION BY s.client_id) as net_order_count,
        SUM(CASE WHEN o.order_id IS NOT NULL THEN v.price_per_unit * (v.add_to_cart_quantity-v.remove_from_cart_quantity) ELSE 0 END) OVER (PARTITION BY s.client_id) as total_price_paid,
        SUM(CASE WHEN o.order_id IS NOT NULL THEN (v.price_per_unit * (v.add_to_cart_quantity-v.remove_from_cart_quantity) * (1+TAX_RATE) + SHIPPING_COST_USD) ELSE 0 END) OVER (PARTITION BY s.client_id) as total_amount_of_orders,
        SUM(CASE WHEN is_refunded IS NOT NULL THEN (v.price_per_unit * (v.add_to_cart_quantity-v.remove_from_cart_quantity) * (1+TAX_RATE) + SHIPPING_COST_USD) ELSE 0 END) OVER (PARTITION BY s.client_id) as net_amount_of_orders,
        
        --only keep the latest client info
        ROW_NUMBER() over(partition by s.client_id order by (CASE WHEN order_at_ts IS NOT NULL THEN order_at_ts ELSE '1900-01-01 00:00:00.000' END) DESC) as rnk
    FROM clean_sessions s 
        LEFT JOIN  clean_orders o ON s.session_id = o.session_id
        LEFT JOIN {{ref ('base_snowflake__ITEM_VIEWS')}} v  ON o.session_id = v.session_id 
        LEFT JOIN clean_returns r ON o.order_id = r.order_id
)


SELECT client_id, client_name, phone, shipping_address, state, payment_info,
            first_order_ts, last_order_ts, first_session_ts, last_session_ts,
            session_count, viewed_item_count, item_views,
           order_count, net_order_count, total_price_paid, total_amount_of_orders
           net_amount_of_orders
FROM raw_client
WHERE rnk = 1
