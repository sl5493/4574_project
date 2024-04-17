WITH raw_client AS (
    SELECT 
        --basic info
        s.client_id, o.client_name, o.phone, o.shipping_address, o.state, o.payment_info, 

        --transcation/order data
        --RFM Model
        MIN(o.order_at_ts) OVER (PARTITION BY s.client_id) as first_order_ts, 
        MAX(o.order_at_ts) OVER (PARTITION BY s.client_id) as last_order_ts, 
        MIN(s.session_at_ts) OVER (PARTITION BY s.client_id) as first_session_ts, 
        MAX(s.session_at_ts) OVER (PARTITION BY s.client_id) as last_session_ts,
        COUNT(distinct o.order_id) OVER (PARTITION BY s.client_id) as order_count,
        COUNT(distinct CASE WHEN r.is_refunded IS NOT NULL THEN o.order_id ELSE NULL END) OVER (PARTITION BY s.client_id) as net_order_count,
        SUM(CASE WHEN o.order_id IS NOT NULL THEN v.price_per_unit * (v.add_to_cart_quantity-v.remove_from_cart_quantity) ELSE 0 END) OVER (PARTITION BY s.client_id) as total_amount_of_orders,
        SUM(CASE WHEN is_refunded IS NOT NULL THEN v.price_per_unit * (v.add_to_cart_quantity-v.remove_from_cart_quantity) ELSE 0 END) OVER (PARTITION BY s.client_id) as net_amount_of_orders,
        
        --only keep the latest client info
        ROW_NUMBER() over(partition by s.client_id order by order_at_ts DESC) as rnk,
    FROM {{ref ('base_snowflake_sessions')}} s 
        INNER JOIN {{ref ('base_snowflake__ORDERS') }} o ON s.session_id = o.session_id
        INNER JOIN {{ref ('base_snowflake__ITEM_VIEWS')}} v  ON o.session_id = v.session_id 
        LEFT JOIN {{ref ('base_google_drive__RETURNS') }} r ON o.order_id = r.order_id
)


    SELECT client_id, client_name, phone, shipping_address, state, payment_info,
            first_order_ts, last_order_ts,first_session_ts,last_session_ts,
           order_count, net_order_count, total_amount_of_orders, net_amount_of_orders
    FROM raw_client
    WHERE rnk = 1