WITH item_bought AS(
SELECT
    SESSION_ID,
    SUM((ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY)*PRICE_PER_UNIT) AS PRICE_PAID
FROM 
    {{ ref('base_snowflake__ITEM_VIEWS') }}
GROUP BY
    SESSION_ID),
order_return AS(SELECT
    ORDER_ID,
    RETURNED_DATE,
    IS_REFUNDED
FROM
    {{ ref('base_google_drive__RETURNS') }})
    
SELECT
    BASE_SNOWFLAKE__ORDERS.ORDER_ID,
    DATE(ORDER_AT_TS) AS ORDER_DATE,
    SHIPPING_ADDRESS,
    PHONE,
    PAYMENT_METHOD,
    CLIENT_NAME,
    SHIPPING_COST_USD,
    STATE,
    TAX_RATE,
    PAYMENT_INFO,
    BASE_SNOWFLAKE__ORDERS.SESSION_ID, 
    PRICE_PAID,
    (PRICE_PAID * (1 + TAX_RATE) + SHIPPING_COST_USD) AS TOTAL_PAID,
    RETURNED_DATE,
    IS_REFUNDED
FROM 
    {{ ref('base_snowflake__ORDERS') }} 
JOIN 
    item_bought ON item_bought.SESSION_ID = BASE_SNOWFLAKE__ORDERS.SESSION_ID
LEFT JOIN
    order_return ON order_return.ORDER_ID = BASE_SNOWFLAKE__ORDERS.ORDER_ID