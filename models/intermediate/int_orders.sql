WITH item_bought AS(
SELECT
    SESSION_ID,
    SUM((ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY)*PRICE_PER_UNIT) AS PRICE_PAID,
    SUM(ADD_TO_CART_QUANTITY - REMOVE_FROM_CART_QUANTITY) AS NUMBER_PRODUCTS
FROM 
    {{ ref('base_snowflake__ITEM_VIEWS') }}
GROUP BY
    SESSION_ID),
orders_ranking AS(
SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY ORDER_AT_TS DESC) as row_n
FROM {{ ref('base_snowflake__ORDERS') }}
),
returns_ranking AS(
SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ORDER_ID ORDER BY RETURNED_DATE DESC) as row_n
FROM {{ ref('base_google_drive__RETURNS') }}
),
order_return AS(SELECT
    ORDER_ID,
    RETURNED_DATE,
    IS_REFUNDED
FROM
    returns_ranking
WHERE row_n = 1)
    
SELECT
    orders_ranking.ORDER_ID,
    DATE(ORDER_AT_TS) AS ORDER_DATE,
    SHIPPING_ADDRESS,
    PHONE,
    PAYMENT_METHOD,
    CLIENT_NAME,
    SHIPPING_COST_USD,
    STATE,
    TAX_RATE,
    PAYMENT_INFO,
    orders_ranking.SESSION_ID, 
    NUMBER_PRODUCTS,
    PRICE_PAID,
    (PRICE_PAID * (1 + TAX_RATE) + SHIPPING_COST_USD) AS TOTAL_PAID,
    RETURNED_DATE,
    IS_REFUNDED
FROM 
    orders_ranking
JOIN 
    item_bought ON item_bought.SESSION_ID = orders_ranking.SESSION_ID
LEFT JOIN
    order_return ON order_return.ORDER_ID = orders_ranking.ORDER_ID
WHERE row_n = 1