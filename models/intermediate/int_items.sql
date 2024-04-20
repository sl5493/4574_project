SELECT
    iv.ITEM_NAME,
    (iv.ADD_TO_CART_QUANTITY - iv.REMOVE_FROM_CART_QUANTITY) AS Purchased_NUMBER,
    iv.PRICE_PER_UNIT,
    iv.PRICE_PER_UNIT * (iv.ADD_TO_CART_QUANTITY - iv.REMOVE_FROM_CART_QUANTITY) AS purchased_price,
    or1.ORDER_AT_TS AS purchase_ts,
    or1.ORDER_ID,
    or1.STATE
FROM {{ ref('base_snowflake__ITEM_VIEWS') }} as iv
INNER JOIN {{ ref('base_snowflake__ORDERS') }} as or1
ON iv.SESSION_ID = or1.SESSION_ID
WHERE iv.PRICE_PER_UNIT * (iv.ADD_TO_CART_QUANTITY - iv.REMOVE_FROM_CART_QUANTITY) > 0
