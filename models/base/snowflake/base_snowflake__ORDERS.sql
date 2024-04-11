SELECT
    _FIVETRAN_ID,
    SHIPPING_ADDRESS,
    ORDER_AT AS ORDER_AT_TS,
    ORDER_ID,
    PHONE,
    PAYMENT_METHOD,
    CLIENT_NAME,
    CAST(SUBSTRING(SHIPPING_COST, 5) AS INT) AS SHIPPING_COST_USD,
    SESSION_ID,
    STATE,
    TAX_RATE,
    PAYMENT_INFO,
    _FIVETRAN_DELETED,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS
FROM {{source('snowflake', 'ORDERS')}}