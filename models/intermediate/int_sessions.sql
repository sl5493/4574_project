WITH rankingS AS (
    SELECT
            OS,
            SESSION_AT_TS,
            SESSION_ID,
           ROW_NUMBER() OVER(PARTITION BY SESSION_ID ORDER BY SESSION_AT_TS DESC) as row_n
    FROM {{ ref('base_snowflake_sessions') }}
),
rankingO AS (
    SELECT ORDER_ID,
            SESSION_ID,
           ROW_NUMBER() OVER(PARTITION BY ORDER_ID ORDER BY ORDER_AT_TS DESC) as row_n
    FROM {{ ref('base_snowflake__ORDERS') }}
),
countp AS (
    SELECT
        SESSION_ID,
        SUM(CASE WHEN PAGE_NAME = 'cart' THEN 1 ELSE 0 END) AS Cart,
        SUM(CASE WHEN PAGE_NAME = 'shop_plants' THEN 1 ELSE 0 END) AS Shop_Plants,
        SUM(CASE WHEN PAGE_NAME = 'faq' THEN 1 ELSE 0 END) AS Faq,
        SUM(CASE WHEN PAGE_NAME = 'plant_care' THEN 1 ELSE 0 END) AS Plant_care,
        SUM(CASE WHEN PAGE_NAME = 'landing_page' THEN 1 ELSE 0 END) AS Landing_page,
        count(PAGE_NAME) AS NUMBER_PAGE_VIEWED
    FROM {{ ref('base_snowflake_page_views') }}
    GROUP BY SESSION_ID
),
countv AS(
    SELECT
        SESSION_ID,
        COUNT(ITEM_NAME) AS ITEM_VIEWED
    FROM {{ ref('base_snowflake__ITEM_VIEWS') }}
    GROUP BY SESSION_ID
)
SELECT 
    s1.OS, 
    s1.SESSION_AT_TS,
    CASE 
        WHEN o1.ORDER_ID IS NOT NULL THEN 1 
        ELSE 0 
    END AS Order_Exists,
    p1.*,
    v1.ITEM_VIEWED,
    
FROM rankingS as s1
FULL JOIN rankingO as o1 ON s1.SESSION_ID = o1.SESSION_ID
FULL JOIN countp as p1 ON s1.SESSION_ID = p1.SESSION_ID
FULL JOIN countv as v1 ON s1.SESSION_ID = v1.SESSION_ID
WHERE s1.row_n = 1 AND o1.row_n = 1