WITH ranking AS(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY SESSION_ID ORDER BY VIEW_AT DESC) as row_n
    FROM {{ source("snowflake", "PAGE_VIEWS") }}
)
SELECT
    page_name,
    session_id,
    view_at as page_view_at_ts,
    _fivetran_deleted,
    _fivetran_id,
    _fivetran_synced as _fivetran_synced_ts
FROM ranking
WHERE row_n = 1