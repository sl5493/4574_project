SELECT
    page_name,
    session_id,
    view_at as page_view_at_ts,
    _fivetran_deleted,
    _fivetran_id,
    _fivetran_synced as _fivetran_synced_ts
FROM {{ source("snowflake", "PAGE_VIEWS") }}