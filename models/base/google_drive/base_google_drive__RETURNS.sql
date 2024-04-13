SELECT
    _FILE,
    _LINE,
    _MODIFIED AS _MODIFIED_TS,
    _FIVETRAN_SYNCED AS _FIVETRAN_SYNCED_TS,
    CAST(ORDER_ID AS STRING) as ORDER_ID,
    CAST(RETURNED_AT AS DATE) as RETURNED_DATE,
    CASE
        WHEN IS_REFUNDED = 'yes' THEN TRUE
        WHEN IS_REFUNDED = 'no' THEN FALSE
        ELSE NULL -- Handling potential data inconsistencies
    END as IS_REFUNDED

FROM {{source('google_drive', 'RETURNS')}}