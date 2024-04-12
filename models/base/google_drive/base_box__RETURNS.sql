SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    _FIVETRAN_SYNCED,
    CAST(ORDER_ID AS STRING) as ORDER_ID,
    CAST(SUBSTRING(RETURNED_AT,4) AS DATE) as RETURNED_AT,
    CASE
        WHEN IS_REFUNDED = 'yes' THEN TRUE
        WHEN IS_REFUNDED = 'no' THEN FALSE
        ELSE NULL -- Handling potential data inconsistencies
    END as IS_REFUNDED

FROM {{source('google_drive', 'RETURNS')}}