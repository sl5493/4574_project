SELECT
    CAST(EXPENSE_RECORD_DATE AS DATE) as EXPENSE_DATE,
    EXPENSE_TYPE,
    SUM(EXPENSE_AMOUNT) as TOTAL_EXPENSE
FROM {{ref ('base_google_drive__EXPENSES')}}
GROUP BY EXPENSE_DATE, EXPENSE_TYPE