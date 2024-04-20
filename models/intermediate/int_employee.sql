SELECT
    j.EMPLOYEE_ID,
    j.EMPLOYEE_NAME_ALIAS as EMPLOYEE_NAME,
    j.CITY,
    j.ADDRESS_ALIAS as ADDRESS,
    j.TITLE,
    j.HIRE_DATE,
    q.QUIT_DATE,
    j.ANNUAL_SALARY
    FROM {{ref ('base_google_drive__HR_JOINS')}} j 
    LEFT JOIN {{ref ('base_google_drive__HR_QUITS')}} q ON j.EMPLOYEE_ID = q.EMPLOYEE_ID