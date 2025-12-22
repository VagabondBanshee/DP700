-- Auto Generated (Do not modify) A9E421A4AE1B3929DFE58A5C5D9CC7384C2E74474F4E3E2BA5F2B142D56C0C1E
CREATE VIEW dp700_e013.vw_employees AS
SELECT 
    employee_id,
    name,
    department_id,
    hire_date,
    salary,
    DATEDIFF(YEAR, hire_date, GETDATE()) AS years_employed,
    CASE 
        WHEN DATEDIFF(YEAR, hire_date, GETDATE()) >= 5 THEN 'Senior'
        WHEN DATEDIFF(YEAR, hire_date, GETDATE()) >= 2 THEN 'Mid-Level'
        ELSE 'Junior'
    END AS seniority_level
FROM dp700_e013.employees;