-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "ded3efd6-fb5d-4116-8a13-b624e22cc67f",
-- META       "default_lakehouse_name": "LH_DP700_Alex",
-- META       "default_lakehouse_workspace_id": "96ade54f-a71f-442d-ada7-eca1dfaf62cd",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "ded3efd6-fb5d-4116-8a13-b624e22cc67f"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!
-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS dp700_e013;

-- Create the employees table
CREATE TABLE IF NOT EXISTS dp700_e013.employees (
    employee_id INT,
    name STRING,
    department_id INT,
    hire_date DATE,
    salary DECIMAL(10, 2)
);

INSERT INTO dp700_e013.employees VALUES
    (10, 'Lily Morgan', 104, '2024-12-10', 71000.00),
    (11, 'Noah Carter', 103, '2025-02-05', 69000.00);

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
